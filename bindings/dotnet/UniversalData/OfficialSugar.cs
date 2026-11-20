using SqlSugar;
using SugarDbType = SqlSugar.DbType;

namespace UniversalData;

internal static class OfficialSugar
{
    internal static ISqlSugarClient Open(SugarDbType dialect, string connectionString, int commandTimeoutSeconds = 30)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(commandTimeoutSeconds);
        return new ScopedSugarProvider(Config(dialect, connectionString, true), commandTimeoutSeconds);
    }

    internal static ISqlSugarClient Enlist(IntPtr activation, string name, CancellationToken cancellationToken = default)
    {
        using var strings = new NativeStrings(name);
        SqlNative.Check(Native.ud_activation_sql_session_open(activation, strings[0], out var pointer));
        var session = new SqlSessionHandle(pointer);
        try
        {
            return Borrow(session, cancellationToken, ownsSession: true);
        }
        catch
        {
            session.Dispose();
            throw;
        }
    }

    internal static LocalSugarTransaction OpenLocal(DataSourceKind kind, string connectionString,
        CancellationToken cancellationToken = default) => new(kind, connectionString, cancellationToken);

    internal static ISqlSugarClient Borrow(SqlSessionHandle session, CancellationToken cancellationToken, bool ownsSession)
    {
        var config = Config(Dialect(SqlNative.Dialect(session)), string.Empty, false);
        return new EnlistedSugarProvider(config, session, cancellationToken, ownsSession);
    }

    internal static SugarDbType Dialect(DataSourceKind dialect) => dialect switch
    {
        DataSourceKind.Sqlite => SugarDbType.Sqlite,
        DataSourceKind.PostgreSql => SugarDbType.PostgreSQL,
        DataSourceKind.MySql or DataSourceKind.MariaDb => SugarDbType.MySql,
        DataSourceKind.SqlServer => SugarDbType.SqlServer,
        DataSourceKind.Oracle => SugarDbType.Oracle,
        _ => throw new ArgumentException("The named source is not a database.", nameof(dialect))
    };

    private static ConnectionConfig Config(SugarDbType dialect, string connectionString, bool autoClose) => new()
    {
        DbType = dialect,
        ConnectionString = connectionString,
        IsAutoCloseConnection = autoClose,
        InitKeyType = InitKeyType.Attribute
    };
}

internal sealed class LocalSugarTransaction : IDisposable
{
    private readonly SqlSessionHandle session;

    internal LocalSugarTransaction(DataSourceKind kind, string connectionString, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        session = SqlNative.Open(kind, SdkNativeSources.PluginPath(kind), connectionString);
        try
        {
            using var cancel = cancellationToken.UnsafeRegister(static value => SqlNative.Cancel((SqlSessionHandle)value!), session);
            cancellationToken.ThrowIfCancellationRequested();
            Transition(SqlNative.TransactionBegin);
            cancellationToken.ThrowIfCancellationRequested();
        }
        catch
        {
            session.Dispose();
            throw;
        }
    }

    internal ISqlSugarClient Borrow(CancellationToken cancellationToken) =>
        OfficialSugar.Borrow(session, cancellationToken, ownsSession: false);

    internal void Commit() => Transition(SqlNative.TransactionCommit);
    internal void Rollback() => Transition(SqlNative.TransactionRollback);

    private void Transition(int operation)
    {
        Native.ud_context_clear_error(SqlNative.Context(session));
        var status = SqlNative.Transaction(session, operation, 0);
        EnlistedAdoProvider.Check(session, status);
    }

    public void Dispose() => session.Dispose();
}

internal class ScopedSugarProvider(ConnectionConfig config, int commandTimeoutSeconds) : SqlSugarProvider(config)
{
    private readonly object lifetime = new();
    private bool disposed;

    public override IAdo Ado
    {
        get
        {
            lock (lifetime)
            {
                ObjectDisposedException.ThrowIf(disposed, this);
                if (ContextAdo is { } existing) return existing;
                var ado = base.Ado;
                ado.CommandTimeOut = commandTimeoutSeconds;
                return ado;
            }
        }
    }

    public override void Dispose()
    {
        IAdo? ado;
        lock (lifetime)
        {
            if (disposed) return;
            disposed = true;
            ado = ContextAdo;
        }
        try { ado?.Dispose(); }
        finally
        {
            // Official ADO lazily recreates its provider connection after Dispose.
            if (CurrentConnectionConfig is { } configuration)
                configuration.ConnectionString = string.Empty;
        }
    }
}

internal sealed class EnlistedSugarProvider : ScopedSugarProvider, ISqlSugarClient
{
    private readonly EnlistedAdoProvider participant;

    internal EnlistedSugarProvider(ConnectionConfig config, SqlSessionHandle handle, CancellationToken cancellationToken,
        bool ownsSession)
        : base(config, 30)
    {
        participant = new EnlistedAdoProvider(this, handle, config.DbType, cancellationToken, ownsSession);
        ContextAdo = participant;
    }

    public override ICodeFirst CodeFirst => throw EnlistedAdoProvider.Unsupported("Schema changes require a scope outside a transaction.");
    IFastest<T> ISqlSugarClient.Fastest<T>() => new EnlistedFastest<T>(participant);
    SqlSugarClient ISqlSugarClient.CopyNew() => throw EnlistedAdoProvider.Unsupported("A participant cannot be cloned.");
    ITenant ISqlSugarClient.AsTenant() => throw EnlistedAdoProvider.Unsupported("A participant cannot switch resources.");
    T ISqlSugarClient.CreateContext<T>(bool isTran) => throw EnlistedAdoProvider.CoordinatorOwnsTransaction();
    SugarUnitOfWork ISqlSugarClient.CreateContext(bool isTran) => throw EnlistedAdoProvider.CoordinatorOwnsTransaction();

    ConnectionConfig ISqlSugarClient.CurrentConnectionConfig
    {
        get => CurrentConnectionConfig;
        set => throw EnlistedAdoProvider.Unsupported("A participant cannot replace its connection configuration.");
    }
}
