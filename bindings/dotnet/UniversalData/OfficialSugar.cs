using SqlSugar;
using SugarDbType = SqlSugar.DbType;

namespace UniversalData;

internal static class OfficialSugar
{
    internal static ISqlSugarClient OpenActivation(IntPtr activation, string name, CancellationToken cancellationToken = default)
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

    internal static ISqlSugarClient Borrow(SqlSessionHandle session, CancellationToken cancellationToken, bool ownsSession)
    {
        var config = Config(Dialect(SqlNative.Dialect(session)), string.Empty, false);
        return new NativeSugarProvider(config, session, cancellationToken, ownsSession);
    }

    internal static ISqlSugarClient Open(SqlSessionHandle session, CancellationToken cancellationToken = default)
    {
        try { return Borrow(session, cancellationToken, ownsSession: true); }
        catch { session.Dispose(); throw; }
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
