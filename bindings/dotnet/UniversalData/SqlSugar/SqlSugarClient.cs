using System.IO;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Threading;
using UniversalData;

namespace SqlSugar;

/// <summary>Strongly typed C# surface over one Native SQL session.</summary>
public sealed class SqlSugarClient : IDisposable
{
    private readonly ConnectionConfig config;
    private readonly SqlSessionHandle session;
    private readonly object gate = new();
    private bool disposed;

    public SqlSugarClient(ConnectionConfig connectionConfig)
    {
        ArgumentNullException.ThrowIfNull(connectionConfig);
        ValidateConfig(connectionConfig);
        config = connectionConfig;
        var opened = SqlNative.Open(config.DbType, ResolvePlugin(config), config.ConnectionString);
        try { SqlNative.Check(SqlNative.SetAutoClose(opened, config.IsAutoCloseConnection)); session = opened; }
        catch { opened.Dispose(); throw; }
        Ado = new AdoProvider(this);
    }

    public SqlSugarClient(ConnectionConfig connectionConfig, Action<SqlSugarClient> configure)
        : this(connectionConfig) => configure?.Invoke(this);

    public IAdo Ado { get; }
    public ConnectionConfig CurrentConnectionConfig => config;

    public ISugarQueryable<T> Queryable<T>() => new SugarQueryable<T>(this, EntityMapping.For<T>());
    public IInsertable<T> Insertable<T>(T entity) => new Insertable<T>(this, new[] { entity });
    public IInsertable<T> Insertable<T>(List<T> entities) => new Insertable<T>(this, entities);
    public IInsertable<T> Insertable<T>(T[] entities) => new Insertable<T>(this, entities);
    public IInsertable<T> Insertable<T>(IEnumerable<T> entities) => new Insertable<T>(this, entities);
    public IUpdateable<T> Updateable<T>(T entity) => new Updateable<T>(this, new[] { entity });
    public IUpdateable<T> Updateable<T>(List<T> entities) => new Updateable<T>(this, entities);
    public IUpdateable<T> Updateable<T>(T[] entities) => new Updateable<T>(this, entities);
    public IUpdateable<T> Updateable<T>(IEnumerable<T> entities) => new Updateable<T>(this, entities);
    public IDeleteable<T> Deleteable<T>(T entity) => new Deleteable<T>(this, new[] { entity });
    public IDeleteable<T> Deleteable<T>(List<T> entities) => new Deleteable<T>(this, entities);
    public IDeleteable<T> Deleteable<T>(T[] entities) => new Deleteable<T>(this, entities);
    public IDeleteable<T> Deleteable<T>(IEnumerable<T> entities) => new Deleteable<T>(this, entities);
    public IFastest<T> Fastest<T>() => new Fastest<T>(this, EntityMapping.For<T>());

    internal SqlSessionHandle Session { get { EnsureOpen(); return session; } }
    internal uint ReadBatchSize => config.ReadBatchSize == 0 ? 512 : config.ReadBatchSize;
    internal uint WriteChunkRows => config.WriteChunkRows == 0 ? 512 : config.WriteChunkRows;

    internal NativeSqlReader OpenQuery(SqlQueryPlan plan, uint? batchRows = null, CancellationToken cancellationToken = default)
    {
        EnsureOpen();
        cancellationToken.ThrowIfCancellationRequested();
        var parameterBatch = plan.Parameters.Count == 0 ? null : SqlBatchBuilder.BuildParameters(session, plan.Parameters);
        try
        {
            using var strings = new NativeStrings(string.Empty, plan.QueryJson, plan.SchemaJson);
            var status = SqlNative.ReadOpen(session, strings[0], strings[1], strings[2], parameterBatch?.DangerousGetHandle() ?? IntPtr.Zero,
                batchRows is > 0 ? batchRows.Value : ReadBatchSize, SqlNative.ReadTyped, out var schema);
            try
            {
                SqlNative.Check(status);
                return new NativeSqlReader(session, schema, parameterBatch, cancellationToken);
            }
            catch { schema?.Dispose(); throw; }
        }
        catch { parameterBatch?.Dispose(); throw; }
    }

    internal NativeSqlReader OpenRawReader(string sql, IReadOnlyList<SugarParameter> parameters, int mode = SqlNative.ReadDiscover, string schemaJson = "")
    {
        EnsureOpen();
        var parameterBatch = parameters.Count == 0 ? null : SqlBatchBuilder.BuildParameters(session, parameters);
        try
        {
            using var strings = new NativeStrings(sql, string.Empty, schemaJson);
            var status = SqlNative.ReadOpen(session, strings[0], strings[1], strings[2], parameterBatch?.DangerousGetHandle() ?? IntPtr.Zero,
                ReadBatchSize, mode, out var schema);
            try
            {
                SqlNative.Check(status);
                return new NativeSqlReader(session, schema, parameterBatch);
            }
            catch { schema?.Dispose(); throw; }
        }
        catch { parameterBatch?.Dispose(); throw; }
    }

    internal int Execute(string sql, IReadOnlyList<SugarParameter> parameters)
    {
        EnsureOpen();
        using var parameterBatch = parameters.Count == 0 ? null : SqlBatchBuilder.BuildParameters(session, parameters);
        using var strings = new NativeStrings(sql);
        SqlNative.Check(SqlNative.Execute(session, strings[0], parameterBatch?.DangerousGetHandle() ?? IntPtr.Zero, out var affected));
        return checked((int)Math.Min(affected, int.MaxValue));
    }

    internal int Write<T>(int mode, int loadMode, EntityMapping mapping, IReadOnlyList<EntityMember> members,
        IEnumerable<T> entities, IReadOnlyList<EntityMember> keys, uint? chunkRows = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entities);
        lock (gate)
        {
            EnsureOpen();
            cancellationToken.ThrowIfCancellationRequested();
            var knownCount = entities is IReadOnlyCollection<T> collection ? collection.Count
                : entities.TryGetNonEnumeratedCount(out var count) ? count : -1;
            if (knownCount == 0) return 0;

            uint[] keyOrdinals = keys.Count == 0 ? [] : new uint[keys.Count];
            for (var k = 0; k < keyOrdinals.Length; k++)
            {
                var column = 0;
                while (column < members.Count && members[column] != keys[k]) column++;
                if (column == members.Count)
                    throw new InvalidOperationException("The write key is missing from the Native batch.");
                keyOrdinals[k] = (uint)column;
            }

            var context = SqlNative.Context(session);
            using var values = new SqlBatchBuilder.PinnedValues(members.Count);
            using var table = new NativeStrings(mapping.TableName);
            IEnumerator<T>? rows = entities.GetEnumerator();
            SqlBatchHandle? batch = null, schema = null;
            ExceptionDispatchInfo? inputError = null;
            GCHandle pinned = default;
            try
            {
                if (keyOrdinals.Length != 0) pinned = GCHandle.Alloc(keyOrdinals, GCHandleType.Pinned);
                var status = SqlNative.WriteStream(session, mode, loadMode, table[0],
                    pinned.IsAllocated ? pinned.AddrOfPinnedObject() : IntPtr.Zero, (uint)keyOrdinals.Length,
                    chunkRows is > 0 ? chunkRows.Value : WriteChunkRows, Next, IntPtr.Zero, out var affected);
                try { CloseInput(); }
                catch when (status != (int)StatusCode.Ok) { /* Preserve the write failure. */ }
                if (status != (int)StatusCode.CommitUnknown)
                {
                    inputError?.Throw();
                    if (status == (int)StatusCode.Cancelled) cancellationToken.ThrowIfCancellationRequested();
                }
                SqlNative.Check(status);
                return checked((int)Math.Min(affected, int.MaxValue));
            }
            finally
            {
                batch?.Dispose();
                schema?.Dispose();
                if (pinned.IsAllocated) pinned.Free();
                CloseInput();
            }

            void CloseInput()
            {
                var input = rows;
                rows = null;
                input?.Dispose();
            }

            int Next(IntPtr _, uint capacity, out IntPtr pointer, out int last)
            {
                pointer = IntPtr.Zero;
                last = 0;
                try
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (schema is null)
                    {
                        if (!rows!.MoveNext())
                        {
                            CloseInput();
                            return (int)StatusCode.End;
                        }
                        schema = SqlNative.SchemaFromJson(context, mapping.SchemaJson(members));
                    }
                    if (knownCount > 0) capacity = Math.Min(capacity, (uint)knownCount);
                    batch?.Dispose();
                    batch = SqlBatchBuilder.BuildChunk(context, schema, members, rows!, values,
                        capacity, cancellationToken, out var hasMore);
                    if (!hasMore) CloseInput();
                    cancellationToken.ThrowIfCancellationRequested();
                    pointer = batch.DangerousGetHandle();
                    last = hasMore ? 0 : 1;
                    return (int)StatusCode.Ok;
                }
                catch (Exception error)
                {
                    inputError = ExceptionDispatchInfo.Capture(error);
                    return (int)(error is OperationCanceledException ? StatusCode.Cancelled : StatusCode.Invalid);
                }
            }
        }
    }

    internal void BeginTransaction(int isolation)
    { EnsureOpen(); SqlNative.Check(SqlNative.Transaction(session, SqlNative.TransactionBegin, isolation)); }
    internal void CommitTransaction()
    { EnsureOpen(); SqlNative.Check(SqlNative.Transaction(session, SqlNative.TransactionCommit, 0)); }
    internal void RollbackTransaction()
    { EnsureOpen(); SqlNative.Check(SqlNative.Transaction(session, SqlNative.TransactionRollback, 0)); }

    internal static SqlSugarClient FromExecution(ExecutionHandle execution, string connection)
    {
        var session = SqlNative.Open(execution, connection);
        try
        {
            var config = new ConnectionConfig { ConfigId = connection, DbType = SqlNative.Dialect(session) };
            return new SqlSugarClient(config, session);
        }
        catch { session.Dispose(); throw; }
    }

    private SqlSugarClient(ConnectionConfig config, SqlSessionHandle session)
    {
        this.config = config;
        try { SqlNative.Check(SqlNative.SetAutoClose(session, config.IsAutoCloseConnection)); this.session = session; }
        catch { session.Dispose(); throw; }
        Ado = new AdoProvider(this);
    }

    private void EnsureOpen() => ObjectDisposedException.ThrowIf(disposed, this);
    private static void ValidateConfig(ConnectionConfig value)
    {
        if (!Enum.IsDefined(value.DbType)) throw new ArgumentOutOfRangeException(nameof(value.DbType));
        if (value.InitKeyType != InitKeyType.Attribute)
            throw new NotSupportedException("Declare entity keys with SugarColumn attributes.");
        if (string.IsNullOrWhiteSpace(value.ConnectionString)) throw new ArgumentException("ConnectionString is required.", nameof(value));
    }
    private static string ResolvePlugin(ConnectionConfig value)
    {
        if (!string.IsNullOrWhiteSpace(value.PluginPath))
        {
            if (!Path.IsPathFullyQualified(value.PluginPath)) throw new ArgumentException("PluginPath must be absolute.", nameof(value));
            if (!File.Exists(value.PluginPath)) throw new FileNotFoundException("Native SQL connector plugin was not found.", value.PluginPath);
            return value.PluginPath;
        }
        var stem = value.DbType switch
        {
            DbType.Sqlite => "ud_connector_sqlite",
            DbType.PostgreSQL => "ud_connector_postgresql",
            DbType.MySql => "ud_connector_mysql",
            DbType.MariaDb => "ud_connector_mariadb",
            DbType.SqlServer => "ud_connector_odbc",
            DbType.Oracle => "ud_connector_oracle_oci",
            _ => throw new ArgumentOutOfRangeException(nameof(value.DbType))
        };
        var extension = OperatingSystem.IsWindows() ? ".dll" : OperatingSystem.IsMacOS() ? ".dylib" : ".so";
        var names = OperatingSystem.IsWindows() ? new[] { stem + extension } : new[] { stem + extension, "lib" + stem + extension };
        var candidates = names.SelectMany(name => new[]
        {
            Path.Combine(AppContext.BaseDirectory, name),
            Path.Combine(AppContext.BaseDirectory, "UniversalData", "plugins", name)
        }).ToArray();
        var path = candidates.FirstOrDefault(File.Exists);
        return path ?? throw new FileNotFoundException($"Native SQL connector plugin '{stem}' was not found. Specify ConnectionConfig.PluginPath.");
    }

    public void Dispose()
    {
        if (disposed) return;
        lock (gate)
        {
            if (disposed) return;
            disposed = true;
            SqlNative.Cancel(session);
            session.Dispose();
        }
        GC.SuppressFinalize(this);
    }
}
