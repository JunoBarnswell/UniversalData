using System.IO;
using System.Threading;
using UniversalData;

namespace SqlSugar;

/// <summary>Strongly typed C# surface over one Native SQL session.</summary>
public sealed class SqlSugarClient : IDisposable
{
    private readonly ConnectionConfig config;
    private readonly WorkerSession? worker;
    private readonly NativeSqlWorker? hosted;
    private readonly ContextHandle data = DataOwnership.Context(268435456);
    private readonly object gate = new();
    private bool disposed;

    private SqlSugarClient(ConnectionConfig connectionConfig, WorkerSession? worker, NativeSqlWorker? hosted)
    {
        config = connectionConfig;
        this.worker = worker;
        this.hosted = hosted;
        Ado = new AdoProvider(this);
    }

    public static async ValueTask<Result<SqlSugarClient>> OpenAsync(ConnectionConfig connectionConfig,
        CancellationToken cancellation = default)
    {
        try { SqlConnectionConfiguration.Validate(connectionConfig); }
        catch (Exception error) when (error is ArgumentException or NotSupportedException or NullReferenceException)
        {
            return new(StatusCode.Invalid, null, new(ErrorSource.Native, StatusCode.Invalid, "Invalid SQL connection configuration"));
        }
        var start = WorkerSession.Start(connectionConfig.WorkerPath);
        if (!start.IsSuccess)
            return new(start.Code, null, start.Error);
        var worker = start.Value!;
        var opened = await worker.InvokeAsync("sql.open", connectionConfig, cancellation: cancellation).ConfigureAwait(false);
        if (opened.Response.Code != StatusCode.Ok)
        {
            await worker.DisposeAsync().ConfigureAwait(false);
            return new(opened.Response.Code, null, opened.Response.Error);
        }
        try { return Result<SqlSugarClient>.Success(new(connectionConfig, worker, null)); }
        catch (Exception error) when (error is DllNotFoundException or UniversalDataException)
        {
            await worker.DisposeAsync().ConfigureAwait(false);
            var status = error is UniversalDataException native ? native.Code : StatusCode.DriverUnavailable;
            return new(status, null, new(ErrorSource.Native, status, "Native columnar data is unavailable"));
        }
    }

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

    public static async ValueTask<Result<SqlSugarClient>> OpenAsync(Package package, string connection,
        EtlClientOptions options, CancellationToken cancellation = default)
    {
        var sent = WorkerSession.Start(options.WorkerPath);
        if (!sent.IsSuccess)
            return new(sent.Code, null, sent.Error);
        var worker = sent.Value!;
        var reply = (await worker.InvokeAsync("sql.openAuthorized", new
        {
            package = package.ToJson(), connection, options.PolicyFile, options.Tenant
        }, cancellation: cancellation).ConfigureAwait(false)).Response;
        if (reply.Code == StatusCode.Ok)
        {
            try
            {
                return Result<SqlSugarClient>.Success(new(new ConnectionConfig
                    { ConfigId = connection, DbType = (DbType)reply.Value!.Value.GetInt32() }, worker, null));
            }
            catch (Exception error) when (SqlResult.Expected(error))
            {
                await worker.DisposeAsync().ConfigureAwait(false);
                return Result<SqlSugarClient>.Failure(SqlResult.Error(error));
            }
        }
        await worker.DisposeAsync().ConfigureAwait(false);
        return new(reply.Code, null, reply.Error);
    }

    /// <summary>The process that hosts this SQL session.</summary>
    public int WorkerProcessId => worker?.ProcessId ?? Environment.ProcessId;
    internal IntPtr DataContext => data.DangerousGetHandle();
    internal uint ReadBatchSize => config.ReadBatchSize == 0 ? 512 : config.ReadBatchSize;
    internal uint WriteChunkRows => config.WriteChunkRows == 0 ? 512 : config.WriteChunkRows;

    internal NativeSqlReader OpenQuery(SqlQueryPlan plan, uint? batchRows = null,
        CancellationToken cancellationToken = default) => OpenReader("", plan.QueryJson, plan.SchemaJson,
            plan.Parameters, SqlNative.ReadTyped, batchRows.GetValueOrDefault(ReadBatchSize), cancellationToken);

    internal NativeSqlReader OpenRawReader(string sql, IReadOnlyList<SugarParameter> parameters,
        int mode = SqlNative.ReadDiscover, string schemaJson = "", CancellationToken cancellation = default) =>
        OpenReader(sql, "", schemaJson, parameters, mode, ReadBatchSize, cancellation);

    private NativeSqlReader OpenReader(string sql, string query, string schema,
        IReadOnlyList<SugarParameter> parameters, int mode, uint rows, CancellationToken cancellation)
    {
        using var arguments = ParameterWire(parameters);
        var reply = Invoke("sql.readOpen", new { sql, query, schema, rows, mode }, arguments,
            cancellation: cancellation).AsTask().GetAwaiter().GetResult().Response;
        Check(reply);
        var returned = reply.Value?.GetString() ?? "";
        return new NativeSqlReader(this, returned, cancellation);
    }

    internal int Execute(string sql, IReadOnlyList<SugarParameter> parameters)
    {
        using var arguments = ParameterWire(parameters);
        var reply = Invoke("sql.execute", new { sql }, arguments, mayWrite: true)
            .AsTask().GetAwaiter().GetResult().Response;
        Check(reply);
        return checked((int)Math.Min(reply.Value!.Value.GetUInt64(), int.MaxValue));
    }

    private WorkerBatchBuffer? ParameterWire(IReadOnlyList<SugarParameter> parameters)
    {
        EnsureOpen();
        if (parameters.Count == 0)
            return null;
        using var batch = SqlBatchBuilder.BuildParameters(DataContext, parameters);
        return SqlWire.Encode(batch);
    }

    internal ValueTask<(WorkerResponseFrame Response, WorkerBatchBuffer? Batch)> Invoke(string operation,
        object? values = null, WorkerBatchBuffer? batch = null, bool mayWrite = false,
        Func<uint, (WorkerBatchFrame Control, WorkerBatchBuffer? Batch)>? source = null,
        CancellationToken cancellation = default)
    {
        EnsureOpen();
        if (worker is not null)
            return worker.InvokeAsync(operation, values, batch, mayWrite, source, cancellation);
        var request = new WorkerRequestFrame(ProductInfo.WorkerRevision, 1, operation,
            System.Text.Json.JsonSerializer.SerializeToElement(values ?? new {}, WorkerProtocol.Json));
        if (cancellation.IsCancellationRequested)
            return ValueTask.FromResult(NativeSqlWorker.Reply(request, (int)StatusCode.Cancelled));
        return ValueTask.FromResult(hosted!.Invoke(request, batch, capacity =>
        {
            var next = source!(capacity);
            return (next.Control with { Revision = request.Revision, Sequence = request.Sequence }, next.Batch);
        }));
    }

    internal static void Check(WorkerResponseFrame response)
    {
        if (response.Code != StatusCode.Ok)
            throw new UniversalDataException((int)response.Code, response.Error?.Message ?? response.Code.ToString(),
                response.Error);
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

            var context = DataContext;
            using var values = new SqlBatchBuilder.PinnedValues(members.Count);
            using var input = entities.GetEnumerator();
            if (!input.MoveNext())
                return 0;
            using var schema = SqlNative.SchemaFromJson(context, mapping.SchemaJson(members));
            var response = Invoke("sql.write", new { mode, loadMode, table = mapping.TableName,
                keys = keyOrdinals, rows = chunkRows is > 0 ? chunkRows.Value : WriteChunkRows },
                mayWrite: true, source: Next, cancellation: cancellationToken)
                .AsTask().GetAwaiter().GetResult().Response;
            Check(response);
            return checked((int)Math.Min(response.Value!.Value.GetUInt64(), int.MaxValue));

            (WorkerBatchFrame, WorkerBatchBuffer?) Next(uint capacity)
            {
                try
                {
                    if (knownCount > 0)
                        capacity = Math.Min(capacity, (uint)knownCount);
                    using var batch = SqlBatchBuilder.BuildChunk(context, schema, members, input, values,
                        capacity, cancellationToken, out var hasMore);
                    return (new(0, 0, StatusCode.Ok, !hasMore), SqlWire.Encode(batch));
                }
                catch (Exception error)
                {
                    var status = error switch
                    {
                        OperationCanceledException => StatusCode.Cancelled,
                        OverflowException => StatusCode.Overflow,
                        UniversalDataException native => native.Code,
                        _ => StatusCode.ManagedComponent
                    };
                    return (new(0, 0, status, true,
                        new(ErrorSource.Component, status, $"SQL input batch failed ({status})")), null);
                }
            }
        }
    }

    internal void BeginTransaction(int isolation) => Transaction(SqlNative.TransactionBegin, isolation);
    internal void CommitTransaction() => Transaction(SqlNative.TransactionCommit, 0);
    internal void RollbackTransaction() => Transaction(SqlNative.TransactionRollback, 0);

    private void Transaction(int operation, int isolation)
    {
        var response = Invoke("sql.transaction", new { operation, isolation },
            mayWrite: operation == SqlNative.TransactionCommit)
            .AsTask().GetAwaiter().GetResult().Response;
        Check(response);
    }

    internal static SqlSugarClient FromExecution(ExecutionHandle execution, string connection)
    {
        var backend = new NativeSqlWorker(id => SqlNative.Open(execution, id));
        var configuration = new ConnectionConfig { ConfigId = connection };
        var client = new SqlSugarClient(configuration, null, backend);
        try
        {
            var opened = client.Invoke("sql.open", configuration).GetAwaiter().GetResult().Response;
            Check(opened);
            configuration.DbType = (DbType)opened.Value!.Value.GetInt32();
            return client;
        }
        catch { client.Dispose(); throw; }
    }

    private void EnsureOpen() => ObjectDisposedException.ThrowIf(disposed, this);

    public void Dispose()
    {
        lock (gate)
        {
            if (disposed)
                return;
            disposed = true;
            hosted?.Dispose();
            worker?.DisposeAsync().AsTask().GetAwaiter().GetResult();
            data.Dispose();
        }
    }
}
