using System.Data;
using System.Data.Common;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using SqlSugar;
using SugarDbType = SqlSugar.DbType;

namespace UniversalData;

internal sealed class NativeAdoProvider : AdoProvider, IAdo
{
    private readonly SqlSessionHandle handle;
    private readonly SqlSugarProvider context;
    private readonly SugarDbType dialect;
    private readonly CancellationToken lifetime;
    private readonly bool ownsSession;
    private readonly object operations = new();
    private NativeDataReader? reader;
    private bool disposed;

    internal NativeAdoProvider(SqlSugarProvider context, SqlSessionHandle handle, SugarDbType dialect,
        CancellationToken cancellationToken, bool ownsSession)
    {
        this.context = context;
        this.handle = handle;
        this.dialect = dialect;
        this.ownsSession = ownsSession;
        lifetime = cancellationToken;
        CommandTimeOut = 30;
        IsDisableMasterSlaveSeparation = true;
        base.Transaction = NativeCoordinatorTransaction.Instance;
    }

    public override SqlSugarProvider Context
    {
        get => context;
        set => throw Unsupported("A participant cannot replace its SqlSugar context.");
    }
    public override string SqlParameterKeyWord => dialect == SugarDbType.Oracle ? ":" : "@";
    public override IDbConnection Connection
    {
        get => throw Unsupported("Physical connections are owned by the native transaction coordinator.");
        set => throw Unsupported("A participant cannot replace its physical connection.");
    }
    IDbTransaction IAdo.Transaction
    {
        get => NativeCoordinatorTransaction.Instance;
        set => throw CoordinatorOwnsTransaction();
    }

    public override void Open() { using var lease = EnterOperation(null); }
    public override Task OpenAsync() { Open(); return Task.CompletedTask; }
    public override void Close() { using var lease = EnterOperation(null); }
    public override bool IsValidConnection() { using var lease = EnterOperation(null); return true; }
    public override bool IsValidConnectionNoClose() => IsValidConnection();
    public override bool IsAnyTran() => true;
    public override bool IsNoTran() => false;
    public override void BeginTran() => throw CoordinatorOwnsTransaction();
    public override void BeginTran(IsolationLevel iso) => throw CoordinatorOwnsTransaction();
    public override void BeginTran(string transactionName) => throw CoordinatorOwnsTransaction();
    public override void BeginTran(IsolationLevel iso, string transactionName) => throw CoordinatorOwnsTransaction();
    public override Task BeginTranAsync() => Task.FromException(CoordinatorOwnsTransaction());
    public override Task BeginTranAsync(IsolationLevel iso) => Task.FromException(CoordinatorOwnsTransaction());
    public override void CommitTran() => throw CoordinatorOwnsTransaction();
    public override void RollbackTran() => throw CoordinatorOwnsTransaction();
    public override Task CommitTranAsync() => Task.FromException(CoordinatorOwnsTransaction());
    public override Task RollbackTranAsync() => Task.FromException(CoordinatorOwnsTransaction());
    public override DbCommand GetCommand(string sql, SugarParameter[] pars) =>
        throw Unsupported("Request a result through the ADO query or command methods.");
    public override Task<DbCommand> GetCommandAsync(string sql, SugarParameter[] parameters) =>
        Task.FromException<DbCommand>(Unsupported("Request a result through the ADO query or command methods."));
    public override IDataParameter[] ToIDbDataParameter(params SugarParameter[] pars) => pars;
    public override SqlSugar.IDataAdapter GetAdapter() => throw Unsupported("Provider data adapters are unavailable in a native transaction.");
    public override void SetCommandToAdapter(SqlSugar.IDataAdapter adapter, DbCommand command) =>
        throw Unsupported("Provider data adapters are unavailable in a native transaction.");
    public override int ExecuteCommandWithGo(string sql, params SugarParameter[] parameters) =>
        throw Unsupported("The native transaction accepts one command per call.");

    public override int ExecuteCommand(string sql, params SugarParameter[] parameters) =>
        InOperation(_ =>
        {
            parameters ??= [];
            DemandCommand(parameters);
            using var statement = new NativeStrings(sql);
            using var values = Parameters(parameters);
            Check(SqlNative.Execute(handle, statement[0], Pointer(values), out var affected));
            return checked((int)affected);
        });

    public override Task<int> ExecuteCommandAsync(string sql, params SugarParameter[] parameters) =>
        Task.Run(() => ExecuteCommand(sql, parameters), lifetime);

    public override IDataReader GetDataReader(string sql, params SugarParameter[] parameters) =>
        OpenReader(sql, parameters, SqlNative.ReadDiscover);

    public override Task<IDataReader> GetDataReaderAsync(string sql, params SugarParameter[] parameters) =>
        Task.Run(() => GetDataReader(sql, parameters), lifetime);

    public override object GetScalar(string sql, params SugarParameter[] parameters)
    {
        using var result = OpenReader(sql, parameters, SqlNative.ReadScalar);
        return result.Read() ? result.GetValue(0) : DBNull.Value;
    }

    public override Task<object> GetScalarAsync(string sql, params SugarParameter[] parameters) =>
        Task.Run(() => GetScalar(sql, parameters), lifetime);

    public override DataSet GetDataSetAll(string sql, params SugarParameter[] parameters)
    {
        using var result = GetDataReader(sql, parameters);
        var table = new DataTable();
        table.Load(result);
        var data = new DataSet();
        data.Tables.Add(table);
        return data;
    }

    public override Task<DataSet> GetDataSetAllAsync(string sql, params SugarParameter[] parameters) =>
        Task.Run(() => GetDataSetAll(sql, parameters), lifetime);

    private NativeDataReader OpenReader(string sql, SugarParameter[] parameters, int mode) => InOperation(_ =>
    {
        parameters ??= [];
        DemandCommand(parameters);
        using var statement = new NativeStrings(sql, string.Empty);
        using var values = Parameters(parameters);
        var status = SqlNative.ReadOpen(handle, statement[0], statement[1], statement[1], Pointer(values), 256, mode, out var shape);
        using (shape)
        {
            Check(status);
            try
            {
                if (shape is null) throw new UniversalDataException((int)StatusCode.Protocol, "The native cursor returned no schema.");
                return reader = new NativeDataReader(this, SqlNative.SchemaJson(shape));
            }
            catch { SqlNative.ReadClose(handle); throw; }
        }
    });

    // The reader holds the participant lease while moving between native batches.
    internal SqlBatchHandle? ReadBatch(CancellationToken cancellationToken) => RunNative(_ =>
    {
        var status = SqlNative.ReadNext(handle, out var rows);
        if (status == (int)StatusCode.End)
        {
            rows?.Dispose();
            return null;
        }
        try
        {
            Check(status);
            return rows ?? throw new UniversalDataException((int)StatusCode.Protocol, "The native cursor returned no batch.");
        }
        catch { rows?.Dispose(); throw; }
    }, cancellationToken);

    internal void CloseReader(NativeDataReader owner)
    {
        lock (operations)
        {
            owner.CloseState();
            if (!ReferenceEquals(reader, owner)) return;
            reader = null;
            Check(SqlNative.ReadClose(handle));
        }
    }

    // SqlSugar's PostgreSQL and Oracle binders reject the Native ABI names for
    // these CLR-compatible values. Keep the Native schema and scalar payload
    // unchanged; expose only the provider's canonical metadata name here.
    internal string GetDataTypeName(string nativeType) => dialect switch
    {
        SugarDbType.PostgreSQL when nativeType == "int64" => "bigint",
        SugarDbType.PostgreSQL when nativeType == "utf8" => "text",
        SugarDbType.PostgreSQL when nativeType == "decimal128" => "numeric",
        SugarDbType.Oracle when nativeType == "utf8" => "varchar2",
        SugarDbType.Oracle when nativeType == "decimal128" => "decimal",
        _ => nativeType
    };

    internal int Write<T>(string table, IReadOnlyList<EntityMember> members, uint[] keys,
        IEnumerable<T> rows, int chunkRows, int mode) => InOperation(token =>
    {
        // Oracle's ALL_TAB_COLUMNS contract publishes unquoted identifiers in
        // uppercase, while SqlSugar attributes conventionally retain the
        // source casing.  The native writer validates the batch schema against
        // that metadata byte-for-byte.  Adapt only the ABI field names used by
        // this provider; CLR member access and values remain unchanged.
        var providerMembers = dialect == SugarDbType.Oracle
            ? members.Select(member => member with { ColumnName = member.ColumnName.ToUpperInvariant() }).ToArray()
            : members;
        using var input = rows.GetEnumerator();
        if (!input.MoveNext()) return 0;
        using var schema = SqlNative.SchemaFromJson(SqlNative.Context(handle), EntityMapping.FormatSchema(providerMembers));
        using var pinned = new SqlBatchBuilder.PinnedValues(providerMembers.Count);
        using var name = new NativeStrings(table);
        var keyPin = keys.Length == 0 ? default : GCHandle.Alloc(keys, GCHandleType.Pinned);
        SqlBatchHandle? batch = null;
        ExceptionDispatchInfo? failure = null;
        try
        {
            var status = SqlNative.WriteStream(handle, mode, SqlNative.LoadParameters, name[0],
                keyPin.IsAllocated ? keyPin.AddrOfPinnedObject() : IntPtr.Zero, (uint)keys.Length,
                checked((uint)chunkRows), Next, IntPtr.Zero, out var affected);
            failure?.Throw();
            Check(status);
            return checked((int)affected);
        }
        finally
        {
            batch?.Dispose();
            if (keyPin.IsAllocated) keyPin.Free();
        }

        int Next(IntPtr _, uint capacity, out IntPtr data, out int last)
        {
            data = IntPtr.Zero;
            last = 0;
            try
            {
                batch?.Dispose();
                batch = SqlBatchBuilder.BuildChunk(SqlNative.Context(handle), schema, providerMembers, input, pinned,
                    capacity, token, out var more);
                data = batch.DangerousGetHandle();
                last = more ? 0 : 1;
                return 0;
            }
            catch (Exception e) when (e is not OutOfMemoryException)
            {
                failure = ExceptionDispatchInfo.Capture(e);
                return (int)(e is OperationCanceledException ? StatusCode.Cancelled : StatusCode.ManagedComponent);
            }
        }
    });

    private void DemandCommand(SugarParameter[] parameters)
    {
        if (CommandType != System.Data.CommandType.Text)
        throw Unsupported("A native participant accepts only text commands.");
        foreach (var parameter in parameters)
            // SqlSugar's scalar constructors leave Direction unset, which its providers treat as input.
            if (parameter.Direction is not (0 or ParameterDirection.Input) || parameter.IsArray ||
                parameter.IsJson || parameter.IsRefCursor || parameter.IsClob || parameter.IsNClob ||
                parameter.IsNvarchar2 || parameter.CustomDbType is not null ||
                !string.IsNullOrEmpty(parameter.TypeName) || !string.IsNullOrEmpty(parameter.UdtTypeName))
                throw Unsupported("The native participant accepts scalar input parameters.");
    }

    private SqlBatchHandle? Parameters(SugarParameter[] parameters) => parameters.Length == 0 ? null :
        SqlBatchBuilder.BuildParameters(SqlNative.Context(handle), parameters);

    private static IntPtr Pointer(SqlBatchHandle? batch) => batch?.DangerousGetHandle() ?? IntPtr.Zero;

    internal OperationLease EnterOperation(NativeDataReader? owner)
    {
        if (Monitor.IsEntered(operations) || !Monitor.TryEnter(operations)) throw Busy();
        try
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            if (!ReferenceEquals(reader, owner)) throw Busy();
            return new OperationLease(operations);
        }
        catch
        {
            Monitor.Exit(operations);
            throw;
        }
    }

    internal readonly struct OperationLease(object gate) : IDisposable
    {
        public void Dispose() => Monitor.Exit(gate);
    }

    private T InOperation<T>(Func<CancellationToken, T> operation)
    {
        using var lease = EnterOperation(null);
        return RunNative(operation, default);
    }

    private T RunNative<T>(Func<CancellationToken, T> operation, CancellationToken cancellationToken)
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(
            lifetime, CancellationToken.GetValueOrDefault(), cancellationToken);
        if (CommandTimeOut > 0) cancellation.CancelAfter(TimeSpan.FromSeconds(CommandTimeOut));
        cancellation.Token.ThrowIfCancellationRequested();
        Native.ud_context_clear_error(SqlNative.Context(handle));
        using var interrupt = cancellation.Token.UnsafeRegister(static session => SqlNative.Cancel((SqlSessionHandle)session!), handle);
        try { return operation(cancellation.Token); }
        catch (UniversalDataException) when (cancellation.IsCancellationRequested)
        {
            cancellation.Token.ThrowIfCancellationRequested();
            throw;
        }
    }

    private void Check(int status) => Check(handle, status);

    internal static void Check(SqlSessionHandle session, int status)
    {
        if (status == 0) return;
        var error = WorkerNativeError.Create((StatusCode)status, SqlNative.Context(session));
        throw new UniversalDataException(status, error.Message, error);
    }

    public override void Dispose()
    {
        lock (operations)
        {
            if (disposed) return;
            disposed = true;
            try { reader?.Close(); }
            finally { if (ownsSession) handle.Dispose(); }
        }
    }

    private static UniversalDataException Busy() => new((int)StatusCode.Busy,
        "The native participant already has an active command or reader.");
    internal static UniversalDataException Unsupported(string reason) => new((int)StatusCode.Unsupported, reason);
    internal static UniversalDataException CoordinatorOwnsTransaction() => Unsupported("The native coordinator owns the transaction decision.");

    private sealed class NativeCoordinatorTransaction : IDbTransaction
    {
        internal static readonly NativeCoordinatorTransaction Instance = new();
        public IDbConnection? Connection => null;
        public IsolationLevel IsolationLevel => IsolationLevel.Unspecified;
        public void Commit() => throw CoordinatorOwnsTransaction();
        public void Rollback() => throw CoordinatorOwnsTransaction();
        public void Dispose() { }
    }
}
