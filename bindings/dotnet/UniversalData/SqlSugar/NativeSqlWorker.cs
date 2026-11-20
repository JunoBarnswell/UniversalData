using System.Text.Json;
using UniversalData;

namespace SqlSugar;

internal sealed class NativeSqlWorker : IDisposable
{
    private SqlSessionHandle? session;
    private SqlBatchHandle? arguments;
    private readonly Func<string, SqlSessionHandle>? openAuthorized;
    internal NativeSqlWorker(Func<string, SqlSessionHandle>? openAuthorized = null)
    {
        this.openAuthorized = openAuthorized;
    }

    internal (WorkerResponseFrame Response, WorkerBatchBuffer? Batch) Invoke(WorkerRequestFrame request,
        WorkerBatchBuffer? batch, Func<uint, (WorkerBatchFrame Control, WorkerBatchBuffer? Batch)>? source)
    {
        int status;
        object? value = null;
        WorkerBatchBuffer? data = null;
        OperationError? error = null;
        try
        {
            if (request.Operation == "sql.open")
            {
                if (session is not null)
                    throw new UniversalDataException((int)StatusCode.Busy, "SQL session is already open");
                var config = request.Values.Deserialize<ConnectionConfig>(WorkerProtocol.Json)
                    ?? throw new ArgumentException("SQL connection configuration is required");
                session = openAuthorized is null
                    ? SqlNative.Open(config.DbType, SqlConnectionConfiguration.Plugin(config), config.ConnectionString)
                    : openAuthorized(config.ConfigId);
                status = SqlNative.SetAutoClose(session, config.IsAutoCloseConnection);
                if (status != 0)
                {
                    session.Dispose();
                    session = null;
                }
                else
                    value = SqlNative.Dialect(session);
                return Reply(request, status, value, data);
            }

            var handle = session ?? throw new UniversalDataException((int)StatusCode.Invalid, "SQL session is closed");
            IntPtr context = SqlNative.Context(handle);
            Native.ud_context_clear_error(context);
            switch (request.Operation)
            {
                case "sql.readOpen":
                    status = ReadOpen(handle, context, request.Values, batch, out value);
                    break;
                case "sql.read":
                    status = SqlNative.ReadNext(handle, out var rows);
                    using (rows)
                        if (status == 0 && rows is not null)
                            data = SqlWire.Encode(rows);
                    break;
                case "sql.readClose":
                    status = SqlNative.ReadClose(handle);
                    arguments?.Dispose();
                    arguments = null;
                    break;
                case "sql.execute":
                    using (var parameters = batch is null ? null : SqlWire.Decode(context, batch))
                    using (var sql = new NativeStrings(Text(request.Values, "sql")))
                    {
                        status = SqlNative.Execute(handle, sql[0], parameters?.DangerousGetHandle() ?? IntPtr.Zero,
                            out var affected);
                        value = affected;
                    }
                    break;
                case "sql.transaction":
                    status = SqlNative.Transaction(handle, request.Values.GetProperty("operation").GetInt32(),
                        request.Values.GetProperty("isolation").GetInt32());
                    break;
                case "sql.write":
                    status = Write(handle, context, request, source ??
                        throw new InvalidDataException("Missing SQL batch input"), out value, out error);
                    break;
                default:
                    status = (int)StatusCode.Protocol;
                    break;
            }
            if (status != 0 && status != (int)StatusCode.End && error is null)
                error = WorkerNativeError.Create((StatusCode)status, context);
        }
        catch (Exception exception) when (exception is UniversalDataException or IOException or InvalidDataException or
            JsonException or ArgumentException or InvalidOperationException or NotSupportedException or DllNotFoundException)
        {
            status = exception switch
            {
                UniversalDataException native => native.Status,
                FileNotFoundException or DllNotFoundException => (int)StatusCode.DriverUnavailable,
                InvalidDataException => (int)StatusCode.Protocol,
                _ => (int)StatusCode.Invalid
            };
        }
        return Reply(request, status, value, data, error);
    }

    private int ReadOpen(SqlSessionHandle handle, IntPtr context, JsonElement values, WorkerBatchBuffer? data, out object? result)
    {
        result = null;
        var parameters = data is null ? null : SqlWire.Decode(context, data);
        try
        {
            using var strings = new NativeStrings(Text(values, "sql"), Text(values, "query"), Text(values, "schema"));
            var status = SqlNative.ReadOpen(handle, strings[0], strings[1], strings[2],
                parameters?.DangerousGetHandle() ?? IntPtr.Zero,
                values.GetProperty("rows").GetUInt32(), values.GetProperty("mode").GetInt32(), out var schema);
            using (schema)
            {
                if (status == 0)
                {
                    arguments?.Dispose();
                    arguments = parameters;
                    parameters = null;
                    result = schema is null ? "" : SqlNative.SchemaJson(schema);
                }
            }
            return status;
        }
        finally { parameters?.Dispose(); }
    }

    private static int Write(SqlSessionHandle handle, IntPtr context, WorkerRequestFrame request,
        Func<uint, (WorkerBatchFrame Control, WorkerBatchBuffer? Batch)> source, out object affected, out OperationError? error)
    {
        var values = request.Values;
        var keys = values.GetProperty("keys").Deserialize<uint[]>(WorkerProtocol.Json) ?? [];
        using var table = new NativeStrings(Text(values, "table"));
        var pinned = keys.Length == 0 ? default : System.Runtime.InteropServices.GCHandle.Alloc(keys,
            System.Runtime.InteropServices.GCHandleType.Pinned);
        SqlBatchHandle? current = null;
        OperationError? inputError = null;
        try
        {
            var status = SqlNative.WriteStream(handle, values.GetProperty("mode").GetInt32(),
                values.GetProperty("loadMode").GetInt32(), table[0],
                pinned.IsAllocated ? pinned.AddrOfPinnedObject() : IntPtr.Zero, (uint)keys.Length,
                values.GetProperty("rows").GetUInt32(), Next, IntPtr.Zero, out var count);
            affected = count;
            error = inputError;
            return status;
        }
        finally
        {
            current?.Dispose();
            if (pinned.IsAllocated)
                pinned.Free();
        }

        int Next(IntPtr _, uint capacity, out IntPtr pointer, out int last)
        {
            pointer = IntPtr.Zero;
            last = 0;
            try
            {
                var frame = source(capacity);
                using var payload = frame.Batch;
                if (frame.Control.Sequence != request.Sequence ||
                    frame.Control.Revision != ProductInfo.WorkerRevision)
                    return (int)StatusCode.Protocol;
                inputError = frame.Control.Error;
                if (frame.Control.Code != StatusCode.Ok)
                    return (int)frame.Control.Code;
                if (payload is null)
                    return (int)StatusCode.Protocol;
                current?.Dispose();
                current = SqlWire.Decode(context, payload);
                pointer = current.DangerousGetHandle();
                last = frame.Control.Last ? 1 : 0;
                return 0;
            }
            catch (Exception error) when (error is IOException or InvalidDataException or UniversalDataException or JsonException)
            {
                return error is UniversalDataException native ? native.Status :
                    error is InvalidDataException or JsonException ? (int)StatusCode.Protocol : (int)StatusCode.WorkerLost;
            }
        }
    }

    internal static (WorkerResponseFrame, WorkerBatchBuffer?) Reply(WorkerRequestFrame request, int status,
        object? value = null, WorkerBatchBuffer? batch = null, OperationError? error = null)
    {
        StatusCode code = (StatusCode)status;
        return (new(ProductInfo.WorkerRevision, request.Sequence, "result", code,
            value is null ? null : JsonSerializer.SerializeToElement(value, WorkerProtocol.Json),
            code is StatusCode.Ok or StatusCode.End ? null : error ?? WorkerNativeError.Create(code)), batch);
    }

    internal static string Text(JsonElement element, string name) =>
        element.TryGetProperty(name, out var value) ? value.GetString() ?? "" : "";

    public void Dispose()
    {
        arguments?.Dispose();
        session?.Dispose();
    }
}
