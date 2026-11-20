using System.Runtime.InteropServices;

namespace UniversalData;

internal static partial class SqlNative
{
    private const string Library = "ud_data";

    internal const int ReadTyped = 0;
    internal const int ReadDiscover = 1;
    internal const int ReadScalar = 2;
    internal const int WriteAppend = 1;
    internal const int WriteDelete = 3;
    internal const int WriteUpdate = 5;
    internal const int LoadAuto = 0;
    internal const int LoadParameters = 1;
    internal const int LoadNative = 2;
    internal const int TransactionBegin = 1;
    internal const int TransactionCommit = 2;
    internal const int TransactionRollback = 3;

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    private static extern int ud_sql_session_open(int dialect, Native.StringValue pluginPath,
        Native.StringValue connectionString, out IntPtr session);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    private static extern int ud_execution_sql_session_open(ExecutionHandle execution,
        Native.StringValue connection, out IntPtr session);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    private static extern IntPtr ud_sql_session_context(SqlSessionHandle session);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    private static extern int ud_sql_session_dialect(SqlSessionHandle session);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    private static extern int ud_sql_session_set_auto_close(SqlSessionHandle session, int enabled);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    private static extern int ud_sql_session_read_open(SqlSessionHandle session, Native.StringValue sql,
        Native.StringValue queryJson, Native.StringValue schemaJson, IntPtr parameters, uint batchRows,
        int resultMode, out IntPtr resultSchema);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    private static extern int ud_sql_session_read_next(SqlSessionHandle session, out IntPtr batch);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    private static extern int ud_sql_session_read_close(SqlSessionHandle session);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    private static extern int ud_sql_session_execute(SqlSessionHandle session, Native.StringValue sql,
        IntPtr parameters, out ulong affectedRows);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate int BatchSource(IntPtr user, uint capacity, out IntPtr batch, out int last);

    [DllImport(Library, EntryPoint = "ud_sql_session_write_stream", CallingConvention = CallingConvention.Cdecl)]
    internal static extern int WriteStream(SqlSessionHandle session, int mode, int loadMode,
        Native.StringValue table, IntPtr keys, uint keyCount, uint chunkRows,
        BatchSource next, IntPtr user, out ulong affectedRows);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    private static extern int ud_sql_session_transaction(SqlSessionHandle session, int operation, int isolation);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    private static extern void ud_sql_session_cancel(SqlSessionHandle session);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    private static extern int ud_sql_session_close(IntPtr session);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    private static extern int ud_batch_create_like(IntPtr context, IntPtr metadataBatch, uint capacity,
        out IntPtr batch);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    private static extern int ud_batch_append(IntPtr batch, IntPtr values, uint columns);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    private static extern int ud_batch_seal(IntPtr batch);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    private static extern int ud_batch_get(IntPtr batch, uint row, uint column, out Native.ScalarValue value);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    private static extern uint ud_batch_rows(IntPtr batch);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    private static extern int ud_schema_to_json(IntPtr batch, out IntPtr text, out uint length);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    private static extern int ud_schema_from_json(IntPtr context, byte[] schema, uint schemaLength,
        Native.StringValue scope, out IntPtr batch);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    private static extern void ud_batch_destroy(IntPtr batch);

    internal static SqlSessionHandle Open(DataSourceKind dialect, string pluginPath, string connectionString)
    {
        var nativeDialect = dialect switch
        {
            DataSourceKind.Sqlite => 1,
            DataSourceKind.PostgreSql => 2,
            DataSourceKind.MySql => 3,
            DataSourceKind.MariaDb => 4,
            DataSourceKind.SqlServer => 5,
            DataSourceKind.Oracle => 6,
            _ => throw new ArgumentOutOfRangeException(nameof(dialect))
        };
        using var values = new NativeStrings(pluginPath, connectionString);
        Check(ud_sql_session_open(nativeDialect, values[0], values[1], out var pointer));
        return new SqlSessionHandle(pointer);
    }

    internal static SqlSessionHandle Open(ExecutionHandle execution, string connection)
    {
        using var values = new NativeStrings(connection);
        Check(ud_execution_sql_session_open(execution, values[0], out var pointer));
        return new SqlSessionHandle(pointer);
    }

    internal static IntPtr Context(SqlSessionHandle session) => ud_sql_session_context(session);
    internal static DataSourceKind Dialect(SqlSessionHandle session) => ud_sql_session_dialect(session) switch
    {
        1 => DataSourceKind.Sqlite,
        2 => DataSourceKind.PostgreSql,
        3 => DataSourceKind.MySql,
        4 => DataSourceKind.MariaDb,
        5 => DataSourceKind.SqlServer,
        6 => DataSourceKind.Oracle,
        _ => throw new NotSupportedException("The authorized Native connector has no supported database dialect.")
    };
    internal static int SetAutoClose(SqlSessionHandle session, bool enabled) => ud_sql_session_set_auto_close(session, enabled ? 1 : 0);

    internal static int ReadOpen(SqlSessionHandle session, Native.StringValue sql, Native.StringValue query,
        Native.StringValue schema, IntPtr parameters, uint batchRows, int mode, out SqlBatchHandle? resultSchema)
    {
        var status = ud_sql_session_read_open(session, sql, query, schema, parameters, batchRows, mode, out var pointer);
        resultSchema = pointer == IntPtr.Zero ? null : new SqlBatchHandle(pointer);
        return status;
    }

    internal static int ReadNext(SqlSessionHandle session, out SqlBatchHandle? batch)
    {
        var status = ud_sql_session_read_next(session, out var pointer);
        batch = pointer == IntPtr.Zero ? null : new SqlBatchHandle(pointer);
        return status;
    }

    internal static int ReadClose(SqlSessionHandle session) => ud_sql_session_read_close(session);
    internal static int CloseRaw(IntPtr session) => ud_sql_session_close(session);
    internal static int Execute(SqlSessionHandle session, Native.StringValue sql, IntPtr parameters, out ulong affected) =>
        ud_sql_session_execute(session, sql, parameters, out affected);
    internal static int Transaction(SqlSessionHandle session, int operation, int isolation) =>
        ud_sql_session_transaction(session, operation, isolation);
    internal static void Cancel(SqlSessionHandle session) => ud_sql_session_cancel(session);
    internal static void DestroyBatchRaw(IntPtr batch) => ud_batch_destroy(batch);
    internal static int BatchCreateLike(IntPtr context, IntPtr schema, uint capacity, out SqlBatchHandle batch)
    {
        var status = ud_batch_create_like(context, schema, capacity, out var pointer);
        batch = pointer == IntPtr.Zero ? null! : new SqlBatchHandle(pointer);
        return status;
    }

    internal static int BatchCreateLikeRaw(IntPtr context, IntPtr schema, uint capacity, out IntPtr batch) =>
        ud_batch_create_like(context, schema, capacity, out batch);

    internal static int BatchAppendRaw(IntPtr batch, IntPtr values, uint columns) =>
        ud_batch_append(batch, values, columns);

    internal static int BatchSeal(SqlBatchHandle batch) => ud_batch_seal(batch.DangerousGetHandle());
    internal static int BatchGet(SqlBatchHandle batch, uint row, uint column, out Native.ScalarValue value) =>
        ud_batch_get(batch.DangerousGetHandle(), row, column, out value);
    internal static uint BatchRows(SqlBatchHandle batch) => ud_batch_rows(batch.DangerousGetHandle());

    internal static string SchemaJson(SqlBatchHandle batch)
    {
        Check(ud_schema_to_json(batch.DangerousGetHandle(), out var text, out var length));
        return DataOwnership.Text(text, length);
    }

    internal static SqlBatchHandle SchemaFromJson(IntPtr context, string schema)
    {
        var data = Utf8.Bytes(schema);
        using var strings = new NativeStrings(string.Empty);
        Check(ud_schema_from_json(context, data, checked((uint)data.Length), strings[0], out var pointer));
        return new SqlBatchHandle(pointer);
    }

    internal static void Check(int status)
    {
        if (status != 0)
            throw new UniversalDataException(status, Marshal.PtrToStringUTF8(Native.ud_status_name(status)) ?? "Native SQL operation failed");
    }
}
