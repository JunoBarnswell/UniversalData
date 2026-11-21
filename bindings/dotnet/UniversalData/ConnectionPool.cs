using Microsoft.Win32.SafeHandles;
using System.Runtime.InteropServices;

namespace UniversalData;

internal sealed class ConnectionPool : SafeHandleZeroOrMinusOneIsInvalid
{
    internal ConnectionPool(uint maxSessions, ulong memoryLimit, string? lockFile = null, uint nodeLimit = 0) : base(true)
    {
        SqlNative.Check(Native.ud_connection_pool_create(maxSessions, memoryLimit, out var pointer));
        SetHandle(pointer);
        if (lockFile is null) return;
        IntPtr text = Marshal.StringToCoTaskMemUTF8(lockFile);
        try
        {
            var path = new Native.StringValue(text, checked((uint)System.Text.Encoding.UTF8.GetByteCount(lockFile)));
            SqlNative.Check(Native.ud_connection_pool_set_node_limits(this, path, nodeLimit));
        }
        catch { Dispose(); throw; }
        finally { Marshal.FreeCoTaskMem(text); }
    }

    internal async Task TrimIdleAsync(CancellationToken stopping)
    {
        using var cadence = new PeriodicTimer(TimeSpan.FromMilliseconds(250));
        try
        {
            while (await cadence.WaitForNextTickAsync(stopping) && !IsClosed)
                Native.ud_connection_pool_trim_idle(this);
        }
        catch (OperationCanceledException) when (stopping.IsCancellationRequested) { }
        catch (ObjectDisposedException) when (IsClosed) { }
    }

    protected override bool ReleaseHandle() => Native.ud_connection_pool_destroy(handle) == 0;
}

internal static partial class Native
{
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ud_connection_pool_create(uint maxSessions, ulong memoryLimit, out IntPtr pool);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ud_connection_pool_destroy(IntPtr pool);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ud_connection_pool_set_node_limits(ConnectionPool pool, StringValue lockFile, uint max);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ud_connection_pool_trim_idle(ConnectionPool pool);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    private static extern int ud_execution_create_json_in_pool(ConnectionPool pool, StringValue json,
        ref ExecutionOptions options, out IntPtr execution);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    private static extern int ud_execution_create_partitioned_in_pool(ConnectionPool pool, StringValue json,
        ref ExecutionOptions options, StringValue run, StringValue topic, KafkaPartitionInputValue[] parts,
        uint count, out IntPtr execution);

    internal static int CreateExecution(StringValue document, ref ExecutionOptions options, ConnectionPool? pool,
        out IntPtr execution) => pool is null
            ? ud_execution_create_json(document, ref options, out execution)
            : ud_execution_create_json_in_pool(pool, document, ref options, out execution);

    internal static int CreatePartitionedExecution(StringValue document, ref ExecutionOptions options,
        StringValue run, StringValue topic, KafkaPartitionInputValue[] parts, ConnectionPool? pool,
        out IntPtr execution) => pool is null
            ? ud_execution_create_partitioned(document, ref options, run, topic, parts, (uint)parts.Length, out execution)
            : ud_execution_create_partitioned_in_pool(pool, document, ref options, run, topic, parts, (uint)parts.Length, out execution);
}
