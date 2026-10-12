using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace UniversalData;

public enum RunState { Created = 0, Running = 1, Complete = 2, Failed = 3 }
public enum RunAccess { Read = 101, Write = 102 }
public delegate bool RunAuthorizer(string tenant, string resource, RunAccess access);
public sealed record RunMetrics(RunState State, StatusCode Result, ulong InputRows, ulong OutputRows,
    ulong Batches, ulong QueuedBatches, ulong QueueHighWater, ulong MemoryUsed);

/// <summary>Independent DAG execution with native bounded queues. File operations require authorization.</summary>
public sealed class Run : IDisposable
{
    [ThreadStatic] private static int authorizing;
    private readonly RunHandle run;
    private readonly object controller = new();
    private volatile bool disposed;
    public Run(string dagJson, string tenant, RunAuthorizer? authorize, string? parametersJson = null,
        uint edgeCapacity = 2, uint maxBatchRows = 4096, ulong memoryLimit = 268435456)
    {
        var document = Utf8.Bytes(dagJson); var tenantBytes = Utf8.Bytes(tenant);
        var values = parametersJson is null ? null : Utf8.Bytes(parametersJson);
        Native.Authorizer? callback = authorize is null ? null : (_, scope, resource, action) =>
        {
            authorizing++;
            try
            {
                string scopeText = Marshal.PtrToStringUTF8(scope.Data, checked((int)scope.Length))!;
                string resourceText = Marshal.PtrToStringUTF8(resource.Data, checked((int)resource.Length))!;
                return authorize(scopeText, resourceText, (RunAccess)action) ? 0 : 13;
            }
            catch { return 13; } // Exceptions cannot cross a native execution callback.
            finally { authorizing--; }
        };
        var scopePin = GCHandle.Alloc(tenantBytes.Length == 0 ? new byte[1] : tenantBytes, GCHandleType.Pinned);
        GCHandle valuesPin = default;
        try
        {
            if (values is not null) valuesPin = GCHandle.Alloc(values.Length == 0 ? new byte[1] : values, GCHandleType.Pinned);
            var options = new Native.RunOptions
            {
                Size = (uint)Marshal.SizeOf<Native.RunOptions>(), Abi = 2,
                Context = new Native.ContextOptions { Size = (uint)Marshal.SizeOf<Native.ContextOptions>(), Abi = 2, Limit = memoryLimit },
                EdgeCapacity = edgeCapacity, MaxRows = maxBatchRows,
                Parameters = values is null ? default : new Native.StringValue(valuesPin.AddrOfPinnedObject(), checked((uint)values.Length)),
                Tenant = new Native.StringValue(scopePin.AddrOfPinnedObject(), checked((uint)tenantBytes.Length)),
                Authorize = callback is null ? IntPtr.Zero : Marshal.GetFunctionPointerForDelegate(callback)
            };
            Check(Native.ud_run_create_json(document, checked((uint)document.Length), ref options, out var pointer));
            run = new RunHandle(pointer, callback);
        }
        finally { scopePin.Free(); if (valuesPin.IsAllocated) valuesPin.Free(); GC.KeepAlive(callback); }
    }
    public void Execute() { EnsureController(); lock (controller) { EnsureOpen(); Check(Native.ud_run_execute(run)); } }
    public void Start() { EnsureController(); lock (controller) { EnsureOpen(); Check(Native.ud_run_start(run)); } }
    /// <summary>Returns false on timeout; execution failures retain the native error category.</summary>
    public bool Wait(uint timeoutMilliseconds)
    {
        EnsureController();
        lock (controller)
        {
            EnsureOpen(); int status = Native.ud_run_wait(run, timeoutMilliseconds);
            if (status == 11) return false; Check(status); return true;
        }
    }
    public void Cancel()
    {
        if (!run.IsClosed) { try { Native.ud_run_cancel(run); } catch (ObjectDisposedException) { } }
    }
    public RunMetrics Metrics
    {
        get
        {
            EnsureOpen(); Native.ud_run_get_metrics(run, out var metrics);
            return new((RunState)metrics.State, (StatusCode)metrics.Result, metrics.InputRows, metrics.OutputRows,
                metrics.Batches, metrics.Queued, metrics.HighWater, metrics.Memory);
        }
    }
    public int NodeState(uint node)
    {
        EnsureOpen(); Check(Native.ud_run_node_state(run, node, out int state)); return state;
    }
    private static void Check(int status)
    {
        if (status != 0) throw new UniversalDataException(status, Marshal.PtrToStringUTF8(Native.ud_status_name(status)) ?? "Native execution error");
    }
    private void EnsureOpen() => ObjectDisposedException.ThrowIf(disposed, this);
    private static void EnsureController()
    {
        if (authorizing != 0) throw new InvalidOperationException("Authorization callbacks may inspect metrics or cancel, but cannot control or dispose runs");
    }
    public void Dispose()
    {
        EnsureController();
        Cancel(); lock (controller) { if (disposed) return; disposed = true; run.Dispose(); }
        GC.SuppressFinalize(this);
    }
}
internal sealed class RunHandle : SafeHandleZeroOrMinusOneIsInvalid
{
    private readonly Native.Authorizer? authorize;
    internal RunHandle(IntPtr pointer, Native.Authorizer? callback) : base(true) { authorize = callback; SetHandle(pointer); }
    protected override bool ReleaseHandle()
    {
        Native.ud_run_cancel_raw(handle);
        while (Native.ud_run_wait_raw(handle, 100) == 11) { Native.ud_run_get_metrics_raw(handle, out var m); if (m.State != 1) break; }
        int status = Native.ud_run_destroy(handle); GC.KeepAlive(authorize); return status == 0;
    }
}
internal static partial class Native
{
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] internal delegate int Authorizer(IntPtr user, StringValue tenant, StringValue resource, int action);
    [StructLayout(LayoutKind.Sequential)] internal struct RunOptions
    {
        internal uint Size, Abi; internal ContextOptions Context; internal uint EdgeCapacity, MaxRows;
        internal IntPtr Handlers; internal uint HandlerCount; internal StringValue Parameters, Tenant;
        internal IntPtr Authorize, AuthorizeUser;
    }
    [StructLayout(LayoutKind.Sequential)] internal struct RunMetricsValue
    {
        internal int State, Result; internal ulong InputRows, OutputRows, Batches, Queued, HighWater, Memory;
    }
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_run_create_json(byte[] graph, uint length, ref RunOptions options, out IntPtr run);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_run_execute(RunHandle run);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_run_start(RunHandle run);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_run_wait(RunHandle run, uint timeout);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern void ud_run_cancel(RunHandle run);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern void ud_run_get_metrics(RunHandle run, out RunMetricsValue metrics);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_run_node_state(RunHandle run, uint node, out int state);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ud_run_cancel")] internal static extern void ud_run_cancel_raw(IntPtr run);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ud_run_wait")] internal static extern int ud_run_wait_raw(IntPtr run, uint timeout);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ud_run_get_metrics")] internal static extern void ud_run_get_metrics_raw(IntPtr run, out RunMetricsValue metrics);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_run_destroy(IntPtr run);
}
