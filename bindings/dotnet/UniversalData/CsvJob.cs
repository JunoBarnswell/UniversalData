using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Win32.SafeHandles;

namespace UniversalData;

public sealed class CsvJob : IDisposable
{
    private readonly ContextHandle context;
    private readonly JobHandle job;
    private readonly object execution = new();
    private bool disposed;

    public CsvJob(string inputDirectory, string stateDirectory, string schema, string commands,
        ulong memoryLimit = 268435456)
    {
        Utf8.Bytes(inputDirectory); Utf8.Bytes(stateDirectory); Utf8.Bytes(schema); Utf8.Bytes(commands);
        var options = new Native.ContextOptions { Size = (uint)Marshal.SizeOf<Native.ContextOptions>(), Abi = 2, Limit = memoryLimit };
        int status = Native.ud_context_create(ref options, out IntPtr pointer);
        if (status != 0) throw new UniversalDataException(status, "Cannot create native context");
        context = new ContextHandle(pointer);
        try
        {
            var config = new Native.JobOptions
            {
                Size = (uint)Marshal.SizeOf<Native.JobOptions>(), Abi = 2,
                Input = Path.GetFullPath(inputDirectory), State = Path.GetFullPath(stateDirectory),
                Schema = schema, SchemaLength = checked((uint)Encoding.UTF8.GetByteCount(schema)),
                Commands = commands, CommandsLength = checked((uint)Encoding.UTF8.GetByteCount(commands))
            };
            Check(Native.ud_job_create(context, ref config, out pointer));
            job = new JobHandle(pointer, context);
        }
        catch { context.Dispose(); throw; }
    }

    public ulong Generation { get { lock (execution) { EnsureOpen(); return Native.ud_job_generation(job); } } }
    public void Poll() { lock (execution) { EnsureOpen(); Check(Native.ud_job_poll(job)); } }
    /// <returns>False on cancellation; true if the native watch loop completes.</returns>
    public bool Run(CancellationToken cancellationToken = default)
    {
        lock (execution)
        {
            EnsureOpen();
            using var registration = cancellationToken.Register(Cancel);
            int status = Native.ud_job_run(job);
            if (status == 8) return false;
            Check(status); return true;
        }
    }
    public void Cancel()
    {
        // SafeHandle retains the context for each P/Invoke, including concurrent cancellation.
        if (!context.IsClosed) { try { Native.ud_cancel(context); } catch (ObjectDisposedException) { } }
    }
    public static string? LatestSnapshot(string stateDirectory)
    {
        var options = new Native.ContextOptions { Size = (uint)Marshal.SizeOf<Native.ContextOptions>(), Abi = 2 };
        int status = Native.ud_context_create(ref options, out var pointer);
        if (status != 0) throw new UniversalDataException(status, "Cannot create context");
        using var ctx = new ContextHandle(pointer);
        status = Native.ud_snapshot_latest(ctx, Path.GetFullPath(stateDirectory), out pointer, out _);
        if (status == 1) return null;
        if (status != 0) throw new UniversalDataException(status, Native.Error(ctx));
        try { return Marshal.PtrToStringUTF8(pointer)!; } finally { Native.ud_free(pointer); }
    }
    private void Check(int status) { if (status != 0) throw new UniversalDataException(status, Native.Error(context)); }
    private void EnsureOpen() => ObjectDisposedException.ThrowIf(disposed, this);
    public void Dispose()
    {
        Cancel();
        lock (execution) { if (disposed) return; disposed = true; job.Dispose(); context.Dispose(); }
        GC.SuppressFinalize(this);
    }
}

internal sealed class ContextHandle : SafeHandleZeroOrMinusOneIsInvalid
{
    internal ContextHandle(IntPtr pointer) : base(true) => SetHandle(pointer);
    protected override bool ReleaseHandle() => Native.ud_context_destroy(handle) == 0;
}
internal sealed class JobHandle : SafeHandleZeroOrMinusOneIsInvalid
{
    private readonly ContextHandle context;
    internal JobHandle(IntPtr pointer, ContextHandle owner) : base(true)
    {
        context = owner; bool retained = false; context.DangerousAddRef(ref retained); SetHandle(pointer);
    }
    protected override bool ReleaseHandle() { Native.ud_job_destroy(handle); context.DangerousRelease(); return true; }
}

internal static partial class Native
{
    private const string Library = "ud_data";
    [StructLayout(LayoutKind.Sequential)] internal struct ContextOptions
    { internal uint Size, Abi; internal ulong Limit; internal IntPtr User, Allocate, Deallocate; }
    [StructLayout(LayoutKind.Sequential)] internal struct JobOptions
    {
        internal uint Size, Abi;
        [MarshalAs(UnmanagedType.LPUTF8Str)] internal string Input;
        [MarshalAs(UnmanagedType.LPUTF8Str)] internal string State;
        [MarshalAs(UnmanagedType.LPUTF8Str)] internal string Schema;
        internal uint SchemaLength;
        [MarshalAs(UnmanagedType.LPUTF8Str)] internal string Commands;
        internal uint CommandsLength;
        [MarshalAs(UnmanagedType.LPUTF8Str)] internal string? NullMarker;
        internal uint NullLength, BatchSize, CheckpointRows, CheckpointMs, PollMs;
        internal IntPtr Observer, ObserverUser;
    }
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_context_create(ref ContextOptions options, out IntPtr context);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_context_destroy(IntPtr context);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern IntPtr ud_error(ContextHandle context);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_job_create(ContextHandle context, ref JobOptions options, out IntPtr job);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_job_poll(JobHandle job);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_job_run(JobHandle job);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern void ud_job_destroy(IntPtr job);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern ulong ud_job_generation(JobHandle job);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern void ud_cancel(ContextHandle context);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_snapshot_latest(ContextHandle context, [MarshalAs(UnmanagedType.LPUTF8Str)] string state, out IntPtr path, out ulong generation);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern void ud_free(IntPtr pointer);
    internal static string Error(ContextHandle context) => Marshal.PtrToStringUTF8(ud_error(context)) ?? "Native error";
}
