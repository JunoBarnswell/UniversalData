using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace UniversalData;

/// <summary>Immutable structured DAG contract; the generic execution runtime is under development.</summary>
public sealed class Dag : IDisposable
{
    private readonly ContextHandle context;
    private readonly DagHandle dag;
    private readonly object execution = new();
    private bool disposed;
    public Dag(string json, ulong memoryLimit = 268435456)
    {
        var bytes = Utf8.Bytes(json); context = DataOwnership.Context(memoryLimit);
        try
        {
            DataOwnership.Check(context, Native.ud_dag_create(context, bytes, checked((uint)bytes.Length), out var pointer));
            dag = new DagHandle(pointer, context);
        }
        catch { context.Dispose(); throw; }
    }
    public uint NodeCount { get { lock (execution) { EnsureOpen(); return Native.ud_dag_nodes(dag); } } }
    public uint EdgeCount { get { lock (execution) { EnsureOpen(); return Native.ud_dag_edges(dag); } } }
    public byte[] Fingerprint
    {
        get { lock (execution) { EnsureOpen(); var hash = new byte[32]; Native.ud_dag_fingerprint(dag, hash); return hash; } }
    }
    public string ToJson()
    {
        lock (execution)
        {
            EnsureOpen(); DataOwnership.Check(context, Native.ud_dag_serialize(dag, out var pointer, out var length));
            return DataOwnership.Text(pointer, length);
        }
    }
    public Dag Bind(string parametersJson, ulong memoryLimit = 268435456)
    {
        var values = Utf8.Bytes(parametersJson);
        lock (execution)
        {
            EnsureOpen(); var pinned = GCHandle.Alloc(values.Length == 0 ? new byte[1] : values, GCHandleType.Pinned);
            try
            {
                DataOwnership.Check(context, Native.ud_dag_bind(dag, new Native.StringValue(pinned.AddrOfPinnedObject(), checked((uint)values.Length)), out var pointer));
                using var bound = new DagHandle(pointer, context);
                DataOwnership.Check(context, Native.ud_dag_serialize(bound, out pointer, out var length));
                return new Dag(DataOwnership.Text(pointer, length), memoryLimit);
            }
            finally { pinned.Free(); }
        }
    }
    private void EnsureOpen() => ObjectDisposedException.ThrowIf(disposed, this);
    public void Dispose()
    {
        lock (execution) { if (disposed) return; disposed = true; dag.Dispose(); context.Dispose(); }
        GC.SuppressFinalize(this);
    }
}
internal sealed class DagHandle : SafeHandleZeroOrMinusOneIsInvalid
{
    private readonly ContextHandle owner;
    internal DagHandle(IntPtr pointer, ContextHandle context) : base(true)
    { owner = context; bool retained = false; context.DangerousAddRef(ref retained); SetHandle(pointer); }
    protected override bool ReleaseHandle() { Native.ud_dag_destroy(handle); owner.DangerousRelease(); return true; }
}
internal static partial class Native
{
    [StructLayout(LayoutKind.Sequential)] internal struct StringValue
    {
        internal IntPtr Data;
        internal uint Length;
        internal StringValue(IntPtr data, uint length) { Data = data; Length = length; }
    }
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_dag_create(ContextHandle ctx, byte[] json, uint length, out IntPtr dag);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern uint ud_dag_nodes(DagHandle dag);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern uint ud_dag_edges(DagHandle dag);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern void ud_dag_fingerprint(DagHandle dag, [Out] byte[] hash);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_dag_serialize(DagHandle dag, out IntPtr json, out uint length);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_dag_bind(DagHandle dag, StringValue values, out IntPtr bound);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern void ud_dag_destroy(IntPtr dag);
}
