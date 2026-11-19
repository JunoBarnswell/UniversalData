using Microsoft.Win32.SafeHandles;
using System.Runtime.InteropServices;

namespace UniversalData;

/// <summary>Immutable native Package definition; Current ABI. No managed planning.</summary>
public sealed class Package : IDisposable
{
    private readonly ContextHandle context;
    private readonly PackageHandle package;
    private readonly object lifetime = new();
    private bool disposed;

    public Package(string document, ulong memoryLimit = 268435456)
    {
        context = DataOwnership.Context(memoryLimit);
        try
        {
            var data = Utf8.Bytes(document); var pin = GCHandle.Alloc(data, GCHandleType.Pinned);
            try
            {
                DataOwnership.Check(context, Native.ud_package_create(context, new Native.StringValue(pin.AddrOfPinnedObject(), checked((uint)data.Length)), out var pointer));
                package = new PackageHandle(pointer, context);
            }
            finally { pin.Free(); }
        }
        catch { context.Dispose(); throw; }
    }

    public string ToJson()
    {
        lock (lifetime) { ObjectDisposedException.ThrowIf(disposed, this); DataOwnership.Check(context, Native.ud_package_serialize(package, out var text, out var length)); return DataOwnership.Text(text, length); }
    }

    /// <summary>Returns the Native recovery decision for this immutable package.</summary>
    public bool RequiresJournal
    {
        get
        {
            lock (lifetime)
            {
                ObjectDisposedException.ThrowIf(disposed, this);
                DataOwnership.Check(context, Native.ud_package_requires_journal(package, out var required));
                return required != 0;
            }
        }
    }

    public Execution CreateExecution(string tenant, PackageAuthorizer? authorize, string parametersJson = "{}", string executionId = "", PackageCredentialResolver? resolve = null, ulong memoryLimit = 268435456)
        => new(ToJson(), tenant, authorize, parametersJson, executionId, resolve, memoryLimit: memoryLimit);

    public void Dispose()
    { lock (lifetime) { if (disposed) return; disposed = true; package.Dispose(); context.Dispose(); } }
}

internal sealed class PackageHandle : SafeHandleZeroOrMinusOneIsInvalid
{
    private readonly ContextHandle context;

    internal PackageHandle(IntPtr pointer, ContextHandle owner) : base(true)
    {
        context = owner; bool retained = false; owner.DangerousAddRef(ref retained); SetHandle(pointer);
    }

    protected override bool ReleaseHandle()
    { Native.ud_package_destroy(handle); context.DangerousRelease(); return true; }
}

internal static partial class Native
{
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_package_create(ContextHandle context, StringValue document, out IntPtr package);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_package_serialize(PackageHandle package, out IntPtr document, out uint length);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_package_requires_journal(PackageHandle package, out int required);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern void ud_package_destroy(IntPtr package);
}
