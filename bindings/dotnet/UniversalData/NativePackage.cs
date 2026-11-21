using Microsoft.Win32.SafeHandles;
using System.Runtime.InteropServices;

namespace UniversalData;

/// <summary>Immutable native Package definition; Current ABI. No managed planning.</summary>
internal sealed class NativePackage : IDisposable
{
    private readonly ContextHandle context;
    private readonly PackageHandle package;
    private readonly object lifetime = new();
    private bool disposed;

    internal NativePackage(string document, ulong memoryLimit = 268435456)
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

    internal bool IsContinuous
    {
        get
        {
            lock (lifetime)
            {
                ObjectDisposedException.ThrowIf(disposed, this);
                DataOwnership.Check(context, Native.ud_package_is_continuous(package, out var continuous));
                return continuous != 0;
            }
        }
    }

    internal (string Package, string Parameters) BindProject(string projectJson, ulong revision,
        string environmentJson, string parametersJson)
    {
        lock (lifetime)
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            using var strings = new NativeStrings(projectJson, environmentJson, "{}", parametersJson);
            DataOwnership.Check(context, Native.ud_project_create(context, strings[0], out var project));
            try
            {
                var binding = new Native.ProjectBinding {
                    Size = (uint)Marshal.SizeOf<Native.ProjectBinding>(), Abi = ProductVersion.Abi,
                    Revision = revision, Environment = strings[1], ProjectParameters = strings[2],
                    PackageParameters = strings[3]
                };
                DataOwnership.Check(context, Native.ud_project_bind(project, package, ref binding,
                    out var bound, out var parameters, out var parameterLength));
                try
                {
                    var values = DataOwnership.Text(parameters, parameterLength);
                    DataOwnership.Check(context, Native.ud_package_serialize(bound, out var text, out var length));
                    return (DataOwnership.Text(text, length), values);
                }
                finally { Native.ud_package_destroy(bound); }
            }
            finally { Native.ud_project_destroy(project); }
        }
    }

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
    [StructLayout(LayoutKind.Sequential)]
    internal struct ProjectBinding
    {
        internal uint Size, Abi;
        internal ulong Revision;
        internal StringValue Environment, ProjectParameters, PackageParameters;
    }

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_project_create(ContextHandle context, StringValue document, out IntPtr project);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_project_bind(IntPtr project, PackageHandle package, ref ProjectBinding binding, out IntPtr bound, out IntPtr parameters, out uint length);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern void ud_project_destroy(IntPtr project);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_package_create(ContextHandle context, StringValue document, out IntPtr package);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_package_serialize(PackageHandle package, out IntPtr document, out uint length);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_package_requires_journal(PackageHandle package, out int required);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_package_is_continuous(PackageHandle package, out int continuous);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern void ud_package_destroy(IntPtr package);
}
