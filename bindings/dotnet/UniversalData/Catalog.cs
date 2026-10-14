using System.Runtime.InteropServices;

namespace UniversalData;

public enum CatalogKind { Project = 1, Package, Environment, Deployment, Execution, Event, Audit }

/// <summary>Projection of the Native SQLite Catalog; Native owns storage and validation.</summary>
public sealed class Catalog : IDisposable
{
    private readonly ContextHandle context;
    private readonly Native.Authorizer grant;
    private readonly object sync = new();
    private IntPtr catalog;
    public Catalog(string directory, string tenant, PackageAuthorizer authorize, bool createDirectory = true,
        ulong memoryLimit = 268435456)
    {
        ArgumentNullException.ThrowIfNull(authorize);
        context = DataOwnership.Context(memoryLimit);
        grant = (_, scope, resource, action) =>
        { try { return authorize(Native.Text(scope), Native.Text(resource), (ConnectionAccess)action) ? 0 : 13; } catch { return 13; } };
        try
        {
            using var strings = new NativeStrings(directory, tenant);
            var options = new Native.CatalogOptions { Size = (uint)Marshal.SizeOf<Native.CatalogOptions>(), Abi = ProductVersion.Abi,
                Directory = strings[0], Tenant = strings[1], Authorize = Marshal.GetFunctionPointerForDelegate(grant), Create = createDirectory ? 1 : 0 };
            DataOwnership.Check(context, Native.ud_catalog_open(context, ref options, out catalog));
        }
        catch { context.Dispose(); throw; }
    }
    public void Put(CatalogKind kind, string id, ulong revision, string documentJson)
    {
        lock (sync)
        {
            Open(); using var values = new NativeStrings(id, documentJson);
            var key = Key(kind, values[0], revision);
            DataOwnership.Check(context, Native.ud_catalog_put(catalog, ref key, values[1]));
        }
    }
    public string? Get(CatalogKind kind, string id, ulong revision)
    {
        lock (sync)
        {
            Open(); using var values = new NativeStrings(id);
            var key = Key(kind, values[0], revision);
            int status = Native.ud_catalog_get(catalog, ref key, out var text, out var length);
            if (status == (int)StatusCode.End) return null;
            DataOwnership.Check(context, status); return DataOwnership.Text(text, length);
        }
    }
    public string Query(string nativeQueryJson)
    {
        lock (sync)
        {
            Open(); using var values = new NativeStrings(nativeQueryJson);
            DataOwnership.Check(context, Native.ud_catalog_query(catalog, values[0], out var text, out var length));
            return DataOwnership.Text(text, length);
        }
    }
    public ulong LatestRevision(CatalogKind kind, string id)
    {
        lock (sync) {
            Open(); using var values = new NativeStrings(id);
            int status = Native.ud_catalog_latest_revision(catalog, (int)kind, values[0], out var revision);
            if (status == (int)StatusCode.End) return 0;
            DataOwnership.Check(context, status); return revision;
        }
    }
    public (string Package, string Parameters) BindPackage(string deploymentId, ulong deploymentRevision,
        string packageId, string projectParameters = "{}", string packageParameters = "{}")
    {
        lock (sync) {
            Open(); using var values = new NativeStrings(deploymentId, packageId, projectParameters, packageParameters);
            var binding = new Native.CatalogBinding { Size = (uint)Marshal.SizeOf<Native.CatalogBinding>(), Abi = ProductVersion.Abi,
                DeploymentId = values[0], DeploymentRevision = deploymentRevision, PackageId = values[1],
                ProjectParameters = values[2], PackageParameters = values[3] };
            DataOwnership.Check(context, Native.ud_catalog_bind_package(catalog, ref binding, out var package, out var parameters, out var parameterLength));
            try {
                var boundParameters = DataOwnership.Text(parameters, parameterLength); parameters = IntPtr.Zero;
                DataOwnership.Check(context, Native.ud_package_serialize(package, out var text, out var length));
                return (DataOwnership.Text(text, length), boundParameters);
            } finally { Native.ud_free(parameters); Native.ud_package_destroy(package); }
        }
    }
    public void Record(ExecutionEvent executionEvent)
    {
        lock (sync)
        {
            Open(); using var strings = new NativeStrings(executionEvent.PackageId, executionEvent.ExecutionId,
                executionEvent.TaskId, executionEvent.PipelineId, executionEvent.ComponentId);
            var value = new Native.ExecutionEventValue { Size = (uint)Marshal.SizeOf<Native.ExecutionEventValue>(), Abi = ProductVersion.Abi,
                PackageId = strings[0], ExecutionId = strings[1], TaskId = strings[2], PipelineId = strings[3], ComponentId = strings[4],
                State = (int)executionEvent.State, Result = (int)executionEvent.Result, Timestamp = executionEvent.TimestampMilliseconds,
                Kind = (int)executionEvent.Kind, Sequence = executionEvent.Sequence };
            DataOwnership.Check(context, Native.ud_catalog_record_event(catalog, ref value, executionEvent.Sequence));
        }
    }
    public void Record(Execution execution, ulong revision)
    { lock (sync) { Open(); execution.RecordCatalog(catalog, revision); } }
    public ulong PruneEvents(ulong beforeMilliseconds, uint limit = 1000)
    { lock (sync) { Open(); DataOwnership.Check(context, Native.ud_catalog_prune_events(catalog, beforeMilliseconds, limit, out var removed)); return removed; } }
    private static Native.CatalogKey Key(CatalogKind kind, Native.StringValue id, ulong revision) =>
        new() { Size = (uint)Marshal.SizeOf<Native.CatalogKey>(), Abi = ProductVersion.Abi, Kind = (int)kind, Id = id, Revision = revision };
    private void Open() => ObjectDisposedException.ThrowIf(catalog == IntPtr.Zero, this);
    public void Dispose()
    {
        lock (sync) { if (catalog == IntPtr.Zero) return; Native.ud_catalog_close(catalog); catalog = IntPtr.Zero; context.Dispose(); }
        GC.KeepAlive(grant); GC.SuppressFinalize(this);
    }
    ~Catalog() { Dispose(); }
}

internal static partial class Native
{
    [StructLayout(LayoutKind.Sequential)] internal struct CatalogBinding
    { internal uint Size, Abi; internal StringValue DeploymentId; internal ulong DeploymentRevision;
      internal StringValue PackageId, ProjectParameters, PackageParameters; }
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_catalog_bind_package(IntPtr catalog, ref CatalogBinding binding, out IntPtr package, out IntPtr parameters, out uint length);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_package_serialize(IntPtr package, out IntPtr text, out uint length);
    [StructLayout(LayoutKind.Sequential)] internal struct CatalogOptions
    { internal uint Size, Abi; internal StringValue Directory, Tenant; internal IntPtr Authorize, User; internal int Create; }
    [StructLayout(LayoutKind.Sequential)] internal struct CatalogKey
    { internal uint Size, Abi; internal int Kind; internal StringValue Id; internal ulong Revision; }
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_catalog_open(ContextHandle context, ref CatalogOptions options, out IntPtr catalog);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_catalog_put(IntPtr catalog, ref CatalogKey key, StringValue json);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_catalog_get(IntPtr catalog, ref CatalogKey key, out IntPtr json, out uint length);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_catalog_query(IntPtr catalog, StringValue query, out IntPtr json, out uint length);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_catalog_record_event(IntPtr catalog, ref ExecutionEventValue executionEvent, ulong sequence);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_catalog_latest_revision(IntPtr catalog, int kind, StringValue id, out ulong revision);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_catalog_prune_events(IntPtr catalog, ulong beforeMilliseconds, uint limit, out ulong removed);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern void ud_catalog_close(IntPtr catalog);
}
