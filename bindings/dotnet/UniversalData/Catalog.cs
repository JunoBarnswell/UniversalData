using System.Runtime.InteropServices;

namespace UniversalData;

public enum CatalogKind { Project = 1, Package, Environment, Deployment, Execution, Event, Audit }
public sealed record CatalogLease(CatalogKind Kind,string Id,string Owner,ulong Generation,ulong UntilMilliseconds);

/// <summary>Projection of the Native SQLite Catalog; Native owns storage and validation.</summary>
public sealed class Catalog : IDisposable
{
    private readonly ContextHandle context;
    private readonly Native.Authorizer grant;
    private readonly object sync = new();
    private IntPtr catalog;
    private int authorizing;
    public Catalog(string directory, string tenant, PackageAuthorizer authorize, bool createDirectory = true,
        ulong memoryLimit = 268435456)
    {
        ArgumentNullException.ThrowIfNull(authorize);
        context = DataOwnership.Context(memoryLimit);
        grant = (_, scope, resource, action) =>
        {
            using var callback=Execution.EnterCallback();Interlocked.Increment(ref authorizing);
            try { return authorize(Native.Text(scope), Native.Text(resource), (ConnectionAccess)action) ? 0 : 13; }
            catch { return 13; }
            finally { Interlocked.Decrement(ref authorizing); }
        };
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
    public void CompareExchange(CatalogKind kind,string id,ulong expectedRevision,string documentJson,CatalogLease? lease = null)
    {
        lock(sync) {
            Open(); using var strings=new NativeStrings(id,documentJson,lease?.Owner ?? "");
            var key=Key(kind,strings[0],checked(expectedRevision+1));
            int result;
            if (lease is null) result=Native.ud_catalog_compare_exchange_unleased(catalog,ref key,strings[1],expectedRevision,IntPtr.Zero);
            else {
                if (lease.Kind!=kind || lease.Id!=id) throw new ArgumentException("Lease differs from the document");
                var guard=LeaseValue(lease,strings[0],strings[2]);
                result=Native.ud_catalog_compare_exchange(catalog,ref key,strings[1],expectedRevision,ref guard);
            }
            DataOwnership.Check(context,result);
        }
    }
    public CatalogLease? TryAcquireLease(CatalogKind kind,string id,string owner,uint durationMilliseconds = 30000,
        string pool = "",uint capacity = 0)
    {
        lock(sync) {
            Open(); using var strings=new NativeStrings(id,owner,pool);
            var request=new Native.CatalogLeaseRequest { Size=(uint)Marshal.SizeOf<Native.CatalogLeaseRequest>(),Abi=ProductVersion.Abi,
                Kind=(int)kind,Id=strings[0],Owner=strings[1],Pool=strings[2],Duration=durationMilliseconds,Capacity=capacity };
            int status=Native.ud_catalog_lease_acquire(catalog,ref request,out var generation,out var until);
            if (status==(int)StatusCode.Busy) return null;
            DataOwnership.Check(context,status); return new(kind,id,owner,generation,until);
        }
    }
    public CatalogLease RenewLease(CatalogLease lease,uint durationMilliseconds = 30000)
    {
        lock(sync) {
            Open(); using var strings=new NativeStrings(lease.Id,lease.Owner);var value=LeaseValue(lease,strings[0],strings[1]);
            DataOwnership.Check(context,Native.ud_catalog_lease_renew(catalog,ref value,durationMilliseconds,out var until));
            return lease with { UntilMilliseconds=until };
        }
    }
    public void ReleaseLease(CatalogLease lease)
    {
        lock(sync) {
            Open(); using var strings=new NativeStrings(lease.Id,lease.Owner);var value=LeaseValue(lease,strings[0],strings[1]);
            DataOwnership.Check(context,Native.ud_catalog_lease_release(catalog,ref value));
        }
    }
    public bool LeaseActive(CatalogKind kind,string id)
    {
        lock(sync) { Open();using var strings=new NativeStrings(id);
            DataOwnership.Check(context,Native.ud_catalog_lease_active(catalog,(int)kind,strings[0],out var active));return active!=0; }
    }
    private static Native.CatalogLease LeaseValue(CatalogLease lease,Native.StringValue id,Native.StringValue owner) =>
        new() { Size=(uint)Marshal.SizeOf<Native.CatalogLease>(),Abi=ProductVersion.Abi,Kind=(int)lease.Kind,
            Id=id,Owner=owner,Generation=lease.Generation };
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
    public string BindingSnapshot(string deploymentId,ulong deploymentRevision,string packageId) {
        lock (sync) { Open(); using var strings = new NativeStrings(deploymentId,packageId);
            var binding = new Native.CatalogBinding { Size = (uint)Marshal.SizeOf<Native.CatalogBinding>(), Abi = ProductVersion.Abi,
                DeploymentId = strings[0], DeploymentRevision = deploymentRevision, PackageId = strings[1] };
            DataOwnership.Check(context,Native.ud_catalog_binding_snapshot(catalog,ref binding,out var text,out var length)); return DataOwnership.Text(text,length); }
    }
    public void ProtectLegacyParameters(string id,ulong revision,string reference) {
        lock (sync) { Open(); using var strings = new NativeStrings(id,reference); var key = Key(CatalogKind.Audit,strings[0],revision);
            DataOwnership.Check(context,Native.ud_catalog_protect_parameters(catalog,ref key,strings[1])); }
    }
    public void SecureCompact() { lock (sync) { Open(); DataOwnership.Check(context,Native.ud_catalog_secure_compact(catalog)); } }
    public void Record(ExecutionEvent executionEvent)
    {
        lock (sync)
        {
            Open(); using var strings = new NativeStrings(executionEvent.PackageId, executionEvent.ExecutionId,
                executionEvent.TaskId, executionEvent.PipelineId, executionEvent.ComponentId, executionEvent.ErrorChain);
            var value = new Native.ExecutionEventValue { Size = (uint)Marshal.SizeOf<Native.ExecutionEventValue>(), Abi = ProductVersion.Abi,
                PackageId = strings[0], ExecutionId = strings[1], TaskId = strings[2], PipelineId = strings[3], ComponentId = strings[4],
                State = (int)executionEvent.State, Result = (int)executionEvent.Result, Timestamp = executionEvent.TimestampMilliseconds,
                Kind = (int)executionEvent.Kind, Sequence = executionEvent.Sequence, ErrorChain = strings[5] };
            DataOwnership.Check(context, Native.ud_catalog_record_event(catalog, ref value, executionEvent.Sequence));
        }
    }
    public void Record(Execution execution, ulong revision)
    { lock (sync) { Open(); execution.RecordCatalog(catalog, revision); } }
    public ulong PruneEvents(ulong beforeMilliseconds, uint limit = 1000)
    { lock (sync) { Open(); DataOwnership.Check(context, Native.ud_catalog_prune_events(catalog, beforeMilliseconds, limit, out var removed)); return removed; } }
    private static Native.CatalogKey Key(CatalogKind kind, Native.StringValue id, ulong revision) =>
        new() { Size = (uint)Marshal.SizeOf<Native.CatalogKey>(), Abi = ProductVersion.Abi, Kind = (int)kind, Id = id, Revision = revision };
    private void Open() {
        if (Volatile.Read(ref authorizing)!=0) throw new InvalidOperationException("Catalog authorization cannot reenter its handle");
        ObjectDisposedException.ThrowIf(catalog==IntPtr.Zero,this);
    }
    public void Dispose()
    {
        lock (sync) {
            if (Volatile.Read(ref authorizing)!=0) throw new InvalidOperationException("Catalog authorization cannot dispose its handle");
            if (catalog == IntPtr.Zero) return; Native.ud_catalog_close(catalog); catalog = IntPtr.Zero; context.Dispose();
        }
        GC.KeepAlive(grant); GC.SuppressFinalize(this);
    }
    ~Catalog() { Dispose(); }
}

internal static partial class Native
{
    [StructLayout(LayoutKind.Sequential)] internal struct CatalogLeaseRequest {
        internal uint Size,Abi;internal int Kind;internal StringValue Id,Owner,Pool;internal uint Duration,Capacity;
    }
    [StructLayout(LayoutKind.Sequential)] internal struct CatalogLease {
        internal uint Size,Abi;internal int Kind;internal StringValue Id,Owner;internal ulong Generation;
    }
    [DllImport(Library,CallingConvention=CallingConvention.Cdecl)] internal static extern int ud_catalog_lease_acquire(IntPtr catalog,ref CatalogLeaseRequest request,out ulong generation,out ulong until);
    [DllImport(Library,CallingConvention=CallingConvention.Cdecl)] internal static extern int ud_catalog_lease_renew(IntPtr catalog,ref CatalogLease lease,uint duration,out ulong until);
    [DllImport(Library,CallingConvention=CallingConvention.Cdecl)] internal static extern int ud_catalog_lease_release(IntPtr catalog,ref CatalogLease lease);
    [DllImport(Library,CallingConvention=CallingConvention.Cdecl)] internal static extern int ud_catalog_lease_active(IntPtr catalog,int kind,StringValue id,out int active);
    [DllImport(Library,CallingConvention=CallingConvention.Cdecl)] internal static extern int ud_catalog_compare_exchange(IntPtr catalog,ref CatalogKey key,StringValue json,ulong expected,ref CatalogLease lease);
    [DllImport(Library,EntryPoint="ud_catalog_compare_exchange",CallingConvention=CallingConvention.Cdecl)] internal static extern int ud_catalog_compare_exchange_unleased(IntPtr catalog,ref CatalogKey key,StringValue json,ulong expected,IntPtr lease);
    [StructLayout(LayoutKind.Sequential)] internal struct CatalogBinding
    { internal uint Size, Abi; internal StringValue DeploymentId; internal ulong DeploymentRevision;
      internal StringValue PackageId, ProjectParameters, PackageParameters; }
    [DllImport(Library,CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_catalog_binding_snapshot(IntPtr catalog,ref CatalogBinding binding,out IntPtr text,out uint length);
    [DllImport(Library,CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_catalog_protect_parameters(IntPtr catalog,ref CatalogKey key,StringValue reference);
    [DllImport(Library,CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_catalog_secure_compact(IntPtr catalog);
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
