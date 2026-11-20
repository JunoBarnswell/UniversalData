using System.Runtime.InteropServices;

namespace UniversalData;

public enum CatalogKind { Project = 1, Package, Environment, Deployment, Execution, Event, Audit }
public sealed record CatalogLease(CatalogKind Kind,string Id,string Owner,ulong Generation,ulong UntilMilliseconds);
public sealed record CatalogSnapshot(ulong Documents, string Sha256);

/// <summary>Projection of the Native Catalog, which owns storage and validation.</summary>
public sealed class Catalog : IDisposable
{
    private readonly ContextHandle context;
    private readonly Native.Authorizer grant;
    private readonly object sync = new();
    private IntPtr catalog;
    private int authorizing;
    public Catalog(string directory, string tenant, PackageAuthorizer authorize, bool createDirectory = true,
        ulong memoryLimit = 268435456) : this(directory, tenant, authorize, createDirectory, memoryLimit, null) { }

    public static Catalog OpenPostgreSql(string connection, string tenant, PackageAuthorizer authorize,
        bool initialize = false, ulong memoryLimit = 268435456) =>
        new("ud-catalog", tenant, authorize, initialize, memoryLimit,
            string.IsNullOrWhiteSpace(connection) ? throw new ArgumentException("Missing PostgreSQL connection") : connection);

    private Catalog(string directory, string tenant, PackageAuthorizer authorize, bool createDirectory,
        ulong memoryLimit, string? connection)
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
            using var strings = new NativeStrings(directory, tenant, connection ?? "");
            var options = new Native.CatalogOptions { Size = (uint)Marshal.SizeOf<Native.CatalogOptions>(), Abi = ProductVersion.Abi,
                Directory = strings[0], Tenant = strings[1], Authorize = Marshal.GetFunctionPointerForDelegate(grant), Create = createDirectory ? 1 : 0 };
            int status = connection is null ? Native.ud_catalog_open(context, ref options, out catalog)
                : Native.ud_catalog_open_postgresql(context, ref options, strings[2], out catalog);
            Check(status);
        }
        catch { context.Dispose(); throw; }
    }
    public void SetLeadership(CatalogLease leadership)
    {
        lock (sync)
        {
            Open();
            using var values = new NativeStrings(leadership.Owner);
            Check(Native.ud_catalog_set_leadership(catalog, values[0], leadership.Generation));
        }
    }

    public void PutLeased(CatalogKind kind, string id, ulong revision, string documentJson, CatalogLease lease)
    {
        lock (sync)
        {
            Open();
            using var values = new NativeStrings(id, documentJson, lease.Id, lease.Owner);
            var key = Key(kind, values[0], revision);
            var guard = LeaseValue(lease, values[2], values[3]);
            Check(Native.ud_catalog_put_leased(catalog, ref key, values[1], ref guard));
        }
    }

    public string QueryRuns(string state, bool rootsOnly, ulong dueMilliseconds, uint limit,
        ulong afterDue = 0, ulong afterTimestamp = 0, string afterId = "")
    {
        lock (sync)
        {
            Open();
            using var values = new NativeStrings(state,afterId);
            Check(Native.ud_catalog_query_runs(catalog, values[0], rootsOnly ? 1 : 0,
                dueMilliseconds, limit, afterDue, afterTimestamp, values[1], out var text, out var length));
            return DataOwnership.Text(text, length);
        }
    }
    public CatalogSnapshot CheckSnapshot(Catalog? target = null, bool import = false)
    {
        if (ReferenceEquals(this,target)) throw new ArgumentException("Source and target Catalog must differ");
        if (target is not null)
        {
            lock (target.sync)
            {
                target.Open();
                return ReadSnapshot(target.catalog,import);
            }
        }
        return ReadSnapshot(IntPtr.Zero,import);
    }

    private CatalogSnapshot ReadSnapshot(IntPtr target, bool import)
    {
        lock (sync)
        {
            Open();
            var digest = new byte[32];
            Check(Native.ud_catalog_copy_snapshot(catalog,target,import ? 1 : 0,out var count,digest));
            return new(count,Convert.ToHexString(digest).ToLowerInvariant());
        }
    }
    public void Put(CatalogKind kind, string id, ulong revision, string documentJson)
    {
        lock (sync)
        {
            Open(); using var values = new NativeStrings(id, documentJson);
            var key = Key(kind, values[0], revision);
            Check(Native.ud_catalog_put(catalog, ref key, values[1]));
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
            Check(status); return DataOwnership.Text(text, length);
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
            Check(result);
        }
    }

    public void PublishOccurrence(CatalogLease job, ulong revision, string jobDocument,
        string occurrenceId, ulong occurrenceRevision, string occurrenceDocument,
        string? runId, string? runDocument)
    {
        lock (sync)
        {
            Open();
            using var strings = new NativeStrings(job.Id, job.Owner, jobDocument, occurrenceId,
                occurrenceDocument, runId ?? "", runDocument ?? "");
            var request = new Native.JobOccurrence
            {
                Size = (uint)Marshal.SizeOf<Native.JobOccurrence>(),
                Abi = ProductVersion.Abi,
                Job = Key(CatalogKind.Audit, strings[0], revision),
                Occurrence = Key(CatalogKind.Audit, strings[3], occurrenceRevision),
                Run = Key(CatalogKind.Audit, strings[5], 1),
                JobDocument = strings[2], OccurrenceDocument = strings[4], RunDocument = strings[6]
            };
            var lease = LeaseValue(job, strings[0], strings[1]);
            Check(Native.ud_catalog_publish_occurrence(catalog, ref request, ref lease));
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
            Check(status); return new(kind,id,owner,generation,until);
        }
    }
    public CatalogLease RenewLease(CatalogLease lease,uint durationMilliseconds = 30000)
    {
        lock(sync) {
            Open(); using var strings=new NativeStrings(lease.Id,lease.Owner);var value=LeaseValue(lease,strings[0],strings[1]);
            Check(Native.ud_catalog_lease_renew(catalog,ref value,durationMilliseconds,out var until));
            return lease with { UntilMilliseconds=until };
        }
    }
    public CatalogLease AdoptLease(CatalogLease lease, string owner, uint durationMilliseconds)
    {
        lock (sync)
        {
            Open();
            using var strings = new NativeStrings(lease.Id, lease.Owner, owner);
            var value = LeaseValue(lease, strings[0], strings[1]);
            Check(Native.ud_catalog_lease_adopt(catalog, ref value, strings[2], durationMilliseconds, out var until));
            return lease with { Owner = owner, UntilMilliseconds = until };
        }
    }
    public void ReleaseLease(CatalogLease lease)
    {
        lock(sync) {
            Open(); using var strings=new NativeStrings(lease.Id,lease.Owner);var value=LeaseValue(lease,strings[0],strings[1]);
            Check(Native.ud_catalog_lease_release(catalog,ref value));
        }
    }
    public bool LeaseActive(CatalogKind kind,string id)
    {
        lock(sync) { Open();using var strings=new NativeStrings(id);
            Check(Native.ud_catalog_lease_active(catalog,(int)kind,strings[0],out var active));return active!=0; }
    }
    private static Native.CatalogLease LeaseValue(CatalogLease lease,Native.StringValue id,Native.StringValue owner) =>
        new() { Size=(uint)Marshal.SizeOf<Native.CatalogLease>(),Abi=ProductVersion.Abi,Kind=(int)lease.Kind,
            Id=id,Owner=owner,Generation=lease.Generation };
    public string Query(string nativeQueryJson)
    {
        lock (sync)
        {
            Open(); using var values = new NativeStrings(nativeQueryJson);
            Check(Native.ud_catalog_query(catalog, values[0], out var text, out var length));
            return DataOwnership.Text(text, length);
        }
    }
    public ulong LatestRevision(CatalogKind kind, string id)
    {
        lock (sync) {
            Open(); using var values = new NativeStrings(id);
            int status = Native.ud_catalog_latest_revision(catalog, (int)kind, values[0], out var revision);
            if (status == (int)StatusCode.End) return 0;
            Check(status); return revision;
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
            Check(Native.ud_catalog_bind_package(catalog, ref binding, out var package, out var parameters, out var parameterLength));
            try {
                var boundParameters = DataOwnership.Text(parameters, parameterLength); parameters = IntPtr.Zero;
                Check(Native.ud_package_serialize(package, out var text, out var length));
                return (DataOwnership.Text(text, length), boundParameters);
            } finally { Native.ud_free(parameters); Native.ud_package_destroy(package); }
        }
    }
    public string BindingSnapshot(string deploymentId,ulong deploymentRevision,string packageId) {
        lock (sync) { Open(); using var strings = new NativeStrings(deploymentId,packageId);
            var binding = new Native.CatalogBinding { Size = (uint)Marshal.SizeOf<Native.CatalogBinding>(), Abi = ProductVersion.Abi,
                DeploymentId = strings[0], DeploymentRevision = deploymentRevision, PackageId = strings[1] };
            Check(Native.ud_catalog_binding_snapshot(catalog,ref binding,out var text,out var length)); return DataOwnership.Text(text,length); }
    }
    public void ProtectLegacyParameters(string id,ulong revision,string reference) {
        lock (sync) { Open(); using var strings = new NativeStrings(id,reference); var key = Key(CatalogKind.Audit,strings[0],revision);
            Check(Native.ud_catalog_protect_parameters(catalog,ref key,strings[1])); }
    }
    public void SecureCompact() { lock (sync) { Open(); Check(Native.ud_catalog_secure_compact(catalog)); } }
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
            Check(Native.ud_catalog_record_event(catalog, ref value, executionEvent.Sequence));
        }
    }
    internal void RecordProgress(Execution execution, ulong revision)
    { lock (sync) { Open(); execution.RecordProgress(catalog, revision); } }
    internal void Record(Execution execution, ulong revision)
    { lock (sync) { Open(); execution.RecordCatalog(catalog, revision); } }
    public ulong PruneEvents(ulong beforeMilliseconds, uint limit = 1000)
    { lock (sync) { Open(); Check(Native.ud_catalog_prune_events(catalog, beforeMilliseconds, limit, out var removed)); return removed; } }
    private static Native.CatalogKey Key(CatalogKind kind, Native.StringValue id, ulong revision) =>
        new() { Size = (uint)Marshal.SizeOf<Native.CatalogKey>(), Abi = ProductVersion.Abi, Kind = (int)kind, Id = id, Revision = revision };
    private static void Check(int status)
    {
        if (status == 0) return;
        var code = (StatusCode)status;
        string message = Marshal.PtrToStringUTF8(Native.ud_status_name(status)) ?? "Catalog error";
        throw new UniversalDataException(status,message,new(ErrorSource.Catalog,code,message));
    }

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
    [StructLayout(LayoutKind.Sequential)] internal struct JobOccurrence
    {
        internal uint Size, Abi;
        internal CatalogKey Job, Occurrence, Run;
        internal StringValue JobDocument, OccurrenceDocument, RunDocument;
    }
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ud_catalog_publish_occurrence(IntPtr catalog, ref JobOccurrence request,
        ref CatalogLease lease);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_catalog_open(ContextHandle context, ref CatalogOptions options, out IntPtr catalog);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ud_catalog_open_postgresql(ContextHandle context, ref CatalogOptions options, StringValue connection, out IntPtr catalog);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ud_catalog_set_leadership(IntPtr catalog, StringValue owner, ulong generation);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ud_catalog_lease_adopt(IntPtr catalog, ref CatalogLease lease, StringValue owner,
        uint durationMilliseconds, out ulong until);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ud_catalog_put_leased(IntPtr catalog, ref CatalogKey key, StringValue document, ref CatalogLease lease);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ud_catalog_query_runs(IntPtr catalog, StringValue state, int rootsOnly, ulong dueMilliseconds,
        uint limit, ulong afterDue, ulong afterTimestamp, StringValue afterId, out IntPtr text, out uint length);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ud_catalog_copy_snapshot(IntPtr source, IntPtr target, int import,
        out ulong count, [Out] byte[] digest);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_catalog_put(IntPtr catalog, ref CatalogKey key, StringValue json);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_catalog_get(IntPtr catalog, ref CatalogKey key, out IntPtr json, out uint length);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_catalog_query(IntPtr catalog, StringValue query, out IntPtr json, out uint length);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_catalog_record_event(IntPtr catalog, ref ExecutionEventValue executionEvent, ulong sequence);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_catalog_latest_revision(IntPtr catalog, int kind, StringValue id, out ulong revision);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_catalog_prune_events(IntPtr catalog, ulong beforeMilliseconds, uint limit, out ulong removed);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern void ud_catalog_close(IntPtr catalog);
}
