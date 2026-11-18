using Microsoft.Win32.SafeHandles;
using System.Runtime.InteropServices;

namespace UniversalData;

public enum ExecutionState
{ Created, Validating, Ready, Running, Draining, Succeeded, Failed, Cancelled, Skipped, RecoveryRequired, Stopped }

public enum ConnectionAccess
{ Open = 1, Discover, Read, Write, Receipt, Delete = 8, Execute = 9, Network = 10,
  CheckpointOpen = 11, CheckpointRead = 12, CheckpointWrite = 13 }

public delegate bool PackageAuthorizer(string tenant, string resource, ConnectionAccess access);

public delegate string PackageCredentialResolver(string tenant, string resource, string reference);
public enum ExecutionEventKind
{ State = 1, BeforeCheckpoint, AfterCheckpoint, TargetCommitted, TaskExecuted, TaskConfirmed, TaskUncertain, Warning, Error }
public sealed record ExecutionEvent(string PackageId, string ExecutionId, string TaskId, string PipelineId,
    string ComponentId, ExecutionState State, StatusCode Result, ulong TimestampMilliseconds, ExecutionEventKind Kind, ulong Sequence, string ErrorChain = "[]");

public sealed record ExecutionMetrics(ExecutionState State, StatusCode Result, ulong InputRows, ulong OutputRows, ulong RejectedRows, ulong Batches, ulong QueuedBatches, ulong QueueHighWater, ulong MemoryUsed, ulong ExecutedTasks, ulong DurableTasks, ulong RecoveryTasks, ulong ResumedTasks, ulong SpillBytes, ulong PipelineTrees, ulong TreeHighWater, ulong WorkerHighWater, ulong CommittedRows, ulong AcknowledgedRows, ulong ErrorCount, ulong ErrorRows, ulong CommittedErrorRows, ulong AcknowledgedErrorRows);

/// <summary>Thin projection of the only native Package execution lifecycle.</summary>
public sealed class Execution : IDisposable
{
    [ThreadStatic] private static int callbackDepth;
    private readonly ExecutionHandle execution;
    private readonly object controller = new();
    private bool disposed;

    public Execution(string packageJson, string tenant, PackageAuthorizer? authorize, string parametersJson = "{}", string executionId = "", PackageCredentialResolver? resolve = null,
        uint edgeCapacity = 2, uint maxBatchRows = 4096, ulong memoryLimit = 268435456, ulong timeoutMilliseconds = 0, string? stateDirectory = null, bool resume = false, uint maxConcurrency = 1,
        Registry? registry = null, Action<ExecutionEvent>? observe = null, ulong eventSequenceStart = 0,
        uint workerMember = 0, uint workerMembers = 0, uint consumerLanes = 0,
        bool brokerRestore = false)
    {
        bool ownedRegistry = false;
        using (var document = System.Text.Json.JsonDocument.Parse(packageJson))
            if (document.RootElement.TryGetProperty("extensions", out var extensions) && extensions.GetArrayLength() != 0) {
                if (authorize is null) throw new UniversalDataException((int)StatusCode.Auth, "Managed extension loading needs a host scope");
                if (registry is null) { registry = new Registry(memoryLimit); ownedRegistry = true; }
                try { registry.LoadPackageExtensions(packageJson, tenant, authorize); }
                catch { if (ownedRegistry) registry.Dispose(); throw; }
            }
        Native.Authorizer? grant = authorize is null ? null : (_, scope, resource, action) =>
        {
            callbackDepth++; try { return authorize(Text(scope), Text(resource), (ConnectionAccess)action) ? 0 : 13; } catch { return 13; } finally { callbackDepth--; }
        };
        Native.Resolver? resolver = resolve is null ? null : delegate (IntPtr user, IntPtr context, Native.StringValue scope, Native.StringValue resource, Native.StringValue reference, out Native.StringValue output)
        {
            output = default; callbackDepth++;
            try
            {
                var data = Utf8.Bytes(resolve(Text(scope), Text(resource), Text(reference))); var pointer = Native.ud_alloc(context, (UIntPtr)checked((uint)data.Length + 1));
                if (pointer == IntPtr.Zero) return 4; Marshal.Copy(data, 0, pointer, data.Length); Marshal.WriteByte(pointer, data.Length, 0); output = new(pointer, checked((uint)data.Length)); return 0;
            }
            catch { return 13; }
            finally { callbackDepth--; }
        };
        Native.Observer? observer = observe is null ? null : (_, pointer) =>
        {
            var value = Marshal.PtrToStructure<Native.ExecutionEventValue>(pointer);
            callbackDepth++;
            try { observe(new(Text(value.PackageId), Text(value.ExecutionId), Text(value.TaskId), Text(value.PipelineId),
                Text(value.ComponentId), (ExecutionState)value.State, (StatusCode)value.Result, value.Timestamp,
                (ExecutionEventKind)value.Kind, value.Sequence, value.ErrorChain.Length == 0 ? "[]" : Text(value.ErrorChain))); }
            catch (Exception error)
            {
                using var strings = new NativeStrings();
                Native.ud_activation_report_fault(value.Activation, (int)StatusCode.Io,
                    strings.Pin("Managed observer failed: " + error.GetType().FullName));
            }
            finally { callbackDepth--; }
        };
        var pins = new List<GCHandle>();
        Native.StringValue Pin(string value)
        {
            var data = Utf8.Bytes(value); var pin = GCHandle.Alloc(data.Length == 0 ? new byte[1] : data, GCHandleType.Pinned); pins.Add(pin); return new(pin.AddrOfPinnedObject(), checked((uint)data.Length));
        }
        try
        {
            var document = Pin(packageJson);
            var options = new Native.ExecutionOptions
            {
                Size = (uint)Marshal.SizeOf<Native.ExecutionOptions>(),
                Abi = ProductVersion.Abi,
                Context = new Native.ContextOptions { Size = (uint)Marshal.SizeOf<Native.ContextOptions>(), Abi = ProductVersion.Abi, Limit = memoryLimit },
                Capacity = edgeCapacity,
                MaxRows = maxBatchRows,
                Depth = 32,
                Timeout = timeoutMilliseconds,
                Parameters = Pin(parametersJson),
                Tenant = Pin(tenant),
                ExecutionId = Pin(executionId),
                StateDirectory = Pin(stateDirectory ?? ""),
                Resume = resume ? 1 : 0,
                MaxConcurrency = maxConcurrency,
                EventSequenceStart = eventSequenceStart,
                WorkerMember = workerMember,
                WorkerMembers = workerMembers,
                ConsumerLanes = consumerLanes,
                BrokerRestore = brokerRestore ? 1 : 0,
                Authorize = grant is null ? IntPtr.Zero : Marshal.GetFunctionPointerForDelegate(grant),
                Resolve = resolver is null ? IntPtr.Zero : Marshal.GetFunctionPointerForDelegate(resolver)
                , Observe = observer is null ? IntPtr.Zero : Marshal.GetFunctionPointerForDelegate(observer)
            };
            IntPtr pointer;
            Check(registry is null ? Native.ud_execution_create_json(document, ref options, out pointer)
                                   : registry.CreateExecution(document, ref options, out pointer));
            execution = new ExecutionHandle(pointer, grant, resolver, observer, registry, ownedRegistry);
        }
        catch { if (ownedRegistry) registry?.Dispose(); throw; }
        finally { foreach (var pin in pins) pin.Free(); GC.KeepAlive(grant); GC.KeepAlive(resolver); GC.KeepAlive(observer); GC.KeepAlive(registry); }
    }

    private static string Text(Native.StringValue text) => Native.Text(text);

    private static void Check(int status)
    { if (status != 0) throw new UniversalDataException(status, Marshal.PtrToStringUTF8(Native.ud_status_name(status)) ?? "Native error"); }

    private void Open() => ObjectDisposedException.ThrowIf(disposed, this);

    internal static IDisposable EnterCallback()
    { callbackDepth++; return new CallbackLease(); }
    private sealed class CallbackLease : IDisposable
    { public void Dispose() { callbackDepth--; } }

    private static void Control()
    { if (callbackDepth != 0) throw new InvalidOperationException("Execution callbacks cannot start, wait or dispose an execution"); }

    public string Id
    {
        get { lock (controller) { Open(); Check(Native.ud_execution_id(execution, out var value)); return Text(value); } }
    }

    public void Execute()
    { Control(); lock (controller) { Open(); Check(Native.ud_execution_execute(execution)); } }

    public void Start()
    { Control(); lock (controller) { Open(); Check(Native.ud_execution_start(execution)); } }

    public bool Wait(uint timeoutMilliseconds)
    { Control(); lock (controller) { Open(); int s = Native.ud_execution_wait(execution, timeoutMilliseconds); if (s == 11) return false; Check(s); return true; } }

    public void Stop() { Open(); Native.ud_execution_stop(execution); }

    public void Cancel()
    { try { if (!execution.IsClosed) Native.ud_execution_cancel(execution); } catch (ObjectDisposedException) { } }

    public ExecutionMetrics Metrics
    {
        get { Open(); var m = new Native.ExecutionMetricsValue { Size = (uint)Marshal.SizeOf<Native.ExecutionMetricsValue>(), Abi = ProductVersion.Abi }; Check(Native.ud_execution_get_metrics(execution, ref m)); return new((ExecutionState)m.State, (StatusCode)m.Result, m.Input, m.Output, m.Rejected, m.Batches, m.Queued, m.HighWater, m.Memory, m.Executed, m.Durable, m.Recovery, m.Resumed, m.Spill, m.Trees, m.TreeHighWater, m.WorkerHighWater, m.Committed, m.Acknowledged, m.Errors, m.ErrorRows, m.CommittedErrors, m.AcknowledgedErrors); }
    }

    private static void WorkbenchContract() { if((Registry.Features & RegistryFeatures.ReadOnlyPreview)==0)throw new UniversalDataException((int)StatusCode.Incompatible,"Native runtime does not provide the designer workbench contract"); }

    public string ConnectorCatalog(string pathsJson)
    { Control(); lock(controller) { Open();WorkbenchContract(); using var strings=new NativeStrings(pathsJson);
        Check(Native.ud_execution_connector_catalog(execution,strings[0],out var text,out var length));return DataOwnership.Text(text,length); } }
    public string ReidentifyPackage(string id)
    {Control();lock(controller){Open();WorkbenchContract();using var strings=new NativeStrings(id);Check(Native.ud_execution_reidentify_package(execution,strings[0],out var text,out var length));return DataOwnership.Text(text,length);}}
    public string Metadata(string connection, int kind, string parent = "", ulong offset = 0)
    { Control(); lock(controller) { Open();WorkbenchContract(); using var strings=new NativeStrings(connection,parent);
        Check(Native.ud_execution_metadata(execution,strings[0],kind,strings[1],offset,out var text,out var length));return DataOwnership.Text(text,length); } }
    public (ulong Capabilities, ulong ElapsedMs) TestMessageConnection(string connection)
    {
        Control();
        lock (controller)
        {
            Open();
            using var strings = new NativeStrings(connection);
            Check(Native.ud_execution_test_message_connection(execution, strings[0],
                out var capabilities, out var elapsedMs));
            return (capabilities, elapsedMs);
        }
    }
    public string InspectFile(string connection, string configuration)
    { Control(); lock(controller) { Open();WorkbenchContract(); using var strings=new NativeStrings(connection,configuration);
        Check(Native.ud_execution_inspect_file(execution,strings[0],strings[1],out var text,out var length)); return DataOwnership.Text(text,length); } }
    public string DiscoverSchema(string connection, string table)
    {
        Control(); lock (controller)
        {
            Open(); using var inputs = new NativeStrings(connection, table);
            Check(Native.ud_execution_discover_schema(execution, inputs[0], inputs[1], out var text, out var length));
            return DataOwnership.Text(text, length);
        }
    }

    public string CompileExpression(string schemaJson,string source)
    { Control();lock(controller){Open();using var strings=new NativeStrings(schemaJson,source);
        Check(Native.ud_execution_compile_expression(execution,strings[0],strings[1],out var text,out var length));return DataOwnership.Text(text,length);} }

    public string ComponentCatalog()
    {
        Control(); lock (controller)
        {
            Open(); Check(Native.ud_execution_component_catalog(execution, out var text, out var length));
            return DataOwnership.Text(text, length);
        }
    }

    public string ComponentPorts(string pipelineId, string componentId, string kind, string configuration)
    { Control(); lock (controller) { Open(); using var strings = new NativeStrings(pipelineId,componentId,kind,configuration);
        Check(Native.ud_execution_component_ports(execution, strings[0], strings[1], strings[2], strings[3], out var text, out var length));
        return DataOwnership.Text(text,length); } }
    public string Preview(string pipelineJson, string componentId, uint sourceRows = 1000, uint sampleRows = 32)
    {
        Control(); lock (controller) {
            Open(); WorkbenchContract();
            using var strings = new NativeStrings(pipelineJson, componentId);
            Check(Native.ud_execution_preview(execution, strings[0], strings[1], sourceRows, sampleRows, out var text, out var length));
            return DataOwnership.Text(text, length);
        }
    }
    public string Explain() => Inspect(false);

    public void ConfigureDebug(string configuration)
    {
        Control(); lock (controller) { Open(); using var strings = new NativeStrings(configuration);
            Check(Native.ud_execution_debug_configure(execution, strings[0])); }
    }
    public void DebugControl(string command)
    {
        Control(); Open(); using var strings = new NativeStrings(command);
        Check(Native.ud_execution_debug_control(execution, strings[0]));
    }
    public string Progress()
    {
        Control();Open();WorkbenchContract();bool retained=false;
        try {execution.DangerousAddRef(ref retained);Check(Native.ud_execution_progress(execution,out var text,out var length));return DataOwnership.Text(text,length);}
        finally {if(retained)execution.DangerousRelease();}
    }
    public string DebugSnapshot()
    {
        Control(); Open(); bool retained = false;
        try { execution.DangerousAddRef(ref retained);
            Check(Native.ud_execution_debug_snapshot(execution, out var text, out var length));
            return DataOwnership.Text(text, length);
        } finally { if (retained) execution.DangerousRelease(); }
    }

    public string Diagnostics() => Inspect(true);
    internal void RecordProgress(IntPtr catalog, ulong revision)
    { Control(); lock (controller) { Open(); Check(Native.ud_catalog_record_progress(catalog, execution, revision)); } }

    internal void RecordCatalog(IntPtr catalog, ulong revision)
    { Control(); lock (controller) { Open(); Check(Native.ud_catalog_record_execution(catalog, execution, revision)); } }

    private string Inspect(bool diagnostics)
    {
        Control(); lock (controller)
        {
            Open(); IntPtr text; uint length;
            Check(diagnostics ? Native.ud_execution_diagnostics(execution, out text, out length)
                              : Native.ud_execution_explain(execution, out text, out length));
            try { return Marshal.PtrToStringUTF8(text, checked((int)length)) ?? ""; }
            finally { Native.ud_free(text); }
        }
    }

    public void Dispose()
    { Control(); Cancel(); lock (controller) { if (disposed) return; disposed = true; execution.Dispose(); } }
}

internal sealed class ExecutionHandle : SafeHandleZeroOrMinusOneIsInvalid
{
    private readonly Native.Authorizer? authorizer; private readonly Native.Resolver? resolver;
    private readonly Native.Observer? observer; private readonly Registry? registry; private readonly bool ownedRegistry;

    internal ExecutionHandle(IntPtr pointer, Native.Authorizer? grant, Native.Resolver? resolve, Native.Observer? observation, Registry? registrations, bool ownRegistrations = false) : base(true)
    {
        authorizer = grant; resolver = resolve; observer = observation; registry = registrations; ownedRegistry = ownRegistrations; SetHandle(pointer);
    }

    protected override bool ReleaseHandle()
    {
        Native.ud_execution_cancel_raw(handle); int result;
        do { result = Native.ud_execution_destroy(handle); if (result == 11) Native.ud_execution_wait_raw(handle, 100); } while (result == 11);
        GC.KeepAlive(authorizer); GC.KeepAlive(resolver); GC.KeepAlive(observer);
        if (result == 0 && ownedRegistry) registry?.Dispose();
        GC.KeepAlive(registry); return result == 0;
    }
}

internal static partial class Native
{
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_execution_component_ports(ExecutionHandle execution, StringValue pipelineId, StringValue componentId, StringValue kind, StringValue configuration, out IntPtr text, out uint length);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_execution_debug_configure(ExecutionHandle execution, StringValue configuration);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_execution_debug_control(ExecutionHandle execution, StringValue command);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_execution_metadata(ExecutionHandle execution, StringValue connection, int kind, StringValue parent, ulong offset, out IntPtr text, out uint length);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_execution_test_message_connection(ExecutionHandle execution, StringValue connection, out ulong capabilities, out ulong elapsedMs);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_execution_connector_catalog(ExecutionHandle execution, StringValue paths, out IntPtr text, out uint length);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_execution_reidentify_package(ExecutionHandle execution, StringValue id, out IntPtr text, out uint length);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_execution_inspect_file(ExecutionHandle execution, StringValue connection, StringValue config, out IntPtr text, out uint length);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_execution_preview(ExecutionHandle execution, StringValue pipeline, StringValue component, uint sourceRows, uint sampleRows, out IntPtr text, out uint length);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_execution_progress(ExecutionHandle execution, out IntPtr text, out uint length);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_execution_debug_snapshot(ExecutionHandle execution, out IntPtr text, out uint length);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_catalog_record_execution(IntPtr catalog, ExecutionHandle execution, ulong revision);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_catalog_record_progress(IntPtr catalog, ExecutionHandle execution, ulong revision);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] internal delegate void Observer(IntPtr user, IntPtr executionEvent);
    [StructLayout(LayoutKind.Sequential)] internal struct ExecutionEventValue
    { internal uint Size, Abi; internal StringValue PackageId, ExecutionId, TaskId, PipelineId, ComponentId;
      internal int State, Result; internal ulong Timestamp; internal int Kind; internal ulong Sequence; internal IntPtr Activation; internal StringValue ErrorChain; }
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_activation_report_fault(IntPtr activation, int status, StringValue message);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] internal delegate int Authorizer(IntPtr user, StringValue tenant, StringValue resource, int action);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] internal delegate int Resolver(IntPtr user, IntPtr context, StringValue tenant, StringValue resource, StringValue reference, out StringValue value);

    [StructLayout(LayoutKind.Sequential)]
    internal struct ExecutionOptions
    {
        internal uint Size, Abi; internal ContextOptions Context; internal uint Capacity, MaxRows, Depth; internal ulong Timeout;
        internal StringValue Parameters, Tenant, ExecutionId; internal IntPtr Registry, Authorize, AuthorizeUser, Resolve, ResolveUser, Observe, ObserveUser; internal StringValue StateDirectory; internal int Resume; internal uint MaxConcurrency; internal ulong EventSequenceStart;
        internal uint WorkerMember, WorkerMembers, ConsumerLanes;
        internal int BrokerRestore;
    }

    [StructLayout(LayoutKind.Sequential)]
    internal struct ExecutionMetricsValue
    { internal uint Size, Abi; internal int State, Result; internal ulong Input, Output, Rejected, Batches, Queued, HighWater, Memory, Executed, Durable, Recovery, Resumed, Spill, Trees, TreeHighWater, WorkerHighWater, Committed, Acknowledged, Errors, ErrorRows, CommittedErrors, AcknowledgedErrors; }

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_execution_create_json(StringValue json, ref ExecutionOptions options, out IntPtr execution);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_execution_id(ExecutionHandle execution, out StringValue id);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_execution_execute(ExecutionHandle execution);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_execution_start(ExecutionHandle execution);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_execution_wait(ExecutionHandle execution, uint timeout);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern void ud_execution_cancel(ExecutionHandle execution);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern void ud_execution_stop(ExecutionHandle execution);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_execution_get_metrics(ExecutionHandle execution, ref ExecutionMetricsValue metrics);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ud_execution_cancel")] internal static extern void ud_execution_cancel_raw(IntPtr execution);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ud_execution_wait")] internal static extern int ud_execution_wait_raw(IntPtr execution, uint timeout);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_execution_compile_expression(ExecutionHandle execution,StringValue schema,StringValue source,out IntPtr text,out uint length);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_execution_component_catalog(ExecutionHandle execution, out IntPtr text, out uint length);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_execution_explain(ExecutionHandle execution, out IntPtr text, out uint length);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_execution_diagnostics(ExecutionHandle execution, out IntPtr text, out uint length);

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_execution_destroy(IntPtr execution);
}
