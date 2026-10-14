using System.Runtime.InteropServices;

namespace UniversalData;

public interface IManagedTask : IDisposable
{
    void Execute(ManagedTaskContext context, string configurationJson);
    void Finish() { }
    void Cancel() { }
}

public sealed record ManagedTaskDefinition(string Id, string Name, string ConfigurationSchema,
    Func<IManagedTask> Factory, bool ReplaySafe = false, bool ThreadSafe = false,
    string? Version = null, ComponentEffects Effects = ComponentEffects.None);

/// <summary>Borrowed native activation, valid only during the current callback.</summary>
public sealed class ManagedTaskContext
{
    private readonly Native.CallbackContext value;
    private bool active = true;
    internal ManagedTaskContext(IntPtr pointer) => value = Marshal.PtrToStructure<Native.CallbackContext>(pointer);
    internal void End() => active = false;
    public string TaskId { get { Open(); return Native.Text(value.TaskId); } }
    public string PipelineId { get { Open(); return Native.Text(value.PipelineId); } }
    public string ComponentId { get { Open(); return Native.Text(value.ComponentId); } }
    public void CheckCancellation()
    {
        Open(); int status = Native.ud_activation_check(value.Activation);
        if (status != 0) throw new UniversalDataException(status, "Native activation is cancelled or expired");
    }
    private void Open() => ObjectDisposedException.ThrowIf(!active, this);
}

/// <summary>Registrations and callback owners belong to the sole Native Registry.</summary>
public sealed partial class Registry : IDisposable
{
    private readonly ContextHandle context;
    private readonly object sync = new();
    private IntPtr registry;
    internal int CreateExecution(Native.StringValue document, ref Native.ExecutionOptions options, out IntPtr pointer)
    { lock (sync) { Open(); options.Registry = registry; return Native.ud_execution_create_json(document, ref options, out pointer); } }
    public Registry(ulong memoryLimit = 268435456)
    {
        context = DataOwnership.Context(memoryLimit);
        try { DataOwnership.Check(context, Native.ud_registry_create(context, out registry)); }
        catch { context.Dispose(); throw; }
    }
    public string Describe()
    {
        lock (sync) { Open(); DataOwnership.Check(context, Native.ud_registry_describe(registry, out var text, out var length)); return DataOwnership.Text(text, length); }
    }
    public void RegisterTask(ManagedTaskDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(definition.Factory);
        lock (sync)
        {
            Open(); var registration = new TaskRegistration(definition);
            using var pins = new NativeStrings();
            var descriptor = new Native.ComponentDescriptor
            {
                Size = (uint)Marshal.SizeOf<Native.ComponentDescriptor>(), Abi = ProductVersion.Abi,
                Id = pins.Pin(definition.Id), Name = pins.Pin(definition.Name), Version = pins.Pin(definition.Version ?? ProductVersion.Sdk),
                Configuration = pins.Pin(definition.ConfigurationSchema), Category = 1, SynchronousInput = -1, ErrorOutput = -1,
                Capabilities = (definition.ReplaySafe ? 0x8000000000000000UL : 0) | (definition.ThreadSafe ? 0x4000000000000000UL : 0),
                User = registration.Pointer, Effects = (uint)definition.Effects,
                Prepare = Marshal.GetFunctionPointerForDelegate(registration.Prepare),
                Invoke = Marshal.GetFunctionPointerForDelegate(registration.Invoke),
                Finish = Marshal.GetFunctionPointerForDelegate(registration.Finish),
                Cancel = Marshal.GetFunctionPointerForDelegate(registration.Cancel),
                Dispose = Marshal.GetFunctionPointerForDelegate(registration.Dispose)
            };
            try
            {
                DataOwnership.Check(context, Native.ud_registry_register_owned(registry, ref descriptor, 1,
                    registration.Pointer, registration.Retain, registration.Release));
            }
            finally { registration.ReleaseTemporary(); }
        }
    }
    private void Open() {
        ObjectDisposedException.ThrowIf(registry == IntPtr.Zero, this);
        if (failedModule) throw new UniversalDataException((int)StatusCode.Fenced, "Module registration failed; create a new Registry");
    }
    public void Dispose()
    {
        lock (sync)
        {
            if (registry == IntPtr.Zero) return;
            // Native returns BUSY while an Execution leases this Registry. Keep it usable for retry.
            DataOwnership.Check(context, Native.ud_registry_destroy(registry));
            registry = IntPtr.Zero; ReleaseModules(); context.Dispose();
        }
        GC.SuppressFinalize(this);
    }
    ~Registry() { try { Dispose(); } catch { /* An owning Execution retains the Registry until teardown. */ } }
}

internal sealed class TaskRegistration
{
    private sealed record Instance(IManagedTask Task, IntPtr Memory, IntPtr Activation);
    private readonly ManagedTaskDefinition definition;
    private GCHandle root;
    private int references = 1;
    internal IntPtr Pointer => GCHandle.ToIntPtr(root);
    internal readonly Native.OwnerRetain Retain;
    internal readonly Native.OwnerRelease Release;
    internal readonly Native.PrepareCallback Prepare;
    internal readonly Native.InvokeCallback Invoke;
    internal readonly Native.InstanceStatus Finish;
    internal readonly Native.InstanceCancel Cancel;
    internal readonly Native.InstanceDispose Dispose;
    internal TaskRegistration(ManagedTaskDefinition value)
    {
        definition = value; root = GCHandle.Alloc(this);
        Retain = _ => { Interlocked.Increment(ref references); return 0; };
        Release = _ => ReleaseTemporary();
        Prepare = PrepareInstance; Invoke = InvokeInstance; Finish = FinishInstance;
        Cancel = CancelInstance; Dispose = DisposeInstance;
    }
    internal void ReleaseTemporary() { if (Interlocked.Decrement(ref references) == 0) root.Free(); }
    private static int Failure(IntPtr memory, Exception error)
    {
        int code = error is UniversalDataException native ? native.Status : (int)StatusCode.Io;
        if (code <= 1 || code > 20 || code == 19) code = (int)StatusCode.Io;
        using var text = new NativeStrings();
        return Native.ud_context_report(memory, code, text.Pin("Managed callback failed: " + error.GetType().FullName));
    }
    private int PrepareInstance(IntPtr user, IntPtr context, Native.StringValue configuration, IntPtr inputs, IntPtr outputs, out IntPtr instance)
    {
        using var guard = Execution.EnterCallback();
        instance = IntPtr.Zero; IManagedTask? task = null;
        var callback = Marshal.PtrToStructure<Native.CallbackContext>(context);
        try
        {
            task = definition.Factory() ?? throw new InvalidOperationException("Task factory returned null");
            instance = GCHandle.ToIntPtr(GCHandle.Alloc(new Instance(task, callback.Memory, callback.Activation)));
            return 0;
        }
        catch (Exception error) { try { task?.Dispose(); } catch { } return Failure(callback.Memory, error); }
    }
    private static Instance State(IntPtr pointer) => (Instance)GCHandle.FromIntPtr(pointer).Target!;
    private int InvokeInstance(IntPtr pointer, IntPtr context, Native.StringValue configuration)
    {
        using var guard = Execution.EnterCallback();
        var state = State(pointer); var callback = new ManagedTaskContext(context);
        try { state.Task.Execute(callback, Native.Text(configuration)); return 0; }
        catch (Exception error) { return Failure(state.Memory, error); }
        finally { callback.End(); }
    }
    private int FinishInstance(IntPtr pointer)
    { using var guard = Execution.EnterCallback(); var state = State(pointer); try { state.Task.Finish(); return 0; } catch (Exception error) { return Failure(state.Memory, error); } }
    private void CancelInstance(IntPtr pointer)
    { using var guard = Execution.EnterCallback(); var state = State(pointer); try { state.Task.Cancel(); } catch (Exception error) { Fault(state, error); } }
    private void DisposeInstance(IntPtr pointer, int status)
    {
        using var guard = Execution.EnterCallback();
        var handle = GCHandle.FromIntPtr(pointer); var state = (Instance)handle.Target!;
        try { state.Task.Dispose(); } catch (Exception error) { Fault(state, error); }
        finally { handle.Free(); }
    }
    private static void Fault(Instance state, Exception error)
    {
        using var strings = new NativeStrings();
        Native.ud_activation_report_fault(state.Activation, (int)StatusCode.Io,
            strings.Pin("Managed lifecycle callback failed: " + error.GetType().FullName));
    }
}

internal static partial class Native
{
    internal static string Text(StringValue value) => Marshal.PtrToStringUTF8(value.Data, checked((int)value.Length)) ?? "";
    [StructLayout(LayoutKind.Sequential)] internal struct CallbackContext
    { internal uint Size, Abi; internal IntPtr Memory, Execution, Activation; internal StringValue TaskId, PipelineId, ComponentId; }
    [StructLayout(LayoutKind.Sequential)] internal struct ComponentDescriptor
    {
        internal uint Size, Abi; internal StringValue Id, Name, Version, Configuration;
        internal int Category; internal uint Inputs, Outputs; internal int SynchronousInput, ErrorOutput;
        internal ulong Capabilities; internal IntPtr User, Configure, Prepare, Step, Finish, Dispose, Invoke;
        internal uint MinArguments, MaxArguments; internal IntPtr FunctionBind, FunctionEvaluate, Cancel, EnumerateNext, EnumeratePosition;
        internal ulong OptionalInputs; internal IntPtr Confirm, Diagnostics; internal ulong OptionalOutputs;
        internal IntPtr DescribePlan, BindPorts, InputDemand; internal uint StateVersion, Effects;
        internal IntPtr Checkpoint, Restore, ConfirmEpoch;
    }
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] internal delegate int OwnerRetain(IntPtr owner);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] internal delegate void OwnerRelease(IntPtr owner);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] internal delegate int PrepareCallback(IntPtr owner, IntPtr context, StringValue configuration, IntPtr inputs, IntPtr outputs, out IntPtr instance);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] internal delegate int InvokeCallback(IntPtr instance, IntPtr context, StringValue configuration);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] internal delegate int InstanceStatus(IntPtr instance);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] internal delegate void InstanceCancel(IntPtr instance);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] internal delegate void InstanceDispose(IntPtr instance, int status);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_registry_create(ContextHandle context, out IntPtr registry);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_registry_register_owned(IntPtr registry, ref ComponentDescriptor descriptor, uint count, IntPtr owner, OwnerRetain retain, OwnerRelease release);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_registry_describe(IntPtr registry, out IntPtr text, out uint length);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_registry_destroy(IntPtr registry);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_activation_check(IntPtr activation);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_context_report(IntPtr context, int status, StringValue message);
}
