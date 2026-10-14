using System.Runtime.InteropServices;

namespace UniversalData;

public enum ComponentCategory { Source = 2, Transform = 3, Destination = 4 }
public enum ComponentEventKind { Data = 1, InputEnd = 2, Poll = 3, Watermark = 4, Barrier = 5, SchemaChange = 6 }
[Flags] public enum ComponentEffects { None = 0, ExternalRead = 1, ExternalWrite = 2, Transactional = 4, Receipted = 8, CommitUnknown = 16, Idempotent = 32, KeyedReplace = 64 }
public sealed record ManagedComponentDefinition(string Id, string Name, ComponentCategory Category,
    uint Inputs, uint Outputs, string ConfigurationSchema,
    Func<string, string[], string[]> Configure, Func<IManagedComponent> Factory,
    int SynchronousInput = -1, int ErrorOutput = -1, ulong OptionalInputs = 0, ulong OptionalOutputs = 0,
    bool ThreadSafe = false, bool Stateful = false, bool Blocking = false, uint StateVersion = 0,
    ComponentEffects Effects = ComponentEffects.None, string? Version = null,
    bool ReplaySafe = false, bool ConfirmsSourcePosition = false, bool HandlesControlEvents = true,
    Func<string, ManagedPortBinding>? BindPorts = null,
    Func<ManagedComponentContext, string, string[], ManagedPhysicalPlan>? DescribePlan = null);
public interface IManagedComponent : IDisposable
{
    void Open(ManagedComponentContext context, string configuration, string[] inputs, string[] outputs) { }
    StatusCode Step(ManagedComponentContext context, ManagedComponentEvent input);
    void Finish() { }
    void Cancel() { }
    void Confirm() { }
    void ConfirmEpoch(ulong epoch, string sourcePosition) => throw new UniversalDataException((int)StatusCode.Unsupported, "Source has no position acknowledgement");
    ManagedBatch Checkpoint(ManagedComponentContext context) => throw new UniversalDataException((int)StatusCode.Unsupported, "Component has no checkpoint");
    void Restore(ManagedComponentContext context, ManagedBatch snapshot) => throw new UniversalDataException((int)StatusCode.Unsupported, "Component has no restore");
}
public sealed record ManagedComponentEvent(ComponentEventKind Kind, uint Input, ManagedBatch? Batch,
    ulong Epoch, ulong SchemaVersion, long Watermark, string SourcePosition);
internal sealed class CallbackLifetime
{
    internal volatile bool Active = true;
    private readonly HashSet<ManagedBatch> batches = [];
    internal void Open() => ObjectDisposedException.ThrowIf(!Active, this);
    internal void Own(ManagedBatch batch) { lock (batches) { Open(); batches.Add(batch); } }
    internal void Release(ManagedBatch batch) { lock (batches) batches.Remove(batch); }
    internal void Close() { lock (batches) { foreach (var batch in batches.ToArray()) batch.Dispose(); Active = false; } }
}

/// <summary>Native Batch in the owning activation's memory quota. Borrowed input expires at callback return.</summary>
public sealed class ManagedBatch : IDisposable
{
    private IntPtr pointer; private readonly IntPtr memory; private readonly CallbackLifetime lifetime; private readonly bool owned;
    internal ManagedBatch(IntPtr context, IntPtr value, CallbackLifetime validity, bool ownership)
    { memory = context; pointer = value; lifetime = validity; owned = ownership; if (owned) lifetime.Own(this); }
    internal IntPtr Pointer { get { lifetime.Open(); ObjectDisposedException.ThrowIf(pointer == IntPtr.Zero, this); return pointer; } }
    internal IntPtr Memory => memory;
    public uint Rows => Native.ud_batch_rows_raw(Pointer);
    public uint Columns => Native.ud_batch_columns_raw(Pointer);
    public string SchemaJson { get { Check(Native.ud_schema_to_json_raw(Pointer, out var text, out var length)); return DataOwnership.Text(text, length); } }
    public string DataJson { get { Check(Native.ud_batch_to_json_raw(Pointer, out var text, out var length)); return DataOwnership.Text(text, length); } }
    public string RowIdentity(uint row) { Check(Native.ud_batch_row_info_json_raw(Pointer, row, out var text, out var length)); return DataOwnership.Text(text, length); }
    public void CopyRowIdentity(uint row, ManagedBatch source, uint sourceRow) {
        if (!owned || source.Memory != memory) throw new ArgumentException("Identity needs an owned Batch in the same execution");
        Check(Native.ud_batch_row_info_raw(source.Pointer, sourceRow, out var identity));
        Check(Native.ud_batch_set_row_info_raw(Pointer, row, ref identity));
    }
    public bool IsPresent(uint row, uint column) { Check(Native.ud_batch_cell_present_raw(Pointer, row, column, out var present)); return present != 0; }
    public ulong? GetUInt64(uint row, uint column) { var value = Scalar(row, column, DataType.UInt64); return value.IsNull != 0 ? null : value.Unsigned; }
    public long? GetInt64(uint row, uint column) { var value = Scalar(row, column, DataType.Int64); return value.IsNull != 0 ? null : unchecked((long)value.Unsigned); }
    public long? GetDuration(uint row, uint column) { var value = Scalar(row, column, DataType.Duration); return value.IsNull != 0 ? null : unchecked((long)value.Unsigned); }
    public Interval? GetInterval(uint row, uint column) { var value = Scalar(row, column, DataType.Interval); return value.IsNull != 0 ? null : new(value.Interval.Months, value.Interval.Days, value.Interval.Microseconds); }
    private Native.ScalarValue Scalar(uint row, uint column, DataType type) {
        Check(Native.ud_batch_get_raw(Pointer, row, column, out var value));
        if (value.Type != (int)type || value.IsAbsent != 0) throw new ArgumentException("Field is absent or has another type"); return value;
    }
    private static void Check(int status) { if (status != 0) throw new UniversalDataException(status, "Native Batch operation failed"); }
    public void Dispose() { if (pointer == IntPtr.Zero) return; if (owned) { lifetime.Open(); Native.ud_batch_destroy(pointer); lifetime.Release(this); } pointer = IntPtr.Zero; }
}

/// <summary>Borrowed activation context. Emission uses Native queues and confirmation barriers.</summary>
public sealed class ManagedComponentContext
{
    private readonly Native.DataFlowContext value; private readonly CallbackLifetime callback, instance;
    internal ManagedComponentContext(Native.DataFlowContext data, CallbackLifetime validity, CallbackLifetime owner)
    { value = data; callback = validity; instance = owner; }
    public string TaskId { get { callback.Open(); return Native.Text(value.TaskId); } }
    public string PipelineId { get { callback.Open(); return Native.Text(value.PipelineId); } }
    public ulong RecoveryEpoch { get { callback.Open(); return value.RecoveryEpoch; } }
    public string RecoveryPosition { get { callback.Open(); return Native.Text(value.RecoveryPosition); } }
    public bool RecoveryTerminal { get { callback.Open(); return value.RecoveryTerminal != 0; } }
    public uint InputCount { get { callback.Open(); return value.InputCount; } }
    public uint OutputCount { get { callback.Open(); return value.OutputCount; } }
    public ulong ConnectedOutputs { get { callback.Open(); return value.ConnectedOutputs; } }
    public uint RecommendedBatchRows { get { callback.Open(); return value.RecommendedRows == IntPtr.Zero ? value.MaxRows : Marshal.GetDelegateForFunctionPointer<Native.RecommendedRowsCallback>(value.RecommendedRows)(value.EmitUser); } }
    public ManagedPhysicalPlan InputPlan(uint input) {
        callback.Open(); if (input >= value.InputCount || value.InputPlans == IntPtr.Zero) throw new ArgumentOutOfRangeException(nameof(input));
        return ManagedPhysicalPlan.FromNative(Marshal.PtrToStructure<Native.ComponentPlan>(value.InputPlans + checked((int)input * Marshal.SizeOf<Native.ComponentPlan>())));
    }
    public void EmitEvent(uint output, ManagedComponentEvent input) {
        callback.Open(); if (output >= value.OutputCount || value.EmitEvent == IntPtr.Zero) throw new ArgumentException("Invalid control output");
        if (input.Batch is not null && input.Batch.Memory != value.Memory) throw new ArgumentException("Batch belongs to another execution");
        using var strings = new NativeStrings(input.SourcePosition);
        var frame = new Native.ComponentEvent { Size = (uint)Marshal.SizeOf<Native.ComponentEvent>(), Abi = ProductVersion.Abi,
            Kind = (int)input.Kind, Input = input.Input, Batch = input.Batch?.Pointer ?? IntPtr.Zero,
            Epoch = input.Epoch, SchemaVersion = input.SchemaVersion, Watermark = input.Watermark, SourcePosition = strings[0] };
        if (input.Batch is not null) Check(Native.ud_batch_seal_raw(input.Batch.Pointer));
        Check(Marshal.GetDelegateForFunctionPointer<Native.EmitEventCallback>(value.EmitEvent)(value.EmitUser, output, ref frame));
    }
    public void SetSourceIdentity(ManagedBatch batch,uint row,ulong physicalPosition,string source = "") {
        callback.Open(); if (batch.Memory != value.Memory) throw new ArgumentException("Batch belongs to another execution");
        string symbol = value.Errors.FieldScope.High.ToString("x16") + value.Errors.FieldScope.Low.ToString("x16") + "/" + source;
        using var strings = new NativeStrings(Native.Text(value.Errors.ExecutionId),symbol);
        Check(Native.ud_id_make_raw(value.Memory,strings[0],strings[1],out var scope));
        Check(Native.ud_row_info_source_raw(scope,physicalPosition,out var info)); Check(Native.ud_batch_set_row_info_raw(batch.Pointer,row,ref info));
    }
    public ManagedBatch CreateErrorBatch(uint capacity, bool includeInput = false) {
        callback.Open(); var errors = value.Errors; Check(Native.ud_error_batch(ref errors, includeInput ? 1 : 0, capacity, out var batch));
        return new(value.Memory, batch, instance, true);
    }
    public void AppendError(ManagedBatch errors, ManagedBatch input, uint row, string rule, uint ruleVersion,
        StatusCode code, RecordErrorKind kind = RecordErrorKind.Automatic, FieldIdentity field = default, string providerCode = "") {
        callback.Open(); if (errors.Memory != value.Memory || input.Memory != value.Memory) throw new ArgumentException("Error Batch belongs to another execution");
        using var strings = new NativeStrings(rule, providerCode);
        var context = value.Errors;
        var record = new Native.RecordError { Size = (uint)Marshal.SizeOf<Native.RecordError>(), Abi = ProductVersion.Abi,
            Input = input.Pointer, Row = row, Rule = strings[0], RuleVersion = ruleVersion, Code = (int)code, Kind = (int)kind,
            Column = new() { High = field.High, Low = field.Low }, ProviderCode = strings[1] };
        Check(Native.ud_error_append(ref context, errors.Pointer, ref record));
    }
    public string ComponentId { get { callback.Open(); return Native.Text(value.ComponentId); } }
    public string OutputSchema(uint output) {
        callback.Open(); if (output >= value.OutputCount || value.OutputSchemas == IntPtr.Zero) throw new ArgumentOutOfRangeException(nameof(output));
        var schema = Marshal.ReadIntPtr(value.OutputSchemas,checked((int)output * IntPtr.Size));
        Check(Native.ud_schema_to_json_raw(schema,out var text,out var length)); return DataOwnership.Text(text,length);
    }
    public uint MaximumBatchRows { get { callback.Open(); return value.MaxRows; } }
    public void CheckCancellation() { callback.Open(); Check(Native.ud_activation_check(value.Activation)); }
    public ManagedBatch CreateBatch(string schemaJson, string rowsJson) {
        callback.Open(); var schema = Utf8.Bytes(schemaJson); var rows = Utf8.Bytes(rowsJson);
        Check(Native.ud_batch_from_json_schema_raw(value.Memory, schema, (uint)schema.Length, rows, (uint)rows.Length, 0, out var batch));
        return new(value.Memory, batch, instance, true);
    }
    public ManagedBatch Copy(ManagedBatch batch) {
        callback.Open(); if (batch.Memory != value.Memory) throw new ArgumentException("Batch belongs to another execution");
        Check(Native.ud_batch_slice_raw(value.Memory, batch.Pointer, 0, batch.Rows, out var copy)); return new(value.Memory, copy, instance, true);
    }
    public void Emit(uint output, ManagedBatch batch) {
        callback.Open(); if (batch.Memory != value.Memory || output >= value.OutputCount) throw new ArgumentException("Invalid output Batch");
        Check(Native.ud_batch_seal_raw(batch.Pointer));
        Check(Marshal.GetDelegateForFunctionPointer<Native.EmitCallback>(value.Emit)(value.EmitUser, output, batch.Pointer));
    }
    private static void Check(int status) { if (status != 0) throw new UniversalDataException(status, "Native component operation failed"); }
}

public sealed partial class Registry
{
    public void RegisterComponent(ManagedComponentDefinition definition) {
        ArgumentNullException.ThrowIfNull(definition); ArgumentNullException.ThrowIfNull(definition.Factory); ArgumentNullException.ThrowIfNull(definition.Configure);
        lock (sync) {
            Open();
            if (definition.HandlesControlEvents && (Features & RegistryFeatures.ControlEvents) == 0)
                throw new UniversalDataException((int)StatusCode.Incompatible,"Native control-event contract is unavailable");
            if (definition.Stateful != (definition.StateVersion != 0) || definition.ConfirmsSourcePosition && definition.Category != ComponentCategory.Source)
                throw new ArgumentException("State versions and source confirmation must match the declared component contract");
            var registration = new DataFlowRegistration(definition); using var strings = new NativeStrings();
            var descriptor = new Native.ComponentDescriptor { Size = (uint)Marshal.SizeOf<Native.ComponentDescriptor>(), Abi = ProductVersion.Abi,
                Id = strings.Pin(definition.Id), Name = strings.Pin(definition.Name), Version = strings.Pin(definition.Version ?? ProductVersion.Sdk),
                Configuration = strings.Pin(definition.ConfigurationSchema), Category = (int)definition.Category, Inputs = definition.Inputs, Outputs = definition.Outputs,
                SynchronousInput = definition.SynchronousInput, ErrorOutput = definition.ErrorOutput, OptionalInputs = definition.OptionalInputs, OptionalOutputs = definition.OptionalOutputs,
                Capabilities = (definition.HandlesControlEvents ? 0x0004000000000000UL : 0) | (definition.ReplaySafe ? 0x8000000000000000UL : 0) | (definition.ThreadSafe ? 0x4000000000000000UL : 0) | (definition.Stateful ? 0x0800000000000000UL : 0) | (definition.Blocking ? 0x0400000000000000UL : 0),
                StateVersion = definition.StateVersion, Effects = (uint)definition.Effects, User = registration.Pointer,
                Configure = Marshal.GetFunctionPointerForDelegate(registration.Configure), Prepare = Marshal.GetFunctionPointerForDelegate(registration.Prepare),
                Step = Marshal.GetFunctionPointerForDelegate(registration.Step), Finish = Marshal.GetFunctionPointerForDelegate(registration.Finish),
                Dispose = Marshal.GetFunctionPointerForDelegate(registration.Dispose), Cancel = Marshal.GetFunctionPointerForDelegate(registration.Cancel),
                Checkpoint = definition.Stateful ? Marshal.GetFunctionPointerForDelegate(registration.Checkpoint) : IntPtr.Zero,
                Restore = definition.Stateful ? Marshal.GetFunctionPointerForDelegate(registration.Restore) : IntPtr.Zero,
                Confirm = definition.ConfirmsSourcePosition ? Marshal.GetFunctionPointerForDelegate(registration.Confirm) : IntPtr.Zero,
                ConfirmEpoch = definition.ConfirmsSourcePosition ? Marshal.GetFunctionPointerForDelegate(registration.ConfirmEpoch) : IntPtr.Zero,
                BindPorts = definition.BindPorts is null ? IntPtr.Zero : Marshal.GetFunctionPointerForDelegate(registration.BindPorts),
                DescribePlan = definition.DescribePlan is null ? IntPtr.Zero : Marshal.GetFunctionPointerForDelegate(registration.DescribePlan) };
            try { DataOwnership.Check(context, Native.ud_registry_register_owned(registry, ref descriptor, 1, registration.Pointer, registration.Retain, registration.Release)); }
            finally { registration.Drop(); }
        }
    }
}

internal sealed class DataFlowRegistration
{
    private sealed record Instance(IManagedComponent Component, Native.DataFlowContext Context, CallbackLifetime Lifetime);
    private readonly ManagedComponentDefinition definition; private GCHandle root; private int references = 1;
    internal IntPtr Pointer => GCHandle.ToIntPtr(root);
    internal readonly Native.OwnerRetain Retain; internal readonly Native.OwnerRelease Release;
    internal readonly Native.ConfigureCallback Configure; internal readonly Native.PrepareCallback Prepare;
    internal readonly Native.StepCallback Step; internal readonly Native.InstanceStatus Finish;
    internal readonly Native.InstanceCancel Cancel; internal readonly Native.InstanceDispose Dispose;
    internal readonly Native.CheckpointCallback Checkpoint; internal readonly Native.RestoreCallback Restore;
    internal readonly Native.InstanceStatus Confirm; internal readonly Native.ConfirmEpochCallback ConfirmEpoch;
    internal readonly Native.BindPortsCallback BindPorts; internal readonly Native.DescribePlanCallback DescribePlan;
    internal DataFlowRegistration(ManagedComponentDefinition value) {
        definition = value; root = GCHandle.Alloc(this); Retain = _ => { Interlocked.Increment(ref references); return 0; }; Release = _ => Drop();
        Configure = Bind; Prepare = Open; Step = Run; Finish = Complete; Cancel = Stop; Dispose = Close; Checkpoint = Save; Restore = Load; Confirm = Acknowledge; ConfirmEpoch = AcknowledgeEpoch; BindPorts = Ports; DescribePlan = Plan;
    }
    internal void Drop() { if (Interlocked.Decrement(ref references) == 0) root.Free(); }
    private static Instance State(IntPtr pointer) => (Instance)GCHandle.FromIntPtr(pointer).Target!;
    private static int Failure(IntPtr memory, Exception error) {
        using var strings = new NativeStrings(); int status = error is UniversalDataException native ? native.Status : (int)StatusCode.Io;
        if (status <= 1 || status == 19 || status > 20) status = (int)StatusCode.Io;
        return Native.ud_context_report(memory, status, strings.Pin("Managed component failed: " + error.GetType().FullName));
    }
    private static string[] Schemas(IntPtr pointer, uint count) {
        var result = new string[count];
        for (uint at = 0; at < count; at++) { var batch = Marshal.ReadIntPtr(pointer, checked((int)at * IntPtr.Size));
            if (batch == IntPtr.Zero) { result[at] = "null"; continue; }
            int status = Native.ud_schema_to_json_raw(batch, out var text, out var length);
            if (status != 0) throw new UniversalDataException(status, "Schema projection failed"); result[at] = DataOwnership.Text(text, length); }
        return result;
    }
    private int Bind(IntPtr user, IntPtr context, Native.StringValue configuration, IntPtr inputs, IntPtr outputs) {
        using var guard = Execution.EnterCallback(); var data = Marshal.PtrToStructure<Native.DataFlowContext>(context); var allocated = new List<IntPtr>();
        try {
            var schemas = definition.Configure(Native.Text(configuration), Schemas(inputs, data.InputCount));
            if (schemas.Length != data.OutputCount) throw new ArgumentException("Output schema count differs from descriptor");
            int status = Native.ud_component_schema_namespace(context, out var scopeText, out var scopeLength);
            if (status != 0) throw new UniversalDataException(status, "Schema namespace failed");
            string scope = DataOwnership.Text(scopeText, scopeLength); using var strings = new NativeStrings(scope);
            for (uint at = 0; at < data.OutputCount; at++) { var schema = Utf8.Bytes(schemas[at]);
                status = Native.ud_schema_from_json_raw(data.Memory, schema, (uint)schema.Length, strings[0], out var batch);
                if (status != 0) throw new UniversalDataException(status, "Output schema invalid"); allocated.Add(batch); }
            for (int at = 0; at < allocated.Count; at++) Marshal.WriteIntPtr(outputs, at * IntPtr.Size, allocated[at]); allocated.Clear(); return 0;
        } catch (Exception error) { return Failure(data.Memory, error); }
        finally { foreach (var batch in allocated) Native.ud_batch_destroy(batch); }
    }
    private int Open(IntPtr user, IntPtr context, Native.StringValue configuration, IntPtr inputs, IntPtr outputs, out IntPtr pointer) {
        using var guard = Execution.EnterCallback(); var data = Marshal.PtrToStructure<Native.DataFlowContext>(context); pointer = IntPtr.Zero;
        IManagedComponent? component = null; var lifetime = new CallbackLifetime(); var callback = new CallbackLifetime();
        try { component = definition.Factory() ?? throw new ArgumentException("Component factory returned null");
            component.Open(new(data, callback, lifetime), Native.Text(configuration), Schemas(inputs, data.InputCount), Schemas(outputs, data.OutputCount));
            pointer = GCHandle.ToIntPtr(GCHandle.Alloc(new Instance(component, data, lifetime))); return 0;
        } catch (Exception error) { try { component?.Dispose(); } catch { } lifetime.Close(); return Failure(data.Memory, error); }
        finally { callback.Active = false; }
    }
    private int Acknowledge(IntPtr pointer) { using var guard = Execution.EnterCallback(); var instance = State(pointer);
        try { instance.Component.Confirm(); return 0; } catch (Exception error) { return Failure(instance.Context.Memory,error); } }
    private int AcknowledgeEpoch(IntPtr pointer, ulong epoch, Native.StringValue position) { using var guard = Execution.EnterCallback(); var instance = State(pointer);
        try { instance.Component.ConfirmEpoch(epoch, Native.Text(position)); return 0; } catch (Exception error) { return Failure(instance.Context.Memory,error); } }
    private int Ports(IntPtr user, IntPtr context, Native.StringValue configuration, out Native.ComponentPorts output) {
        using var guard = Execution.EnterCallback(); var data = Marshal.PtrToStructure<Native.DataFlowContext>(context); output = default;
        var owned = new List<IntPtr>();
        try {
            var ports = definition.BindPorts!(Native.Text(configuration));
            if (ports.Inputs.Length > 64 || ports.Outputs.Length > 64) throw new ArgumentException("Port count exceeds Native limit");
            IntPtr Build(ManagedPort[] entries) {
                int size = Marshal.SizeOf<Native.ComponentPort>();
                var array = Native.ud_alloc(data.Memory,(UIntPtr)checked((entries.Length + 1) * size));
                if (array == IntPtr.Zero) throw new UniversalDataException((int)StatusCode.Resource,"Port allocation failed"); owned.Add(array);
                for (int at = 0; at < entries.Length; at++) {
                    var name = Native.OwnedText(data.Memory,entries[at].Name); owned.Add(name.Data);
                    Marshal.StructureToPtr(new Native.ComponentPort { Name = name, Flags = (entries[at].Required ? 1U : 0) | (entries[at].Error ? 2U : 0),
                        Sort = Native.SortContract.Empty(), PartitionKeys = new Native.FieldId[16] },array + at * size,false);
                }
                return array;
            }
            output = new() { InputCount = (uint)ports.Inputs.Length, OutputCount = (uint)ports.Outputs.Length, Inputs = Build(ports.Inputs), Outputs = Build(ports.Outputs) };
            owned.Clear(); return 0;
        } catch (Exception error) { output = default; return Failure(data.Memory,error); }
        finally { foreach (var pointer in owned) Native.ud_free(pointer); }
    }
    private int Plan(IntPtr user, IntPtr context, Native.StringValue configuration, IntPtr inputs, IntPtr output) {
        using var guard = Execution.EnterCallback(); var data = Marshal.PtrToStructure<Native.DataFlowContext>(context);
        var callback = new CallbackLifetime(); var binding = new CallbackLifetime();
        try {
            var plan = definition.DescribePlan!(new(data,callback,binding),Native.Text(configuration),Schemas(inputs,data.InputCount));
            var native = plan.ToNative(data.Memory); Marshal.StructureToPtr(native,output,false); return 0;
        } catch (Exception error) { return Failure(data.Memory,error); }
        finally { callback.Active = false; binding.Close(); }
    }
    private int Run(IntPtr pointer, IntPtr input) {
        using var guard = Execution.EnterCallback(); var instance = State(pointer); var valid = new CallbackLifetime();
        try { var eventValue = Marshal.PtrToStructure<Native.ComponentEvent>(input);
            var batch = eventValue.Batch == IntPtr.Zero ? null : new ManagedBatch(instance.Context.Memory, eventValue.Batch, valid, false);
            return (int)instance.Component.Step(new(instance.Context, valid, instance.Lifetime),
                new((ComponentEventKind)eventValue.Kind, eventValue.Input, batch, eventValue.Epoch, eventValue.SchemaVersion, eventValue.Watermark, Native.Text(eventValue.SourcePosition)));
        } catch (Exception error) { return Failure(instance.Context.Memory, error); } finally { valid.Active = false; }
    }
    private int Complete(IntPtr pointer) { using var guard = Execution.EnterCallback(); var instance = State(pointer);
        try { instance.Component.Finish(); return 0; } catch (Exception error) { return Failure(instance.Context.Memory, error); } }
    private void Stop(IntPtr pointer) { using var guard = Execution.EnterCallback(); var instance = State(pointer);
        try { instance.Component.Cancel(); } catch (Exception error) { Fault(instance, error); } }
    private void Close(IntPtr pointer, int outcome) { using var guard = Execution.EnterCallback(); var handle = GCHandle.FromIntPtr(pointer); var instance = (Instance)handle.Target!;
        try { instance.Component.Dispose(); } catch (Exception error) { Fault(instance, error); }
        finally { instance.Lifetime.Close(); handle.Free(); } }
    private static void Fault(Instance instance, Exception error) { using var strings = new NativeStrings();
        Native.ud_activation_report_fault(instance.Context.Activation, (int)StatusCode.Io, strings.Pin("Managed component lifecycle failed: " + error.GetType().FullName)); }
    private int Save(IntPtr pointer, out IntPtr batch) { using var guard = Execution.EnterCallback(); var instance = State(pointer); var valid = new CallbackLifetime(); batch = IntPtr.Zero;
        try { using var snapshot = instance.Component.Checkpoint(new(instance.Context, valid, instance.Lifetime));
            if (snapshot.Memory != instance.Context.Memory) throw new ArgumentException("Snapshot belongs to another execution");
            int status = Native.ud_batch_seal_raw(snapshot.Pointer); if (status != 0) return status;
            status = Native.ud_batch_retain_raw(snapshot.Pointer); if (status == 0) batch = snapshot.Pointer; return status;
        } catch (Exception error) { return Failure(instance.Context.Memory, error); } finally { valid.Active = false; } }
    private int Load(IntPtr pointer, IntPtr batch) { using var guard = Execution.EnterCallback(); var instance = State(pointer); var valid = new CallbackLifetime();
        try { instance.Component.Restore(new(instance.Context, valid, instance.Lifetime), new(instance.Context.Memory, batch, valid, false)); return 0; }
        catch (Exception error) { return Failure(instance.Context.Memory, error); } finally { valid.Active = false; } }
}

internal static partial class Native
{
    [StructLayout(LayoutKind.Sequential)] internal struct RowInfo {
        internal ulong IdHigh, IdLow, RuleHigh, RuleLow, OriginsHigh, OriginsLow;
        internal uint Flags, Operation; internal ulong Position; internal uint HasPosition;
    }
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ud_batch_row_info")] internal static extern int ud_batch_row_info_raw(IntPtr batch, uint row, out RowInfo info);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ud_batch_set_row_info")] internal static extern int ud_batch_set_row_info_raw(IntPtr batch, uint row, ref RowInfo info);
    [StructLayout(LayoutKind.Sequential)] internal struct DataFlowContext {
        internal uint Size, Abi; internal IntPtr Memory, Execution, Activation; internal StringValue TaskId, PipelineId, ComponentId;
        internal uint MaxRows; internal IntPtr RecommendedRows, Emit, EmitUser, Resolve, ResolveUser, ResolveField, InputPlans;
        internal uint InputCount, OutputCount; internal ulong ConnectedOutputs; internal IntPtr OutputSchemas;
        internal ErrorContext Errors; internal IntPtr EmitEvent; internal ulong RecoveryEpoch; internal StringValue RecoveryPosition; internal int RecoveryTerminal;
    }
    [StructLayout(LayoutKind.Sequential)] internal struct ComponentEvent {
        internal uint Size, Abi; internal int Kind; internal uint Input; internal IntPtr Batch;
        internal ulong Epoch, SchemaVersion; internal long Watermark; internal StringValue SourcePosition;
    }
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] internal delegate int ConfigureCallback(IntPtr user, IntPtr context, StringValue configuration, IntPtr inputs, IntPtr outputs);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] internal delegate int StepCallback(IntPtr instance, IntPtr input);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] internal delegate int EmitCallback(IntPtr user, uint port, IntPtr batch);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] internal delegate int CheckpointCallback(IntPtr instance, out IntPtr batch);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] internal delegate int RestoreCallback(IntPtr instance, IntPtr batch);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_component_schema_namespace(IntPtr context, out IntPtr text, out uint length);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ud_batch_rows")] internal static extern uint ud_batch_rows_raw(IntPtr batch);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ud_batch_columns")] internal static extern uint ud_batch_columns_raw(IntPtr batch);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ud_schema_to_json")] internal static extern int ud_schema_to_json_raw(IntPtr batch, out IntPtr text, out uint length);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ud_batch_to_json")] internal static extern int ud_batch_to_json_raw(IntPtr batch, out IntPtr text, out uint length);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ud_schema_from_json")] internal static extern int ud_schema_from_json_raw(IntPtr memory, byte[] schema, uint length, StringValue scope, out IntPtr batch);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ud_batch_from_json_schema")] internal static extern int ud_batch_from_json_schema_raw(IntPtr memory, byte[] schema, uint length, byte[] data, uint dataLength, int rounding, out IntPtr batch);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ud_batch_row_info_json")] internal static extern int ud_batch_row_info_json_raw(IntPtr batch, uint row, out IntPtr text, out uint length);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ud_batch_get")] internal static extern int ud_batch_get_raw(IntPtr batch, uint row, uint column, out ScalarValue value);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ud_batch_cell_present")] internal static extern int ud_batch_cell_present_raw(IntPtr batch, uint row, uint column, out int present);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ud_batch_slice")] internal static extern int ud_batch_slice_raw(IntPtr context, IntPtr batch, uint start, uint count, out IntPtr output);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ud_batch_seal")] internal static extern int ud_batch_seal_raw(IntPtr batch);
    [DllImport(Library, CallingConvention = CallingConvention.Cdecl, EntryPoint = "ud_batch_retain")] internal static extern int ud_batch_retain_raw(IntPtr batch);
}
