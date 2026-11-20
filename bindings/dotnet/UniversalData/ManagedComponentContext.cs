using System.Runtime.InteropServices;
using System.Text.Json;

namespace UniversalData;

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
        using var strings=new NativeStrings(source);var context=value;
        Check(Native.ud_component_source_identity(ref context,batch.Pointer,row,physicalPosition,strings[0]));
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
    public string OutputSchema(uint output)
    {
        Check(Native.ud_schema_to_json_raw(OutputMetadata(output), out var text, out var length));
        return DataOwnership.Text(text, length);
    }
    private IntPtr OutputMetadata(uint output)
    {
        callback.Open();
        if (output >= value.OutputCount || value.OutputSchemas == IntPtr.Zero)
            throw new ArgumentOutOfRangeException(nameof(output));
        return Marshal.ReadIntPtr(value.OutputSchemas, checked((int)output * IntPtr.Size));
    }
    public uint MaximumBatchRows { get { callback.Open(); return value.MaxRows; } }
    public bool IsStopping { get { callback.Open(); return Native.ud_activation_stopping(value.Activation) != 0; } }
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

    /// <summary>Create a Native output batch from strong entities without serializing row JSON.</summary>
    public ManagedBatch CreateBatch<T>(IEnumerable<T> entities, uint output = 0)
    {
        ArgumentNullException.ThrowIfNull(entities);
        var metadata = OutputMetadata(output);
        var members = EntityMapping.For<T>().Members.ToDictionary(member => member.ColumnName,
            StringComparer.OrdinalIgnoreCase);
        Check(Native.ud_schema_to_json_raw(metadata, out var text, out var length));
        using var shape = JsonDocument.Parse(DataOwnership.Text(text, length));
        var projection = new List<(EntityMember Member, int? Precision, int? Scale)>();
        foreach (var field in shape.RootElement.GetProperty("fields").EnumerateArray())
        {
            var name = field.GetProperty("name").GetString()!;
            var type = field.GetProperty("type").GetString();
            if (!members.TryGetValue(name, out var member))
                throw new InvalidOperationException($"Managed output column '{name}' has no member on '{typeof(T).FullName}'.");
            if (type != SqlValueCodec.NativeTypeName(member.NativeType))
                throw new InvalidOperationException($"Managed output column '{name}' expects '{type}', but '{member.Property.Name}' uses '{member.ValueType.FullName}'.");
            var numeric = member.NativeType == DataType.Decimal128;
            projection.Add((member, numeric ? field.GetProperty("precision").GetInt32() : null,
                numeric ? field.GetProperty("scale").GetInt32() : null));
        }
        var pointer = SqlBatchBuilder.BuildManaged(value.Memory, metadata, projection, entities, value.MaxRows);
        return new ManagedBatch(value.Memory, pointer, instance, true);
    }
}
