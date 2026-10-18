using System.Runtime.InteropServices;

namespace UniversalData;

public readonly record struct FieldIdentity(ulong High, ulong Low);
public enum RecordErrorKind { Automatic = 0, Conversion = 1, Truncation, Constraint, Execution }
public sealed record ManagedPort(string Name, bool Required = true, bool Error = false);
public sealed record ManagedPortBinding(ManagedPort[] Inputs, ManagedPort[] Outputs);
public sealed record ManagedSortKey(FieldIdentity Field, bool Descending = false, bool NullsFirst = false);
public enum ComponentOrdering { Unordered = 1, Partition = 2, Global = 3 }
public sealed record ManagedPhysicalPlan(uint Partitions = 1, ComponentOrdering Ordering = ComponentOrdering.Unordered,
    ManagedSortKey[]? Sort = null, bool StableSort = false, ulong EstimatedRows = 0, ulong RowWidth = 0,
    ulong MemoryBudget = 0, ulong SpillBudget = 0, ulong EstimatedCost = 0, double Selectivity = 1,
    uint Algorithm = 0, uint StateVersion = 0, ComponentEffects Effects = ComponentEffects.None,
    FieldIdentity[]? PartitionKeys = null, string PhysicalTree = "", uint Boundedness = 0)
{
    internal Native.ComponentPlan ToNative(IntPtr memory)
    {
        if ((Sort?.Length ?? 0) > 16 || (PartitionKeys?.Length ?? 0) > 16 || Partitions == 0 || !double.IsFinite(Selectivity) || Selectivity is < 0 or > 1)
            throw new ArgumentException("Physical plan exceeds Native limits");
        var sort = Native.SortContract.Empty(); sort.Count = (uint)(Sort?.Length ?? 0); sort.Stable = StableSort ? 1U : 0;
        for (int at = 0; at < (Sort?.Length ?? 0); at++) sort.Keys[at] = new() { Field = new() { High = Sort![at].Field.High, Low = Sort[at].Field.Low },
            Descending = Sort[at].Descending ? 1 : 0, NullsFirst = Sort[at].NullsFirst ? 1 : 0 };
        var keys = new Native.FieldId[16];
        for (int at = 0; at < (PartitionKeys?.Length ?? 0); at++) keys[at] = new() { High = PartitionKeys![at].High, Low = PartitionKeys[at].Low };
        return new() { Size = (uint)Marshal.SizeOf<Native.ComponentPlan>(), Abi = ProductVersion.Abi, Partitions = Partitions, Ordering = (int)Ordering,
            Sort = sort, EstimatedRows = EstimatedRows, RowWidth = RowWidth, MemoryBudget = MemoryBudget, SpillBudget = SpillBudget,
            EstimatedCost = EstimatedCost, Selectivity = Selectivity, Algorithm = Algorithm, StateVersion = StateVersion,
            Effects = (uint)Effects, PartitionKeyCount = (uint)(PartitionKeys?.Length ?? 0), PartitionKeys = keys, PhysicalTree = Native.OwnedText(memory,PhysicalTree), Boundedness = Boundedness };
    }
    internal static ManagedPhysicalPlan FromNative(Native.ComponentPlan plan) => new(plan.Partitions,(ComponentOrdering)plan.Ordering,
        plan.Sort.Keys.Take(checked((int)plan.Sort.Count)).Select(key => new ManagedSortKey(new(key.Field.High,key.Field.Low),key.Descending != 0,key.NullsFirst != 0)).ToArray(),
        plan.Sort.Stable != 0,plan.EstimatedRows,plan.RowWidth,plan.MemoryBudget,plan.SpillBudget,plan.EstimatedCost,plan.Selectivity,plan.Algorithm,
        plan.StateVersion,(ComponentEffects)plan.Effects,plan.PartitionKeys.Take(checked((int)plan.PartitionKeyCount)).Select(key => new FieldIdentity(key.High,key.Low)).ToArray(),Native.Text(plan.PhysicalTree),plan.Boundedness);
}
internal static partial class Native
{
    [DllImport(Library,CallingConvention = CallingConvention.Cdecl)] internal static extern ulong ud_registry_features();
    internal static StringValue OwnedText(IntPtr memory, string text) {
        var bytes = Utf8.Bytes(text); var pointer = ud_alloc(memory,(UIntPtr)checked(bytes.Length + 1));
        if (pointer == IntPtr.Zero) throw new UniversalDataException((int)StatusCode.Resource,"Native text allocation failed");
        Marshal.Copy(bytes,0,pointer,bytes.Length); Marshal.WriteByte(pointer,bytes.Length,0); return new(pointer,(uint)bytes.Length);
    }
    [StructLayout(LayoutKind.Sequential)] internal struct FieldId { internal ulong High, Low; }
    [StructLayout(LayoutKind.Sequential)] internal struct SortKey { internal FieldId Field; internal int Descending, NullsFirst; }
    [StructLayout(LayoutKind.Sequential)] internal struct SortContract {
        internal uint Count, Stable;
        [MarshalAs(UnmanagedType.ByValArray,SizeConst = 16)] internal SortKey[] Keys;
        internal static SortContract Empty() => new() { Keys = new SortKey[16] };
    }
    [StructLayout(LayoutKind.Sequential)] internal struct ComponentPlan {
        internal uint Size, Abi, Partitions; internal int Ordering; internal SortContract Sort;
        internal ulong EstimatedRows, RowWidth, MemoryBudget, SpillBudget, EstimatedCost; internal double Selectivity;
        internal uint Algorithm, StateVersion, PartitionKeyCount, Effects;
        [MarshalAs(UnmanagedType.ByValArray,SizeConst = 16)] internal FieldId[] PartitionKeys;
        internal StringValue PhysicalTree;
        internal uint Boundedness;
    }
    [StructLayout(LayoutKind.Sequential)] internal struct ComponentPort {
        internal FieldId Id; internal StringValue Name; internal uint Direction, Index, Flags; internal IntPtr Schema;
        internal SortContract Sort; internal uint PartitionCount, PartitionKeyCount;
        [MarshalAs(UnmanagedType.ByValArray,SizeConst = 16)] internal FieldId[] PartitionKeys;
    }
    [StructLayout(LayoutKind.Sequential)] internal struct ComponentPorts { internal uint InputCount, OutputCount; internal IntPtr Inputs, Outputs; }
    [StructLayout(LayoutKind.Sequential)] internal struct ErrorContext {
        internal uint Size, Abi; internal IntPtr Memory; internal StringValue ExecutionId, PackageId, TaskId, PipelineId, ComponentId; internal FieldId FieldScope;
    }
    [StructLayout(LayoutKind.Sequential)] internal struct RecordError {
        internal uint Size, Abi; internal IntPtr Input; internal uint Row; internal RowInfo Source;
        internal StringValue Rule; internal uint RuleVersion; internal FieldId Column; internal int Code, Kind, Policy;
        internal StringValue ProviderCode, InputFormat; internal IntPtr Raw; internal uint RawLength;
    }
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] internal delegate uint RecommendedRowsCallback(IntPtr user);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] internal delegate int EmitEventCallback(IntPtr user,uint output,ref ComponentEvent frame);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] internal delegate int ConfirmEpochCallback(IntPtr instance,ulong epoch,StringValue position);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] internal delegate int BindPortsCallback(IntPtr user,IntPtr context,StringValue config,out ComponentPorts ports);
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)] internal delegate int DescribePlanCallback(IntPtr user,IntPtr context,StringValue config,IntPtr inputs,IntPtr plan);
    [DllImport(Library,CallingConvention = CallingConvention.Cdecl,EntryPoint = "ud_id_make")] internal static extern int ud_id_make_raw(IntPtr memory,StringValue scope,StringValue symbol,out FieldId identity);
    [DllImport(Library,CallingConvention = CallingConvention.Cdecl,EntryPoint = "ud_row_info_source")] internal static extern int ud_row_info_source_raw(FieldId scope,ulong position,out RowInfo info);
    [DllImport(Library,CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_error_batch(ref ErrorContext context,int includeInput,uint capacity,out IntPtr batch);
    [DllImport(Library,CallingConvention = CallingConvention.Cdecl)] internal static extern int ud_error_append(ref ErrorContext context,IntPtr batch,ref RecordError error);
}
