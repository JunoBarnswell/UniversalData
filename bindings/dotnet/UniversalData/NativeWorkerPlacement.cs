using System.Runtime.InteropServices;

namespace UniversalData;

internal readonly record struct NativePlacementBudget(uint CpuSlots, uint ConsumerLanes,
    ulong NativeMemoryBytes, ulong ProcessMemoryBytes);

internal readonly record struct NativePlacementNode(string Id, bool Remote, bool Online, bool Eligible,
    bool SupportsPackage, uint MaxCpu, uint MaxProcesses, uint UsedCpu, uint UsedProcesses,
    uint AvailableCpu, uint AvailableProcesses, ulong MaxMemoryBytes, ulong UsedMemoryBytes,
    ulong AvailableMemoryBytes);

internal readonly record struct NativePlacementPin(string NodeId, bool Remote, NativePlacementBudget Budget);

internal readonly record struct NativePlacementValue(string NodeId, bool Remote, NativePlacementBudget Budget);

/// <summary>Native deterministic placement over an immutable node snapshot.</summary>
internal static class NativeWorkerPlacement
{
    internal static NativePlacementValue[]? Plan(NativePlacementBudget budget, ulong maxNativeMemoryBytes,
        IReadOnlyList<NativePlacementNode> nodes, int count, IReadOnlyList<NativePlacementPin?>? pins = null,
        NativePlacementPin? excluded = null, IReadOnlyList<NativePlacementBudget>? budgets = null)
    {
        ArgumentNullException.ThrowIfNull(nodes);
        if (count is < 1 or > 64 || nodes.Count is < 1 or > 256 || pins is not null && pins.Count != count ||
            budgets is not null && budgets.Count != count)
            throw new ArgumentException("Invalid Native placement shape");

        var strings = new NativeStrings(nodes.Select(value => value.Id)
            .Concat(pins?.Select(value => value?.NodeId ?? "") ?? [])
            .Concat(excluded is { } value ? [value.NodeId] : [])
            .ToArray());
        var nativeNodes = new Native.PlacementNode[nodes.Count];
        for (int index = 0; index < nodes.Count; index++)
        {
            var node = nodes[index];
            nativeNodes[index] = new Native.PlacementNode
            {
                Size = (uint)Marshal.SizeOf<Native.PlacementNode>(), Abi = ProductVersion.Abi,
                Id = strings[index], Remote = node.Remote ? 1 : 0, Online = node.Online ? 1 : 0,
                Eligible = node.Eligible ? 1 : 0, SupportsPackage = node.SupportsPackage ? 1 : 0,
                MaxCpu = node.MaxCpu, MaxProcesses = node.MaxProcesses, UsedCpu = node.UsedCpu,
                UsedProcesses = node.UsedProcesses, AvailableCpu = node.AvailableCpu,
                AvailableProcesses = node.AvailableProcesses, MaxMemoryBytes = node.MaxMemoryBytes,
                UsedMemoryBytes = node.UsedMemoryBytes, AvailableMemoryBytes = node.AvailableMemoryBytes
            };
        }
        var nativeBudgets = budgets is null ? null : budgets.Select(Budget).ToArray();
        var nativePins = pins is null ? null : new Native.Placement[ count ];
        if (nativePins is not null)
            for (int index = 0; index < count; index++)
                if (pins![index] is { } pin)
                    nativePins[index] = Placement(pin, strings, nodes.Count + index);
        var nativeExcluded = excluded is { } item
            ? Placement(item, strings, nodes.Count + (pins?.Count ?? 0)) : default;
        var output = new Native.Placement[count];
        GCHandle nodeHandle = default, budgetHandle = default, pinHandle = default, outputHandle = default;
        try
        {
            nodeHandle = GCHandle.Alloc(nativeNodes, GCHandleType.Pinned);
            if (nativePins is not null) pinHandle = GCHandle.Alloc(nativePins, GCHandleType.Pinned);
            outputHandle = GCHandle.Alloc(output, GCHandleType.Pinned);
            var request = new Native.PlacementRequest
            {
                Size = (uint)Marshal.SizeOf<Native.PlacementRequest>(), Abi = ProductVersion.Abi,
                DefaultBudget = Budget(budget), MaxNativeMemoryBytes = maxNativeMemoryBytes,
                Count = (uint)count, NodeCount = (uint)nodes.Count,
                Nodes = nodeHandle.AddrOfPinnedObject(),
                Budgets = nativeBudgets is null ? IntPtr.Zero : Pin(nativeBudgets, ref budgetHandle),
                Pins = pinHandle.IsAllocated ? pinHandle.AddrOfPinnedObject() : IntPtr.Zero,
                Excluded = excluded is null ? IntPtr.Zero : Marshal.AllocHGlobal(Marshal.SizeOf<Native.Placement>())
            };
            try
            {
                if (excluded is not null) Marshal.StructureToPtr(nativeExcluded, request.Excluded, false);
                int status = Native.ud_scheduler_place(ref request, outputHandle.AddrOfPinnedObject());
                if (status is (int)StatusCode.Resource or (int)StatusCode.Busy or (int)StatusCode.End)
                    return null;
                if (status != 0)
                    throw new UniversalDataException(status,
                        Marshal.PtrToStringUTF8(Native.ud_status_name(status)) ?? "Native placement failed");
            }
            finally
            {
                if (request.Excluded != IntPtr.Zero) Marshal.FreeHGlobal(request.Excluded);
            }
            return output.Select(value => new NativePlacementValue(Native.Text(value.NodeId), value.Remote != 0,
                new(value.Budget.CpuSlots, value.Budget.ConsumerLanes, value.Budget.NativeMemoryBytes,
                    value.Budget.ProcessMemoryBytes))).ToArray();
        }
        finally
        {
            if (outputHandle.IsAllocated) outputHandle.Free();
            if (pinHandle.IsAllocated) pinHandle.Free();
            if (budgetHandle.IsAllocated) budgetHandle.Free();
            if (nodeHandle.IsAllocated) nodeHandle.Free();
            strings.Dispose();
        }
    }

    private static IntPtr Pin<T>(T[] values, ref GCHandle handle)
    {
        handle = GCHandle.Alloc(values, GCHandleType.Pinned);
        return handle.AddrOfPinnedObject();
    }

    private static Native.Placement Placement(NativePlacementPin pin, NativeStrings strings, int index) =>
        new() { Size = (uint)Marshal.SizeOf<Native.Placement>(), Abi = ProductVersion.Abi,
            NodeId = strings[index], Remote = pin.Remote ? 1 : 0, Budget = Budget(pin.Budget) };

    private static Native.Budget Budget(NativePlacementBudget value) => new()
    {
        Size = (uint)Marshal.SizeOf<Native.Budget>(), Abi = ProductVersion.Abi,
        CpuSlots = value.CpuSlots, ConsumerLanes = value.ConsumerLanes,
        NativeMemoryBytes = value.NativeMemoryBytes, ProcessMemoryBytes = value.ProcessMemoryBytes
    };
}

internal static partial class Native
{
    [StructLayout(LayoutKind.Sequential)]
    internal struct Budget
    {
        internal uint Size, Abi, CpuSlots, ConsumerLanes;
        internal ulong NativeMemoryBytes, ProcessMemoryBytes;
    }

    [StructLayout(LayoutKind.Sequential)]
    internal struct PlacementNode
    {
        internal uint Size, Abi;
        internal StringValue Id;
        internal int Remote, Online, Eligible, SupportsPackage;
        internal uint MaxCpu, MaxProcesses, UsedCpu, UsedProcesses, AvailableCpu, AvailableProcesses;
        internal ulong MaxMemoryBytes, UsedMemoryBytes, AvailableMemoryBytes;
    }

    [StructLayout(LayoutKind.Sequential)]
    internal struct Placement
    {
        internal uint Size, Abi;
        internal StringValue NodeId;
        internal int Remote;
        internal Budget Budget;
    }

    [StructLayout(LayoutKind.Sequential)]
    internal struct PlacementRequest
    {
        internal uint Size, Abi;
        internal Budget DefaultBudget;
        internal ulong MaxNativeMemoryBytes;
        internal uint Count, NodeCount;
        internal IntPtr Nodes, Budgets, Pins, Excluded;
    }

    [DllImport(Library, CallingConvention = CallingConvention.Cdecl)]
    internal static extern int ud_scheduler_place(ref PlacementRequest request, IntPtr placements);
}
