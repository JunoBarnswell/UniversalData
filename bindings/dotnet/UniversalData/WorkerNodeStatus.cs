namespace UniversalData;

public sealed record WorkerNodeStatus(string NodeId, bool Remote, bool Online, long LastHeartbeatMs,
    uint MaxProcesses, uint ActiveProcesses, uint MaxCpu, uint UsedCpu, ulong MaxMemoryBytes,
    ulong UsedMemoryBytes, string State = "Online", WorkerCapacityStatus? Capacity = null);

public sealed record WorkerCapacityStatus(uint Starting, int[] PrewarmedPids, uint Running, uint Draining,
    uint UsedProcesses, uint UsedCpu, ulong UsedMemoryBytes, uint AvailableProcesses,
    uint AvailableCpu, ulong AvailableMemoryBytes, string? Reason);
