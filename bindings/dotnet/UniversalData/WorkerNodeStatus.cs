namespace UniversalData;

public sealed record WorkerNodeStatus(string NodeId, bool Remote, bool Online, long LastHeartbeatMs,
    uint MaxProcesses, uint ActiveProcesses, uint MaxCpu, uint UsedCpu, ulong MaxMemoryBytes,
    ulong UsedMemoryBytes);
