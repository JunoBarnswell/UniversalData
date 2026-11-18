namespace UniversalData.Management;

public sealed record RemoteWorkerNode(string NodeId, uint MaxCpu, uint MaxProcesses, ulong MaxMemoryBytes,
    string[] AllowedTenants);
