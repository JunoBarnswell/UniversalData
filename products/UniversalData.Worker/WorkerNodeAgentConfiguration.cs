namespace UniversalData.Worker;

internal sealed record WorkerNodeAgentConfiguration(WorkerManagementEndpoint[] ManagementEndpoints, string NodeId, string CertificatePath,
    string PolicyPath, uint MaxCpu, uint MaxProcesses, ulong MaxMemoryBytes,
    string[] Tenants, string? CertificatePasswordFile = null, string? SupervisorPath = null);

internal sealed record WorkerManagementEndpoint(Uri Url, string CertificateSha256);
