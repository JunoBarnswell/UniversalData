namespace UniversalData.Worker;

internal sealed record WorkerNodeAgentConfiguration(string ManagementUrl, string NodeId, string CertificatePath,
    string ManagementCertificateSha256, string PolicyPath, uint MaxCpu, uint MaxProcesses, ulong MaxMemoryBytes,
    string[] Tenants, string? CertificatePasswordFile = null);
