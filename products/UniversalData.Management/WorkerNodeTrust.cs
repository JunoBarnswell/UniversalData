namespace UniversalData.Management;

public sealed record WorkerNodeTrust(string CertificateSha256, string[] Tenants);
