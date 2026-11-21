namespace UniversalData.Management;

public sealed record DeploymentState(int Version, string Id, string PackageId, string PackageRevision,
    string DeploymentRevision, bool Enabled, long TimestampMs, ulong Revision = 1,
    UniversalData.JobEnvironmentReference? Environment = null);
