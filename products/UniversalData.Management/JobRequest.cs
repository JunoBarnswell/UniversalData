namespace UniversalData.Management;

public sealed record JobRequest(int Version, string Id, string PackageId, string PackageRevision, string? Parameters,
    bool Enabled, uint EverySeconds, long NextDueMs, uint MaxConcurrency = 1, uint MaxAttempts = 1,
    uint RetryDelaySeconds = 30, string? DeploymentId = null, string? DeploymentRevision = null, string? ParametersReference = null);
