namespace UniversalData.Management;

public sealed record JobState(int Version, string Id, string PackageId, string PackageRevision, string ParametersReference,
    bool Enabled, uint EverySeconds, long NextDueMs, string Subject, string SubjectStamp, uint MaxConcurrency = 1, uint MaxAttempts = 1,
    uint RetryDelaySeconds = 30, long TimestampMs = 0, ulong Revision = 1, string? DeploymentId = null, string? DeploymentRevision = null, int RecordVersion = 2);
