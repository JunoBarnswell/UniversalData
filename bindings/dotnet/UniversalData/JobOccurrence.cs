namespace UniversalData;

public sealed record JobOccurrence(int Version, string Id, string JobId, string? ScheduleId,
    long TimestampMs, string State, string PackageId, string PackageRevision,
    string? DeploymentId, string? DeploymentRevision, string? RunId, ulong Revision = 1);
