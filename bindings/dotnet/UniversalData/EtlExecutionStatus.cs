namespace UniversalData;

/// <summary>The public identity and lifecycle fields of a Management execution.</summary>
public sealed record EtlExecutionStatus(int Version, string Id, string PackageId, string PackageRevision,
    string State, string ExecutionId, bool Resume, StatusCode Result, long TimestampMs, ulong Revision,
    string? JobId, int? ProcessId, string? WorkerNode, string? QueueReason, long QueueWaitMs);
