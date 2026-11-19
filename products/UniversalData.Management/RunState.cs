using System.Text.Json;
using UniversalData.Hosting;

namespace UniversalData.Management;

public sealed record RunState(int Version, string Id, string PackageId, string PackageRevision, string ParametersReference,
    string State, string Subject, string SubjectStamp, string ControlReference, string ExecutionId = "", bool Resume = false, ulong ExecutionRevision = 0,
    int Result = 0, int? ProcessId = null, string? Owner = null, long TimestampMs = 0, ulong Revision = 1,
    long? ProcessStartedAtMs = null, string? JobId = null, uint Attempt = 1, long NotBeforeMs = 0,
    JsonElement? Debug = null, string? DeploymentId = null, string? DeploymentRevision = null, int RecordVersion = 2, RunFailure? Failure = null,
    WorkerParallelism? Parallelism = null, string? ParentId = null, string[]? MemberIds = null,
    WorkerShard? Shard = null, WorkerPlacement? Placement = null, string? HomeNodeId = null,
    bool BrokerRestore = false, string? JournalTicket = null, long RecoverySafeAfterMs = 0,
    bool RequiresJournal = true);
