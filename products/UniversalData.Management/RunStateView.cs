using System.Text.Json;
using UniversalData.Hosting;

namespace UniversalData.Management;

public sealed record RunStateView(int Version, string Id, string PackageId, string PackageRevision, string State,
    string ExecutionId, bool Resume, int Result, RunFailure? Failure, long TimestampMs, ulong Revision, string? JobId,
    uint Attempt, long NotBeforeMs, JsonElement? Debug, string? DeploymentId, string? DeploymentRevision, string Subject,
    int? ProcessId, WorkerParallelism? Parallelism, string[]? MemberIds, WorkerShard? Shard, string? ParentId,
    string? HomeNodeId, bool BrokerRestore, long RecoverySafeAfterMs, WorkerBudget? Budget, string? WorkerNode,
    bool RequiresJournal)
{
    internal static RunStateView From(RunState state) =>
        new(state.Version, state.Id, state.PackageId, state.PackageRevision, state.State,
            state.ExecutionId, state.Resume, state.Result, state.Failure, state.TimestampMs, state.Revision, state.JobId,
            state.Attempt, state.NotBeforeMs, state.Debug, state.DeploymentId, state.DeploymentRevision, state.Subject,
            state.ProcessId, state.Parallelism, state.MemberIds, state.Shard, state.ParentId, state.HomeNodeId,
            state.BrokerRestore, state.RecoverySafeAfterMs, state.Placement?.Budget, state.Placement?.NodeId ?? state.Owner,
            state.RequiresJournal);
}
