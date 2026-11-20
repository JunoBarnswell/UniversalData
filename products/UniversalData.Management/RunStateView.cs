using System.Text.Json;
using UniversalData.Hosting;

namespace UniversalData.Management;

public sealed record RunStateView(int Version, string Id, string PackageId, string PackageRevision, string State,
    string ExecutionId, bool Resume, int Result, RunFailure? Failure, long TimestampMs, ulong Revision, string? JobId,
    uint Attempt, long NotBeforeMs, JsonElement? Debug, string? DeploymentId, string? DeploymentRevision, string Subject,
    int? ProcessId, WorkerParallelism? Parallelism, string[]? MemberIds, WorkerShard? Shard, string? ParentId,
    string? HomeNodeId, bool BrokerRestore, long RecoverySafeAfterMs, WorkerBudget? Budget, string? WorkerNode,
    bool RequiresJournal, string? QueueReason, long QueueWaitMs, ElasticRunView? Elastic)
{
    internal static RunStateView From(RunState state, string? queueReason = null, int activeProcesses = 0) =>
        new(state.Version, state.Id, state.PackageId, state.PackageRevision, state.State,
            state.ExecutionId, state.Resume, state.Result, state.Failure, state.TimestampMs, state.Revision, state.JobId,
            state.Attempt, state.NotBeforeMs, state.Debug, state.DeploymentId, state.DeploymentRevision, state.Subject,
            state.ProcessId, state.Parallelism, state.MemberIds, state.Shard, state.ParentId, state.HomeNodeId,
            state.BrokerRestore, state.RecoverySafeAfterMs, state.Placement?.Budget, state.Placement?.NodeId ?? state.Owner,
            state.RequiresJournal, queueReason, state.State == "Queued" ?
                Math.Max(0, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - state.TimestampMs) : 0,
            state.ElasticPlan is { } plan ? new(plan.Revision,(plan.Next ?? plan.Groups).Length,activeProcesses,
                plan.Lag,plan.Next is not null,plan.Reason,plan.Groups,plan.Next) : null);
}

public sealed record ElasticRunView(ulong PlanRevision, int TargetProcesses, int ActiveProcesses, ulong Lag,
    bool Reassigning, string? Reason, KafkaElasticGroup[] Groups, KafkaElasticGroup[]? PlannedGroups);
