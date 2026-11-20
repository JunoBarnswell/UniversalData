using System.Text.Json;

namespace UniversalData.Hosting;

public sealed record WorkerNodeFrame(int WorkerRevision, string Kind, string NodeId, string MemberId,
    ulong Generation, string Lease, JsonElement? Data = null, string? Correlation = null,
    ulong LeadershipGeneration = 0, string AgentBootId = "", ulong Sequence = 0)
{
    public static WorkerNodeFrame Registration(string kind, string nodeId, string boot, ulong generation = 0, JsonElement? data = null) =>
        new(ProductInfo.WorkerRevision, kind, nodeId, "", 0, "", data, LeadershipGeneration: generation, AgentBootId: boot);

    public WorkerNodeFrame Reply(string kind, JsonElement? data = null, string? correlation = null) =>
        this with { Kind = kind, Data = data, Correlation = correlation };

    public bool SameLease(WorkerNodeFrame other) =>
        WorkerRevision == other.WorkerRevision && NodeId == other.NodeId && MemberId == other.MemberId &&
        Generation == other.Generation && Lease == other.Lease && LeadershipGeneration == other.LeadershipGeneration &&
        AgentBootId == other.AgentBootId;

    public bool ValidLease => HostIdentity.ValidId(MemberId) && Generation != 0 &&
        Guid.TryParseExact(Lease, "N", out _) && Guid.TryParseExact(AgentBootId,"N",out _);
}
