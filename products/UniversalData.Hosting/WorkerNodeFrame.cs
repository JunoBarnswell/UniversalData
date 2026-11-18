using System.Text.Json;

namespace UniversalData.Hosting;

public sealed record WorkerNodeFrame(int WorkerRevision, string Kind, string NodeId, string MemberId,
    ulong Generation, string Lease, JsonElement? Data = null, string? Correlation = null)
{
    public static WorkerNodeFrame Registration(string kind, string nodeId, JsonElement? data = null) =>
        new(ProductInfo.WorkerRevision, kind, nodeId, "", 0, "", data);

    public WorkerNodeFrame Reply(string kind, JsonElement? data = null, string? correlation = null) =>
        this with { Kind = kind, Data = data, Correlation = correlation };

    public bool SameLease(WorkerNodeFrame other) =>
        WorkerRevision == other.WorkerRevision && NodeId == other.NodeId && MemberId == other.MemberId &&
        Generation == other.Generation && Lease == other.Lease;

    public bool ValidLease => HostIdentity.ValidId(MemberId) && Generation != 0 && Guid.TryParseExact(Lease, "N", out _);
}
