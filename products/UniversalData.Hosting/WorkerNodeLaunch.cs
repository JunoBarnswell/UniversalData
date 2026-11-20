using System.Text.Json;

namespace UniversalData.Hosting;

/// <summary>The dispatchable execution contract. Filesystem vault paths and control secrets stay on their host.</summary>
public sealed record WorkerNodeLaunch(string Ticket, string Tenant, string Subject, ResourceGrant[] Grants,
    string Package, string? Parameters, string ExecutionId, bool Resume, ulong Revision, ulong EventSequence,
    JsonElement? Debug, bool Isolated, JsonElement? BindingSnapshot, string? DeploymentId, ulong DeploymentRevision,
    WorkerBudget Budget, WorkerShard? Shard, string[] FileRoots, string[] ReadOnlyRoots, string[] PluginRoots, string[] NetworkOrigins,
    string[] CredentialReferences, Dictionary<string, ConnectionAccess[]>? Resources,
    bool BrokerRestore = false, string? JournalTicket = null, bool GovernProcess = false, KafkaAssignment? Assignment = null)
{
    public static bool SamplesRows(JsonElement? debug) =>
        debug is { ValueKind: JsonValueKind.Object } settings && settings.TryGetProperty("previewRows", out var rows) &&
        (rows.ValueKind != JsonValueKind.Number || !rows.TryGetUInt32(out var sampleCount) || sampleCount != 0);

    public static WorkerNodeLaunch From(WorkerRequest request, WorkerBudget reservation)
    {
        if (request.Mode != "execute" || request.ContractRevision != ProductInfo.WorkerRevision ||
            request.Version != ProductInfo.Protocol || SamplesRows(request.Debug) ||
            request.Budget is not null && request.Budget != reservation)
            throw new InvalidDataException("Worker execution cannot be dispatched remotely");
        var policy = request.Policy;
        string? parameters = request.ParametersReference.Length == 0 ? null :
            new ParameterVault(policy).Resolve(request.ParametersReference);
        return new(request.Ticket, request.Identity.Tenant, request.Identity.Subject, request.Identity.Grants,
            request.Package, parameters, request.ExecutionId, request.Resume, request.Revision,
            request.EventSequence, request.Debug, request.Isolated, request.BindingSnapshot, request.DeploymentId,
            request.DeploymentRevision, reservation, request.Shard, policy.FileRoots, policy.ReadOnlyRoots, policy.PluginRoots,
            policy.NetworkOrigins, policy.CredentialFiles.Keys.ToArray(), policy.Resources, request.BrokerRestore,
            request.JournalTicket, request.Budget is not null, request.Assignment);
    }
}
