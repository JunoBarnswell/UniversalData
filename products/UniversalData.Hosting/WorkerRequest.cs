using System.Text.Json;

namespace UniversalData.Hosting;

/// <summary>Private inherited-pipe protocol. Contains credential references, never persisted parameter values.</summary>
public sealed record WorkerRequest(int Version, string Ticket, string Mode, HostPolicy Policy, HostIdentity Identity,
    string Package = "", string ParametersReference = "", string ExecutionId = "", bool Resume = false,
    ulong Revision = 0, ulong EventSequence = 0, JsonElement? Debug = null, JsonElement? Payload = null,
    string ControlReference = "", bool Isolated = false, JsonElement? BindingSnapshot = null, string? DeploymentId = null, ulong DeploymentRevision = 0,
    int ContractRevision = ProductInfo.WorkerRevision, WorkerBudget? Budget = null, WorkerShard? Shard = null, bool BrokerRestore = false, string? JournalTicket = null,
    KafkaAssignment? Assignment = null, WorkerSourceAdmission? SourceAdmission = null);
