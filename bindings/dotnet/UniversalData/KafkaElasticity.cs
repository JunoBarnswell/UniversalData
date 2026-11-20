using System.Text.Json.Serialization;

namespace UniversalData;

/// <summary>Process bounds for a directly bound transactional Kafka Data Flow.</summary>
[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed record KafkaElasticity(uint MinProcesses = 1, uint MaxProcesses = 8,
    uint TargetLagPerProcess = 10000, uint MaxPartitionsPerProcess = 16);

/// <summary>Retry a submission with the same request identity and input.</summary>
public sealed record ExecutionSubmission(string PackageId, string PackageRevision, string RequestId,
    string Parameters = "{}", string? DeploymentId = null, string? DeploymentRevision = null,
    KafkaElasticity? Elastic = null);
