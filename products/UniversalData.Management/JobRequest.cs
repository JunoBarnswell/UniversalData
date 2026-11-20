using UniversalData;

namespace UniversalData.Management;

public sealed record JobRequest(int Version, string Id, string PackageId, string PackageRevision,
    bool Enabled, JobSchedule[] Schedules, string? Parameters = null, uint MaxConcurrency = 1, uint MaxAttempts = 1,
    uint RetryDelaySeconds = 30, string? DeploymentId = null, string? DeploymentRevision = null,
    string? ParametersReference = null, KafkaElasticity? Elastic = null);
