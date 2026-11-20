namespace UniversalData;

public sealed record JobDefinition(string Id, string PackageId, string PackageRevision, JobSchedule[] Schedules,
    bool Enabled = true, string? Parameters = null, uint MaxConcurrency = 1, uint MaxAttempts = 1,
    uint RetryDelaySeconds = 30, string? DeploymentId = null, string? DeploymentRevision = null,
    string? ParametersReference = null, KafkaElasticity? Elastic = null)
{
    public int Version => ProductInfo.Protocol;
}
