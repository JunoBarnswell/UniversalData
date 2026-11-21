namespace UniversalData.Management;

public sealed record ElasticRunView(ulong PlanRevision, int TargetProcesses, int ActiveProcesses, ulong Lag,
    bool Reassigning, string? Reason, KafkaElasticGroup[] Groups, KafkaElasticGroup[]? PlannedGroups);
