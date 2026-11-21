namespace UniversalData;

/// <summary>Per-job limits that can be selected by configuration or an execution caller.</summary>
public sealed record JobExecutionOptions(
    uint? BatchRows = null,
    uint? EdgeCapacity = null,
    uint? Parallelism = null,
    ulong? MemoryBytes = null,
    uint? TimeoutMilliseconds = null,
    uint? MaxConnectionsPerSource = null,
    uint? MessagePrefetch = null);
