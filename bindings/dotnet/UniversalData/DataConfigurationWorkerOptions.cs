namespace UniversalData;

internal sealed record DataConfigurationWorkerOptions(
    int? MaxConcurrentSessions = null,
    int? MaxSessionsPerWorker = null,
    int? MaxBranchesPerSession = null,
    int? MaxConnectionsPerSource = null,
    int? QueueCapacity = null,
    TimeSpan? QueueTimeout = null,
    TimeSpan? ExecutionTimeout = null,
    TimeSpan? CancellationGrace = null,
    ulong? MemoryPerSession = null,
    bool? RequireResourceIsolation = null,
    string? WorkerPath = null);
