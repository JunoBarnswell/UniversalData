using System.Text.Json;

namespace UniversalData;

internal sealed record SdkHostOptions(int MaxConcurrentSessions, int MaxBranchesPerSession, int QueueCapacity,
    TimeSpan QueueTimeout, TimeSpan ExecutionTimeout, TimeSpan CancellationGrace, ulong MemoryPerSession,
    int MaxConnectionsPerSource, bool RequireIsolation, int MaxSessionsPerWorker);
