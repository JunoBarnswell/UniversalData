namespace FluentAcceptance;

internal sealed record LatencyWorkerBudget(string ControlGroup, string CpuQuota, string MemoryLimit, string CpuAffinity);
