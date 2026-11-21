namespace FluentAcceptance;

internal sealed record LatencySummary(double P95Ms, double P99Ms, double MaxRequestMs,
    double P95ScheduleToResultMs, double P99ScheduleToResultMs, double P95DispatchDelayMs, double MaxDispatchDelayMs,
    double P95ReleaseDelayMs, double P95QueueDelayMs,
    double P95LaneBacklogMs, double P95ReadyToSubmitMs,
    double P95StartupMs, double P95ResultMs, double ObservedRequestsPerSecond);
