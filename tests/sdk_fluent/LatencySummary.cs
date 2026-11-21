namespace FluentAcceptance;

internal sealed record LatencySummary(double P95Ms, double P99Ms, double MaxRequestMs,
    double P95ScheduleToResultMs, double P99ScheduleToResultMs, double P95DispatchDelayMs, double MaxDispatchDelayMs,
    double P95ConnectionLeaseMs, double P95SqlMs, double ObservedRequestsPerSecond);
