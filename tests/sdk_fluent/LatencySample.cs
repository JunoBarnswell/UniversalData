namespace FluentAcceptance;

internal readonly record struct LatencySample(double RequestMs, double PlannedMs, double DispatchMs,
    double StartupMs, double ResultMs, int ProcessId);
