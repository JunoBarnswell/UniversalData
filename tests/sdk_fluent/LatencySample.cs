namespace FluentAcceptance;

internal readonly record struct LatencySample(double RequestMs, double PlannedMs, double ReleaseMs,
    double QueueMs, double StartupMs, double ResultMs, int ProcessId)
{
    internal double DispatchMs => ReleaseMs + QueueMs;
}
