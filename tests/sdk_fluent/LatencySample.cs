namespace FluentAcceptance;

internal readonly record struct LatencySample(double RequestMs, double PlannedMs, double ReleaseMs,
    double LaneBacklogMs, double ReadyToSubmitMs, double StartupMs, double ResultMs, int ProcessId)
{
    internal double QueueMs => LaneBacklogMs + ReadyToSubmitMs;
    internal double DispatchMs => ReleaseMs + QueueMs;
}
