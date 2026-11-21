namespace FluentAcceptance;

internal sealed record LatencyMeasurement(LatencySample[] Samples, double ElapsedSeconds);
