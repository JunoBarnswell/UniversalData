namespace FluentAcceptance;

public sealed record FastTiming(string Value, double ConnectionLeaseMs, double SqlMs, int ProcessId);
