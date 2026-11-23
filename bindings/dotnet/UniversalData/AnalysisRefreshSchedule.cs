namespace UniversalData;

public sealed record AnalysisRefreshSchedule(
    string Revision, bool Enabled, int IntervalMinutes, DateTimeOffset? NextDueAt);
