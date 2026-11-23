namespace UniversalData;

public sealed record AnalysisRefreshScheduleWrite(
    string ExpectedRevision, bool Enabled, int IntervalMinutes);
