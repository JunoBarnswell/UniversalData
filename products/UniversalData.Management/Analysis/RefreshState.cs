namespace UniversalData.Management;

internal sealed record RefreshState(string Id, int Version, string DatasetId, string Subject, string Stamp,
    bool Enabled, int IntervalMinutes, long? NextDueMs, AnalysisRefreshTicket? Active = null);
