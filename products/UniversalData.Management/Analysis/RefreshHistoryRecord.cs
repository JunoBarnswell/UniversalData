namespace UniversalData.Management;

internal sealed record RefreshHistoryRecord(string Id, int Version, AnalysisRefreshReceipt Receipt);
