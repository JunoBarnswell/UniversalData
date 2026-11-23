namespace UniversalData.Management;

internal sealed record ReportRequest(string Key, AnalysisWorkspace.QueryBinding Binding, string Token);
