namespace UniversalData.Management;

internal sealed record DashboardReport(string Key, AnalysisWorkspace.QueryBinding Binding, string Context,
    AnalysisDashboardCard[] Cards, AnalysisQuery Query, string Period);
