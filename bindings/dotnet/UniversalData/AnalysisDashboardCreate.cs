namespace UniversalData;

public sealed record AnalysisDashboardCreate(
    string Name, string DatasetId, AnalysisDashboardCard[] Cards);
