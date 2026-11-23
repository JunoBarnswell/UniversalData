namespace UniversalData;

public sealed record AnalysisDashboardQuery(
    string Revision, AnalysisFilter[] Filters, string? ReleaseId = null);
