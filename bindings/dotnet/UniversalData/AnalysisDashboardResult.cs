namespace UniversalData;

public sealed record AnalysisDashboardResult(
    string Revision, string ReleaseId, DateTimeOffset DataAsOf, AnalysisDashboardCardResult[] Cards);
