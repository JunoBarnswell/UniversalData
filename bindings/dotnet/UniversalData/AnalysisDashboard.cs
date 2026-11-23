namespace UniversalData;

public sealed record AnalysisDashboard(
    string Id, string Name, string DatasetId, string Revision, string? PublishedRevision,
    bool Owner, AnalysisDashboardCard[] Cards, AnalysisDashboardMember[] Members,
    bool Details, bool Export, AnalysisRule[] Fields, AnalysisMeasure[] Measures,
    DateTimeOffset? DataAsOf, string? ReleaseId);
