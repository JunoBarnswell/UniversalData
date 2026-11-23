namespace UniversalData;

public sealed record AnalysisDashboardCard(
    string Id, string Title, string Chart, string MeasureId, AnalysisDimension[] Dimensions,
    string Comparison = "none");
