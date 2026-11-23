namespace UniversalData;

public sealed record AnalysisDashboardCardResult(
    string Id, AnalysisResult Current, AnalysisResult? Previous, string? Delta,
    string? ChangeRate);
