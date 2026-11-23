namespace UniversalData;

public sealed record AnalysisDashboardAccessWrite(
    string ExpectedRevision, AnalysisDashboardMember[] Members);
