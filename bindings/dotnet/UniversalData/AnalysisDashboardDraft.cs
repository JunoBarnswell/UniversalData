namespace UniversalData;

public sealed record AnalysisDashboardDraft(
    string ExpectedRevision, string Name, AnalysisDashboardCard[] Cards);
