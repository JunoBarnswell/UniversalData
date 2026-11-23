namespace UniversalData.Management;

internal sealed record DashboardState(string Id, int Version, string Owner, string OwnerKey, string DatasetId,
    DashboardDefinition Draft, DashboardDefinition? Published, string? PublishedRevision,
    Dictionary<string, AnalysisDashboardMember> Members);
