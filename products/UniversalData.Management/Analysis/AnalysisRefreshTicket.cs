namespace UniversalData.Management;

internal sealed record AnalysisRefreshTicket(string OperationId, string ExpectedRevision, string ExpectedRelease,
    DateTimeOffset StartedAt);
