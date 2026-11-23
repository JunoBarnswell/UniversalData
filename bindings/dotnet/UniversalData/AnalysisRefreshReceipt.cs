namespace UniversalData;

public sealed record AnalysisRefreshReceipt(
    string OperationId, string State, DateTimeOffset StartedAt, DateTimeOffset? FinishedAt,
    string? Error, string? ReleaseId);
