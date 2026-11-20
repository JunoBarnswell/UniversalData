namespace UniversalData.Management;

public sealed record JobRunRequest(string RequestId, string? OccurrenceId = null);
