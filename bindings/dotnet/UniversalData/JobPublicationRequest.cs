using System.Text.Json;

namespace UniversalData;

/// <summary>One authenticated, idempotent package-to-job publication request.</summary>
public sealed record JobPublicationRequest(string RequestId, JsonElement Document,
    string ExpectedPackageRevision, JobSchedule[]? Schedules = null, bool Run = false,
    string? Parameters = null);
