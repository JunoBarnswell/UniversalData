namespace UniversalData;

/// <summary>Options for one idempotent publication request.</summary>
public sealed record JobPublicationOptions(
    string? RequestId = null,
    string? ExpectedPackageRevision = null,
    JobSchedule[]? Schedules = null,
    bool Run = false,
    string? Parameters = null,
    JobEnvironmentReference? Environment = null)
{
    internal string EffectiveRequestId => string.IsNullOrWhiteSpace(RequestId)
        ? Guid.NewGuid().ToString("N") : RequestId;

    internal string EffectiveExpectedPackageRevision =>
        string.IsNullOrWhiteSpace(ExpectedPackageRevision) ? "0" : ExpectedPackageRevision;
}
