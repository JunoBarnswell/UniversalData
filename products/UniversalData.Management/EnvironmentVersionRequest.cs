using System.Text.Json;

namespace UniversalData.Management;

public sealed record EnvironmentVersionRequest(string PackageId, string ExpectedRevision,
    JsonElement? PackageParameters = null);
