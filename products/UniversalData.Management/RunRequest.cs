using System.Text.Json;

namespace UniversalData.Management;

public sealed record RunRequest(string PackageId, string PackageRevision, string Parameters = "{}", JsonElement? Debug = null,
    string? DeploymentId = null, string? DeploymentRevision = null);
