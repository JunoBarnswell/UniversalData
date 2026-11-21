using System.Text.Json.Nodes;

namespace UniversalData;

internal sealed record DataConfigurationJob(string? PackageJson, JsonObject DefaultParameters);
