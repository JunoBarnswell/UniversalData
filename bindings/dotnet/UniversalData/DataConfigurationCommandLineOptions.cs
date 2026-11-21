using System.Text.Json;

namespace UniversalData;

internal sealed record DataConfigurationCommandLineOptions(string? ConfigPath, string? ParametersFile,
    IReadOnlyDictionary<string, JsonElement> Overrides);
