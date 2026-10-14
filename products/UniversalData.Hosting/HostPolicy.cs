using System.Text.Json;

namespace UniversalData.Hosting;

/// <summary>Host-owned resource and credential scopes, shared by product hosts.</summary>
public sealed record HostPolicy(string DataDirectory, string[] FileRoots, string[] NetworkOrigins,
    string[] PluginRoots, Dictionary<string, string> CredentialFiles, uint MaxConcurrency = 4,
    ulong MemoryLimit = 268435456, Dictionary<string, ConnectionAccess[]>? Resources = null)
{
    public static HostPolicy Load(string path)
    {
        var policy = JsonSerializer.Deserialize<HostPolicy>(File.ReadAllText(path), PolicyJsonOptions)
                     ?? throw new InvalidDataException("Host policy is empty");
        if (!Path.IsPathFullyQualified(policy.DataDirectory) || policy.MaxConcurrency is < 1 or > 64 || policy.MemoryLimit == 0)
            throw new InvalidDataException("Invalid host resource policy");
        return policy;
    }
    public static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private static readonly JsonSerializerOptions PolicyJsonOptions = CreateJsonOptions();
    private static JsonSerializerOptions CreateJsonOptions() {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web);
        options.Converters.Add(new System.Text.Json.Serialization.JsonStringEnumConverter()); return options;
    }
    public string CatalogDirectory => Path.Combine(DataDirectory, "catalog");
    public string ExecutionDirectory(string ticket) => Path.Combine(DataDirectory, "executions", ticket);
    private static bool Inside(string resource, string root)
    {
        if (!Path.IsPathFullyQualified(resource) || !Path.IsPathFullyQualified(root)) return false;
        var target = Path.GetFullPath(resource); var parent = Path.GetFullPath(root).TrimEnd(Path.DirectorySeparatorChar);
        var comparison = OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        return target.Equals(parent, comparison) || target.StartsWith(parent + Path.DirectorySeparatorChar, comparison);
    }
    public bool Authorize(string tenant, string resource, ConnectionAccess access)
    {
        if (tenant != "local") return false;
        if (Resources is not null && Resources.TryGetValue(resource, out var grants) && grants.Contains(access)) return true;
        if (access == ConnectionAccess.Execute) return PluginRoots.Any(root => Inside(resource, root));
        if (Inside(resource, DataDirectory)) return true;
        if (Uri.TryCreate(resource, UriKind.Absolute, out var uri) && uri.Scheme is "http" or "https" or "ftp" or "ftps" or "sftp" or "smtp" or "smtps")
            return NetworkOrigins.Any(origin => Uri.TryCreate(origin, UriKind.Absolute, out var permitted) &&
                uri.Scheme == permitted.Scheme && uri.Host.Equals(permitted.Host, StringComparison.OrdinalIgnoreCase) && uri.Port == permitted.Port);
        return FileRoots.Any(root => Inside(resource, root));
    }
    public string Resolve(string tenant, string resource, string reference)
    {
        if (tenant != "local" || !CredentialFiles.TryGetValue(reference, out var path) || !Path.IsPathFullyQualified(path))
            throw new UniversalDataException((int)StatusCode.Auth, "Credential reference is outside the host policy");
        // Values stay inside the worker and are consumed by Native; APIs expose references only.
        return File.ReadAllText(path);
    }
    public Catalog OpenCatalog()
    {
        Directory.CreateDirectory(DataDirectory);
        return new(CatalogDirectory, "local", Authorize);
    }
    public Execution CreateExecution(string packageJson, string parametersJson = "{}", string executionId = "",
        string? stateDirectory = null, bool resume = false, Registry? registry = null, Action<ExecutionEvent>? observe = null,
        ulong eventSequenceStart = 0) =>
        new(packageJson, "local", Authorize, parametersJson, executionId, Resolve, stateDirectory: stateDirectory,
            resume: resume, maxConcurrency: MaxConcurrency, memoryLimit: MemoryLimit, registry: registry, observe: observe, eventSequenceStart: eventSequenceStart);
}
