using System.Text.Json;
using System.Text.Json.Serialization;

namespace UniversalData.Hosting;

public sealed record TenantResources(string[] FileRoots, string[] NetworkOrigins, string[] PluginRoots,
    string[] CredentialReferences, Dictionary<string, ConnectionAccess[]>? Resources = null, string[]? ReadOnlyRoots = null, string[]? TestResources = null);

/// <summary>One host resource ceiling, narrowed by the authenticated tenant and subject before Native binding.</summary>
public sealed record HostPolicy(string DataDirectory, string[] FileRoots, string[] NetworkOrigins,
    string[] PluginRoots, Dictionary<string, string> CredentialFiles, uint MaxConcurrency = 4,
    ulong MemoryLimit = 268435456, Dictionary<string, ConnectionAccess[]>? Resources = null,
    string? IdentityFile = null, Dictionary<string, TenantResources>? Tenants = null,
    string[]? TrustedAssemblyHashes = null, uint ExecutionTimeoutSeconds = 3600, uint CancellationGraceSeconds = 10, string[]? TestResources = null)
{
    [JsonIgnore] public string Tenant { get; init; } = "local";
    [JsonIgnore] public HostIdentity Identity { get; init; } = HostIdentity.Operator("local");
    public string[] ReadOnlyRoots { get; init; } = [];
    [JsonIgnore] public bool Isolated { get; init; }
    [JsonIgnore] public string? StateDirectory { get; init; }
    public static HostPolicy Load(string path)
    {
        HostFiles.NoLinks(path);
        var policy = JsonSerializer.Deserialize<HostPolicy>(File.ReadAllText(path), PolicyJsonOptions)
                     ?? throw new InvalidDataException("Host policy is empty");
        if (!Path.IsPathFullyQualified(policy.DataDirectory) || policy.MaxConcurrency is < 1 or > 64 || policy.MemoryLimit == 0 ||
            policy.ExecutionTimeoutSeconds is < 1 or > 604800 || policy.CancellationGraceSeconds is < 1 or > 300)
            throw new InvalidDataException("Invalid host resource policy");
        HostFiles.NoLinks(policy.DataDirectory);
        foreach (var root in policy.FileRoots.Concat(policy.PluginRoots)) HostFiles.NoLinks(root);
        var privateFiles = policy.CredentialFiles.Values.Concat(policy.IdentityFile is null ? [] : new[] { policy.IdentityFile }).Append(path);
        if (privateFiles.Any(file => policy.FileRoots.Concat(policy.PluginRoots).Any(root => Inside(file,root))))
            throw new InvalidDataException("Host policy and credential files must be outside every package/plugin directory");
        return policy;
    }
    public static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web) {
        RespectNullableAnnotations = true, RespectRequiredConstructorParameters = true };
    private static readonly JsonSerializerOptions PolicyJsonOptions = CreateJsonOptions();
    private static JsonSerializerOptions CreateJsonOptions() {
        var options = new JsonSerializerOptions(JsonOptions);
        options.Converters.Add(new JsonStringEnumConverter()); return options;
    }
    public HostPolicy Bind(HostIdentity identity)
    {
        if (!HostIdentity.ValidId(identity.Tenant) || Tenants is null || !Tenants.TryGetValue(identity.Tenant, out var resources))
            throw new UniversalDataException((int)StatusCode.Auth, "Tenant is outside the host policy");
        // Tenant resources must be within the host ceiling. Private product state is never a package file root.
        if (resources.FileRoots.Any(root => !FileRoots.Any(parent => Inside(root, parent))) ||
            resources.PluginRoots.Any(root => !PluginRoots.Any(parent => Inside(root, parent))) ||
            resources.NetworkOrigins.Any(origin => !NetworkOrigins.Contains(origin, StringComparer.Ordinal)) ||
            resources.CredentialReferences.Any(reference => !CredentialFiles.ContainsKey(reference)))
            throw new InvalidDataException("Tenant policy exceeds host resources");
        var roots = resources.FileRoots.Concat(resources.ReadOnlyRoots ?? []).ToArray();
        if (roots.Any(root => Inside(root, DataDirectory) || Inside(DataDirectory, root)) ||
            (resources.ReadOnlyRoots ?? []).Any(root => !FileRoots.Any(parent => Inside(root,parent))))
            throw new InvalidDataException("Product state cannot be a package file root");
        return this with { DataDirectory = Path.Combine(DataDirectory, "tenants", identity.Tenant), Tenant = identity.Tenant, Identity = identity,
            FileRoots = resources.FileRoots, ReadOnlyRoots = resources.ReadOnlyRoots ?? [], NetworkOrigins = resources.NetworkOrigins, PluginRoots = resources.PluginRoots,
            CredentialFiles = CredentialFiles.Where(pair => resources.CredentialReferences.Contains(pair.Key, StringComparer.Ordinal) && identity.Allows("credential.use", pair.Key)).ToDictionary(),
            Resources = resources.Resources, TestResources = (resources.TestResources ?? []).Where(resource => (TestResources ?? []).Contains(resource,StringComparer.Ordinal)).ToArray() };
    }
    public string CatalogDirectory => Path.Combine(DataDirectory, "catalog");
    public string ControlDirectory(string ticket) {
        if (!Guid.TryParseExact(ticket,"N",out _)) throw new ArgumentException("Invalid Worker ticket");
        var digest = System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(DataDirectory));
        return Path.Combine(Path.GetTempPath(),"ud-controls-" + Convert.ToHexString(digest.AsSpan(0,8)).ToLowerInvariant(),ticket);
    }
    public string ExecutionDirectory(string ticket)
    {
        if (!Guid.TryParseExact(ticket, "N", out _)) throw new ArgumentException("Invalid execution ticket");
        return Path.Combine(DataDirectory, "executions", ticket);
    }
    public static bool Inside(string resource, string root)
    {
        if (!Path.IsPathFullyQualified(resource) || !Path.IsPathFullyQualified(root)) return false;
        var target = Path.GetFullPath(resource); var parent = Path.GetFullPath(root).TrimEnd(Path.DirectorySeparatorChar);
        var comparison = OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        return target.Equals(parent, comparison) || target.StartsWith(parent + Path.DirectorySeparatorChar, comparison);
    }
    public bool Authorize(string tenant, string resource, ConnectionAccess access)
    {
        if (tenant != Tenant) return false;
        if (access is ConnectionAccess.CheckpointOpen or ConnectionAccess.CheckpointRead or ConnectionAccess.CheckpointWrite)
            return StateDirectory is not null && Path.IsPathFullyQualified(resource) && Path.GetFullPath(resource) == Path.GetFullPath(StateDirectory);
        if (access == ConnectionAccess.Execute && Inside(resource,Path.Combine(DataDirectory,"assemblies")))
            return Identity.Allows("script.load",resource);
        // A parameter/connection cannot turn a product-private path into a data source.
        if (Path.IsPathFullyQualified(resource)) {
            try { HostFiles.NoLinks(resource); } catch { return false; }
            if (Inside(resource, DataDirectory) || CredentialFiles.Values.Any(path => Inside(resource,path)) || IdentityFile is not null && resource == IdentityFile) return false;
        }
        foreach(string root in FileRoots.Concat(ReadOnlyRoots)) {
            string imports=Path.Combine(root,".ud-imports");
            if(Inside(resource,imports)&&!Inside(resource,Path.Combine(imports,Tenant,Identity.Subject)))return false;
        }
        if (!Identity.Allows("resource." + access.ToString().ToLowerInvariant(), resource)) return false;
        if (Resources is not null && Resources.TryGetValue(resource, out var grants) && grants.Contains(access)) return true;
        if (access is ConnectionAccess.Open or ConnectionAccess.Execute && PluginRoots.Any(root => Inside(resource, root))) return true;
        if (access == ConnectionAccess.Execute) return false;
        if (Uri.TryCreate(resource, UriKind.Absolute, out var uri) && uri.Scheme is "http" or "https" or "ftp" or "ftps" or "sftp" or "smtp" or "smtps")
            return uri.UserInfo.Length == 0 && NetworkOrigins.Any(origin => Uri.TryCreate(origin, UriKind.Absolute, out var permitted) &&
                uri.Scheme == permitted.Scheme && uri.Host.Equals(permitted.Host, StringComparison.OrdinalIgnoreCase) && uri.Port == permitted.Port);
        return FileRoots.Any(root => Inside(resource, root)) || access is ConnectionAccess.Open or ConnectionAccess.Read or ConnectionAccess.Discover && ReadOnlyRoots.Any(root => Inside(resource,root));
    }
    public string Resolve(string tenant, string resource, string reference)
    {
        if (tenant != Tenant || !Identity.Allows("credential.use",reference) || !CredentialFiles.TryGetValue(reference, out var path) ||
            !Path.IsPathFullyQualified(path) || !Authorize(tenant,resource,ConnectionAccess.Open))
            throw new UniversalDataException((int)StatusCode.Auth, "Credential reference is outside the authenticated scope");
        HostFiles.NoLinks(path); return File.ReadAllText(path);
    }
    public Catalog OpenCatalog()
    {
        Directory.CreateDirectory(DataDirectory); HostFiles.NoLinks(CatalogDirectory);
        return new(CatalogDirectory, Tenant, (tenant,resource,_) => tenant == Tenant && Path.GetFullPath(resource) == CatalogDirectory);
    }
    public bool RequiresIsolation(string packageJson)
    {
        using var package = new Package(packageJson); using var document = JsonDocument.Parse(package.ToJson());
        return document.RootElement.TryGetProperty("extensions",out var extensions) && extensions.EnumerateArray().Any(entry =>
            !(TrustedAssemblyHashes ?? []).Contains(entry.GetProperty("sha256").GetString(),StringComparer.Ordinal));
    }
    public Execution CreateExecution(string packageJson, string parametersJson = "{}", string executionId = "",
        string? stateDirectory = null, bool resume = false, Registry? registry = null, Action<ExecutionEvent>? observe = null,
        ulong eventSequenceStart = 0, WorkerShard? shard = null, bool brokerRestore = false)
    {
        if ((Registry.Features & RegistryFeatures.ScopedJournal) == 0)
            throw new UniversalDataException((int)StatusCode.Incompatible,"Native runtime does not distinguish internal recovery storage");
        if (!Isolated && RequiresIsolation(packageJson)) throw new UniversalDataException((int)StatusCode.Auth, "Untrusted module requires an isolated Worker");
        using var package = new Package(packageJson, MemoryLimit);
        bool requiresJournal = package.RequiresJournal;
        if (resume && !requiresJournal)
            throw new ArgumentException("This stream execution has no Native Journal and cannot be resumed", nameof(resume));
        string? journalDirectory = requiresJournal ? stateDirectory : null;
        var scoped = this with { StateDirectory = journalDirectory };
        return new(package.ToJson(), Tenant, scoped.Authorize, parametersJson, executionId, scoped.Resolve, stateDirectory: journalDirectory,
            resume: resume, maxConcurrency: MaxConcurrency, memoryLimit: MemoryLimit, registry: registry, observe: observe,
            timeoutMilliseconds: checked(ExecutionTimeoutSeconds * 1000UL), eventSequenceStart: eventSequenceStart,
            workerMember: shard?.Member ?? 0, workerMembers: shard?.Members ?? 0,
            consumerLanes: shard?.Lanes ?? 0, brokerRestore: brokerRestore, workerRunId: shard?.RunId);
    }
}
