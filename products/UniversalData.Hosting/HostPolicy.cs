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
    string[]? TrustedAssemblyHashes = null, uint ExecutionTimeoutSeconds = 3600, uint CancellationGraceSeconds = 10,
    string[]? TestResources = null, uint MaxConnectionsPerSource = 8)
{
    [JsonIgnore] public string Tenant { get; init; } = "local";
    [JsonIgnore] public HostIdentity Identity { get; init; } = HostIdentity.Operator("local");
    public string[] ReadOnlyRoots { get; init; } = [];
    [JsonIgnore] public bool Isolated { get; init; }
    [JsonIgnore] public string? StateDirectory { get; init; }
    [JsonIgnore] internal ClusterCatalog? Cluster { get; init; }
    public static HostPolicy Load(string path)
    {
        HostFiles.NoLinks(path);
        var policy = JsonSerializer.Deserialize<HostPolicy>(File.ReadAllText(path), PolicyJsonOptions)
                     ?? throw new InvalidDataException("Host policy is empty");
        if (!Path.IsPathFullyQualified(policy.DataDirectory) || policy.MaxConcurrency is < 1 or > 64 || policy.MemoryLimit == 0 ||
            policy.MaxConnectionsPerSource is < 1 or > 64 ||
            policy.ExecutionTimeoutSeconds is < 1 or > 604800 || policy.CancellationGraceSeconds is < 1 or > 300)
            throw new InvalidDataException("Invalid host resource policy");
        HostFiles.NoLinks(policy.DataDirectory);
        foreach (var root in policy.FileRoots.Concat(policy.PluginRoots)) HostFiles.NoLinks(root);
        var privateFiles = policy.CredentialFiles.Values.Concat(policy.IdentityFile is null ? [] : new[] { policy.IdentityFile }).Append(path);
        if (privateFiles.Any(file => policy.FileRoots.Concat(policy.PluginRoots).Any(root => Inside(file,root))))
            throw new InvalidDataException("Host policy and credential files must be outside every package/plugin directory");
        return policy with { DataDirectory = Path.GetFullPath(policy.DataDirectory) };
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
        return target.Equals(parent, PathComparison) || target.StartsWith(parent + Path.DirectorySeparatorChar, PathComparison);
    }
    private static StringComparison PathComparison => OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
    private static bool SamePath(string resource, string path) => Path.IsPathFullyQualified(resource) &&
        Path.GetFullPath(resource).Equals(Path.GetFullPath(path), PathComparison);

    public bool Authorize(string tenant, string resource, ConnectionAccess access)
    {
        if (tenant != Tenant) return false;
        if (access is ConnectionAccess.CheckpointOpen or ConnectionAccess.CheckpointRead or ConnectionAccess.CheckpointWrite)
            return StateDirectory is not null && SamePath(resource, StateDirectory);
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
        if (Cluster is not null) return Cluster.Open(Tenant);
        string catalogDirectory = Path.GetFullPath(CatalogDirectory);
        Directory.CreateDirectory(DataDirectory);
        HostFiles.NoLinks(catalogDirectory);
        return new(catalogDirectory, Tenant, (tenant, resource, _) => tenant == Tenant && SamePath(resource, catalogDirectory));
    }
    public bool RequiresIsolation(string packageJson)
    {
        using var package = new NativePackage(packageJson); using var document = JsonDocument.Parse(package.ToJson());
        return RequiresIsolation(document.RootElement);
    }

    private bool RequiresIsolation(JsonElement definition) =>
        definition.TryGetProperty("extensions", out var extensions) && extensions.EnumerateArray().Any(entry =>
            !(TrustedAssemblyHashes ?? []).Contains(entry.GetProperty("sha256").GetString(), StringComparer.Ordinal));
    internal Execution CreateExecution(string packageJson, string parametersJson = "{}", string executionId = "",
        string? stateDirectory = null, bool resume = false, Registry? registry = null, Action<ExecutionEvent>? observe = null,
        ulong eventSequenceStart = 0, WorkerShard? shard = null, bool brokerRestore = false, KafkaAssignment? assignment = null,
        SdkExecutionEnvironment? environment = null, ConnectionPool? connections = null, NativePackageDefinition? prepared = null)
    {
        if ((Registry.Features & RegistryFeatures.ScopedJournal) == 0)
            throw new UniversalDataException((int)StatusCode.Incompatible,"Native runtime does not distinguish internal recovery storage");
        var package = prepared ?? NativePackageDefinition.Read(packageJson, MemoryLimit);
        if (!Isolated && RequiresIsolation(package.Document))
            throw new UniversalDataException((int)StatusCode.Auth, "Untrusted module requires an isolated Worker");
        var limits = ResolveExecutionLimits(package.Document, package.IsContinuous,
            enforceConcurrency: executionId.Length != 0 && shard is null);
        bool requiresJournal = package.RequiresJournal;
        if (resume && !requiresJournal)
            throw new ArgumentException("This stream execution has no Native Journal and cannot be resumed", nameof(resume));
        string? journalDirectory = requiresJournal ? stateDirectory : null;
        if (journalDirectory is not null)
        {
            HostFiles.NoLinks(journalDirectory);
            if (!resume) Directory.CreateDirectory(journalDirectory);
        }
        var scoped = this with { StateDirectory = journalDirectory };
        PackageAuthorizer authorizer = scoped.Authorize;
        PackageCredentialResolver resolver = scoped.Resolve;
        if (environment is not null)
        {
            if (environment.Tenant != Tenant)
                throw new UniversalDataException((int)StatusCode.Auth, "SDK execution is outside the Worker tenant");
            authorizer = (tenant, resource, access) => tenant == Tenant &&
                (access is ConnectionAccess.CheckpointOpen or ConnectionAccess.CheckpointRead or ConnectionAccess.CheckpointWrite
                    ? scoped.Authorize(tenant, resource, access) : environment.Authorize(resource, access));
            if (environment.Resolve is { } resolve)
                resolver = (tenant, resource, reference) => tenant == Tenant ? resolve(resource, reference) :
                    throw new UniversalDataException((int)StatusCode.Auth, "SDK credential is outside the Worker tenant");
        }
        Execution created = new(package.Json, Tenant, authorizer, parametersJson, executionId, resolver,
            edgeCapacity: limits.EdgeCapacity, maxBatchRows: limits.BatchRows, memoryLimit: limits.MemoryBytes,
            timeoutMilliseconds: limits.TimeoutMilliseconds, stateDirectory: journalDirectory, resume: resume,
            maxConcurrency: limits.MaxConcurrency, registry: registry, observe: observe,
            eventSequenceStart: eventSequenceStart, workerMember: shard?.Member ?? 0,
            workerMembers: shard?.Members ?? 0, consumerLanes: shard?.Lanes ?? 0,
            brokerRestore: brokerRestore, workerRunId: shard?.RunId, assignment: assignment,
            pool: environment?.Pool ?? connections);
        try
        {
            created.LimitConnectionsPerSource(MaxConnectionsPerSource, environment?.ConnectionWaitMilliseconds ?? 0);
            BindNativeConnectors(created, package.Document);
            return created;
        }
        catch
        {
            created.Dispose();
            throw;
        }
    }

    private NativeExecutionLimits ResolveExecutionLimits(JsonElement package, bool continuous, bool enforceConcurrency)
    {
        uint batchRows = 4096;
        uint edgeCapacity = 2;
        ulong memoryBytes = MemoryLimit;
        ulong timeoutMilliseconds = continuous ? 0 : checked(ExecutionTimeoutSeconds * 1000UL);
        uint maxConcurrency = MaxConcurrency;
        if (package.TryGetProperty("policy", out var policy) &&
            policy.ValueKind == JsonValueKind.Object)
        {
            if (policy.TryGetProperty("maxConcurrency", out var requestedConcurrency))
            {
                uint requested = PositiveUInt32(requestedConcurrency, "policy.maxConcurrency");
                if (enforceConcurrency && requested > MaxConcurrency)
                    throw new UniversalDataException((int)StatusCode.Resource,
                        $"policy.maxConcurrency ({requested}) exceeds the host MaxConcurrency ({MaxConcurrency})");
                maxConcurrency = Math.Min(maxConcurrency, requested);
            }
            if (policy.TryGetProperty("execution", out var execution))
            {
                if (execution.ValueKind != JsonValueKind.Object)
                    throw new UniversalDataException((int)StatusCode.Schema,
                        "Native package policy.execution must be an object");
                if (execution.TryGetProperty("batchRows", out var requestedBatchRows))
                    batchRows = PositiveUInt32(requestedBatchRows, "policy.execution.batchRows");
                if (execution.TryGetProperty("edgeCapacity", out var requestedEdgeCapacity))
                    edgeCapacity = PositiveUInt32(requestedEdgeCapacity, "policy.execution.edgeCapacity");
                if (execution.TryGetProperty("memoryBytes", out var requestedMemory))
                {
                    ulong requested = PositiveUInt64(requestedMemory, "policy.execution.memoryBytes");
                    if (requested > MemoryLimit)
                        throw new UniversalDataException((int)StatusCode.Resource,
                            $"policy.execution.memoryBytes ({requested}) exceeds the host MemoryLimit ({MemoryLimit})");
                    memoryBytes = requested;
                }
                if (execution.TryGetProperty("timeoutMilliseconds", out var requestedTimeout))
                {
                    ulong requested = PositiveUInt64(requestedTimeout, "policy.execution.timeoutMilliseconds");
                    ulong hostTimeout = checked(ExecutionTimeoutSeconds * 1000UL);
                    if (requested > hostTimeout)
                        throw new UniversalDataException((int)StatusCode.Resource,
                            $"policy.execution.timeoutMilliseconds ({requested}) exceeds the host limit ({hostTimeout})");
                    timeoutMilliseconds = requested;
                }
                if (execution.TryGetProperty("maxConnectionsPerSource", out var requestedConnections))
                {
                    uint requested = PositiveUInt32(requestedConnections,
                        "policy.execution.maxConnectionsPerSource");
                    if (requested > MaxConnectionsPerSource)
                        throw new UniversalDataException((int)StatusCode.Resource,
                            $"policy.execution.maxConnectionsPerSource ({requested}) exceeds the host MaxConnectionsPerSource ({MaxConnectionsPerSource})");
                }
            }
        }
        return new(batchRows, edgeCapacity, memoryBytes, timeoutMilliseconds, maxConcurrency);
    }

    private void BindNativeConnectors(Execution execution, JsonElement package)
    {
        foreach (var connection in PackageConnections(package))
        {
            if (!connection.TryGetProperty("driver", out var driverValue) ||
                driverValue.ValueKind != JsonValueKind.String)
                continue;
            string driver = driverValue.GetString()!;
            if (!driver.StartsWith("ud:", StringComparison.Ordinal))
                continue;
            var kind = SdkNativeSources.LogicalDriver(driver);
            if (kind is null)
                throw new UniversalDataException((int)StatusCode.DriverUnavailable,
                    $"Native connector '{driver}' is not installed");
            string? path = SdkNativeSources.FindPluginPath(kind.Value);
            if (path is null)
                throw new UniversalDataException((int)StatusCode.DriverUnavailable,
                    $"Native {kind.Value} connector is not installed");
            if (!Authorize(Tenant, path, ConnectionAccess.Open))
                throw new UniversalDataException((int)StatusCode.Auth,
                    $"Native connector '{driver}' is outside the host plugin policy");
            execution.BindConnector(driver, path);
        }
    }

    private static IEnumerable<JsonElement> PackageConnections(JsonElement root)
    {
        if (root.TryGetProperty("connections", out var connections) &&
            connections.ValueKind == JsonValueKind.Array)
            foreach (var connection in connections.EnumerateArray())
                yield return connection;
        if (root.TryGetProperty("project", out var project) &&
            project.ValueKind == JsonValueKind.Object &&
            project.TryGetProperty("connections", out var projectConnections) &&
            projectConnections.ValueKind == JsonValueKind.Array)
            foreach (var connection in projectConnections.EnumerateArray())
                yield return connection;
    }

    private static uint PositiveUInt32(JsonElement value, string option) =>
        value.ValueKind == JsonValueKind.Number && value.TryGetUInt32(out var parsed) && parsed != 0
            ? parsed : throw new UniversalDataException((int)StatusCode.Schema, $"{option} must be a positive integer");

    private static ulong PositiveUInt64(JsonElement value, string option) =>
        value.ValueKind == JsonValueKind.Number && value.TryGetUInt64(out var parsed) && parsed != 0
            ? parsed : throw new UniversalDataException((int)StatusCode.Schema, $"{option} must be a positive integer");

    private sealed record NativeExecutionLimits(uint BatchRows, uint EdgeCapacity, ulong MemoryBytes,
        ulong TimeoutMilliseconds, uint MaxConcurrency);
}
