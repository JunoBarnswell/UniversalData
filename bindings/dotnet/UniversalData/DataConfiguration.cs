using System.Collections;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using YamlDotNet.Core;
using YamlDotNet.RepresentationModel;

namespace UniversalData;

/// <summary>
/// Loads the shared UniversalData YAML configuration at an input boundary.
///
/// The package portion of a job is deliberately kept as the Native v15 document.
/// Configuration-only values such as parameters and schedules are kept outside the
/// package; Native execution policy fields remain in the canonical package document.
/// </summary>
public sealed class DataConfiguration
{
    private static readonly JsonSerializerOptions ParameterJson = new(JsonSerializerDefaults.Web)
    {
        PropertyNameCaseInsensitive = true,
        WriteIndented = false
    };

    private static readonly JsonSerializerOptions ScheduleJson = CreateScheduleJson();
    private static readonly HashSet<string> PackageFields = new(StringComparer.OrdinalIgnoreCase)
    {
        "version", "id", "parameters", "variables", "connections", "controlFlow", "dataFlows", "policy",
        "eventHandlers", "project", "deployment", "caches", "layout", "extensions"
    };

    private readonly string configPath;
    private readonly string hostPolicyPath;
    private readonly string tenant;
    private readonly Dictionary<string, DataSourceDefinition> sources;
    private readonly Dictionary<string, DataConfigurationJob> jobs;
    private readonly Dictionary<string, string> packageRevisions;
    private readonly Dictionary<string, JobExecutionOptions> executionOptions;
    private readonly Dictionary<string, JobSchedule[]> schedules;
    private readonly JsonObject parameterFile;
    private readonly Dictionary<string, JsonElement> overrides;
    private readonly DataConfigurationWorkerOptions worker;
    private readonly string[] assemblyPaths;
    private readonly string[] projectPaths;
    private readonly Dictionary<string, string> managementHeaders;

    private DataConfiguration(
        string configPath,
        string hostPolicyPath,
        string tenant,
        Dictionary<string, DataSourceDefinition> sources,
        Dictionary<string, DataConfigurationJob> jobs,
        Dictionary<string, string> packageRevisions,
        Dictionary<string, JobExecutionOptions> executionOptions,
        Dictionary<string, JobSchedule[]> schedules,
        JsonObject parameterFile,
        Dictionary<string, JsonElement> overrides,
        DataConfigurationWorkerOptions worker,
        string[] assemblyPaths,
        string[] projectPaths,
        string? managementBaseUrl,
        string? managementBearerToken,
        string? managementCookie,
        string? managementCsrf,
        Dictionary<string, string> managementHeaders)
    {
        this.configPath = configPath;
        this.hostPolicyPath = hostPolicyPath;
        this.tenant = tenant;
        this.sources = sources;
        this.jobs = jobs;
        this.packageRevisions = packageRevisions;
        this.executionOptions = executionOptions;
        this.schedules = schedules;
        this.parameterFile = parameterFile;
        this.overrides = overrides;
        this.worker = worker;
        this.assemblyPaths = assemblyPaths;
        this.projectPaths = projectPaths;
        ManagementBaseUrl = managementBaseUrl;
        ManagementBearerToken = managementBearerToken;
        ManagementCookie = managementCookie;
        ManagementCsrf = managementCsrf;
        this.managementHeaders = managementHeaders;
    }

    /// <summary>Absolute path of the YAML file that supplied this configuration.</summary>
    public string ConfigPath => configPath;

    /// <summary>Host resource policy. The policy is owned and validated by Hosting.</summary>
    public string HostPolicyPath => hostPolicyPath;

    /// <summary>Tenant passed to the existing host and Worker builder.</summary>
    public string Tenant => tenant;

    /// <summary>Logical sources declared for this client.</summary>
    public IReadOnlyDictionary<string, DataSourceDefinition> Sources => sources;

    /// <summary>Development assemblies that can contain public static DataJob declarations.</summary>
    public IReadOnlyList<string> AssemblyPaths => assemblyPaths;

    /// <summary>Logical C# project paths retained for compiler/project discovery.</summary>
    public IReadOnlyList<string> ProjectPaths => projectPaths;

    /// <summary>Configured Management base URL, when this configuration is used for publishing.</summary>
    public string? ManagementBaseUrl { get; }

    /// <summary>Optional externally supplied bearer token. No login flow is performed by the CLI.</summary>
    public string? ManagementBearerToken { get; }

    /// <summary>Optional externally supplied cookie header.</summary>
    public string? ManagementCookie { get; }

    /// <summary>Optional CSRF header for the authenticated Management session.</summary>
    public string? ManagementCsrf { get; }

    /// <summary>Additional externally supplied Management headers.</summary>
    public IReadOnlyDictionary<string, string> ManagementHeaders => managementHeaders;

    /// <summary>Names of the YAML jobs in declaration order.</summary>
    public IReadOnlyCollection<string> JobIds => jobs.Keys;

    /// <summary>Finds the default file in the current directory.</summary>
    public static DataConfiguration Load(string? path = null, string? parametersFile = null,
        IReadOnlyDictionary<string, JsonElement>? overrides = null)
    {
        string config = ResolveConfigurationPath(path);
        var root = ReadObject(config);
        EnsureKnown(root, "configuration", "hostPolicy", "sources", "jobs", "worker", "assemblies", "projects", "management");
        string directory = Path.GetDirectoryName(config)!;

        var hostPolicy = ReadHostPolicy(root, directory);
        var parsedSources = ReadSources(root, directory);
        var parsedJobs = ReadJobs(root, out var revisions, out var options, out var parsedSchedules);
        var parsedWorker = ReadWorker(root, directory);
        var parsedAssemblies = ReadPaths(root, "assemblies", directory);
        var parsedProjects = ReadPaths(root, "projects", directory);
        var management = ReadManagement(root);

        JsonObject parameters = new();
        if (parametersFile is not null)
        {
            string parameterPath = ResolvePath(parametersFile, directory);
            parameters = ReadObject(parameterPath);
        }

        var copiedOverrides = new Dictionary<string, JsonElement>(StringComparer.OrdinalIgnoreCase);
        if (overrides is not null)
            foreach (var pair in overrides)
            {
                if (string.IsNullOrWhiteSpace(pair.Key))
                    throw new ArgumentException("Parameter override names cannot be empty", nameof(overrides));
                if (!copiedOverrides.TryAdd(pair.Key, pair.Value.Clone()))
                    throw new ArgumentException($"Duplicate parameter override '{pair.Key}'", nameof(overrides));
            }

        return new DataConfiguration(config, hostPolicy.Path, hostPolicy.Tenant,
            parsedSources, parsedJobs, revisions, options, parsedSchedules, parameters, copiedOverrides,
            parsedWorker, parsedAssemblies, parsedProjects, management.BaseUrl, management.BearerToken,
            management.Cookie, management.Csrf, management.Headers);
    }

    /// <summary>
    /// Loads the shared configuration switches used by <c>DataClient.Create(string[])</c>.
    /// The SDK and CLI use this same command-line boundary.
    /// </summary>
    public static DataConfiguration FromArguments(string[] args)
    {
        var parsed = ParseCommandLine(args);
        return Load(parsed.ConfigPath, parsed.ParametersFile, parsed.Overrides);
    }

    internal static DataConfiguration LoadCommandLine(string[] args) => FromArguments(args);

    /// <summary>Extracts configuration and explicit parameter switches from application arguments.</summary>
    internal static DataConfigurationCommandLineOptions ParseCommandLine(string[] args)
    {
        ArgumentNullException.ThrowIfNull(args);
        string? config = null;
        string? parametersFile = null;
        var overrides = new Dictionary<string, JsonElement>(StringComparer.OrdinalIgnoreCase);
        for (int index = 0; index < args.Length; index++)
        {
            string argument = args[index];
            if (TryOption(argument, "--config", out var inline))
            {
                config = ReadOptionValue(args, ref index, inline, "--config");
                continue;
            }
            if (TryOption(argument, "--params-file", out inline))
            {
                parametersFile = ReadOptionValue(args, ref index, inline, "--params-file");
                continue;
            }
            if (TryOption(argument, "--set", out inline) || TryOption(argument, "--param", out inline) ||
                TryOption(argument, "--parameter", out inline))
            {
                string assignment = ReadOptionValue(args, ref index, inline, "--set/--param");
                AddOverride(overrides, assignment);
            }
        }
        return new DataConfigurationCommandLineOptions(config, parametersFile, overrides);
    }

    /// <summary>Creates a normal SDK client using the existing DataClientBuilder.</summary>
    public DataClient CreateClient()
    {
        var builder = DataClient.CreateBuilder(hostPolicyPath, tenant);
        foreach (var source in sources.Values)
            builder.AddSource(source);
        builder.ConfigureWorkers(ConfigureWorkers);
        var client = builder.Build();
        client.Configuration = this;
        return client;
    }

    /// <summary>Returns the canonical Native v15 package for a YAML job.</summary>
    public Package Package(string jobId)
    {
        var job = FindJob(jobId);
        string packageJson = job.PackageJson ??
            throw new KeyNotFoundException($"Job '{jobId}' has configuration settings but no Native Package document");
        var result = UniversalData.Package.FromJson(packageJson);
        if (!result.IsSuccess || result.Value is null)
            throw new InvalidDataException($"Job '{jobId}' contains an invalid Native Package: {result.Error?.Message ?? result.Code.ToString()}");
        return result.Value;
    }

    /// <summary>Returns the expected package revision used by a publish request.</summary>
    public string PackageRevision(string jobId) => packageRevisions.TryGetValue(jobId, out var revision) ? revision : "0";

    /// <summary>Returns execution limits declared for the named job.</summary>
    public JobExecutionOptions ExecutionOptions(string jobId)
    {
        return executionOptions.TryGetValue(jobId, out var options) ? options : new();
    }

    /// <summary>Returns schedules declared for the named job.</summary>
    public JobSchedule[] Schedules(string jobId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(jobId);
        return schedules.TryGetValue(jobId, out var value) ? value.ToArray() : [];
    }

    /// <summary>
    /// Resolves parameters in the documented order: defaults, parameter file, environment,
    /// explicit command-line overrides. A per-call SDK argument is applied by the caller.
    /// </summary>
    public string Parameters(string jobId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(jobId);
        var result = jobs.TryGetValue(jobId, out var job)
            ? job.DefaultParameters.DeepClone()?.AsObject() ?? new JsonObject()
            : new JsonObject();
        MergeParameters(result, ParameterFileFor(jobId));
        MergeEnvironment(result, jobId);
        MergeOverrides(result, jobId);
        return result.ToJsonString(ParameterJson);
    }

    internal bool HasJob(string jobId) => jobs.TryGetValue(jobId, out var job) && job.PackageJson is not null;

    internal string? WorkerPath => worker.WorkerPath;
    internal DataConfigurationWorkerOptions Worker => worker;

    internal static JsonElement ParseOverrideValue(string text)
    {
        ArgumentNullException.ThrowIfNull(text);
        if (text.Length == 0)
            return JsonSerializer.SerializeToElement(text, ParameterJson);
        try
        {
            using var document = JsonDocument.Parse(text);
            return document.RootElement.Clone();
        }
        catch (JsonException)
        {
            return JsonSerializer.SerializeToElement(text, ParameterJson);
        }
    }

    private void ConfigureWorkers(DataWorkerOptions options)
    {
        if (worker.MaxConcurrentSessions is { } sessions) options.MaxConcurrentSessions(sessions);
        if (worker.MaxSessionsPerWorker is { } sessionsPerWorker) options.MaxSessionsPerWorker(sessionsPerWorker);
        if (worker.MaxBranchesPerSession is { } branches) options.MaxBranchesPerSession(branches);
        if (worker.MaxConnectionsPerSource is { } connections) options.MaxConnectionsPerSource(connections);
        if (worker.QueueCapacity is { } queue) options.QueueCapacity(queue);
        if (worker.QueueTimeout is { } queueTimeout) options.QueueTimeout(queueTimeout);
        if (worker.ExecutionTimeout is { } executionTimeout) options.ExecutionTimeout(executionTimeout);
        if (worker.CancellationGrace is { } cancellationGrace) options.CancellationGrace(cancellationGrace);
        if (worker.MemoryPerSession is { } memory) options.MemoryPerSession(memory);
        if (worker.RequireResourceIsolation is { } isolated) options.RequireResourceIsolation(isolated);
        if (worker.WorkerPath is { } path) options.WorkerPath(path);
    }

    private DataConfigurationJob FindJob(string jobId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(jobId);
        return jobs.TryGetValue(jobId, out var job)
            ? job
            : throw new KeyNotFoundException($"Job '{jobId}' is not declared in '{configPath}'");
    }

    private JsonObject ParameterFileFor(string jobId)
    {
        if (parameterFile.TryGetPropertyValue(jobId, out var direct) && direct is JsonObject directObject)
            return directObject;
        if (parameterFile.TryGetPropertyValue("jobs", out var jobsNode) && jobsNode is JsonObject jobMap &&
            jobMap.TryGetPropertyValue(jobId, out var nested) && nested is JsonObject nestedObject)
            return nestedObject;
        if (parameterFile.TryGetPropertyValue("parameters", out var parameters) && parameters is JsonObject parameterObject)
            return parameterObject;
        return parameterFile;
    }

    private void MergeEnvironment(JsonObject target, string jobId)
    {
        string normalized = NormalizeEnvironmentSegment(jobId);
        string[] prefixes =
        [
            "UD_PARAMETERS_" + normalized,
            "UNIVERSALDATA_PARAMETERS_" + normalized,
            "UD_PARAMETER_" + normalized + "_",
            "UNIVERSALDATA_PARAMETER_" + normalized + "_",
            "UD_PARAM_" + normalized + "_",
            "UNIVERSALDATA_PARAM_" + normalized + "_",
            "UD_" + normalized + "_",
            "UNIVERSALDATA_" + normalized + "_"
        ];

        var environment = Environment.GetEnvironmentVariables();
        foreach (DictionaryEntry item in environment)
        {
            string name = item.Key?.ToString() ?? "";
            string value = item.Value?.ToString() ?? "";
            if (name.Equals(prefixes[0], StringComparison.OrdinalIgnoreCase) ||
                name.Equals(prefixes[1], StringComparison.OrdinalIgnoreCase))
            {
                JsonElement parsed = ParseOverrideValue(value);
                if (parsed.ValueKind != JsonValueKind.Object)
                    throw new InvalidDataException($"Environment parameter '{name}' must contain a JSON object");
                MergeParameters(target, parsed);
                continue;
            }

            string? suffix = null;
            foreach (string prefix in prefixes.Skip(2))
                if (name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                {
                    suffix = name[prefix.Length..];
                    break;
                }
            if (string.IsNullOrWhiteSpace(suffix)) continue;
            string key = ResolveEnvironmentKey(target, suffix);
            SetParameter(target, key, ParseOverrideValue(value));
        }
    }

    private void MergeOverrides(JsonObject target, string jobId)
    {
        string[] prefixes = [jobId + ".", jobId + ":", jobId + "/"];
        foreach (var pair in overrides)
        {
            string? key = prefixes.Select(prefix => pair.Key.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)
                ? pair.Key[prefix.Length..] : null).FirstOrDefault(value => value is not null);
            if (pair.Key.Equals(jobId, StringComparison.OrdinalIgnoreCase))
            {
                if (pair.Value.ValueKind != JsonValueKind.Object)
                    throw new InvalidDataException($"Override '{pair.Key}' must contain a JSON object");
                MergeParameters(target, pair.Value);
            }
            else if (key is not null)
                SetParameter(target, key, pair.Value);
            else if (!jobs.Keys.Any(other => pair.Key.StartsWith(other + ".", StringComparison.OrdinalIgnoreCase) ||
                         pair.Key.StartsWith(other + ":", StringComparison.OrdinalIgnoreCase) ||
                         pair.Key.StartsWith(other + "/", StringComparison.OrdinalIgnoreCase)))
                SetParameter(target, pair.Key, pair.Value);
        }
    }

    private static string ResolveEnvironmentKey(JsonObject target, string suffix)
    {
        string candidate = suffix.Replace("__", ".", StringComparison.Ordinal);
        foreach (string key in FlattenKeys(target))
            if (NormalizeEnvironmentSegment(key).Equals(suffix, StringComparison.OrdinalIgnoreCase) ||
                NormalizeEnvironmentSegment(key).Equals(candidate, StringComparison.OrdinalIgnoreCase))
                return key;
        return candidate;
    }

    private static IEnumerable<string> FlattenKeys(JsonObject value, string prefix = "")
    {
        foreach (var pair in value)
        {
            string key = prefix.Length == 0 ? pair.Key : prefix + "." + pair.Key;
            if (pair.Value is JsonObject nested)
                foreach (string child in FlattenKeys(nested, key)) yield return child;
            else yield return key;
        }
    }

    private static void MergeParameters(JsonObject target, JsonElement overlay)
    {
        if (overlay.ValueKind != JsonValueKind.Object)
            throw new InvalidDataException("Parameters must be a JSON/YAML object");
        foreach (var property in overlay.EnumerateObject())
        {
            if (property.Value.ValueKind == JsonValueKind.Object &&
                target.TryGetPropertyValue(property.Name, out var existing) && existing is JsonObject existingObject)
                MergeParameters(existingObject, property.Value);
            else
                target[property.Name] = JsonNode.Parse(property.Value.GetRawText());
        }
    }

    private static void MergeParameters(JsonObject target, JsonObject overlay)
    {
        using var document = JsonDocument.Parse(overlay.ToJsonString(ParameterJson));
        MergeParameters(target, document.RootElement);
    }

    private static void SetParameter(JsonObject target, string path, JsonElement value)
    {
        string[] segments = path.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (segments.Length == 0) throw new InvalidDataException("Parameter name cannot be empty");
        JsonObject current = target;
        for (int index = 0; index < segments.Length - 1; index++)
        {
            if (current[segments[index]] is not JsonObject nested)
            {
                nested = new JsonObject();
                current[segments[index]] = nested;
            }
            current = nested;
        }
        current[segments[^1]] = JsonNode.Parse(value.GetRawText());
    }

    private static string NormalizeEnvironmentSegment(string value) =>
        new(value.Where(character => char.IsAsciiLetterOrDigit(character)).Select(char.ToUpperInvariant).ToArray());

    private static string ResolveConfigurationPath(string? path)
    {
        if (!string.IsNullOrWhiteSpace(path))
        {
            string explicitPath = Path.GetFullPath(path);
            if (!File.Exists(explicitPath)) throw new FileNotFoundException("UniversalData configuration was not found", explicitPath);
            return explicitPath;
        }

        string yaml = Path.GetFullPath("universaldata.yaml");
        string yml = Path.GetFullPath("universaldata.yml");
        bool hasYaml = File.Exists(yaml), hasYml = File.Exists(yml);
        if (hasYaml && hasYml)
            throw new InvalidDataException("Both universaldata.yaml and universaldata.yml exist; choose one with --config");
        if (hasYaml) return yaml;
        if (hasYml) return yml;
        throw new FileNotFoundException("No universaldata.yaml or universaldata.yml was found", yaml);
    }

    private static (string Path, string Tenant) ReadHostPolicy(JsonObject root, string directory)
    {
        if (!TryGet(root, "hostPolicy", out var hostNode))
            throw new InvalidDataException("Required field 'hostPolicy' is missing");
        JsonObject host = RequireObject(hostNode, "hostPolicy");
        string path = RequiredString(host, "path", "file", "policyFile");
        string tenant = RequiredString(host, "tenant");
        if (string.IsNullOrWhiteSpace(tenant)) throw new InvalidDataException("hostPolicy.tenant cannot be empty");
        EnsureKnown(host, "hostPolicy", "path", "file", "policyFile", "tenant");
        return (ResolvePath(path, directory), tenant);
    }

    private static Dictionary<string, DataSourceDefinition> ReadSources(JsonObject root, string directory)
    {
        var result = new Dictionary<string, DataSourceDefinition>(StringComparer.Ordinal);
        if (!TryGet(root, "sources", out var node)) return result;
        if (node is JsonObject map)
        {
            foreach (var pair in map)
            {
                if (!result.TryAdd(pair.Key, ParseSource(pair.Key, RequireObject(pair.Value, $"sources.{pair.Key}"), directory)))
                    throw new InvalidDataException($"Duplicate source '{pair.Key}'");
            }
            return result;
        }
        if (node is not JsonArray array) throw new InvalidDataException("sources must be a mapping or sequence");
        foreach (JsonNode? item in array)
        {
            JsonObject source = RequireObject(item, "sources[]");
            string name = RequiredString(source, "id", "name");
            EnsureKnown(source, "source", "id", "name", "kind", "connectionString", "connection",
                "resource", "credentialReference", "baseUri", "nativeConnectionString", "initialRevision",
                "provisionTransactionReceipts", "storageRoot", "catalogWarehouse", "url");
            if (!result.TryAdd(name, ParseSource(name, source, directory)))
                throw new InvalidDataException($"Duplicate source '{name}'");
        }
        return result;
    }

    private static DataSourceDefinition ParseSource(string name, JsonObject source, string directory)
    {
        if (string.IsNullOrWhiteSpace(name)) throw new InvalidDataException("A source name cannot be empty");
        EnsureKnown(source, $"source '{name}'", "id", "name", "kind", "connectionString", "connection",
            "resource", "credentialReference", "baseUri", "nativeConnectionString", "initialRevision",
            "provisionTransactionReceipts", "storageRoot", "catalogWarehouse", "url");
        DataSourceKind kind = ParseSourceKind(RequiredString(source, "kind"));
        string connection = OptionalString(source, "connectionString", "connection") ?? "";
        string? resource = OptionalString(source, "resource");
        string? credential = OptionalString(source, "credentialReference");
        string? native = OptionalString(source, "nativeConnectionString");
        Uri? baseUri = OptionalUri(source, "baseUri", "url");
        ulong revision = OptionalUInt64(source, 1, "initialRevision")!.Value;
        bool receipts = OptionalBoolean(source, false, "provisionTransactionReceipts");
        string? storage = OptionalString(source, "storageRoot");
        if (storage is not null) storage = ResolvePath(storage, directory);
        string? warehouse = OptionalString(source, "catalogWarehouse");
        return new(name, kind, connection, resource, credential, baseUri, native, revision, receipts, storage, warehouse);
    }

    private static DataSourceKind ParseSourceKind(string value) => value.Trim().ToLowerInvariant() switch
    {
        "postgres" or "postgresql" => DataSourceKind.PostgreSql,
        "mysql" => DataSourceKind.MySql,
        "mariadb" => DataSourceKind.MariaDb,
        "sqlserver" or "sql-server" or "mssql" => DataSourceKind.SqlServer,
        "oracle" => DataSourceKind.Oracle,
        "sqlite" => DataSourceKind.Sqlite,
        "http" or "https" or "rest" => DataSourceKind.Http,
        "iceberg" => DataSourceKind.Iceberg,
        "kafka" => DataSourceKind.Kafka,
        "rabbit" or "rabbitmq" or "rabbit-mq" => DataSourceKind.RabbitMq,
        "rocket" or "rocketmq" or "rocket-mq" => DataSourceKind.RocketMq,
        "activemq" or "active-mq" or "activemqclassic" or "active-mq-classic" or
            "activemqartemis" or "active-mq-artemis" => DataSourceKind.ActiveMq,
        _ when Enum.TryParse<DataSourceKind>(value, true, out var kind) => kind,
        _ => throw new InvalidDataException($"Unknown data source kind '{value}'")
    };

    private static Dictionary<string, DataConfigurationJob> ReadJobs(JsonObject root,
        out Dictionary<string, string> revisions,
        out Dictionary<string, JobExecutionOptions> options,
        out Dictionary<string, JobSchedule[]> schedules)
    {
        revisions = new(StringComparer.Ordinal);
        options = new(StringComparer.Ordinal);
        schedules = new(StringComparer.Ordinal);
        var result = new Dictionary<string, DataConfigurationJob>(StringComparer.Ordinal);
        if (!TryGet(root, "jobs", out var node)) return result;

        if (node is JsonObject map)
        {
            foreach (var pair in map)
                AddJob(result, pair.Key, RequireObject(pair.Value, $"jobs.{pair.Key}"), revisions, options, schedules);
            return result;
        }
        if (node is not JsonArray array) throw new InvalidDataException("jobs must be a mapping or sequence");
        foreach (JsonNode? item in array)
        {
            JsonObject job = RequireObject(item, "jobs[]");
            string id = RequiredString(job, "id");
            AddJob(result, id, job, revisions, options, schedules);
        }
        return result;
    }

    private static void AddJob(Dictionary<string, DataConfigurationJob> result, string key, JsonObject input,
        Dictionary<string, string> revisions, Dictionary<string, JobExecutionOptions> options,
        Dictionary<string, JobSchedule[]> schedules)
    {
        string id = OptionalString(input, "id") ?? key;
        if (!string.Equals(key, id, StringComparison.Ordinal))
            throw new InvalidDataException($"Job mapping key '{key}' differs from package id '{id}'");
        if (!result.TryAdd(id, BuildJob(input, id, out var revision, out var execution, out var jobSchedules)))
            throw new InvalidDataException($"Duplicate job '{id}'");
        revisions[id] = revision;
        options[id] = execution;
        schedules[id] = jobSchedules;
    }

    private static DataConfigurationJob BuildJob(JsonObject input, string id, out string revision,
        out JobExecutionOptions execution, out JobSchedule[] schedules)
    {
        if (TryGet(input, "package", out _))
            throw new InvalidDataException($"Job '{id}' must declare Native Package fields directly; nested package objects are not supported");
        if (!input.Any(pair => !pair.Key.Equals("id", StringComparison.OrdinalIgnoreCase) && PackageFields.Contains(pair.Key)))
            return BuildSettingsJob(input, id, out revision, out execution, out schedules);
        JsonObject package = input.DeepClone().AsObject();
        JsonObject defaults = new();
        if (TryGet(package, "parameters", out var parameters))
            defaults = DefaultsFromDefinitions(RequireObject(parameters, $"job '{id}'.parameters"));
        JsonNode? explicitDefaults = TakeAny(package, "parameterDefaults", "defaults");
        if (explicitDefaults is not null)
            MergeParameters(defaults, RequireObject(explicitDefaults, $"job '{id}'.parameterDefaults"));
        JsonNode? executionNode = TakeAny(package, "execution", "executionOptions");
        bool topLevelExecution = executionNode is not null;
        JsonObject? packagePolicy = null;
        if (TryGet(package, "policy", out var existingPolicy))
            packagePolicy = RequireObject(existingPolicy, $"job '{id}'.policy");
        uint? policyParallelism = null;
        if (packagePolicy is not null)
        {
            if (TryGet(packagePolicy, "execution", out var policyExecution))
                executionNode ??= policyExecution;
            if (TryGet(packagePolicy, "maxConcurrency", out var maxConcurrency))
                policyParallelism = ParseUInt32(maxConcurrency, $"job '{id}'.policy.maxConcurrency");
        }
        var parsedExecution = executionNode is null ? new JobExecutionOptions() :
            ParseExecutionOptions(RequireObject(executionNode, $"job '{id}'.execution"), id);
        execution = policyParallelism is null ? parsedExecution : parsedExecution with { Parallelism = policyParallelism };
        if (topLevelExecution && packagePolicy is null)
            packagePolicy = new JsonObject();
        if (topLevelExecution)
        {
            JsonObject canonicalExecution = RequireObject(executionNode, $"job '{id}'.execution").DeepClone().AsObject();
            if (TryGet(canonicalExecution, "parallelism", out var parallelism))
            {
                if (packagePolicy is not null && TryGet(packagePolicy, "maxConcurrency", out _))
                    throw new InvalidDataException($"Job '{id}' declares both execution.parallelism and policy.maxConcurrency");
                packagePolicy!["maxConcurrency"] = parallelism!.DeepClone();
                canonicalExecution.Remove("parallelism");
            }
            if (packagePolicy is not null && TryGet(packagePolicy, "execution", out _))
                throw new InvalidDataException($"Job '{id}' declares execution policy more than once");
            packagePolicy!["execution"] = canonicalExecution;
            package["policy"] = packagePolicy;
        }
        JsonNode? schedulesNode = TakeAny(package, "schedules");
        schedules = schedulesNode is null ? [] : ParseSchedules(schedulesNode, id);
        revision = OptionalRevision(package, "expectedPackageRevision", "packageRevision", "revision") ?? "0";
        TakeAny(package, "expectedPackageRevision", "packageRevision", "revision");
        if (string.IsNullOrWhiteSpace(revision)) throw new InvalidDataException($"Job '{id}' has an empty package revision");
        if (!TryGet(package, "id", out _)) package["id"] = id;

        foreach (var pair in package)
            if (!PackageFields.Contains(pair.Key))
                throw new InvalidDataException($"Job '{id}' field '{pair.Key}' is not a canonical Native Package field");
        if (!TryGet(package, "version", out var version) || version is null ||
            ParseUnsigned(version, "version") != ProductInfo.Protocol)
            throw new InvalidDataException($"Job '{id}' must declare Native Package version {ProductInfo.Protocol}");
        if (OptionalString(package, "id") != id)
            throw new InvalidDataException($"Job '{id}' package identity does not match its declaration");
        string document = package.ToJsonString(ParameterJson);
        var parsed = UniversalData.Package.FromJson(document);
        if (!parsed.IsSuccess || parsed.Value is null)
            throw new InvalidDataException($"Job '{id}' contains an invalid Native Package: {parsed.Error?.Message ?? parsed.Code.ToString()}");
        return new DataConfigurationJob(document, defaults);
    }

    private static DataConfigurationJob BuildSettingsJob(JsonObject input, string id, out string revision,
        out JobExecutionOptions execution, out JobSchedule[] schedules)
    {
        JsonObject settings = input.DeepClone().AsObject();
        EnsureKnown(settings, $"job '{id}'", "id", "parameterDefaults", "defaults", "execution",
            "executionOptions", "schedules", "expectedPackageRevision", "packageRevision", "revision");

        JsonObject defaults = new();
        JsonNode? explicitDefaults = TakeAny(settings, "parameterDefaults", "defaults");
        if (explicitDefaults is not null)
            MergeParameters(defaults, RequireObject(explicitDefaults, $"job '{id}'.parameterDefaults"));

        JsonNode? executionNode = TakeAny(settings, "execution", "executionOptions");
        execution = executionNode is null ? new() :
            ParseExecutionOptions(RequireObject(executionNode, $"job '{id}'.execution"), id);
        JsonNode? schedulesNode = TakeAny(settings, "schedules");
        schedules = schedulesNode is null ? [] : ParseSchedules(schedulesNode, id);
        revision = OptionalRevision(settings, "expectedPackageRevision", "packageRevision", "revision") ?? "0";
        TakeAny(settings, "expectedPackageRevision", "packageRevision", "revision");
        if (string.IsNullOrWhiteSpace(revision)) throw new InvalidDataException($"Job '{id}' has an empty package revision");
        return new DataConfigurationJob(null, defaults);
    }

    private static JsonObject DefaultsFromDefinitions(JsonObject definitions)
    {
        var defaults = new JsonObject();
        foreach (var pair in definitions)
        {
            if (pair.Value is not JsonObject definition || !TryGet(definition, "default", out var value))
                continue;
            defaults[pair.Key] = value?.DeepClone();
        }
        return defaults;
    }

    private static JobExecutionOptions ParseExecutionOptions(JsonObject value, string jobId)
    {
        EnsureKnown(value, $"job '{jobId}'.execution", "batchRows", "edgeCapacity", "parallelism", "memoryBytes",
            "timeoutMilliseconds", "maxConnectionsPerSource", "messagePrefetch", "maxConcurrency", "connections");
        if (TryGet(value, "connections", out var connections))
        {
            JsonObject connectionLimits = RequireObject(connections, $"job '{jobId}'.execution.connections");
            EnsureKnown(connectionLimits, $"job '{jobId}'.execution.connections", "maxSessions");
        }
        return new(
            OptionalUInt32(value, "batchRows"), OptionalUInt32(value, "edgeCapacity"), OptionalUInt32(value, "parallelism"),
            OptionalUInt64(value, null, "memoryBytes"), OptionalUInt32(value, "timeoutMilliseconds"),
            OptionalUInt32(value, "maxConnectionsPerSource"), OptionalUInt32(value, "messagePrefetch"));
    }

    private static JobSchedule[] ParseSchedules(JsonNode node, string jobId)
    {
        if (node is not JsonArray array) throw new InvalidDataException($"Job '{jobId}'.schedules must be a sequence");
        var result = new List<JobSchedule>(array.Count);
        var ids = new HashSet<string>(StringComparer.Ordinal);
        foreach (JsonNode? item in array)
        {
            JsonObject scheduleNode = RequireObject(item, $"job '{jobId}'.schedules[]");
            JobSchedule schedule;
            try { schedule = scheduleNode.Deserialize<JobSchedule>(ScheduleJson) ?? throw new InvalidDataException("empty schedule"); }
            catch (Exception error) when (error is JsonException or InvalidOperationException or FormatException)
            { throw new InvalidDataException($"Job '{jobId}' has an invalid schedule", error); }
            if (string.IsNullOrWhiteSpace(schedule.Id) || !ids.Add(schedule.Id))
                throw new InvalidDataException($"Job '{jobId}' schedules must have unique non-empty ids");
            result.Add(schedule);
        }
        return result.ToArray();
    }

    private static DataConfigurationWorkerOptions ReadWorker(JsonObject root, string directory)
    {
        if (!TryGet(root, "worker", out var node)) return new();
        JsonObject value = RequireObject(node, "worker");
        EnsureKnown(value, "worker", "maxConcurrentSessions", "maxSessionsPerWorker", "maxBranchesPerSession",
            "maxConnectionsPerSource", "queueCapacity", "queueTimeout", "executionTimeout", "cancellationGrace",
            "memoryPerSession", "requireResourceIsolation", "workerPath");
        return new DataConfigurationWorkerOptions(
            OptionalInt32(value, "maxConcurrentSessions"), OptionalInt32(value, "maxSessionsPerWorker"),
            OptionalInt32(value, "maxBranchesPerSession"), OptionalInt32(value, "maxConnectionsPerSource"),
            OptionalInt32(value, "queueCapacity"), OptionalTimeSpan(value, "queueTimeout"),
            OptionalTimeSpan(value, "executionTimeout"), OptionalTimeSpan(value, "cancellationGrace"),
            OptionalUInt64(value, null, "memoryPerSession"), OptionalBooleanNullable(value, "requireResourceIsolation"),
            OptionalString(value, "workerPath") is { } path ? ResolvePath(path, directory) : null);
    }

    private static string[] ReadPaths(JsonObject root, string field, string directory)
    {
        if (!TryGet(root, field, out var node)) return [];
        var result = new List<string>();
        IEnumerable<(string Location, JsonNode? Node)> entries = node switch
        {
            JsonArray array => array.Select((item, index) => ($"{field}[{index}]", item)),
            JsonObject map => map.Select(pair => ($"{field}.{pair.Key}", pair.Value)),
            _ => throw new InvalidDataException($"{field} must be a mapping or sequence")
        };
        foreach (var entry in entries)
        {
            string path = entry.Node switch
            {
                JsonValue scalar when scalar.TryGetValue<string>(out var value) => value,
                JsonObject map => RequiredString(map, "path"),
                _ => throw new InvalidDataException($"{entry.Location} must be a path")
            };
            string resolved = ResolvePath(path, directory);
            if (!result.Contains(resolved, PathComparer)) result.Add(resolved);
        }
        return result.ToArray();
    }

    private static DataConfigurationManagement ReadManagement(JsonObject root)
    {
        if (!TryGet(root, "management", out var node)) return new DataConfigurationManagement(null, null, null, null, new(StringComparer.OrdinalIgnoreCase));
        JsonObject value = RequireObject(node, "management");
        EnsureKnown(value, "management", "baseUrl", "url", "bearerToken", "token", "cookie", "csrf", "headers");
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (TryGet(value, "headers", out var headerNode))
        {
            JsonObject headerMap = RequireObject(headerNode, "management.headers");
            foreach (var pair in headerMap)
                headers[pair.Key] = RequiredScalarString(pair.Value, $"management.headers.{pair.Key}");
        }
        return new DataConfigurationManagement(OptionalString(value, "baseUrl", "url"), OptionalString(value, "bearerToken", "token"),
            OptionalString(value, "cookie"), OptionalString(value, "csrf"), headers);
    }

    private static JsonObject ReadObject(string path)
    {
        string text;
        try { text = File.ReadAllText(path); }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException)
        { throw new InvalidDataException($"Unable to read configuration '{path}'", error); }
        try
        {
            var yaml = new YamlStream();
            using var reader = new StringReader(text);
            yaml.Load(reader);
            if (yaml.Documents.Count != 1) throw new InvalidDataException($"Configuration '{path}' must contain one YAML document");
            return ToJsonObject(yaml.Documents[0].RootNode, path);
        }
        catch (YamlException error)
        { throw new InvalidDataException($"Configuration '{path}' is not valid YAML", error); }
    }

    private static JsonObject ToJsonObject(YamlNode node, string path) =>
        ToJsonNode(node, path) as JsonObject ?? throw new InvalidDataException($"Configuration '{path}' must contain a mapping at its root");

    private static JsonNode? ToJsonNode(YamlNode node, string path)
    {
        switch (node)
        {
            case YamlMappingNode mapping:
            {
                var result = new JsonObject();
                foreach (var pair in mapping.Children)
                {
                    if (pair.Key is not YamlScalarNode key || string.IsNullOrWhiteSpace(key.Value))
                        throw new InvalidDataException($"Configuration '{path}' contains a non-scalar mapping key");
                    if (result.ContainsKey(key.Value))
                        throw new InvalidDataException($"Configuration '{path}' contains duplicate key '{key.Value}'");
                    result.Add(key.Value, ToJsonNode(pair.Value, path));
                }
                return result;
            }
            case YamlSequenceNode sequence:
            {
                var result = new JsonArray();
                foreach (YamlNode item in sequence.Children) result.Add(ToJsonNode(item, path));
                return result;
            }
            case YamlScalarNode scalar:
                return ToJsonScalar(scalar);
            default:
                throw new InvalidDataException($"Configuration '{path}' contains an unsupported YAML node");
        }
    }

    private static JsonNode? ToJsonScalar(YamlScalarNode scalar)
    {
        string value = scalar.Value ?? "";
        string style = scalar.Style.ToString();
        if (style.Contains("SingleQuoted", StringComparison.Ordinal) || style.Contains("DoubleQuoted", StringComparison.Ordinal))
            return JsonValue.Create(value);
        if (value is "" or "~" || value.Equals("null", StringComparison.OrdinalIgnoreCase)) return null;
        if (bool.TryParse(value, out bool boolean)) return JsonValue.Create(boolean);
        if (long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out long integer)) return JsonValue.Create(integer);
        if (decimal.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out decimal number)) return JsonValue.Create(number);
        return JsonValue.Create(value);
    }

    private static JsonSerializerOptions CreateScheduleJson()
    {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web) { PropertyNameCaseInsensitive = true };
        options.Converters.Add(new JsonStringEnumConverter());
        return options;
    }

    private static JsonObject RequireObject(JsonNode? node, string location) =>
        node as JsonObject ?? throw new InvalidDataException($"{location} must be a mapping");

    private static bool TryGet(JsonObject value, string name, out JsonNode? node)
    {
        string[] matches = value.Select(pair => pair.Key)
            .Where(key => key.Equals(name, StringComparison.OrdinalIgnoreCase)).ToArray();
        if (matches.Length > 1)
            throw new InvalidDataException($"Duplicate case-insensitive field '{name}'");
        if (matches.Length == 0) { node = null; return false; }
        string matched = matches[0];
        node = value[matched];
        return true;
    }

    private static JsonNode? TakeAny(JsonObject value, params string[] names)
    {
        var found = names.Where(name => TryGet(value, name, out _)).ToArray();
        if (found.Length > 1) throw new InvalidDataException($"Only one of {string.Join(", ", names)} may be specified");
        if (found.Length == 0) return null;
        string key = value.Select(pair => pair.Key).Single(name => name.Equals(found[0], StringComparison.OrdinalIgnoreCase));
        JsonNode? result = value[key];
        value.Remove(key);
        return result;
    }

    private static void EnsureKnown(JsonObject value, string location, params string[] allowed)
    {
        foreach (string key in value.Select(pair => pair.Key))
            if (!allowed.Any(name => name.Equals(key, StringComparison.OrdinalIgnoreCase)))
                throw new InvalidDataException($"{location} contains unknown field '{key}'");
    }

    private static string RequiredString(JsonObject value, params string[] names) =>
        OptionalString(value, names) ?? throw new InvalidDataException($"Required field '{names[0]}' is missing");

    private static string? OptionalString(JsonObject value, params string[] names)
    {
        JsonNode? node = null;
        string? found = null;
        foreach (string name in names)
            if (TryGet(value, name, out var candidate))
            {
                if (found is not null) throw new InvalidDataException($"Only one of {string.Join(", ", names)} may be specified");
                found = name; node = candidate;
            }
        if (found is null || node is null) return null;
        return RequiredScalarString(node, found);
    }

    private static string? OptionalRevision(JsonObject value, params string[] names)
    {
        if (!TryGetAny(value, names, out var node) || node is null) return null;
        if (node is JsonValue scalar)
        {
            if (scalar.TryGetValue<string>(out var text) && !string.IsNullOrWhiteSpace(text)) return text;
            if (scalar.TryGetValue<ulong>(out var number)) return number.ToString(CultureInfo.InvariantCulture);
            if (scalar.TryGetValue<long>(out var signed) && signed >= 0) return signed.ToString(CultureInfo.InvariantCulture);
        }
        throw new InvalidDataException($"Field '{names[0]}' must be a package revision string or integer");
    }

    private static string RequiredScalarString(JsonNode? node, string location)
    {
        if (node is JsonValue scalar && scalar.TryGetValue<string>(out var value) && !string.IsNullOrWhiteSpace(value)) return value;
        throw new InvalidDataException($"Field '{location}' must be a non-empty string");
    }

    private static Uri? OptionalUri(JsonObject value, params string[] names)
    {
        string? text = OptionalString(value, names);
        if (text is null) return null;
        if (!Uri.TryCreate(text, UriKind.Absolute, out var uri) || uri.Scheme is not ("http" or "https") ||
            uri.UserInfo.Length != 0 || uri.Query.Length != 0 || uri.Fragment.Length != 0)
            throw new InvalidDataException($"Field '{names[0]}' must be an absolute HTTP URI without credentials or query");
        return uri;
    }

    private static uint? OptionalUInt32(JsonObject value, params string[] names)
    {
        if (!TryGetAny(value, names, out var node)) return null;
        ulong number = ParseUnsigned(node, names[0]);
        if (number is 0 or > uint.MaxValue) throw new InvalidDataException($"Field '{names[0]}' is outside UInt32 range");
        return (uint)number;
    }

    private static ulong? OptionalUInt64(JsonObject value, ulong? defaultValue, params string[] names)
    {
        if (!TryGetAny(value, names, out var node)) return defaultValue;
        ulong number = ParseUnsigned(node, names[0]);
        if (number == 0) throw new InvalidDataException($"Field '{names[0]}' must be greater than zero");
        return number;
    }

    private static int? OptionalInt32(JsonObject value, params string[] names)
    {
        if (!TryGetAny(value, names, out var node)) return null;
        ulong number = ParseUnsigned(node, names[0]);
        if (number is 0 or > int.MaxValue) throw new InvalidDataException($"Field '{names[0]}' is outside positive Int32 range");
        return (int)number;
    }

    private static bool OptionalBoolean(JsonObject value, bool defaultValue, params string[] names) =>
        OptionalBooleanNullable(value, names) ?? defaultValue;

    private static bool? OptionalBooleanNullable(JsonObject value, params string[] names)
    {
        if (!TryGetAny(value, names, out var node)) return null;
        if (node is JsonValue scalar && scalar.TryGetValue<bool>(out var result)) return result;
        throw new InvalidDataException($"Field '{names[0]}' must be boolean");
    }

    private static TimeSpan? OptionalTimeSpan(JsonObject value, params string[] names)
    {
        if (!TryGetAny(value, names, out var node)) return null;
        if (node is JsonValue scalar && scalar.TryGetValue<string>(out var text) &&
            TimeSpan.TryParse(text, CultureInfo.InvariantCulture, out var parsed) && parsed > TimeSpan.Zero)
            return parsed;
        ulong milliseconds = ParseUnsigned(node, names[0]);
        if (milliseconds == 0 || milliseconds > uint.MaxValue * 1000UL)
            throw new InvalidDataException($"Field '{names[0]}' is not a valid positive duration");
        return TimeSpan.FromMilliseconds(milliseconds);
    }

    private static bool TryGetAny(JsonObject value, string[] names, out JsonNode? node)
    {
        string? found = null;
        node = null;
        foreach (string name in names)
            if (TryGet(value, name, out var candidate))
            {
                if (found is not null) throw new InvalidDataException($"Only one of {string.Join(", ", names)} may be specified");
                found = name; node = candidate;
            }
        return found is not null;
    }

    private static ulong ParseUnsigned(JsonNode? node, string location)
    {
        if (node is JsonValue scalar)
        {
            if (scalar.TryGetValue<ulong>(out var value)) return value;
            if (scalar.TryGetValue<long>(out var signed) && signed > 0) return (ulong)signed;
            if (scalar.TryGetValue<string>(out var text) && ulong.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out value)) return value;
        }
        throw new InvalidDataException($"Field '{location}' must be a positive integer");
    }

    private static uint ParseUInt32(JsonNode? node, string location)
    {
        ulong value = ParseUnsigned(node, location);
        if (value > uint.MaxValue) throw new InvalidDataException($"Field '{location}' is outside UInt32 range");
        return (uint)value;
    }

    private static string ResolvePath(string path, string directory) =>
        Path.GetFullPath(Path.IsPathFullyQualified(path) ? path : Path.Combine(directory, path));

    private static void AddOverride(Dictionary<string, JsonElement> overrides, string assignment)
    {
        int separator = assignment.IndexOf('=');
        if (separator <= 0) throw new ArgumentException("Parameter overrides must use name=value");
        string key = assignment[..separator].Trim();
        if (key.Length == 0 || !overrides.TryAdd(key, ParseOverrideValue(assignment[(separator + 1)..].Trim())))
            throw new ArgumentException($"Duplicate or empty parameter override '{key}'");
    }

    private static string ReadOptionValue(string[] args, ref int index, string? inline, string option)
    {
        string value = inline ?? (++index < args.Length ? args[index] : throw new ArgumentException($"{option} requires a value"));
        if (string.IsNullOrWhiteSpace(value)) throw new ArgumentException($"{option} requires a non-empty value");
        return value;
    }

    private static bool TryOption(string argument, string option, out string? inline)
    {
        if (argument.Equals(option, StringComparison.Ordinal)) { inline = null; return true; }
        string prefix = option + "=";
        if (argument.StartsWith(prefix, StringComparison.Ordinal)) { inline = argument[prefix.Length..]; return true; }
        inline = null;
        return false;
    }

    private static StringComparer PathComparer => OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;

}
