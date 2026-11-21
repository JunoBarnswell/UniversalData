using System.Text.Json;

namespace UniversalData;

internal sealed class DataJobGraph(string id, Type argument)
{
    private readonly Dictionary<string, DataJobSourceRole> sources = new(StringComparer.Ordinal);
    private readonly Dictionary<string, (string Source, string Pipeline, DataJobChanges Options)> changeSources = new(StringComparer.Ordinal);
    private readonly Dictionary<string, (string Source, string Pipeline, string? ExplicitStorage)> storageComponents = new(StringComparer.Ordinal);
    private readonly HashSet<string> steps = new(StringComparer.Ordinal);
    private readonly EtlPackageBuilder plan = new(id);
    private readonly object compilation = new();
    private string[] commands = [];
    private string? activeFlow;
    private bool returnsResults;
    private string[]? participants;
    private JobExecutionOptions? options;
    private CompiledDefinition? local, published;
    internal string Id { get; } = ValidId(id);
    internal JobExecutionOptions? Options
    {
        get => options;
        set { options = value; Invalidate(); }
    }

    internal string Step(string? requested, string kind)
    {
        string name = requested is null ? $"{kind}-{steps.Count + 1}" : ValidId(requested);
        if (!steps.Add(name)) throw new ArgumentException($"Job '{Id}' already declares step '{name}'.");
        Invalidate();
        return name;
    }

    internal void Use(string source, DataJobSourceRole kind)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(source);
        if (sources.TryGetValue(source, out var previous) && previous != kind)
            throw new ArgumentException($"Job '{Id}' uses resource '{source}' with incompatible connector types.");
        sources[source] = kind;
    }

    internal string StartFlow()
    {
        if (activeFlow is not null) return activeFlow;
        string name = Step(null, "flow");
        plan.DataFlow(name, name);
        Sequence(name);
        return activeFlow = name;
    }

    internal EtlPackageBuilder Plan => plan;

    internal void EndFlow() => activeFlow = null;

    internal void ChangeSource(string source, string step, string table, JsonElement schema,
        DataJobChanges options, string pipeline)
    {
        Use(source, DataJobSourceRole.Database);
        changeSources[step] = (source, pipeline, options);
        plan.ChangeSource(step, source, table, schema, options, pipeline);
    }

    internal void Incremental(string pipeline, string step, object config) =>
        plan.SetComponentConfig(pipeline, step, "incremental", config);

    internal void ComponentConfig(string pipeline, string step, string property, object? value) =>
        plan.SetComponentConfig(pipeline, step, property, value);

    internal void ComponentType(string pipeline, string step, string type) =>
        plan.SetComponentType(pipeline, step, type);

    internal void StorageComponent(string source, string step, string pipeline, string? explicitStorage)
    {
        storageComponents[step] = (source, pipeline, explicitStorage);
        Use(source, DataJobSourceRole.Iceberg);
    }

    internal void Sequence(string name, JobStepCondition condition = JobStepCondition.Success)
    {
        string outcome = condition switch
        {
            JobStepCondition.Success => "success",
            JobStepCondition.Failure => "failure",
            JobStepCondition.Completion => "completion",
            _ => throw new ArgumentOutOfRangeException(nameof(condition))
        };
        if (commands.Length == 0 && condition != JobStepCondition.Success)
            throw new ArgumentException($"Job '{Id}', step '{name}' has no predecessor for {outcome}.");
        foreach (string prior in commands) plan.Before(prior, name, outcome);
        commands = [name];
        activeFlow = null;
    }

    internal void Parallel(Action[] branches)
    {
        if (branches.Length < 2) throw new ArgumentException("At least two branches are required", nameof(branches));
        string[] begin = commands;
        var ends = new List<string>(branches.Length);
        foreach (var branch in branches)
        {
            commands = begin;
            activeFlow = null;
            branch();
            if (ReferenceEquals(commands, begin)) throw new ArgumentException($"Job '{Id}' has an empty parallel branch.");
            ends.AddRange(commands);
        }
        commands = ends.ToArray();
        activeFlow = null;
    }

    internal void Return()
    {
        if (returnsResults) throw new ArgumentException($"Job '{Id}' already has a typed result stream.");
        returnsResults = true;
    }

    internal void Transaction(string[] selected)
    {
        if (selected.Length == 0 || selected.Distinct(StringComparer.Ordinal).Count() != selected.Length)
            throw new ArgumentException($"Job '{Id}' needs distinct transaction participants.");
        foreach (var name in selected) Use(name, DataJobSourceRole.Database);
        participants = selected.ToArray();
        Invalidate();
    }

    internal void KafkaTransaction(string checkpointTopic, uint partition)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(checkpointTopic);
        if (activeFlow is null) throw new ArgumentException($"Job '{Id}' has no current Kafka data flow.");
        plan.Delivery(activeFlow, new { mode = "kafkaTransaction", checkpointTopic, checkpointPartition = partition });
        Invalidate();
    }

    private void Invalidate()
    {
        lock (compilation) local = published = null;
    }

    internal Package Compile(IReadOnlyDictionary<string, SdkSourceBinding> inventory, bool publish,
        JobExecutionOptions? configured = null)
    {
        var limits = Combine(configured);
        lock (compilation)
        {
            var cached = publish ? published : local;
            if (cached is not null && cached.Options == limits && ReferenceEquals(cached.Sources, inventory))
                return cached.Package;
            var compiled = new CompiledDefinition(inventory, limits, CompileDefinition(inventory, publish, limits));
            if (publish) published = compiled; else local = compiled;
            return compiled.Package;
        }
    }

    private Package CompileDefinition(IReadOnlyDictionary<string, SdkSourceBinding> available, bool publish, JobExecutionOptions? options)
    {
        if (commands.Length == 0)
            throw new ArgumentException($"Job '{Id}' has no Native steps.");
        var connections = new List<SdkNativeConnection>(sources.Count + 2);
        var connectionIds = new HashSet<string>(StringComparer.Ordinal);
        foreach (var (name, kind) in sources.OrderBy(item => item.Key, StringComparer.Ordinal))
        {
            if (!available.TryGetValue(name, out var binding) || !binding.Enabled || !Supports(binding.Definition, kind))
                throw new UniversalDataException((int)StatusCode.Auth,
                    $"Job '{Id}' cannot access resource '{name}' with its declared connector type.");
            var definition = binding.Definition;
            string driver = definition.Kind switch
            {
                DataSourceKind.Http or DataSourceKind.Iceberg => "http",
                DataSourceKind.S3 => "s3",
                _ => "ud:" + SdkNativeSources.DriverName(definition.Kind)
            };
            string? credential = definition.Kind == DataSourceKind.Http ? null : publish
                ? definition.CredentialReference : SdkNativeSources.CredentialReference(binding);
            if (publish && definition.Kind is not (DataSourceKind.Http or DataSourceKind.Iceberg) &&
                string.IsNullOrWhiteSpace(credential))
                throw new ArgumentException($"Job '{Id}' requires a host credential reference for '{name}'.");
            AddConnection(connections, connectionIds, new(name, driver, definition.Resource, credential,
                checked((int)(options?.MaxConnectionsPerSource ?? 8))));
            if (definition.Kind == DataSourceKind.Iceberg)
            {
                var storageRequests = storageComponents.Values
                    .Where(component => component.Source == name)
                    .Select(component => component.ExplicitStorage is { } explicitStorage
                        ? (Name: explicitStorage, Named: true)
                        : definition.StorageConnection is { } configuredStorage
                            ? (Name: configuredStorage, Named: true)
                            : (Name: name + "-storage", Named: false))
                    .ToList();
                if (storageRequests.Count == 0)
                    storageRequests.Add(definition.StorageConnection is { } configuredStorage
                        ? (configuredStorage, true) : (name + "-storage", false));
                foreach (var storageRequest in storageRequests
                    .GroupBy(request => request.Name, StringComparer.Ordinal)
                    .Select(group => group.Any(request => request.Named)
                        ? (Name: group.Key, Named: true) : (Name: group.Key, Named: false)))
                {
                    if (!storageRequest.Named)
                    {
                        if (definition.StorageRoot is not { } root)
                            throw new ArgumentException($"Job '{Id}' requires a named Iceberg storage connection for '{name}'.");
                        string storageDriver = Uri.TryCreate(root, UriKind.Absolute, out var uri) &&
                            uri.Scheme is "http" or "https" ? "s3" : "file";
                        AddConnection(connections, connectionIds, new(storageRequest.Name, storageDriver, root,
                            storageDriver == "s3" && publish ? definition.CredentialReference :
                            storageDriver == "s3" ? SdkNativeSources.CredentialReference(binding) : null,
                            checked((int)(options?.MaxConnectionsPerSource ?? 8))));
                        continue;
                    }
                    if (!available.TryGetValue(storageRequest.Name, out var storageBinding) || !storageBinding.Enabled ||
                        !storageBinding.Definition.IsObjectStorage)
                        throw new UniversalDataException((int)StatusCode.Auth,
                            $"Job '{Id}' cannot access Iceberg storage connection '{storageRequest.Name}'.");
                    var storageDefinition = storageBinding.Definition;
                    string? storageCredential = publish ? storageDefinition.CredentialReference :
                        SdkNativeSources.CredentialReference(storageBinding);
                    if (publish && string.IsNullOrWhiteSpace(storageCredential))
                        throw new ArgumentException($"Job '{Id}' requires a host credential reference for '{storageRequest.Name}'.");
                    AddConnection(connections, connectionIds, new(storageRequest.Name, "s3", storageDefinition.Resource,
                        storageCredential, checked((int)(options?.MaxConnectionsPerSource ?? 8))));
                }
            }
        }
        foreach (var (step, pending) in changeSources)
            if (available.TryGetValue(pending.Source, out var binding))
                ComponentType(pending.Pipeline, step, CdcComponent(binding.Definition.Kind));
        foreach (var (step, pending) in storageComponents)
            if (available.TryGetValue(pending.Source, out var binding) &&
                binding.Definition.StorageConnection is { } configured && pending.ExplicitStorage is null)
                ComponentConfig(pending.Pipeline, step, "storageConnection", configured);
        var execution = options is null ? null : new
        {
            options.BatchRows, options.EdgeCapacity, options.MemoryBytes, options.TimeoutMilliseconds,
            options.MaxConnectionsPerSource, options.MessagePrefetch
        };
        var policy = new
        {
            maxConcurrency = options?.Parallelism, execution,
            transaction = participants is null ? null : new
            {
                mode = "required", participants,
                isolation = participants.Length == 1 &&
                    available[participants[0]].Definition.Kind == DataSourceKind.Sqlite
                        ? "serializable" : "readCommitted",
                provisionReceipts = participants.All(participant => available[participant].Definition.ProvisionTransactionReceipts)
            }
        };
        var result = plan.Build(connections, DescribeParameters(), policy);
        return result.Value ?? throw new UniversalDataException((int)result.Code, $"Job '{Id}' cannot be compiled", result.Error);
    }

    private static bool Supports(DataSourceDefinition source, DataJobSourceRole kind) => kind switch
    {
        DataJobSourceRole.Database => source.IsDatabase,
        DataJobSourceRole.Http => source.Kind == DataSourceKind.Http,
        DataJobSourceRole.MessageBroker => source.IsMessageBroker,
        DataJobSourceRole.Iceberg => source.Kind == DataSourceKind.Iceberg,
        _ => false
    };

    private static string CdcComponent(DataSourceKind kind) => kind switch
    {
        DataSourceKind.PostgreSql => "postgresql.cdc.source",
        DataSourceKind.MySql => "mysql.cdc.source",
        DataSourceKind.MariaDb => "mariadb.cdc.source",
        DataSourceKind.SqlServer => "sqlserver.cdc.source",
        DataSourceKind.Oracle => "oracle.cdc.source",
        _ => throw new UniversalDataException((int)StatusCode.Unsupported, "The provider has no Native CDC source")
    };

    private static void AddConnection(List<SdkNativeConnection> connections,
        HashSet<string> ids, SdkNativeConnection connection)
    {
        if (ids.Add(connection.Id)) connections.Add(connection);
    }

    private JobExecutionOptions? Combine(JobExecutionOptions? configured)
    {
        if (configured is null) return Options;
        if (Options is null) return configured;
        return new(configured.BatchRows ?? Options.BatchRows, configured.EdgeCapacity ?? Options.EdgeCapacity,
            configured.Parallelism ?? Options.Parallelism, configured.MemoryBytes ?? Options.MemoryBytes,
            configured.TimeoutMilliseconds ?? Options.TimeoutMilliseconds,
            configured.MaxConnectionsPerSource ?? Options.MaxConnectionsPerSource,
            configured.MessagePrefetch ?? Options.MessagePrefetch);
    }

    internal string Parameters(object? value)
    {
        if (argument == typeof(object)) return "{}";
        if (value is null) throw new ArgumentNullException(nameof(value));
        var values = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var member in EntityMapping.For(argument).Members)
        {
            object? field = member.Get(value);
            values.Add(member.Property.Name, field is decimal amount
                ? amount.ToString(System.Globalization.CultureInfo.InvariantCulture) : field);
        }
        return JsonSerializer.Serialize(values, WorkerProtocol.Json);
    }

    private Dictionary<string, JsonElement> DescribeParameters()
    {
        var definitions = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
        if (argument == typeof(object)) return definitions;
        foreach (var member in EntityMapping.For(argument).Members)
        {
            var definition = new Dictionary<string, object>
            {
                ["type"] = SqlValueCodec.NativeTypeName(member.NativeType), ["nullable"] = member.Nullable
            };
            if (member.NativeType == DataType.Decimal128)
            {
                definition.Add("precision", member.Precision);
                definition.Add("scale", member.Scale);
            }
            definitions.Add(member.Property.Name, JsonSerializer.SerializeToElement(definition, WorkerProtocol.Json));
        }
        return definitions;
    }

    private static string ValidId(string value)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Length > 100 ||
            value.Any(character => !char.IsAsciiLetterOrDigit(character) && character is not ('-' or '_' or '.')))
            throw new ArgumentException("Job and step names must be identifiers.");
        return value;
    }

    private sealed record CompiledDefinition(IReadOnlyDictionary<string, SdkSourceBinding> Sources,
        JobExecutionOptions? Options, Package Package);
}
