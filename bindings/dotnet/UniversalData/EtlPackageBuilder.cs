using System.Text.Json;
using System.Text.Json.Serialization;

namespace UniversalData;

internal sealed class EtlPackageBuilder(string id)
{
    internal static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
        { DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull };

    private readonly List<object> connections = [];
    private readonly Dictionary<string, EtlDataFlowDefinition> flows = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> componentFlows = new(StringComparer.Ordinal);
    private readonly List<object> tasks = [];
    private readonly List<object> constraints = [];
    private readonly List<JsonElement> modules = [];
    private readonly Dictionary<string, object> caches = new(StringComparer.Ordinal);
    private readonly Dictionary<string, JsonElement> variables = new(StringComparer.Ordinal);
    private readonly HashSet<string> connectionIds = new(StringComparer.Ordinal);
    private readonly HashSet<string> taskIds = new(StringComparer.Ordinal);
    private readonly List<(string Pipeline, string From, string To)> componentEdges = [];
    private readonly List<(string From, string To)> taskEdges = [];
    private readonly HashSet<string> taskPipelines = new(StringComparer.Ordinal);
    private string? error;

    internal EtlPackageBuilder Connection(string name, string driver, string resource,
        string? credentialReference = null, OAuth2Connection? oauth2 = null)
    {
        if (string.IsNullOrWhiteSpace(name) || !connectionIds.Add(name))
            error = "Connection identities must be unique";
        connections.Add(new { id = name, driver, resource, credentialReference, oauth2 });
        return this;
    }

    public EtlPackageBuilder SqlSource(string name, string connection, string sql,
        JsonElement schema, bool stream = false, uint batchSize = 4096, SqlParameterBatch? parameters = null) =>
        SqlSource(name, connection, null, sql, schema, stream, batchSize, parameters);

    internal EtlPackageBuilder SqlSource(string name, string connection, string? table, string? sql,
        JsonElement schema, bool stream, uint? batchSize, SqlParameterBatch? parameters, string pipeline = "main",
        string schemaPolicy = "compatible", System.Text.Json.Nodes.JsonObject? tableQuery = null) =>
        Component(name, "sql.source", new { connection, table, sql, schema,
            readMode = stream ? "stream" : "snapshot", batchSize, parameters, tableQuery,
            schemaPolicy = table is null ? null : new { policy = schemaPolicy } }, pipeline);

    internal EtlPackageBuilder ChangeSource(string name, string connection, string table,
        JsonElement schema, DataJobChanges options, string pipeline = "main")
    {
        ArgumentNullException.ThrowIfNull(options);
        return Component(name, "cdc.source", new
        {
            connection, table, schema,
            mode = options.Mode ?? (options.Position is null ? "snapshotAndStream" : "stream"),
            options.Position, options.Slot, options.Publication, options.ServerId,
            options.MaxTransactions, options.MicroBatchRows,
            microBatchMs = options.MicroBatchMilliseconds, appendOnly = options.AppendOnly,
            schemaPolicy = new { policy = "compatible" }
        }, pipeline);
    }

    internal EtlPackageBuilder RestSource(string name, string connection, JsonElement schema,
        string? url = null, HttpHeaderDefinition[]? headers = null, uint batchSize = 4096,
        string? caConnection = null, string? rowsField = null, string? cursorField = null,
        string? cursorParameter = null) => Component(name, "rest.source", new { connection, schema,
            url, headers, batchSize, caConnection, rowsField, cursorField, cursorParameter });

    internal EtlPackageBuilder CSharpTransform(string name, string registeredComponent, JsonElement configuration) =>
        Component(name, registeredComponent, configuration);

    internal EtlPackageBuilder SqlDestination(string name, string connection, string table, string mode = "append") =>
        SqlDestination(name, connection, table, mode, null, null, null, null);

    internal EtlPackageBuilder SqlDestination(string name, string connection, string table, string mode,
        SqlColumnMapping[]? mapping, int[]? keys, string? delivery, bool? provisionReceipts, string pipeline = "main") =>
        Component(name, "sql.destination", new { connection, table, mode, mapping, keys, delivery,
            provisionReceipts }, pipeline);

    internal EtlPackageBuilder IcebergDestination(string name, string connection, string storageConnection,
        string tableNamespace, string table, string mode = "append", int[]? keys = null,
        string? warehouse = null, string? beforeField = null, string pipeline = "main") =>
        Component(name, "iceberg.destination", new
        {
            connection, storageConnection, @namespace = tableNamespace, table, mode,
            keys = keys is { Length: > 0 } ? keys : null, warehouse, beforeField
        }, pipeline);

    internal EtlPackageBuilder IcebergSource(string name, string connection, string storageConnection,
        string tableNamespace, string table, JsonElement schema, long? snapshotId = null,
        uint? batchSize = null, string? warehouse = null, string pipeline = "main") =>
        Component(name, "iceberg.source", new
        {
            connection, storageConnection, @namespace = tableNamespace, table, schema, snapshotId, batchSize, warehouse
        }, pipeline);

    internal EtlPackageBuilder SqlCommand(string name, string connection, string statement,
        IReadOnlyList<uint> parameters) => Component(name, "sql.command", new { connection, statement, parameters });

    internal EtlPackageBuilder ExecuteSql(string name, string connection, string statement,
        SqlParameterBatch? parameters = null) => Task(name, "sql.execute", new { connection, statement, parameters });

    internal EtlPackageBuilder HttpTask(string name, HttpRequestDefinition request) => Task(name, "http.request", request);

    public EtlPackageBuilder DataFlow(string name = "run") => DataFlow(name, "main");

    internal EtlPackageBuilder DataFlow(string name, string pipeline)
    {
        taskPipelines.Add(pipeline);
        return Task(name, "dataFlow", new { pipeline });
    }

    internal EtlPackageBuilder Before(string before, string after, string outcome = "success")
    {
        taskEdges.Add((before, after));
        constraints.Add(new { from = before, to = after, @on = outcome });
        return this;
    }

    internal EtlPackageBuilder Connect(string from, string to, uint output = 0, uint input = 0) =>
        Connect("main", from, to, output, input);

    internal EtlPackageBuilder Connect(string pipeline, string from, string to, uint output = 0, uint input = 0)
    {
        componentEdges.Add((pipeline, from, to));
        Flow(pipeline).Paths.Add(new { from, to, output, input });
        return this;
    }

    internal EtlPackageBuilder RegisterModule(JsonElement module) { modules.Add(module.Clone()); return this; }
    internal EtlPackageBuilder Variable(string name, JsonElement definition) { variables.Add(name, definition.Clone()); return this; }

    internal JsonElement DescribeDataFlow() => JsonSerializer.SerializeToElement(Flow("main").Describe(), Json);

    public Result<Package> Build() => Build(null, null, null);

    internal Result<Package> Build(IReadOnlyList<SdkNativeConnection>? boundConnections,
        IReadOnlyDictionary<string, JsonElement>? parameters, object? policy)
    {
        var pipelines = flows.Values.Where(flow => flow.Components.Count != 0).ToArray();
        string? validation = error;
        if (validation is null && componentEdges.Any(edge => componentFlows.GetValueOrDefault(edge.From) != edge.Pipeline ||
            componentFlows.GetValueOrDefault(edge.To) != edge.Pipeline))
            validation = "A data-flow path references a component outside its pipeline";
        bool implicitFlow = tasks.Count == 0 && pipelines is [{ Id: "main" }];
        if (validation is null && !implicitFlow && !taskPipelines.SetEquals(pipelines.Select(flow => flow.Id)))
            validation = "Each data-flow task must name a declared pipeline and each pipeline must have a task";
        if (validation is null && taskEdges.Any(edge => !taskIds.Contains(edge.From) || !taskIds.Contains(edge.To)))
            validation = "A control-flow constraint references an unknown task";
        if (validation is not null)
            return new(StatusCode.Schema, null, new(ErrorSource.Native, StatusCode.Schema, validation));
        var controlTasks = implicitFlow
            ? new object[] { new { id = "run", kind = "dataFlow", config = new { pipeline = "main" } } }
            : tasks.ToArray();
        return Package.FromJson(JsonSerializer.Serialize(new
        {
            version = ProductInfo.Protocol, id, connections = boundConnections is null ? (object)connections : boundConnections,
            controlFlow = new { tasks = controlTasks, constraints },
            dataFlows = pipelines.Select(flow => flow.Describe()).ToArray(),
            extensions = modules.Count == 0 ? null : modules, variables, parameters, policy,
            caches = caches.Count == 0 ? null : caches.Values.ToArray()
        }, Json));
    }

    internal EtlPackageBuilder Component(string name, string type, object config, string pipeline = "main")
    {
        if (string.IsNullOrWhiteSpace(name) || !componentFlows.TryAdd(name, pipeline))
            error = "Component identities must be unique";
        Flow(pipeline).Components.Add(new { id = name, component = type, config });
        return this;
    }

    internal void Delivery(string pipeline, object delivery) => Flow(pipeline).Delivery = delivery;

    internal void Cache(string name, string sourceConnection, JsonElement schema, int[] keys)
    {
        if (!caches.TryAdd(name, new { id = name, sourceConnection, schema, keys, scope = "execution" }))
            throw new ArgumentException($"Cache '{name}' is already declared", nameof(name));
    }

    internal void SetComponentConfig(string pipeline, string component, string property, object? value) =>
        Flow(pipeline).Extend(component, property, value);

    internal bool HasOutputConnection(string pipeline, string component) =>
        componentEdges.Any(edge => edge.Pipeline == pipeline && edge.From == component);

    internal void SetComponentType(string pipeline, string component, string type) =>
        Flow(pipeline).SetComponentType(component, type);

    private EtlDataFlowDefinition Flow(string pipeline)
    {
        if (!flows.TryGetValue(pipeline, out var flow))
            flows.Add(pipeline, flow = new(pipeline));
        return flow;
    }

    internal EtlPackageBuilder Task(string name, string kind, object config)
    {
        if (string.IsNullOrWhiteSpace(name) || !taskIds.Add(name))
            error = "Task identities must be unique";
        tasks.Add(new { id = name, kind, config });
        return this;
    }
}
