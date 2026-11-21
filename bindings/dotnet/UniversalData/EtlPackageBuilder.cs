using System.Text.Json;
using System.Text.Json.Serialization;

namespace UniversalData;

public sealed class EtlPackageBuilder(string id)
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
        { DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull };

    private readonly List<object> connections = [];
    private readonly List<object> components = [];
    private readonly List<object> paths = [];
    private readonly List<object> tasks = [];
    private readonly List<object> constraints = [];
    private readonly List<JsonElement> modules = [];
    private readonly Dictionary<string, JsonElement> variables = new(StringComparer.Ordinal);
    private readonly HashSet<string> connectionIds = new(StringComparer.Ordinal);
    private readonly HashSet<string> componentIds = new(StringComparer.Ordinal);
    private readonly HashSet<string> taskIds = new(StringComparer.Ordinal);
    private readonly List<(string From, string To)> componentEdges = [];
    private readonly List<(string From, string To)> taskEdges = [];
    private string? error;
    private bool hasDataFlow;

    public EtlPackageBuilder Connection(string name, string driver, string resource,
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
        JsonElement schema, bool stream, uint? batchSize, SqlParameterBatch? parameters) =>
        Component(name, "sql.source", new { connection, table, sql, schema,
            readMode = stream ? "stream" : "snapshot", batchSize, parameters,
            schemaPolicy = table is null ? null : new { policy = "compatible" } });

    public EtlPackageBuilder ChangeSource(string name, string connection, DataSourceKind provider, string table,
        JsonElement schema, EtlChangeOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        string component = provider switch
        {
            DataSourceKind.PostgreSql => "postgresql.cdc.source",
            DataSourceKind.MySql => "mysql.cdc.source",
            DataSourceKind.MariaDb => "mariadb.cdc.source",
            DataSourceKind.SqlServer => "sqlserver.cdc.source",
            DataSourceKind.Oracle => "oracle.cdc.source",
            _ => throw new UniversalDataException((int)StatusCode.Unsupported, "The provider has no Native CDC source")
        };
        if ((provider is DataSourceKind.MySql or DataSourceKind.MariaDb) && options.ServerId is null or 0)
            throw new ArgumentException("MySQL and MariaDB CDC require a distinct replication server ID", nameof(options));
        if (options.Position is not null)
            ArgumentException.ThrowIfNullOrWhiteSpace(options.Position);
        return Component(name, component, new
        {
            connection, table, schema,
            mode = options.Position is null ? "snapshotAndStream" : "stream",
            options.Position, options.Slot, options.Publication, options.ServerId,
            options.MaxTransactions, options.MicroBatchRows,
            microBatchMs = options.MicroBatchMilliseconds, schemaPolicy = new { policy = "compatible" }
        });
    }

    public EtlPackageBuilder RestSource(string name, string connection, JsonElement schema,
        string? url = null, HttpHeaderDefinition[]? headers = null, uint batchSize = 4096,
        string? caConnection = null, string? rowsField = null, string? cursorField = null,
        string? cursorParameter = null) => Component(name, "rest.source", new { connection, schema,
            url, headers, batchSize, caConnection, rowsField, cursorField, cursorParameter });

    public EtlPackageBuilder CSharpTransform(string name, string registeredComponent, JsonElement configuration) =>
        Component(name, registeredComponent, configuration);

    public EtlPackageBuilder SqlDestination(string name, string connection, string table, string mode = "append") =>
        SqlDestination(name, connection, table, mode, null, null, null, null);

    internal EtlPackageBuilder SqlDestination(string name, string connection, string table, string mode,
        SqlColumnMapping[]? mapping, int[]? keys, string? delivery, bool? provisionReceipts) =>
        Component(name, "sql.destination", new { connection, table, mode, mapping, keys, delivery,
            provisionReceipts, beforeField = mode == "changeset" ? "before" : null });

    public EtlPackageBuilder IcebergDestination(string name, string connection, string storageConnection,
        string tableNamespace, string table, string mode = "append", int[]? keys = null,
        string? warehouse = null, string? beforeField = null) =>
        Component(name, "iceberg.destination", new
        {
            connection, storageConnection, @namespace = tableNamespace, table, mode,
            keys = keys is { Length: > 0 } ? keys : null, warehouse, beforeField
        });

    public EtlPackageBuilder IcebergSource(string name, string connection, string storageConnection,
        string tableNamespace, string table, JsonElement schema, long? snapshotId = null,
        uint? batchSize = null, string? warehouse = null) =>
        Component(name, "iceberg.source", new
        {
            connection, storageConnection, @namespace = tableNamespace, table, schema, snapshotId, batchSize, warehouse
        });

    public EtlPackageBuilder SqlCommand(string name, string connection, string statement,
        IReadOnlyList<uint> parameters) => Component(name, "sql.command", new { connection, statement, parameters });

    public EtlPackageBuilder ExecuteSql(string name, string connection, string statement,
        SqlParameterBatch? parameters = null) => Task(name, "sql.execute", new { connection, statement, parameters });

    public EtlPackageBuilder HttpTask(string name, HttpRequestDefinition request) => Task(name, "http.request", request);

    public EtlPackageBuilder DataFlow(string name = "run")
    {
        hasDataFlow = true;
        return Task(name, "dataFlow", new { pipeline = "main" });
    }

    public EtlPackageBuilder Before(string before, string after)
    {
        taskEdges.Add((before, after));
        constraints.Add(new { from = before, to = after, @on = "success" });
        return this;
    }

    public EtlPackageBuilder Connect(string from, string to, uint output = 0, uint input = 0)
    {
        componentEdges.Add((from, to));
        paths.Add(new { from, to, output, input });
        return this;
    }

    public EtlPackageBuilder RegisterModule(JsonElement module) { modules.Add(module.Clone()); return this; }
    public EtlPackageBuilder Variable(string name, JsonElement definition) { variables.Add(name, definition.Clone()); return this; }

    internal JsonElement DescribeDataFlow() => JsonSerializer.SerializeToElement(Pipeline(), Json);

    private object Pipeline() => new { id = "main", components, paths };

    public Result<Package> Build()
    {
        if (error is null && componentEdges.Any(edge => !componentIds.Contains(edge.From) || !componentIds.Contains(edge.To)))
            error = "A data-flow path references an unknown component";
        bool implicitFlow = !hasDataFlow && components.Count != 0 && tasks.Count == 0;
        if (error is null && hasDataFlow != (components.Count != 0) && !implicitFlow)
            error = "A data-flow task and its components must be defined together";
        if (error is null && taskEdges.Any(edge => !taskIds.Contains(edge.From) || !taskIds.Contains(edge.To)))
            error = "A control-flow constraint references an unknown task";
        if (error is not null)
            return new(StatusCode.Schema, null, new(ErrorSource.Native, StatusCode.Schema, error));
        var controlTasks = implicitFlow
            ? new object[] { new { id = "run", kind = "dataFlow", config = new { pipeline = "main" } } }
            : tasks.ToArray();
        return Package.FromJson(JsonSerializer.Serialize(new
        {
            version = ProductInfo.Protocol, id, connections,
            controlFlow = new { tasks = controlTasks, constraints },
            dataFlows = components.Count == 0 ? Array.Empty<object>() : new[] { Pipeline() },
            extensions = modules.Count == 0 ? null : modules, variables
        }, Json));
    }

    private EtlPackageBuilder Component(string name, string type, object config)
    {
        if (string.IsNullOrWhiteSpace(name) || !componentIds.Add(name))
            error = "Component identities must be unique";
        components.Add(new { id = name, component = type, config });
        return this;
    }
    private EtlPackageBuilder Task(string name, string kind, object config)
    {
        if (string.IsNullOrWhiteSpace(name) || !taskIds.Add(name))
            error = "Task identities must be unique";
        tasks.Add(new { id = name, kind, config });
        return this;
    }
}
