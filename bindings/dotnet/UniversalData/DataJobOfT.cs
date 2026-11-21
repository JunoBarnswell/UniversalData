using System.Linq.Expressions;
using System.Text.Json;

namespace UniversalData;

public sealed class DataJob<TInput> : DataJob
{
    internal DataJob(DataJobGraph graph) : base(graph) { }

    public DataJob<TInput> WithOptions(JobExecutionOptions options)
    {
        Graph.Options = options ?? throw new ArgumentNullException(nameof(options));
        return this;
    }

    /// <summary>Bind job input members to a Native SQL parameter batch in declaration order.</summary>
    public SqlParameterBatch BindParameters(params Expression<Func<TInput, object?>>[] fields)
    {
        ArgumentNullException.ThrowIfNull(fields);
        if (fields.Length == 0) throw new ArgumentException("At least one input member is required", nameof(fields));
        var mapping = EntityMapping.For<TInput>();
        var selected = fields.Select(selector =>
        {
            string column = new NativeRuleExpression(Id, "sql parameters", selector).Key();
            return mapping.Members.Single(member => member.ColumnName == column);
        }).ToArray();
        var row = new Dictionary<string, Dictionary<string, string>>(StringComparer.Ordinal);
        foreach (var member in selected)
            if (!row.TryAdd(member.ColumnName, new() { ["$parameter"] = member.Property.Name }))
                throw new ArgumentException("SQL parameter members must be distinct", nameof(fields));
        using var schema = JsonDocument.Parse(EntityMapping.FormatSchema(selected));
        return new(schema.RootElement.Clone(), [JsonSerializer.SerializeToElement(row, WorkerProtocol.Json)]);
    }

    public DataJobRows<TInput, TRow> FromTable<TRow>(string source, string? table = null, string? step = null,
        bool stream = false)
    {
        Graph.Use(source, DataJobSourceRole.Database);
        string flow = Graph.StartFlow();
        string name = Graph.Step(step, "read");
        var mapping = EntityMapping.For<TRow>();
        Graph.Plan.SqlSource(name, source, table ?? mapping.TableName, null, Schema<TRow>(includeKeys: true), stream,
            null, null, flow, schemaPolicy: "projection");
        return new(this, flow, name);
    }

    public DataJobRows<TInput, TRow> FromSql<TRow>(string source, string statement,
        SqlParameterBatch? parameters = null, string? step = null, bool stream = false)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(statement);
        Graph.Use(source, DataJobSourceRole.Database);
        string flow = Graph.StartFlow();
        string name = Graph.Step(step, "query");
        Graph.Plan.SqlSource(name, source, null, statement, Schema<TRow>(), stream, null, parameters, flow);
        return new(this, flow, name);
    }

    public DataJobRows<TInput, TRow> FromHttp<TRow>(string source, string path, string? step = null,
        string? rowsField = null, string? cursorField = null, string? cursorParameter = null,
        HttpHeaderDefinition[]? headers = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        Graph.Use(source, DataJobSourceRole.Http);
        string flow = Graph.StartFlow();
        string name = Graph.Step(step, "http");
        Graph.Plan.Component(name, "rest.source", new
        {
            connection = source, schema = Schema<TRow>(), url = path, rowsField, cursorField, cursorParameter, headers
        }, flow);
        return new(this, flow, name);
    }

    public DataJobRows<TInput, TRow> FromMessages<TRow>(string source, string address, string? group = null,
        bool queue = false, string initialPosition = "require", string? step = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(address);
        Graph.Use(source, DataJobSourceRole.MessageBroker);
        string flow = Graph.StartFlow();
        string name = Graph.Step(step, "receive");
        Graph.Plan.Component(name, "mq.source", new
        {
            connection = source, address, targetType = queue ? "queue" : "topic", group, initialPosition,
            format = "json", schema = Schema<TRow>()
        }, flow);
        return new(this, flow, name);
    }

    public DataJob<TInput> ExecuteSql(string source, string statement, SqlParameterBatch? parameters = null,
        string? step = null, JobStepCondition after = JobStepCondition.Success)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(statement);
        Graph.Use(source, DataJobSourceRole.Database);
        string name = Graph.Step(step, "sql");
        Graph.Plan.ExecuteSql(name, source, statement, parameters);
        Graph.Sequence(name, after);
        return this;
    }

    public DataJob<TInput> Request(HttpRequestDefinition request, string? step = null,
        JobStepCondition after = JobStepCondition.Success)
    {
        ArgumentNullException.ThrowIfNull(request);
        Graph.Use(request.Connection, DataJobSourceRole.Http);
        string name = Graph.Step(step, "request");
        Graph.Plan.HttpTask(name, request);
        Graph.Sequence(name, after);
        return this;
    }

    /// <summary>Declare independent branches. The delegates add Native steps during authoring.</summary>
    public DataJob<TInput> Parallel(params Action<DataJob<TInput>>[] branches)
    {
        ArgumentNullException.ThrowIfNull(branches);
        Graph.Parallel(branches.Select(branch => (Action)(() => branch(this))).ToArray());
        return this;
    }

    public DataJob<TInput> Transaction(params string[] participants)
    {
        Graph.Transaction(participants);
        return this;
    }

    public DataJob<TInput> KafkaTransaction(string checkpointTopic, uint checkpointPartition = 0)
    {
        Graph.KafkaTransaction(checkpointTopic, checkpointPartition);
        return this;
    }

    internal static JsonElement Schema<TRow>(bool includeKeys = false)
    {
        using var document = JsonDocument.Parse(EntityMapping.For<TRow>().SchemaJson(includeKeys: includeKeys));
        return document.RootElement.Clone();
    }
}
