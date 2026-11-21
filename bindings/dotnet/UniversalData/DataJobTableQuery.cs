using System.Linq.Expressions;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace UniversalData;

/// <summary>A bounded query compiled into a Native parameterized SQL source.</summary>
public sealed class DataJobTableQuery<TInput, TRow>
{
    private readonly DataJob<TInput> job;
    private readonly DataJobTableDefinition definition;

    internal DataJobTableQuery(DataJob<TInput> job, DataJobTableDefinition definition)
    {
        this.job = job;
        this.definition = definition;
    }

    public DataJobTableQuery<TInput, TRow> Where(Expression<Func<TRow, bool>> predicate) => Filter(predicate);

    public DataJobTableQuery<TInput, TRow> Where(Expression<Func<TRow, TInput, bool>> predicate) => Filter(predicate);

    public DataJobTableQuery<TInput, TRow> OrderBy(Expression<Func<TRow, object?>> key) => Sort(key, false, false);

    public DataJobTableQuery<TInput, TRow> OrderByDescending(Expression<Func<TRow, object?>> key) =>
        Sort(key, true, false);

    public DataJobTableQuery<TInput, TRow> ThenBy(Expression<Func<TRow, object?>> key) => Sort(key, false, true);

    public DataJobTableQuery<TInput, TRow> ThenByDescending(Expression<Func<TRow, object?>> key) => Sort(key, true, true);

    public DataJobTableQuery<TInput, TRow> Skip(long count) => Range(DataJobRange.Constant("skip", count));

    public DataJobTableQuery<TInput, TRow> Skip(Expression<Func<TInput, long>> count) =>
        Range(DataJobRange.Parameter("skip", count, job.Id, Step));

    public DataJobTableQuery<TInput, TRow> Take(long count) => Range(DataJobRange.Constant("take", count));

    public DataJobTableQuery<TInput, TRow> Take(Expression<Func<TInput, long>> count) =>
        Range(DataJobRange.Parameter("take", count, job.Id, Step));

    public DataJobTableQuery<TInput, TOutput> Select<TOutput>(Expression<Func<TRow, TOutput>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var projection = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var field in new NativeRuleExpression(job.Id, Step, selector).Projection(typeof(TOutput), direct: true))
        {
            var column = field!["expression"]!["column"]!.GetValue<string>();
            if (!projection.TryAdd(field["name"]!.GetValue<string>(), Resolve(definition.Projection, column)))
                throw Error("projection aliases must be distinct");
        }
        return new(job, definition with { Projection = projection });
    }

    public DataJobQuery<TInput, TRow> Return(string? step = null)
    {
        if (!definition.Ranges.Any(range => range.Op == "take"))
            throw Error("a table page requires Take");
        var parameters = new SqlTableQueryParameters();
        var query = BuildQuery(parameters);
        var graph = job.Graph;
        graph.Use(definition.Source, DataJobSourceRole.Database);
        string flow = graph.StartFlow();
        string source = graph.Step(definition.Step, "query");
        graph.Plan.SqlSource(source, definition.Source, definition.Table, null, DataJob<TInput>.Schema<TRow>(),
            true, null, parameters.Build(), flow, schemaPolicy: "projection", tableQuery: query);
        return new DataJobRows<TInput, TRow>(job, flow, source).Return(step);
    }

    private string Step => definition.Step ?? "query";

    private DataJobTableQuery<TInput, TRow> Filter(LambdaExpression predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        if (definition.Ranges.Length != 0) throw Error("Where must precede Skip and Take");
        return new(job, definition with { Filters = [.. definition.Filters, (predicate, definition.Projection)] });
    }

    private DataJobTableQuery<TInput, TRow> Sort(LambdaExpression selector, bool descending, bool then)
    {
        ArgumentNullException.ThrowIfNull(selector);
        if (definition.Ranges.Length != 0) throw Error("ordering must precede Skip and Take");
        if (then != (definition.Order.Length != 0))
            throw Error(then ? "ThenBy requires OrderBy" : "use ThenBy to add another ordering key");
        var column = Resolve(definition.Projection, new NativeRuleExpression(job.Id, Step, selector).Key(direct: true));
        if (definition.Order.Any(key => string.Equals(key.Column, column, StringComparison.OrdinalIgnoreCase)))
            throw Error("ordering keys must be distinct");
        return new(job, definition with { Order = [.. definition.Order, (column, descending)] });
    }

    private DataJobTableQuery<TInput, TRow> Range(DataJobRange range) =>
        new(job, definition with { Ranges = [.. definition.Ranges, range] });

    private JsonObject BuildQuery(SqlTableQueryParameters parameters)
    {
        var projection = new JsonArray();
        foreach (var member in EntityMapping.For<TRow>().Members)
            projection.Add(new JsonObject { ["column"] = Resolve(definition.Projection, member.ColumnName),
                ["name"] = member.ColumnName });
        var order = new JsonArray();
        foreach (var key in definition.Order)
            order.Add(new JsonObject { ["column"] = key.Column, ["descending"] = key.Descending });
        var query = new JsonObject { ["projection"] = projection, ["order"] = order,
            ["ranges"] = JsonSerializer.SerializeToNode(definition.Ranges, WorkerProtocol.Json) };
        JsonNode? where = null;
        foreach (var filter in definition.Filters)
        {
            var rule = new NativeRuleExpression(job.Id, Step, filter.Rule)
                .SqlPredicate(parameters, field => Resolve(filter.Projection, field));
            where = where is null ? rule : new JsonObject { ["op"] = "and", ["args"] = new JsonArray(where, rule) };
        }
        if (where is not null) query["where"] = where;
        return query;
    }

    private string Resolve(IReadOnlyDictionary<string, string>? projection, string name) => projection is null
        ? name : projection.TryGetValue(name, out var source) ? source : throw Error($"'{name}' has no source field");

    private ArgumentException Error(string message) => new($"Job '{job.Id}', step '{Step}': {message}.");
}
