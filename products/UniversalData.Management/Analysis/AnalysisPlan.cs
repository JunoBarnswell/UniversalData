using System.Globalization;
using System.Text.Json;
using UniversalData.Hosting;

namespace UniversalData.Management;

internal sealed record AnalysisInput(string Kind, string Label, string? FileId, string Fingerprint, DateTimeOffset DataAsOf,
    AnalysisIcebergSource? Iceberg = null);
internal sealed record AnalysisColumn(string Id, string Name, string Type);
internal sealed record AnalysisSourceSchema(AnalysisColumn[] Columns, JsonElement Schema, JsonElement Options,
    string[] Sheets, string? Sheet);
internal sealed record AnalysisDraft(AnalysisModel Model, AnalysisSourceSchema Source);
internal sealed record AnalysisAsset(AnalysisRelease Release, string File, string ContentHash, JsonElement Schema,
    string OperationId, string ExecutionId, string SourceRunReceipt, AnalysisIcebergSource? Source);

/// <summary>Binds the analysis contract to the existing Package component graph.</summary>
internal sealed class AnalysisPlan(string dataset)
{
    private readonly EtlPackageBuilder plan = new("analysis-" + dataset);
    private static readonly JsonSerializerOptions Json = EtlPackageBuilder.Json;

    internal string Empty(string input)
    {
        plan.Connection("input", "file", input);
        return Build();
    }

    private void Connect(string name, AnalysisConnection connection) =>
        plan.Connection(name, connection.Driver, connection.Resource, connection.CredentialReference);

    internal string Source(AnalysisInput input, AnalysisSourceSchema schema, string? path, AnalysisModel? model = null)
    {
        const string source = "source";
        if (input.Iceberg is { } iceberg)
        {
            Connect("catalog", iceberg.Catalog);
            Connect("storage", iceberg.Storage);
            plan.IcebergSource(source, "catalog", "storage", iceberg.Namespace, iceberg.Table,
                iceberg.Schema, long.Parse(iceberg.SnapshotId, CultureInfo.InvariantCulture), model is null ? 32U : 1024U, iceberg.Warehouse);
            return iceberg.RowFilter is { } where ? Filter(source, "source-scope", where) : source;
        }

        plan.Connection("input", "file", path!);
        if (input.Kind == "xlsx")
        {
            int[]? textColumns = model is null ? null : schema.Columns
                .Select((field, index) => (field, index))
                .Where(pair => model.Fields.Any(rule => rule.FieldId == pair.field.Id && rule.Role == "identifier"))
                .Select(pair => pair.index).ToArray();
            plan.Component(source, "file.xlsx.source", new
            {
                connection = "input", schema.Schema, schema.Sheet, header = true, formulaPolicy = "reject",
                maxDecompressedBytes = 150 * 1024 * 1024,
                strictTable = true, textColumns, batchSize = model is null ? 32 : 256, maxCells = 6700000, maxStringBytes = 65536
            });
        }
        else
            plan.Component(source, "file.csv.source", new
            {
                connection = "input", schema.Schema, fileFormat = schema.Options, batchSize = model is null ? 32 : 512
            });
        return source;
    }

    internal string Sample(string source)
    {
        Sink(source, "sample-result");
        return Build();
    }

    internal string Prepare(string source, string asset, AnalysisDraft draft)
    {
        var model = draft.Model;
        var rules = model.Fields.Where(field => field.Role != "omit").ToDictionary(field => field.FieldId);
        plan.Component("model", "analysis.prepare", new
        {
            fields = draft.Source.Columns.Where(column => rules.ContainsKey(column.Id)).Select(column =>
                new { source = column.Name, column.Id, rules[column.Id].Role }),
            model.DateFormat, model.OffsetMinutes, model.AmountFormat, model.Currency, model.AllowEmptyAmount
        }).Connect(source, "model");
        plan.Connection("published", "file", asset)
            .Component("publish", "file.ndjson.destination", new { connection = "published", fileMode = "create" })
            .Connect("model", "publish");
        Sink("model", "validation-result", 1);
        return Build();
    }

    private string Scan(AnalysisAsset asset, string path, AnalysisQuery query)
    {
        plan.Connection("published", "file", path)
            .Component("scan", "file.ndjson.source", new { connection = "published", asset.Schema, batchSize = 1024 });
        var filters = (query.Filters ?? []).Select(filter => Predicate(
            filter.FieldId, filter.Operator, filter.Value)).ToArray();
        return filters.Length == 0 ? "scan" : Filter("scan", "filter", filters.Aggregate((left, right) => Op("and", left, right)));
    }

    internal string Query(AnalysisAsset asset, string path, AnalysisQuery query)
    {
        string scan = Scan(asset, path, query);
        uint port = 0;
        var groups = Groupings(query);
        for (int index = 0; index < groups.Length; index++)
        {
            if (index == groups.Length - 1)
            {
                Aggregate(scan, port, "group-" + index, asset, query, groups[index]);
                break;
            }
            string fork = "fork-" + index;
            plan.Component(fork, "multicast", new { }).Connect(scan, fork, port);
            Aggregate(fork, 0, "group-" + index, asset, query, groups[index]);
            scan = fork;
            port = 1;
        }
        return Build();
    }

    internal static AnalysisDimension[][] Groupings(AnalysisQuery query) => query.Dimensions.Length switch
    {
        0 => [[]],
        1 => [[], query.Dimensions],
        _ => [[], [query.Dimensions[0]], [query.Dimensions[1]], query.Dimensions]
    };

    private void Aggregate(string source, uint port, string id, AnalysisAsset asset, AnalysisQuery query,
        AnalysisDimension[] dimensions)
    {
        string? amount = asset.Release.Measures.FirstOrDefault(item => item.Id == "amount")?.FieldId;
        string? order = asset.Release.Measures.FirstOrDefault(item => item.Id == "orders")?.FieldId;
        string[] measures = query.Measures!;
        var aggregates = new List<object> { new { function = "count", column = "*", name = "records" } };
        if (amount is not null && (measures.Contains("amount") || measures.Contains("average")))
            aggregates.Add(new { function = "sum", column = amount, name = "amount" });
        if (order is not null && (measures.Contains("orders") || measures.Contains("average")))
            aggregates.Add(new { function = "count", column = order, name = "orders", distinct = true, maxDistinct = 100000 });

        string[] keys = dimensions.Select(Key).ToArray();
        plan.Component(id, "aggregate", new { plan = new { version = ProductInfo.Protocol, group = keys, aggregates } })
            .Connect(source, id, port);
        string input = id;
        if (measures.Contains("average"))
        {
            input = id + "-average";
            var money = asset.Schema.GetProperty("fields").EnumerateArray().Single(field =>
                field.GetProperty("name").GetString() == amount);
            var denominator = new
            {
                cast = new { name = "orders", type = "decimal128", nullable = false,
                    precision = money.GetProperty("precision").GetInt32(), scale = money.GetProperty("scale").GetInt32() },
                value = Col("orders")
            };
            plan.Component(input, "derive", new
            {
                columns = new[] { new { name = "average", expression = Op("div", Col("amount"), denominator) } }
            }).Connect(id, input);
        }

        if (keys.Length != 0)
        {
            string sorted = id + "-sort";
            string[] sort = query.SortMeasure is null ? keys : [query.SortMeasure, .. keys];
            bool[] directions = sort.Select(key => key == query.SortMeasure && query.Descending).ToArray();
            plan.Component(sorted, "sort", new { keys = sort, keyDirections = directions }).Connect(input, sorted);
            input = Range(sorted, id + "-limit", query.Limit + 1);
        }
        Sink(input, id + "-result");
    }

    internal string Details(AnalysisAsset asset, string path, AnalysisQuery query, string?[]? cell, int after, int limit)
    {
        string scan = Scan(asset, path, query);
        object condition = Op("gt", Col("rowKey"), new { literal = after, type = "int64" });
        if (cell is not null)
            for (int index = 0; index < cell.Length; index++)
                condition = Op("and", condition, Predicate(Key(query.Dimensions[index]), "eq", cell[index],
                    query.Dimensions[index].Level));
        var selected = Filter(scan, "detail-filter", condition);
        plan.Component("ordered", "sort", new { keys = new[] { "rowKey" } }).Connect(selected, "ordered");
        Sink(Range("ordered", "detail-limit", limit + 1), "detail-result");
        return Build();
    }

    private static object Predicate(string field, string comparison, string? value, string level = "value")
    {
        if (value is null)
            return Op("isnull", Col(field));
        return Op(comparison switch { "from" => "ge", "before" => "lt", _ => "eq" }, Col(field),
            new { literal = value, type = level != "value" || comparison is "from" or "before" ? "date32" : "utf8" });
    }

    private string Filter(string source, string id, object condition)
    {
        plan.Component(id, "split", new { condition }).Connect(source, id);
        plan.Component(id + "-discard", "discard", new { }).Connect(id, id + "-discard", 1);
        return id;
    }

    private string Range(string source, string id, int count)
    {
        plan.Component(id, "rowRange", new { ranges = new[] { new { op = "take", count } } }).Connect(source, id);
        return id;
    }

    private void Sink(string source, string id, uint port = 0) =>
        plan.Component(id, "result.destination", new { }).Connect(source, id, port);

    private string Build()
    {
        var result = plan.Build();
        var package = result.Value ?? throw new UniversalDataException((int)result.Code, "Invalid analysis plan");
        return package.ToJson();
    }

    internal static string Key(AnalysisDimension dimension) => dimension.Level == "month" ? "calendarMonth" : dimension.FieldId;
    private static object Col(string name) => new { column = name };
    private static object Op(string op, params object[] args) => new { op, args };
    internal static JsonElement Document(object value) => JsonSerializer.SerializeToElement(value, Json);
}
