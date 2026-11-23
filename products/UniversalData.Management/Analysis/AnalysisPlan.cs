using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using UniversalData.Hosting;

namespace UniversalData.Management;

internal sealed record AnalysisInput(string Kind, string Label, string? FileId, string Fingerprint, DateTimeOffset DataAsOf,
    AnalysisIcebergSource? Iceberg = null, AnalysisSqlSource? Sql = null);
internal sealed record AnalysisColumn(string Id, string Name, string Type);
internal sealed record AnalysisSourceSchema(AnalysisColumn[] Columns, JsonElement Schema, JsonElement Options,
    string[] Sheets, string? Sheet);
internal sealed record AnalysisDraft(AnalysisModel Model, AnalysisSourceSchema Source);
internal sealed record AnalysisAsset(AnalysisRelease Release, string File, string ContentHash, JsonElement Schema,
    string OperationId, string ExecutionId, string SourceRunReceipt, AnalysisIcebergSource? Source,
    AnalysisSqlSource? Sql = null, AnalysisShard[]? Shards = null);

/// <summary>Binds the analysis contract to the existing Package component graph.</summary>
internal sealed class AnalysisPlan(string dataset, string? scratch = null)
{
    internal const string BatchMarkerField = "__ud_analysis_batch";
    private readonly EtlPackageBuilder plan = new("analysis-" + dataset);
    private static readonly JsonSerializerOptions Json = EtlPackageBuilder.Json;

    internal string Empty(string input)
    {
        plan.Connection("input", "file", input);
        return Build();
    }

    internal string Schema(AnalysisSqlSource source)
    {
        Connect("input", source.Connection);
        return Build();
    }

    private void Connect(string name, AnalysisConnection connection) =>
        plan.Connection(name, connection.Driver, connection.Resource, connection.CredentialReference);

    internal string Source(AnalysisInput input, AnalysisSourceSchema schema, string? path, AnalysisModel? model = null,
        bool stream = false)
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

        if (input.Sql is { } sql)
        {
            Connect("input", sql.Connection);
            // A bounded preview and a one-pass publication both need the
            // provider cursor. Snapshot mode intentionally materializes every
            // row for replay, which would turn either operation into a full
            // table in-memory snapshot. PostgreSQL's cursor runs inside one
            // repeatable-read transaction, preserving one-read consistency.
            plan.SqlSource(source, "input", sql.Table, null, schema.Schema,
                stream, batchSize: model is null ? 1024U : 4096U, parameters: null);
            return source;
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

    internal string Prepare(string source, string asset, AnalysisDraft draft, uint maxRows = 100000,
        bool sql = false)
        => Prepare(source, [asset], draft, maxRows, sql);

    internal string Prepare(string source, IReadOnlyList<string> assets, AnalysisDraft draft,
        uint maxRows = 100000, bool sql = false)
    {
        if (assets.Count == 0)
            throw new ArgumentException("At least one publication asset is required", nameof(assets));
        if (maxRows is < 1 or > 30000000)
            throw new ArgumentOutOfRangeException(nameof(maxRows));
        var model = draft.Model;
        var rules = model.Fields.Where(field => field.Role != "omit").ToDictionary(field => field.FieldId);
        plan.Component("model", "analysis.prepare", new
        {
            fields = draft.Source.Columns.Where(column => rules.ContainsKey(column.Id)).Select(column =>
                new { source = column.Name, column.Id, rules[column.Id].Role }),
            model.DateFormat, model.OffsetMinutes, model.AmountFormat, model.Currency, model.AllowEmptyAmount,
            maxRows
        }).Connect(source, "model");
        string destination = Path.GetExtension(assets[0]).Equals(".parquet", StringComparison.OrdinalIgnoreCase)
            ? "file.parquet.destination" : "file.ndjson.destination";
        if (assets.Count == 1)
        {
            plan.Connection("published", "file", assets[0])
                .Component("publish", destination, new { connection = "published", fileMode = "create" })
                .Connect("model", "publish");
        }
        else
        {
            var branches = Enumerable.Range(0, assets.Count).Select(index => (object)new
            {
                id = "shard" + index.ToString(CultureInfo.InvariantCulture),
                name = "shard" + index.ToString(CultureInfo.InvariantCulture),
                condition = Op("eq",
                    Op("mod", Col("rowKey"), new { literal = assets.Count, type = "int64" }),
                    new { literal = index, type = "int64" })
            }).ToArray();
            plan.Component("partition", "split", new { branches, @default = "discard" })
                .Connect("model", "partition");
            for (int index = 0; index < assets.Count; index++)
            {
                string connection = "published-" + index.ToString(CultureInfo.InvariantCulture);
                string publish = "publish-" + index.ToString(CultureInfo.InvariantCulture);
                string fan = "shard-fan-" + index.ToString(CultureInfo.InvariantCulture);
                string count = "shard-count-" + index.ToString(CultureInfo.InvariantCulture);
                string tag = "shard-count-tag-" + index.ToString(CultureInfo.InvariantCulture);
                string countResult = "shard-count-result-" + index.ToString(CultureInfo.InvariantCulture);
                plan.Component(fan, "multicast", new { outputs = new[] { "publish", "count" } })
                    .Connect("partition", fan, (uint)index);
                plan.Connection(connection, "file", assets[index])
                    .Component(publish, destination, new { connection, fileMode = "create" })
                    .Connect(fan, publish);
                plan.Component(count, "aggregate", new
                {
                    plan = new
                    {
                        version = ProductInfo.Protocol,
                        group = Array.Empty<string>(),
                        aggregates = new[] { new { function = "count", column = "*", name = "rows" } }
                    }
                }).Connect(fan, count, 1);
                plan.Component(tag, "derive", new
                {
                    columns = new[] { new { name = "shard", expression = new { literal = index, type = "int64" } } }
                }).Connect(count, tag);
                Sink(tag, countResult);
            }
            plan.Component("discard", "discard", new { }).Connect("partition", "discard",
                (uint)assets.Count);
            plan.Component("partition-errors", "discard", new { }).Connect("partition", "partition-errors",
                (uint)assets.Count + 1);
        }
        Sink("model", "validation-result", 1);
        return Build(sql ? SpillPolicy(assets) : null);
    }

    private object SpillPolicy(IReadOnlyList<string> assets)
    {
        string root = Path.GetPathRoot(Path.GetFullPath(assets[0])) ??
            throw new UniversalDataException((int)StatusCode.Resource, "无法确定 SQL 发布目标卷。");
        long available;
        try
        {
            available = new DriveInfo(root).AvailableFreeSpace;
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException or ArgumentException)
        {
            throw new UniversalDataException((int)StatusCode.Resource,
                "无法读取 SQL 发布目标卷的可用空间。");
        }
        const long minimumReserve = 512L * 1024 * 1024;
        long reserve = Math.Max(minimumReserve, available / 5);
        if (available <= reserve)
            throw new UniversalDataException((int)StatusCode.Resource,
                "SQL 发布目标卷可用空间不足，无法安全分配临时计算空间。");
        ulong spillBytes = checked((ulong)(available - reserve));
        if (scratch is null) return new { buffers = new { spillBytes } };
        plan.Connection("query-spill", "file", scratch);
        return new { buffers = new { connection = "query-spill", spillBytes } };
    }

    private string Scan(AnalysisAsset asset, IReadOnlyList<string> paths, AnalysisQuery query, bool drill = false)
    {
        if (paths.Count == 0)
            throw new UniversalDataException((int)StatusCode.Incompatible, "已发布数据没有可读取的物理分片。");
        bool parquet = paths.All(path => Path.GetExtension(path).Equals(".parquet", StringComparison.OrdinalIgnoreCase));
        if (paths.Any(path => Path.GetExtension(path).Equals(".parquet", StringComparison.OrdinalIgnoreCase) != parquet))
            throw new UniversalDataException((int)StatusCode.Incompatible, "同一报表版本的物理分片格式不一致。");
        JsonElement schema = parquet ? ProjectSchema(asset, query, drill) : asset.Schema;
        (int? from, int? before) = MonthBounds(query);
        var scans = new List<string>(paths.Count);
        for (int index = 0; index < paths.Count; index++)
        {
            string suffix = index == 0 ? "" : "-" + index.ToString(CultureInfo.InvariantCulture);
            string connection = "published" + suffix;
            string scan = "scan" + suffix;
            plan.Connection(connection, "file", paths[index])
                .Component(scan, parquet ? "file.parquet.source" : "file.ndjson.source",
                parquet
                    ? new { connection, schema, projectColumns = true,
                        monthFrom = from, monthBefore = before, batchSize = 4096 }
                    : new { connection, schema, batchSize = 4096 });
            scans.Add(scan);
        }
        string source = scans[0];
        for (int index = 1; index < scans.Count; index++)
        {
            string union = "union-" + index.ToString(CultureInfo.InvariantCulture);
            plan.Component(union, "unionAll", new { })
                .Connect(source, union)
                .Connect(scans[index], union, 0, 1);
            source = union;
        }
        var filters = (query.Filters ?? []).Select(filter => Predicate(
            filter.FieldId, filter.Operator, filter.Value)).ToArray();
        return filters.Length == 0 ? source : Filter(source, "filter", filters.Aggregate((left, right) => Op("and", left, right)));
    }

    private string Scan(AnalysisAsset asset, string path, AnalysisQuery query, bool drill = false) =>
        Scan(asset, [path], query, drill);

    private static JsonElement ProjectSchema(AnalysisAsset asset, AnalysisQuery query, bool drill)
    {
        if (asset.Schema.ValueKind != JsonValueKind.Object ||
            !asset.Schema.TryGetProperty("fields", out var fields) || fields.ValueKind != JsonValueKind.Array)
            throw new UniversalDataException((int)StatusCode.Schema, "已发布数据的列式结构无效。");

        var required = new HashSet<string>(StringComparer.Ordinal);
        if (drill)
        {
            foreach (var field in asset.Release.Model.Fields.Where(field => field.Role != "omit"))
                required.Add(field.FieldId);
            required.Add("rowKey");
            if (asset.Release.Model.Fields.Any(field => field.Role == "date"))
                required.Add("calendarMonth");
        }
        else
        {
            foreach (var dimension in query.Dimensions)
                required.Add(Key(dimension));
            foreach (var filter in query.Filters ?? [])
                required.Add(filter.FieldId);
            foreach (string measure in query.Measures ?? [])
                AddMetricFields(asset.Release.Model, asset.Release.Measures, measure, required,
                    new HashSet<string>(StringComparer.Ordinal));
        }

        var selected = fields.EnumerateArray()
            .Where(field => field.TryGetProperty("name", out var name) &&
                name.ValueKind == JsonValueKind.String && required.Contains(name.GetString()!))
            .Select(field => JsonNode.Parse(field.GetRawText())!)
            .ToArray();

        // A count-only query can legitimately need no value column.  Keeping one
        // physical column preserves Parquet row counts on Arrow builds that do not
        // expose a zero-column record batch.
        if (selected.Length == 0 && fields.GetArrayLength() != 0)
            selected = [JsonNode.Parse(fields[0].GetRawText())!];

        var root = JsonNode.Parse(asset.Schema.GetRawText())!.AsObject();
        root["fields"] = new JsonArray(selected);
        return JsonSerializer.SerializeToElement(root, Json);
    }

    private static void AddMetricFields(AnalysisModel model, AnalysisMeasure[] measures, string id,
        ISet<string> required, ISet<string> visiting)
    {
        if (!visiting.Add(id)) return;
        var custom = (model.Metrics ?? []).FirstOrDefault(metric => metric.Id == id);
        if (custom is not null)
        {
            if (custom.FieldId is not null)
                required.Add(custom.FieldId);
            if (custom.Left is not null)
                AddMetricFields(model, measures, custom.Left, required, visiting);
            if (custom.Right is not null)
                AddMetricFields(model, measures, custom.Right, required, visiting);
            visiting.Remove(id);
            return;
        }

        var measure = measures.FirstOrDefault(item => item.Id == id);
        if (measure?.FieldId is not null)
            required.Add(measure.FieldId);
        else if (id == "average")
        {
            AddMetricFields(model, measures, "amount", required, visiting);
            AddMetricFields(model, measures, "orders", required, visiting);
        }
        visiting.Remove(id);
    }

    private static (int? From, int? Before) MonthBounds(AnalysisQuery query)
    {
        DateOnly? from = ParseDate(query.Filters, "from");
        DateOnly? before = ParseDate(query.Filters, "before");
        int? monthFrom = from is null ? null : DateOnly.FromDateTime(new DateTime(from.Value.Year, from.Value.Month, 1)).DayNumber -
            DateOnly.FromDateTime(new DateTime(1970, 1, 1)).DayNumber;
        int? monthBefore = before is null ? null : MonthStartAfter(before.Value).DayNumber -
            DateOnly.FromDateTime(new DateTime(1970, 1, 1)).DayNumber;
        return (monthFrom, monthBefore);
    }

    private static DateOnly? ParseDate(AnalysisFilter[]? filters, string operation)
    {
        string? value = filters?.FirstOrDefault(filter => filter.Operator == operation)?.Value;
        return value is not null && DateOnly.TryParseExact(value, "yyyy-MM-dd", CultureInfo.InvariantCulture,
            DateTimeStyles.None, out var date) ? date : null;
    }

    private static DateOnly MonthStartAfter(DateOnly date) => date.Day == 1
        ? date
        : new DateOnly(date.Year, date.Month, 1).AddMonths(1);

    internal string Query(AnalysisAsset asset, string path, AnalysisQuery query)
        => Query(asset, [path], query);

    internal string Query(AnalysisAsset asset, IReadOnlyList<string> paths, AnalysisQuery query)
    {
        string scan = Scan(asset, paths, query);
        BuildQueryFromSource(scan, 0, "query", asset, query, marker: null);
        return Build(asset.Sql is not null ? SpillPolicy(paths) : null);
    }

    /// <summary>
    /// Builds several independent report aggregations over one source scan.
    /// The caller supplies already-bound queries, so this method does not
    /// change public query or dashboard contracts.  Each result row carries a
    /// private marker used by the managed result splitter.
    /// </summary>
    internal string QueryBatch(AnalysisAsset asset, IReadOnlyList<string> paths,
        IReadOnlyList<(string Key, AnalysisQuery Query)> reports)
    {
        if (reports.Count == 0)
            throw new ArgumentException("At least one report is required", nameof(reports));
        if (reports.Select(report => report.Key).Distinct(StringComparer.Ordinal).Count() != reports.Count ||
            reports.Any(report => string.IsNullOrWhiteSpace(report.Key)))
            throw new ArgumentException("Report keys must be unique", nameof(reports));

        AnalysisQuery scanQuery = SharedScanQuery(reports);
        string scan = Scan(asset, paths, scanQuery);
        var filterGroups = reports.Select((report, index) => new
            {
                report,
                index,
                extra = ExtraFilters(report.Query.Filters, scanQuery.Filters)
            })
            .GroupBy(item => JsonSerializer.Serialize(item.extra, Json), StringComparer.Ordinal)
            .Select(group => group.ToArray())
            .ToArray();
        string fanout = "batch-fanout";
        if (filterGroups.Length > 1)
            plan.Component(fanout, "multicast", new
            {
                outputs = filterGroups.Select((_, index) => "filter-group-" + index.ToString(CultureInfo.InvariantCulture)).ToArray()
            }).Connect(scan, fanout);

        for (int groupIndex = 0; groupIndex < filterGroups.Length; groupIndex++)
        {
            var members = filterGroups[groupIndex];
            string source = filterGroups.Length == 1 ? scan : fanout;
            uint port = filterGroups.Length == 1 ? 0U : (uint)groupIndex;
            AnalysisFilter[] extra = members[0].extra;
            if (extra.Length != 0)
            {
                source = Filter(source, "batch-filter-" + groupIndex.ToString(CultureInfo.InvariantCulture),
                    extra.Select(filter => Predicate(filter.FieldId, filter.Operator, filter.Value))
                        .Aggregate((left, right) => Op("and", left, right)), port);
                port = 0;
            }

            if (members.Length == 1)
            {
                var member = members[0];
                BuildQueryFromSource(source, port,
                    "batch-" + member.index.ToString(CultureInfo.InvariantCulture), asset,
                    member.report.Query, member.report.Key);
                continue;
            }

            string groupFanout = "batch-filter-fanout-" + groupIndex.ToString(CultureInfo.InvariantCulture);
            plan.Component(groupFanout, "multicast", new
            {
                outputs = members.Select(member => "batch" + member.index.ToString(CultureInfo.InvariantCulture)).ToArray()
            }).Connect(source, groupFanout, port);
            for (int memberIndex = 0; memberIndex < members.Length; memberIndex++)
            {
                var member = members[memberIndex];
                BuildQueryFromSource(groupFanout, (uint)memberIndex,
                    "batch-" + member.index.ToString(CultureInfo.InvariantCulture), asset,
                    member.report.Query, member.report.Key);
            }
        }
        return Build(asset.Sql is not null ? SpillPolicy(paths) : null);
    }

    private static AnalysisQuery SharedScanQuery(IReadOnlyList<(string Key, AnalysisQuery Query)> reports)
    {
        AnalysisQuery first = reports[0].Query;
        AnalysisDimension[] dimensions = reports.SelectMany(report => report.Query.Dimensions)
            .Distinct().ToArray();
        string[] measures = reports.SelectMany(report => report.Query.Measures ?? [])
            .Distinct(StringComparer.Ordinal).ToArray();
        AnalysisFilter[][] filterSets = reports.Select(report => report.Query.Filters ?? []).ToArray();
        AnalysisFilter[] common = (first.Filters ?? [])
            .Where(filter => filter.Operator is not ("from" or "before") &&
                filterSets.All(filters => filters.Contains(filter)))
            .ToArray();

        string? dateField = reports.SelectMany(report => report.Query.Filters ?? [])
            .Where(filter => filter.Operator is "from" or "before")
            .Select(filter => filter.FieldId).Distinct(StringComparer.Ordinal).SingleOrDefault();
        if (dateField is not null)
        {
            string? from = DateBoundary(reports, dateField, "from", minimum: true);
            string? before = DateBoundary(reports, dateField, "before", minimum: false);
            if (from is not null) common = [.. common, new(dateField, "from", from)];
            if (before is not null) common = [.. common, new(dateField, "before", before)];
        }
        return first with
        {
            Dimensions = dimensions,
            Measures = measures,
            Filters = common,
            SortMeasure = null,
            Descending = false
        };
    }

    private static string? DateBoundary(IReadOnlyList<(string Key, AnalysisQuery Query)> reports,
        string field, string operation, bool minimum)
    {
        var values = reports.Select(report => (report.Query.Filters ?? [])
                .Where(filter => filter.FieldId == field && filter.Operator == operation)
                .Select(filter => filter.Value).SingleOrDefault())
            .ToArray();
        if (values.Any(value => value is null) || values.Length == 0)
            return null;
        var dates = values.Select(value => DateOnly.TryParseExact(value, "yyyy-MM-dd",
                CultureInfo.InvariantCulture, DateTimeStyles.None, out var date) ? date : (DateOnly?)null)
            .ToArray();
        if (dates.Any(date => date is null)) return null;
        DateOnly selected = minimum ? dates.Min()!.Value : dates.Max()!.Value;
        return selected.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
    }

    private static AnalysisFilter[] ExtraFilters(AnalysisFilter[]? filters, AnalysisFilter[]? shared)
    {
        var common = (shared ?? []).ToHashSet();
        return (filters ?? []).Where(filter => !common.Contains(filter)).ToArray();
    }

    private void BuildQueryFromSource(string source, uint port, string prefix, AnalysisAsset asset,
        AnalysisQuery query, string? marker)
    {
        uint currentPort = port;
        var groups = Groupings(query);
        for (int index = 0; index < groups.Length; index++)
        {
            if (index == groups.Length - 1)
            {
                Aggregate(source, currentPort, prefix + "-group-" + index, asset, query, groups[index], marker);
                break;
            }
            string fork = prefix + "-fork-" + index;
            plan.Component(fork, "multicast", new { }).Connect(source, fork, currentPort);
            Aggregate(fork, 0, prefix + "-group-" + index, asset, query, groups[index], marker);
            source = fork;
            currentPort = 1;
        }
    }

    internal static AnalysisDimension[][] Groupings(AnalysisQuery query) => query.Dimensions.Length switch
    {
        0 => [[]],
        1 => [[], query.Dimensions],
        _ => [[], [query.Dimensions[0]], [query.Dimensions[1]], query.Dimensions]
    };

    private void Aggregate(string source, uint port, string id, AnalysisAsset asset, AnalysisQuery query,
        AnalysisDimension[] dimensions, string? marker = null)
    {
        string[] measures = query.Measures!;
        string[] keys = dimensions.Select(Key).ToArray();
        string customInput = AnalysisMetricBinder.Build(plan, id, source, port, keys, asset, measures);
        if (keys.Length != 0)
        {
            string sorted = id + "-sort";
            string[] sort = query.SortMeasure is null ? keys : [query.SortMeasure, .. keys];
            bool[] directions = sort.Select(key => key == query.SortMeasure && query.Descending).ToArray();
            plan.Component(sorted, "sort", new { keys = sort, keyDirections = directions }).Connect(customInput, sorted);
            customInput = Range(sorted, id + "-limit", query.Limit + 1);
        }
        if (marker is null)
            Sink(customInput, id + "-result");
        else
        {
            string tagged = id + "-tag";
            plan.Component(tagged, "derive", new
            {
                columns = new[]
                {
                    new { name = BatchMarkerField,
                        expression = new { literal = marker, type = "utf8" } }
                }
            }).Connect(customInput, tagged);
            Sink(tagged, id + "-result");
        }
    }

    internal string Details(AnalysisAsset asset, string path, AnalysisQuery query, string?[]? cell, int after, int limit)
        => Details(asset, [path], query, cell, after, limit);

    internal string Details(AnalysisAsset asset, IReadOnlyList<string> paths, AnalysisQuery query, string?[]? cell,
        int after, int limit)
    {
        string scan = Scan(asset, paths, query, drill: true);
        object condition = Op("gt", Col("rowKey"), new { literal = after, type = "int64" });
        if (cell is not null)
            for (int index = 0; index < cell.Length; index++)
                condition = Op("and", condition, Predicate(Key(query.Dimensions[index]), "eq", cell[index],
                    query.Dimensions[index].Level));
        var selected = Filter(scan, "detail-filter", condition);
        plan.Component("ordered", "sort", new { keys = new[] { "rowKey" } }).Connect(selected, "ordered");
        Sink(Range("ordered", "detail-limit", limit + 1), "detail-result");
        return Build(asset.Sql is not null ? SpillPolicy(paths) : null);
    }

    private static object Predicate(string field, string comparison, string? value, string level = "value")
    {
        if (value is null)
            return Op("isnull", Col(field));
        return Op(comparison switch { "from" => "ge", "before" => "lt", _ => "eq" }, Col(field),
            new { literal = value, type = level != "value" || comparison is "from" or "before" ? "date32" : "utf8" });
    }

    private string Filter(string source, string id, object condition, uint port = 0)
    {
        plan.Component(id, "split", new { condition }).Connect(source, id, port);
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

    private string Build(object? policy = null)
    {
        var result = plan.Build(null, null, policy);
        var package = result.Value ?? throw new UniversalDataException((int)result.Code, "Invalid analysis plan");
        return package.ToJson();
    }

    internal static string Key(AnalysisDimension dimension) => dimension.Level == "month" ? "calendarMonth" : dimension.FieldId;
    private static object Col(string name) => new { column = name };
    private static object Op(string op, params object[] args) => new { op, args };
    internal static JsonElement Document(object value) => JsonSerializer.SerializeToElement(value, Json);

}
