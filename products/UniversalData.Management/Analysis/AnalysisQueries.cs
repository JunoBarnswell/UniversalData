using System.Collections.Concurrent;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.DataProtection;
using UniversalData.Hosting;

namespace UniversalData.Management;

internal sealed partial class AnalysisWorkspace
{
    private const int DetailPageSize = 100;
    private const int DetailExportLimit = 2000;
    private const int MaximumSharedReportBatches = 32;
    private const int MaximumNativeResultRows = 5005;
    private const int MaximumCachedReportBatches = 8;
    private const int MaximumCachedReportBytes = 32 * 1024 * 1024;
    internal sealed record QueryBinding(string DatasetId, string Subject, string Tenant, AnalysisQuery Query);
    private sealed record CursorBinding(string Context, string?[]? Cell, int After);
    private readonly ConcurrentDictionary<string, Lazy<Task<Dictionary<string, AnalysisResult>>>> sharedReports = new(StringComparer.Ordinal);
    private readonly object sharedReportsGate = new();
    private readonly ConcurrentDictionary<string, Lazy<Task<string[]>>> sharedAssetPaths = new(StringComparer.Ordinal);
    private readonly object sharedAssetPathsGate = new();
    private readonly Dictionary<string, (Dictionary<string, AnalysisResult> Values, int Bytes)> cachedReports = new(StringComparer.Ordinal);
    private readonly object cachedReportsGate = new();
    private int cachedReportBytes;

    private AnalysisAsset Asset(HostPolicy scope, string id, string releaseId)
    {
        scope.Identity.Require("analysis.read", id);
        using var catalog = scope.OpenCatalog();
        var result = State(catalog, scope, id, Revision(releaseId)).Asset;
        if (result?.Release.Id != releaseId)
            throw Failure(StatusCode.Invalid, "指定的报表版本不存在。");
        RequireSource(scope, result);
        return result;
    }

    private static bool IsDate(string? value) => DateOnly.TryParseExact(value, "yyyy-MM-dd",
        CultureInfo.InvariantCulture, DateTimeStyles.None, out _);

    private static AnalysisQuery BindQuery(AnalysisRelease release, AnalysisQuery query)
    {
        var model = release.Model;
        if (query.Dimensions.Length > 2 || query.Dimensions.Select(dimension => dimension.FieldId).Distinct().Count() != query.Dimensions.Length ||
            query.Dimensions.Any(dimension => !model.Fields.Any(field => field.FieldId == dimension.FieldId &&
                (field.Role == "date" && dimension.Level is "day" or "month" ||
                 field.Role == "category" && dimension.Level == "value"))))
            throw Failure(StatusCode.Invalid, "按日期或分类最多选择两个不同维度。");

        string[] measures = query.Measures ?? release.Measures.Select(measure => measure.Id).ToArray();
        if (measures.Length == 0 || measures.Distinct().Count() != measures.Length ||
            measures.Any(id => !release.Measures.Any(measure => measure.Id == id)) ||
            query.SortMeasure is not null && !measures.Contains(query.SortMeasure))
            throw Failure(StatusCode.Invalid, "所选版本中没有该指标。");
        if (query.Limit is < 1 or > 1000)
            throw Failure(StatusCode.Invalid, "汇总范围需要在 1 到 1000 组之间。");
        var filters = query.Filters ?? [];
        if (filters.Length > 8 || filters.Select(filter => (filter.FieldId, filter.Operator)).Distinct().Count() != filters.Length ||
            filters.Any(filter => !model.Fields.Any(field => field.FieldId == filter.FieldId &&
                (field.Role == "date" && (filter.Operator is "from" or "before") && IsDate(filter.Value) ||
                 field.Role == "category" && (filter.Operator == "isNull" && filter.Value is null ||
                    filter.Operator == "eq" && filter.Value is { Length: <= 4096 })))))
            throw Failure(StatusCode.Invalid, "筛选条件与字段用途不匹配。");
        var from = filters.FirstOrDefault(filter => filter.Operator == "from")?.Value;
        var before = filters.FirstOrDefault(filter => filter.Operator == "before")?.Value;
        if (from is not null && before is not null && StringComparer.Ordinal.Compare(from, before) >= 0)
            throw Failure(StatusCode.Invalid, "结束日期须晚于开始日期。");
        return query with { Measures = measures, Filters = filters };
    }

    private Task<AnalysisResult> Query(HostPolicy scope, string id, AnalysisQuery request, CancellationToken cancellation)
    {
        var asset = Asset(scope, id, request.ReleaseId);
        var query = BindQuery(asset.Release, request);
        var binding = new QueryBinding(id, scope.Identity.Subject, scope.Tenant, query);
        return ReadReport(scope, asset, binding, contexts.Protect(JsonSerializer.Serialize(binding, Json)), cancellation);
    }

    private async Task<AnalysisResult> ReadReport(HostPolicy scope, AnalysisAsset asset, QueryBinding binding,
        string token, CancellationToken cancellation, string[]? verifiedPaths = null)
    {
        var reports = await ReadReports(scope, asset,
            [new ReportRequest("single", binding, token)], cancellation, verifiedPaths);
        return reports["single"];
    }

    private async Task<Dictionary<string, AnalysisResult>> ReadReports(HostPolicy scope, AnalysisAsset asset,
        IReadOnlyList<ReportRequest> reports, CancellationToken cancellation, string[]? verifiedPaths = null)
    {
        if (reports.Count == 0 || reports.Select(report => report.Key).Distinct(StringComparer.Ordinal).Count() != reports.Count)
            throw new ArgumentException("Report keys must be unique", nameof(reports));

        string[] paths = verifiedPaths ?? await AssetPaths(scope, reports[0].Binding.DatasetId, asset, cancellation);
        int resultLimit = checked(reports.Sum(report => ReportLimit(report.Binding.Query)));
        if (reports.Count > 1 && resultLimit > MaximumNativeResultRows)
        {
            // Native result transport has a fixed bounded response. Preserve
            // correctness for unusually wide boards by falling back to the
            // existing per-report topology; ordinary six-card boards remain a
            // single shared execution.
            var individual = new Dictionary<string, AnalysisResult>(StringComparer.Ordinal);
            foreach (var report in reports)
            {
                var result = await ReadReports(scope, asset, [report], cancellation, paths);
                individual.Add(report.Key, result[report.Key]);
            }
            return individual;
        }

        string cacheKey = SharedReportKey(scope, asset, reports);
        if (TryGetCachedReports(cacheKey, out Dictionary<string, AnalysisResult>? cached))
        {
            VerifyRead(scope, reports[0].Binding.DatasetId, asset);
            return reports.ToDictionary(report => report.Key,
                report => cached![report.Key] with { Context = report.Token }, StringComparer.Ordinal);
        }

        Lazy<Task<Dictionary<string, AnalysisResult>>>? shared = null;
        bool created = false;
        lock (sharedReportsGate)
        {
            if (!sharedReports.TryGetValue(cacheKey, out shared) &&
                sharedReports.Count < MaximumSharedReportBatches)
            {
                shared = new(() => ReadReportsCore(scope, asset, reports, paths, CancellationToken.None),
                    LazyThreadSafetyMode.ExecutionAndPublication);
                sharedReports[cacheKey] = shared;
                created = true;
            }
        }
        if (created)
            _ = shared!.Value.ContinueWith(_ => RemoveSharedReport(cacheKey, shared),
                CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);

        Dictionary<string, AnalysisResult> values;
        if (shared is null)
            values = await ReadReportsCore(scope, asset, reports, paths, cancellation);
        else
            values = await shared.Value.WaitAsync(cancellation);

        CacheReports(cacheKey, values);

        // The computation is shareable, but authorization is not. Every caller
        // revalidates the source after the shared work completes, then receives
        // a context token bound to its own identity.
        VerifyRead(scope, reports[0].Binding.DatasetId, asset);
        return reports.ToDictionary(report => report.Key,
            report => values[report.Key] with { Context = report.Token }, StringComparer.Ordinal);
    }

    private async Task<Dictionary<string, AnalysisResult>> ReadReportsCore(HostPolicy scope,
        AnalysisAsset asset, IReadOnlyList<ReportRequest> reports, string[]? verifiedPaths,
        CancellationToken cancellation)
    {
        string[] paths = verifiedPaths ?? await AssetPaths(scope, reports[0].Binding.DatasetId, asset, cancellation);
        var files = QueryFileScope(reports[0].Binding.DatasetId, asset);
        var plan = new AnalysisPlan(files.DatasetId, files.ScratchPath(scope));
        string package = reports.Count == 1
            ? plan.Query(asset, paths, reports[0].Binding.Query)
            : plan.QueryBatch(asset, paths, reports.Select(report => (report.Key, report.Binding.Query)).ToArray());
        var result = await Run(scope, files, package, reports.Sum(report => ReportLimit(report.Binding.Query)),
            cancellation, "当前版本的汇总无法计算，请检查字段规则与指标设置。");
        var values = new Dictionary<string, AnalysisResult>(StringComparer.Ordinal);
        foreach (var report in reports)
        {
            IEnumerable<Dictionary<string, string?>> rows = result.Rows;
            if (reports.Count > 1)
            {
                rows = rows.Where(row => row.GetValueOrDefault(AnalysisPlan.BatchMarkerField) == report.Key)
                    .Select(row => row.Where(item => item.Key != AnalysisPlan.BatchMarkerField)
                        .ToDictionary(item => item.Key, item => item.Value, StringComparer.Ordinal));
            }
            values.Add(report.Key, BuildReportResult(asset, report.Binding, rows));
        }
        return values;
    }

    private static AnalysisFileScope QueryFileScope(string datasetId, AnalysisAsset asset) =>
        new(datasetId, Asset: asset.File, Assets: asset.Shards?.Select(shard => shard.File).ToArray(),
            ScratchTicket: asset.Sql is null ? null : Guid.NewGuid().ToString("N"));

    private static int ReportLimit(AnalysisQuery query) =>
        checked(1 + (AnalysisPlan.Groupings(query).Length - 1) * (query.Limit + 1));

    private static AnalysisResult BuildReportResult(AnalysisAsset asset, QueryBinding binding,
        IEnumerable<Dictionary<string, string?>> inputRows)
    {
        AnalysisQuery query = binding.Query;
        var rows = inputRows.ToArray();
        var grouping = AnalysisPlan.Groupings(query);
        string[] keys = query.Dimensions.Select(AnalysisPlan.Key).ToArray();
        var totals = rows.Where(row => !keys.Any(row.ContainsKey)).ToArray();
        if (totals.Length > 1)
            throw new InvalidDataException("Duplicate Native analysis totals");
        var measures = asset.Release.Measures.Where(measure => query.Measures!.Contains(measure.Id)).ToArray();
        Dictionary<string, string?> Values(Dictionary<string, string?>? row) =>
            measures.ToDictionary(measure => measure.Id, measure => row?.GetValueOrDefault(measure.Id), StringComparer.Ordinal);
        var groups = new List<AnalysisGrouping>();
        foreach (var dimensions in grouping.Skip(1))
        {
            string[] groupKeys = dimensions.Select(AnalysisPlan.Key).ToArray();
            var groupRows = rows.Where(row => keys.Count(row.ContainsKey) == groupKeys.Length && groupKeys.All(row.ContainsKey)).ToArray();
            if (groupRows.Length > query.Limit)
                throw Failure(StatusCode.Resource, "汇总分组太多，请缩小日期或分类范围。");
            groups.Add(new(dimensions, groupRows.Select(row =>
                new AnalysisCell(groupKeys.Select(key => row[key]).ToArray(), Values(row))).ToArray()));
        }
        var total = totals.SingleOrDefault();
        bool empty = total?.GetValueOrDefault("records") is null or "0";
        return new(string.Empty, asset.Release.Id, measures, Values(total), groups.ToArray(), empty);
    }

    private static string SharedReportKey(HostPolicy scope, AnalysisAsset asset,
        IReadOnlyList<ReportRequest> reports) =>
        JsonSerializer.Serialize(new
        {
            scope.Tenant,
            scope.Identity.Subject,
            dataset = reports[0].Binding.DatasetId,
            release = asset.Release.Id,
            files = asset.Shards?.Select(shard => new { shard.File, shard.ContentHash, shard.Rows }).ToArray()
                ?? [new { File = asset.File, ContentHash = asset.ContentHash, Rows = (int?)asset.Release.Rows }],
            queries = reports.Select(report => new { report.Key, report.Binding.Query }).ToArray()
        }, Json);

    private bool TryGetCachedReports(string key, out Dictionary<string, AnalysisResult>? values)
    {
        lock (cachedReportsGate)
        {
            if (cachedReports.TryGetValue(key, out var entry))
            {
                values = entry.Values;
                return true;
            }
        }
        values = null;
        return false;
    }

    private void CacheReports(string key, Dictionary<string, AnalysisResult> values)
    {
        int bytes = JsonSerializer.SerializeToUtf8Bytes(values, Json).Length;
        if (bytes > MaximumCachedReportBytes) return;
        lock (cachedReportsGate)
        {
            if (cachedReports.ContainsKey(key) || cachedReports.Count >= MaximumCachedReportBatches ||
                cachedReportBytes > MaximumCachedReportBytes - bytes)
                return;
            cachedReports.Add(key, (values, bytes));
            cachedReportBytes += bytes;
        }
    }

    private void RemoveSharedReport(string key, Lazy<Task<Dictionary<string, AnalysisResult>>> value)
    {
        if (!value.IsValueCreated || !value.Value.IsCompleted) return;
        lock (sharedReportsGate)
            ((ICollection<KeyValuePair<string, Lazy<Task<Dictionary<string, AnalysisResult>>>>>)sharedReports)
                .Remove(new(key, value));
    }

    private async Task<string[]> AssetPaths(HostPolicy scope, string datasetId, AnalysisAsset asset,
        CancellationToken cancellation)
    {
        string key = AssetPathsKey(scope, datasetId, asset);
        Lazy<Task<string[]>>? shared = null;
        bool created = false;
        lock (sharedAssetPathsGate)
        {
            if (sharedAssetPaths.TryGetValue(key, out shared) && shared.IsValueCreated && shared.Value.IsCompleted)
            {
                ((ICollection<KeyValuePair<string, Lazy<Task<string[]>>>>)sharedAssetPaths)
                    .Remove(new(key, shared));
                shared = null;
            }
            if (shared is null && sharedAssetPaths.Count < MaximumSharedReportBatches)
            {
                shared = new(() => AssetPathsCore(scope, datasetId, asset, CancellationToken.None),
                    LazyThreadSafetyMode.ExecutionAndPublication);
                sharedAssetPaths[key] = shared;
                created = true;
            }
        }
        if (created)
            _ = shared!.Value.ContinueWith(_ => RemoveSharedAssetPaths(key, shared),
                CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        if (shared is null)
            return await AssetPathsCore(scope, datasetId, asset, cancellation);
        return await shared!.Value.WaitAsync(cancellation);
    }

    private static string AssetPathsKey(HostPolicy scope, string datasetId, AnalysisAsset asset) =>
        JsonSerializer.Serialize(new
        {
            scope.Tenant,
            scope.Identity.Subject,
            datasetId,
            release = asset.Release.Id,
            files = asset.Shards?.Select(shard => new { shard.File, shard.ContentHash, shard.Rows }).ToArray()
                ?? [new { File = asset.File, ContentHash = asset.ContentHash, Rows = (int?)asset.Release.Rows }]
        }, Json);

    private void RemoveSharedAssetPaths(string key, Lazy<Task<string[]>> value)
    {
        if (!value.IsValueCreated || !value.Value.IsCompleted) return;
        lock (sharedAssetPathsGate)
            ((ICollection<KeyValuePair<string, Lazy<Task<string[]>>>>)sharedAssetPaths)
                .Remove(new(key, value));
    }

    private static async Task<string[]> AssetPathsCore(HostPolicy scope, string datasetId, AnalysisAsset asset,
        CancellationToken cancellation)
    {
        AnalysisShard[]? manifest = asset.Shards is { Length: > 0 } value ? value : null;
        bool hasManifest = manifest is not null;
        AnalysisShard[] shards = manifest ??
            [new AnalysisShard(asset.File, asset.ContentHash, asset.Release.Rows)];
        if (hasManifest && shards.Any(shard => !shard.Rows.HasValue) ||
            shards.Any(shard => shard.Rows < 0 || string.IsNullOrWhiteSpace(shard.File) ||
                string.IsNullOrWhiteSpace(shard.ContentHash)) ||
            shards.Select(shard => shard.File).Distinct(StringComparer.Ordinal).Count() != shards.Length ||
            !StringComparer.Ordinal.Equals(shards[0].File, asset.File) ||
            !StringComparer.Ordinal.Equals(shards[0].ContentHash, asset.ContentHash) ||
            shards.All(shard => shard.Rows.HasValue) &&
            shards.Sum(shard => (long)shard.Rows!.Value) != asset.Release.Rows)
            throw Failure(StatusCode.Corrupt, "此版本的物理分片清单无效。");
        var paths = new string[shards.Length];
        for (int index = 0; index < shards.Length; index++)
        {
            var files = new AnalysisFileScope(datasetId, Asset: shards[index].File);
            string path = files.AssetPath(scope)!;
            HostFiles.NoLinks(path);
            if (!File.Exists(path))
                throw Failure(StatusCode.Incompatible, "此版本的数据文件已不可读取。请从可用版本重新生成报表。");
            if (await FileHash(path, cancellation) != shards[index].ContentHash)
                throw Failure(StatusCode.Corrupt, "此版本的数据与发布时不一致。");
            paths[index] = path;
        }
        return paths;
    }

    private QueryBinding BindContext(HostPolicy scope, string id, string value)
    {
        var binding = Unprotect<QueryBinding>(contexts, value);
        if (binding.DatasetId != id || binding.Subject != scope.Identity.Subject || binding.Tenant != scope.Tenant)
            throw Failure(StatusCode.Auth, "此查询属于另一份分析或账户。");
        return binding;
    }

    private static T Unprotect<T>(IDataProtector protector, string value) where T : class
    {
        if (value.Length > 65536) throw Failure(StatusCode.Invalid, "查询上下文过长。");
        try
        {
            return JsonSerializer.Deserialize<T>(protector.Unprotect(value), Json)
                ?? throw Failure(StatusCode.Invalid, "查询上下文无效。");
        }
        catch (Exception error) when (error is CryptographicException or JsonException or FormatException)
        {
            throw Failure(StatusCode.Invalid, "查询上下文无效，请重新打开报表。");
        }
    }

    private void VerifyRead(HostPolicy scope, string id, AnalysisAsset asset)
    {
        scope = Reauthorize(scope);
        scope.Identity.Require("analysis.read", id);
        RequireSource(scope, asset);
    }

    private static void VerifyCell(AnalysisQuery query, string?[]? cell)
    {
        if (cell is null) return;
        if (cell.Length == 0 || cell.Length > query.Dimensions.Length || cell.Select((value, index) =>
                query.Dimensions[index].Level == "value" ? value?.Length > 4096 : !IsDate(value)).Any(invalid => invalid))
            throw Failure(StatusCode.Invalid, "明细位置和当前报表的维度不匹配。");
    }

    private async Task<AnalysisDetails> Drill(HostPolicy scope, string id, AnalysisDrill request,
        CancellationToken cancellation)
    {
        var binding = BindContext(scope, id, request.Context);
        var asset = Asset(scope, id, binding.Query.ReleaseId);
        VerifyCell(binding.Query, request.Cell);
        int after = 0;
        if (request.Cursor is not null)
        {
            var cursor = Unprotect<CursorBinding>(cursors, request.Cursor);
            if (cursor.Context != Hash(request.Context) ||
                JsonSerializer.Serialize(cursor.Cell, Json) != JsonSerializer.Serialize(request.Cell, Json))
                throw Failure(StatusCode.Invalid, "明细页码和当前筛选不匹配。");
            after = cursor.After;
        }
        var rows = await ReadDetails(scope, asset, binding, request.Cell, after, DetailPageSize, cancellation);
        string? next = null;
        if (rows.Length > DetailPageSize)
        {
            int last = int.Parse(rows[DetailPageSize - 1]["rowKey"]!, CultureInfo.InvariantCulture);
            next = cursors.Protect(JsonSerializer.Serialize(new CursorBinding(Hash(request.Context), request.Cell, last), Json));
        }
        var fields = asset.Release.Model.Fields.Where(field => field.Role != "omit").ToArray();
        return new(asset.Release.Id, fields, rows.Take(DetailPageSize).Select(row => Project(row, fields)).ToArray(), next);
    }

    private async Task<Dictionary<string, string?>[]> ReadDetails(HostPolicy scope, AnalysisAsset asset, QueryBinding binding,
        string?[]? cell, int after, int limit, CancellationToken cancellation)
    {
        string[] paths = await AssetPaths(scope, binding.DatasetId, asset, cancellation);
        var files = QueryFileScope(binding.DatasetId, asset);
        var plan = new AnalysisPlan(files.DatasetId, files.ScratchPath(scope));
        var result = await Run(scope, files, plan.Details(asset, paths, binding.Query, cell, after, limit),
            limit + 1, cancellation, "无法读取该版本的明细，请重新打开报表。");
        VerifyRead(scope, binding.DatasetId, asset);
        return result.Rows;
    }

    private static Dictionary<string, string?> Project(Dictionary<string, string?> row, AnalysisRule[] fields) =>
        fields.ToDictionary(field => field.FieldId, field => row.GetValueOrDefault(field.FieldId), StringComparer.Ordinal);

    private async Task<IResult> Export(HostPolicy scope, string id, AnalysisExport request, CancellationToken cancellation)
    {
        var binding = BindContext(scope, id, request.Context);
        var asset = Asset(scope, id, binding.Query.ReleaseId);
        VerifyCell(binding.Query, request.Cell);
        var fields = asset.Release.Model.Fields.Where(field => field.Role != "omit").ToArray();
        var csv = new StringBuilder("\uFEFF");
        if (request.Details)
        {
            var details = await ReadDetails(scope, asset, binding, request.Cell, 0, DetailExportLimit, cancellation);
            if (details.Length > DetailExportLimit)
                throw Failure(StatusCode.Resource, "明细超过 2,000 条，请缩小筛选后导出。");
            CsvRow(csv, fields.Select(field => ((string?)field.Label, false)));
            foreach (var row in details)
                CsvRow(csv, fields.Select(field => (row.GetValueOrDefault(field.FieldId), field.Role == "amount")));
        }
        else
        {
            var report = await ReadReport(scope, asset, binding, request.Context, cancellation);
            var group = report.Groups.LastOrDefault();
            var dimensions = group?.Dimensions ?? [];
            var names = dimensions.Select(dimension => fields.Single(field => field.FieldId == dimension.FieldId).Label);
            CsvRow(csv, names.Append("汇总级别").Concat(report.Measures.Select(measure => measure.Label)).Select(text => ((string?)text, false)));
            foreach (var grouping in report.Groups.Reverse())
                foreach (var cell in grouping.Cells)
                {
                    var labels = dimensions.Select(dimension =>
                    {
                        int index = Array.IndexOf(grouping.Dimensions, dimension);
                        string? value = index < 0 ? null : cell.Keys[index] switch
                        {
                            null => "未填写",
                            "未填写" => "未填写（原文）",
                            var text => text
                        };
                        return (value, false);
                    });
                    CsvRow(csv, labels.Append(grouping.Dimensions.Length == dimensions.Length ? ("分组", false) : ("小计", false))
                        .Concat(report.Measures.Select(measure => (cell.Values[measure.Id], true))));
                }
            CsvRow(csv, dimensions.Select(_ => ((string?)null, false)).Append(("总计", false))
                .Concat(report.Measures.Select(measure => (report.Empty ? null : report.Total[measure.Id], true))));
        }
        VerifyRead(scope, id, asset);
        return Results.File(Encoding.UTF8.GetBytes(csv.ToString()), "text/csv; charset=utf-8", "analysis.csv");
    }

    private static void CsvRow(StringBuilder csv, IEnumerable<(string? Value, bool Numeric)> cells)
    {
        bool first = true;
        foreach (var (value, numeric) in cells)
        {
            if (!first) csv.Append(',');
            first = false;
            string text = value ?? "";
            csv.Append('"');
            var trimmed = text.TrimStart();
            if (!numeric && (text.Length != 0 && char.IsControl(text[0]) ||
                    trimmed.Length != 0 && trimmed[0] is '=' or '+' or '-' or '@' or '＝' or '＋' or '－' or '＠'))
                csv.Append('\'');
            csv.Append(text.Replace("\"", "\"\"", StringComparison.Ordinal)).Append('"');
        }
        csv.Append("\r\n");
    }

    private AnalysisDataset SaveReport(HostPolicy scope, string id, AnalysisReportWrite request)
    {
        scope.Identity.Require("analysis.write", id);
        var report = request.Report;
        var release = Asset(scope, id, report.Query.ReleaseId).Release;
        var query = BindQuery(release, report.Query);
        if (report.Chart is not ("line" or "bar" or "table") ||
            report.Chart == "line" && query.Dimensions.FirstOrDefault()?.Level is not ("day" or "month") ||
            report.Chart == "bar" && query.Dimensions.Length == 0 ||
            report.ChartMeasure is not null && !query.Measures!.Contains(report.ChartMeasure))
            throw Failure(StatusCode.Invalid, "图表或指标与当前分组不匹配。");
        using var catalog = scope.OpenCatalog();
        ulong expected = Revision(request.ExpectedRevision);
        RequireCurrent(catalog, Key(scope, id), expected);
        var state = State(catalog, scope, id, expected) with
        {
            Report = report with { Name = Name(report.Name), Query = query }
        };
        using var lease = LockDataset(catalog, state.Id);
        CommitDataset(catalog, state, expected, lease);
        return View(state, expected + 1);
    }
}
