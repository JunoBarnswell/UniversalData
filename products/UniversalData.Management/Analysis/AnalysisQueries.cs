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
    private sealed record QueryBinding(string DatasetId, string Subject, string Tenant, AnalysisQuery Query);
    private sealed record CursorBinding(string Context, string?[]? Cell, int After);

    private AnalysisAsset Asset(HostPolicy scope, string id, string releaseId)
    {
        scope.Identity.Require("analysis.read", id);
        using var catalog = scope.OpenCatalog();
        var result = State(catalog, scope, id, Revision(releaseId)).Asset;
        if (result?.Release.Id != releaseId)
            throw Failure(StatusCode.Invalid, "指定的报表版本不存在。");
        RequireSource(scope, result.Source);
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
        string token, CancellationToken cancellation)
    {
        var query = binding.Query;
        var files = new AnalysisFileScope(binding.DatasetId, Asset: asset.File);
        string path = await AssetPath(scope, files, asset.ContentHash, cancellation);
        var grouping = AnalysisPlan.Groupings(query);
        var rows = await Run(scope, files, new AnalysisPlan(binding.DatasetId).Query(asset, path, query),
            1 + (grouping.Length - 1) * (query.Limit + 1), cancellation,
            "当前版本的汇总无法计算，请检查字段规则与指标设置。");
        VerifyRead(scope, binding.DatasetId, asset);
        string[] keys = query.Dimensions.Select(AnalysisPlan.Key).ToArray();
        var totals = rows.Rows.Where(row => !keys.Any(row.ContainsKey)).ToArray();
        if (totals.Length > 1)
            throw new InvalidDataException("Duplicate Native analysis totals");
        var measures = asset.Release.Measures.Where(measure => query.Measures!.Contains(measure.Id)).ToArray();
        Dictionary<string, string?> Values(Dictionary<string, string?>? row) =>
            measures.ToDictionary(measure => measure.Id, measure => row?.GetValueOrDefault(measure.Id), StringComparer.Ordinal);
        var groups = new List<AnalysisGrouping>();
        foreach (var dimensions in grouping.Skip(1))
        {
            string[] groupKeys = dimensions.Select(AnalysisPlan.Key).ToArray();
            var groupRows = rows.Rows.Where(row => keys.Count(row.ContainsKey) == groupKeys.Length && groupKeys.All(row.ContainsKey)).ToArray();
            if (groupRows.Length > query.Limit)
                throw Failure(StatusCode.Resource, "汇总分组太多，请缩小日期或分类范围。");
            groups.Add(new(dimensions, groupRows.Select(row =>
                new AnalysisCell(groupKeys.Select(key => row[key]).ToArray(), Values(row))).ToArray()));
        }
        var total = totals.SingleOrDefault();
        bool empty = total?.GetValueOrDefault("records") is null or "0";
        return new(token, asset.Release.Id, measures, Values(total), groups.ToArray(), empty);
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

    private static async Task<string> AssetPath(HostPolicy scope, AnalysisFileScope files, string contentHash, CancellationToken cancellation)
    {
        string path = files.AssetPath(scope)!;
        HostFiles.NoLinks(path);
        if (!File.Exists(path))
            throw Failure(StatusCode.Incompatible, "此版本的数据文件已不可读取。请从可用版本重新生成报表。");
        if (await FileHash(path, cancellation) != contentHash)
            throw Failure(StatusCode.Corrupt, "此版本的数据与发布时不一致。");
        return path;
    }

    private void VerifyRead(HostPolicy scope, string id, AnalysisAsset asset)
    {
        scope = Reauthorize(scope);
        scope.Identity.Require("analysis.read", id);
        RequireSource(scope, asset.Source);
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
        var files = new AnalysisFileScope(binding.DatasetId, Asset: asset.File);
        string path = await AssetPath(scope, files, asset.ContentHash, cancellation);
        var result = await Run(scope, files, new AnalysisPlan(binding.DatasetId).Details(asset, path, binding.Query, cell, after, limit),
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
