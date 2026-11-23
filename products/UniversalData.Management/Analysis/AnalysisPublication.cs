using System.Globalization;
using System.Security.Cryptography;
using System.Text.Json;
using UniversalData.Hosting;

namespace UniversalData.Management;

internal sealed partial class AnalysisWorkspace
{
    private const int PublicationShardCount = 8;

    private AnalysisPublication Operation(HostPolicy scope, string id, string operationId)
    {
        scope.Identity.Require("analysis.read", id);
        using var catalog = scope.OpenCatalog();
        var state = State(catalog, scope, id, catalog.LatestRevision(CatalogKind.Audit, Key(scope, id)));
        RequireSource(scope, state.Input);
        string key = OperationKey(scope, id, operationId);
        ulong revision = catalog.LatestRevision(CatalogKind.Audit, key);
        var operation = Read<OperationState>(catalog.Get(CatalogKind.Audit, key, revision));
        if (operation.Publication.State == "checking" &&
            PublishedForOperation(catalog, state.Id, operation.ExpectedRevision, operationId) is { } receipt)
            return receipt;
        return operation.Publication;
    }

    private async Task<AnalysisPublication> Publish(HostPolicy scope, string id, AnalysisPublish request,
        CancellationToken cancellation, bool refresh = false)
    {
        scope.Identity.Require("analysis.write", id);
        scope.Identity.Require("analysis.read", id);
        ulong expected = Revision(request.ExpectedRevision);
        string datasetKey = Key(scope, id);
        string operationKey = OperationKey(scope, id, request.OperationId);
        string fingerprint = Hash(JsonSerializer.Serialize(new { request, refresh }, Json));
        using var catalog = scope.OpenCatalog();
        using var lease = LockDataset(catalog, datasetKey);
        ulong latest = catalog.LatestRevision(CatalogKind.Audit, datasetKey);
        var current = State(catalog, scope, id, latest);
        RequireSource(scope, current.Input);
        ulong operationRevision = catalog.LatestRevision(CatalogKind.Audit, operationKey);
        OperationState? existing = operationRevision == 0 ? null
            : Read<OperationState>(catalog.Get(CatalogKind.Audit, operationKey, operationRevision));
        if (existing is not null)
        {
            if (existing.Fingerprint != fingerprint)
                throw Failure(StatusCode.Fenced, "同一发布编号已经用于另一份草稿。");
            if (existing.Publication.State != "checking")
                return existing.Publication;

            // The pointer and the version commit together. Recover an acknowledgement lost
            // after the compare-and-exchange from that exact catalog revision.
            if (PublishedForOperation(catalog, datasetKey, existing.ExpectedRevision, request.OperationId) is { } recovered)
            {
                SaveOperation(catalog, existing with { Publication = recovered }, operationRevision);
                return recovered;
            }
        }
        RequireCurrent(catalog, datasetKey, expected);
        if (request.ExpectedReleaseId != current.Asset?.Release.Id)
            throw Failure(StatusCode.Fenced, "当前报表版本已变化，请重新打开分析。");
        AnalysisDraft draft;
        if (refresh)
        {
            if (current.Input.Sql is null || current.Asset is not { } last ||
                !SourceMatches(current.Input, last))
                throw Failure(StatusCode.Fenced, "刷新来源已变化，请重新打开分析并重新发布。");
            // Refresh always uses the model and source identity of the last
            // published release; an edited draft can never silently change it.
            var sourceSchema = await Inspect(scope, current, null, cancellation);
            draft = new(BindModel(sourceSchema, last.Release.Model, confirmed: true), sourceSchema);
        }
        else
        {
            var saved = current.Draft ?? throw Failure(StatusCode.Invalid, "请先确认并保存字段规则。");
            draft = saved with { Model = BindModel(saved.Source, saved.Model, confirmed: true) };
        }

        if (existing is not null)
            RemoveAttempt(scope, id, existing.Attempt, existing.Assets ?? [existing.Attempt]);
        // SQL snapshots use the columnar asset so published reads can project
        // only the requested fields. Keep the established NDJSON asset for
        // CSV/XLSX uploads; that path remains usable with legacy CORE builds
        // which predate the Arrow/Parquet codec.
        string assetExtension = current.Input.Kind is "csv" or "xlsx" ? ".ndjson" : ".parquet";
        int shardCount = current.Input.Kind == "sql" ? PublicationShardCount : 1;
        string[] shardFiles = Enumerable.Range(0, shardCount)
            .Select(_ => "release-" + Guid.NewGuid().ToString("N") + assetExtension).ToArray();
        string asset = shardFiles[0];
        var operation = new OperationState(operationKey, ProductInfo.Protocol, fingerprint, asset, expected,
            new(request.OperationId, "checking"), shardFiles);
        SaveOperation(catalog, operation, operationRevision++);
        var files = new AnalysisFileScope(id, current.Input.FileId, asset, CreateAsset: true,
            Assets: shardFiles);
        string file = files.AssetPath(scope)!;
        HostFiles.NoLinks(file);
        string directory = Path.GetDirectoryName(file)!;
        if (OperatingSystem.IsWindows()) Directory.CreateDirectory(directory);
        else Directory.CreateDirectory(directory, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);

        if (files.InputPath(scope) is { } input && await FileHash(input, cancellation) != current.Input.Fingerprint)
            throw Failure(StatusCode.Corrupt, "原始导入文件已变化，请重新上传。");

        var plan = new AnalysisPlan(id);
        var source = plan.Source(current.Input, draft.Source, files.InputPath(scope), draft.Model,
            stream: current.Input.Kind == "sql");
        NativeRows result;
        DateTimeOffset dataAsOf = current.Input.DataAsOf;
        try
        {
            uint maxRows = current.Input.Kind == "sql" ? 30000000U : 100000U;
            string publicationError = current.Input.Kind == "sql"
                ? "数据库来源无法完整读取，请检查连接、表或字段类型。"
                : "表格无法完整读取。请检查字段类型、工作表表头及公式。Excel 订单号请保存为文本，公式请粘贴为值。";
            string prepared = plan.Prepare(source,
                    shardFiles.Select(fileName => Path.Combine(directory, fileName)).ToArray(), draft, maxRows,
                    sql: current.Input.Kind == "sql");
            // For SQL, the release timestamp is the point immediately before
            // the one-pass Worker read starts. File/Iceberg sources retain
            // their established source timestamp semantics.
            dataAsOf = current.Input.Kind == "sql" ? DateTimeOffset.UtcNow : current.Input.DataAsOf;
            result = await Run(scope, files, prepared,
                1 + shardCount, cancellation, publicationError);

        }
        catch (UniversalDataException failure) when (failure.Code is StatusCode.Schema or StatusCode.Quality
            or StatusCode.Parse or StatusCode.Incompatible or StatusCode.Unsupported or StatusCode.Resource or StatusCode.Overflow)
        {
            RemoveAttempt(scope, id, asset, shardFiles);
            string message = failure.Code == StatusCode.Resource
                ? current.Input.Kind is "csv" or "xlsx"
                    ? "超过 10 万行或当前处理限额，请缩小表格。"
                    : "来源数据超出当前处理限额，请缩小来源范围。"
                : failure.Message;
            var rejected = new AnalysisPublication(request.OperationId, "rejected", Error: message);
            SaveOperation(catalog, operation with { Publication = rejected }, operationRevision);
            return rejected;
        }

        var fact = result.Rows.Single(row => row.ContainsKey("scannedRows"));
        int count = int.Parse(fact["scannedRows"]!, CultureInfo.InvariantCulture);
        int affected = int.Parse(fact["affectedRows"]!, CultureInfo.InvariantCulture);
        var shardRows = new int[shardCount];
        bool shardCountsValid = shardCount == 1;
        if (shardCount == 1)
            shardRows[0] = count;
        var shardFacts = result.Rows.Where(row => row.Keys.Count == 2 && row.ContainsKey("rows") &&
            row.ContainsKey("shard")).ToArray();
        var seenShards = new bool[shardCount];
        if (shardCount > 1)
        {
            shardCountsValid = shardFacts.Length == shardCount;
            foreach (var shard in shardFacts)
            {
                shardCountsValid &= int.TryParse(shard["shard"], NumberStyles.None,
                    CultureInfo.InvariantCulture, out int index) && index >= 0 && index < shardCount &&
                    !seenShards[index] && int.TryParse(shard["rows"], NumberStyles.None,
                        CultureInfo.InvariantCulture, out shardRows[index]) && shardRows[index] >= 0;
                if (shardCountsValid)
                    seenShards[index] = true;
            }
            shardCountsValid &= seenShards.All(value => value);
        }
        if (!shardCountsValid || shardRows.Sum() != count)
        {
            RemoveAttempt(scope, id, asset, shardFiles);
            throw Failure(StatusCode.Corrupt, "Native 分片统计与发布汇总不一致。");
        }
        scope = Reauthorize(scope);
        scope.Identity.Require("analysis.write", id);
        scope.Identity.Require("analysis.read", id);
        RequireSource(scope, current.Input);
        if (affected > 0)
        {
            var issues = JsonSerializer.Deserialize<RecordIssue[]>(fact["issues"]!, Json)!.Select(issue =>
            {
                var field = draft.Model.Fields[issue.Column];
                return new AnalysisIssue(issue.Row, field.FieldId, IssueText(field.Role));
            }).ToArray();
            var rejected = new AnalysisPublication(request.OperationId, "rejected", count, affected, issues);
            RemoveAttempt(scope, id, asset, shardFiles);
            SaveOperation(catalog, operation with { Publication = rejected }, operationRevision);
            return rejected;
        }

        string schemaText = fact["schema"]!;
        using var schema = JsonDocument.Parse(schemaText);
        string[] shardHashes = new string[shardFiles.Length];
        for (int index = 0; index < shardFiles.Length; index++)
        {
            string shardPath = new AnalysisFileScope(id, Asset: shardFiles[index]).AssetPath(scope)!;
            if (!File.Exists(shardPath))
            {
                RemoveAttempt(scope, id, asset, shardFiles);
                throw Failure(StatusCode.Corrupt, "Native 未生成完整的物理分片。");
            }
            shardHashes[index] = await FileHash(shardPath, cancellation);
            if (!OperatingSystem.IsWindows()) File.SetUnixFileMode(shardPath, UnixFileMode.UserRead);
        }
        string digest = shardHashes[0];
        string releaseId = Version(expected + 1);
        var measures = Measures(draft.Model);
        var startingReport = DefaultReport(current.Name, releaseId, draft.Model, measures);
        string modelRevision = refresh && current.Asset is { } previous
            ? previous.Release.ModelRevisionId : Version(expected);
        var release = new AnalysisRelease(releaseId, modelRevision, draft.Model, measures,
            count, DateTimeOffset.UtcNow, dataAsOf, Hash(schemaText), current.Input.Iceberg?.SnapshotId,
            current.Input.Label, startingReport);
        var physical = new AnalysisAsset(release, asset, digest, schema.RootElement.Clone(), request.OperationId,
            result.ExecutionId, current.Input.Iceberg?.RunReceipt ?? current.Input.Fingerprint, current.Input.Iceberg,
            current.Input.Sql, shardFiles.Select((fileName, index) =>
                new AnalysisShard(fileName, shardHashes[index], shardRows[index])).ToArray());
        var next = current with { Asset = physical, Report = current.Report ?? startingReport };

        scope = Reauthorize(scope);
        scope.Identity.Require("analysis.write", id);
        RequireSource(scope, current.Input);
        CommitDataset(catalog, next, expected, lease);
        var receipt = new AnalysisPublication(request.OperationId, "published", count, Release: release);
        SaveOperation(catalog, operation with { Publication = receipt }, operationRevision);
        return receipt;
    }

    private static AnalysisPublication? PublishedForOperation(Catalog catalog, string dataset, ulong expected,
        string operation)
    {
        string? receipt = catalog.Get(CatalogKind.Audit, dataset, expected + 1);
        var asset = receipt is null ? null : Read<DatasetState>(receipt).Asset;
        return asset?.OperationId == operation
            ? new(operation, "published", asset.Release.Rows, Release: asset.Release) : null;
    }

    private static bool SourceMatches(AnalysisInput input, AnalysisAsset asset)
    {
        if (input.Sql is { } sql && asset.Sql is { } publishedSql)
            return SqlAccessShape(sql) == SqlAccessShape(publishedSql);
        if (input.Iceberg is { } iceberg && asset.Source is { } publishedIceberg)
            return AccessShape(iceberg) == AccessShape(publishedIceberg);
        return input.Sql is null && input.Iceberg is null && asset.Sql is null && asset.Source is null &&
            StringComparer.Ordinal.Equals(input.Fingerprint, asset.SourceRunReceipt);
    }

    private static void SaveOperation(Catalog catalog, OperationState state, ulong expected) =>
        catalog.CompareExchange(CatalogKind.Audit, state.Id, expected, JsonSerializer.Serialize(state, Json));

    private static async Task<string> FileHash(string path, CancellationToken cancellation)
    {
        HostFiles.NoLinks(path);
        await using var input = File.OpenRead(path);
        return Convert.ToHexStringLower(await SHA256.HashDataAsync(input, cancellation));
    }

    private static void RemoveAttempt(HostPolicy scope, string id, string asset, IReadOnlyList<string>? assets = null)
    {
        string path = new AnalysisFileScope(id, Asset: asset).AssetPath(scope)!;
        HostFiles.NoLinks(path);
        string parent = Path.GetDirectoryName(path)!;
        if (!Directory.Exists(parent)) return;
        // Only the attempt reserved by this operation. A catalog-visible release
        // is found and returned before this cleanup is reached.
        var names = (assets ?? [asset]).ToHashSet(StringComparer.Ordinal);
        foreach (var file in Directory.EnumerateFiles(parent, "release-*", SearchOption.TopDirectoryOnly))
        {
            HostFiles.NoLinks(file);
            string name = Path.GetFileName(file);
            bool generation = names.Any(candidate =>
                name.StartsWith(candidate + ".ud-generation-", StringComparison.Ordinal) &&
                Guid.TryParseExact(name.AsSpan(candidate.Length + ".ud-generation-".Length), "N", out _));
            if (names.Contains(name) || generation)
                File.Delete(file);
        }
    }

    private static string IssueText(string role) => role switch
    {
        "date" => "日期为空或与所选格式不符。",
        "amount" => "金额无法按所选格式精确转换，请检查小数位和空值。",
        "currency" => "币种为空或与确认的币种不一致。",
        "identifier" => "订单号为空或超出可读范围。",
        _ => "字段超出可读范围。"
    };

    private static AnalysisMeasure[] Measures(AnalysisModel model)
    {
        var order = model.Fields.FirstOrDefault(field => field.Role == "identifier");
        var money = model.Fields.FirstOrDefault(field => field.Role == "amount");
        var measures = new List<AnalysisMeasure>
        {
            new("records", order is null ? "记录数" : "明细条数", "按记录计数")
        };
        if (money is not null)
            measures.Add(new("amount", money.Label + "合计", $"对「{money.Label}」求和，空金额不参与计算", money.FieldId, model.Currency));
        if (order is not null)
            measures.Add(new("orders", "订单数", $"按「{order.Label}」精确去重", order.FieldId));
        if (money is not null && order is not null)
            measures.Add(new("average", "客单价", "当前范围的金额合计 ÷ 订单数", Unit: model.Currency));
        return AnalysisMetricBinder.Describe(model, measures.ToArray());
    }

    private static AnalysisSavedReport DefaultReport(string name, string releaseId, AnalysisModel model, AnalysisMeasure[] measures)
    {
        var date = model.Fields.FirstOrDefault(field => field.Role == "date");
        var category = model.Fields.FirstOrDefault(field => field.Role == "category");
        AnalysisDimension[] dimensions = date is not null ? [new(date.FieldId, "day")]
            : category is not null ? [new(category.FieldId)] : [];
        var chart = date is not null ? "line" : category is not null ? "bar" : "table";
        string measure = measures.Any(item => item.Id == "amount") ? "amount" : "records";
        return new(name, new(releaseId, dimensions), chart, measure);
    }
}
