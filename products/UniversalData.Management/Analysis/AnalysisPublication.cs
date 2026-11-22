using System.Globalization;
using System.Security.Cryptography;
using System.Text.Json;
using UniversalData.Hosting;

namespace UniversalData.Management;

internal sealed partial class AnalysisWorkspace
{
    private AnalysisPublication Operation(HostPolicy scope, string id, string operationId)
    {
        scope.Identity.Require("analysis.read", id);
        using var catalog = scope.OpenCatalog();
        var state = State(catalog, scope, id, catalog.LatestRevision(CatalogKind.Audit, Key(scope, id)));
        RequireSource(scope, state.Input.Iceberg);
        string key = OperationKey(scope, id, operationId);
        ulong revision = catalog.LatestRevision(CatalogKind.Audit, key);
        var operation = Read<OperationState>(catalog.Get(CatalogKind.Audit, key, revision));
        if (operation.Publication.State == "checking" &&
            PublishedForOperation(catalog, state.Id, operation.ExpectedRevision, operationId) is { } receipt)
            return receipt;
        return operation.Publication;
    }

    private async Task<AnalysisPublication> Publish(HostPolicy scope, string id, AnalysisPublish request,
        CancellationToken cancellation)
    {
        scope.Identity.Require("analysis.write", id);
        scope.Identity.Require("analysis.read", id);
        ulong expected = Revision(request.ExpectedRevision);
        string datasetKey = Key(scope, id);
        string operationKey = OperationKey(scope, id, request.OperationId);
        string fingerprint = Hash(JsonSerializer.Serialize(request, Json));
        using var catalog = scope.OpenCatalog();
        using var lease = LockDataset(catalog, datasetKey);
        ulong latest = catalog.LatestRevision(CatalogKind.Audit, datasetKey);
        var current = State(catalog, scope, id, latest);
        RequireSource(scope, current.Input.Iceberg);
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
            if (PublishedForOperation(catalog, datasetKey, expected, request.OperationId) is { } recovered)
            {
                SaveOperation(catalog, existing with { Publication = recovered }, operationRevision);
                return recovered;
            }
        }
        RequireCurrent(catalog, datasetKey, expected);
        if (request.ExpectedReleaseId != current.Asset?.Release.Id)
            throw Failure(StatusCode.Fenced, "当前报表版本已变化，请重新打开分析。");
        var draft = current.Draft ?? throw Failure(StatusCode.Invalid, "请先确认并保存字段规则。");
        draft = draft with { Model = BindModel(draft.Source, draft.Model, confirmed: true) };

        if (existing is not null)
            RemoveAttempt(scope, id, existing.Attempt);
        string asset = "release-" + Guid.NewGuid().ToString("N") + ".ndjson";
        var operation = new OperationState(operationKey, ProductInfo.Protocol, fingerprint, asset, expected,
            new(request.OperationId, "checking"));
        SaveOperation(catalog, operation, operationRevision++);
        var files = new AnalysisFileScope(id, current.Input.FileId, asset, CreateAsset: true);
        string file = files.AssetPath(scope)!;
        HostFiles.NoLinks(file);
        string directory = Path.GetDirectoryName(file)!;
        if (OperatingSystem.IsWindows()) Directory.CreateDirectory(directory);
        else Directory.CreateDirectory(directory, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);

        if (files.InputPath(scope) is { } input && await FileHash(input, cancellation) != current.Input.Fingerprint)
            throw Failure(StatusCode.Corrupt, "原始导入文件已变化，请重新上传。");

        var plan = new AnalysisPlan(id);
        var source = plan.Source(current.Input, draft.Source, files.InputPath(scope), draft.Model);
        NativeRows result;
        try
        {
            result = await Run(scope, files, plan.Prepare(source, file, draft), 1, cancellation,
                "表格无法完整读取。请检查字段类型、工作表表头及公式。Excel 订单号请保存为文本，公式请粘贴为值。");
        }
        catch (UniversalDataException failure) when (failure.Code is StatusCode.Schema or StatusCode.Quality
            or StatusCode.Parse or StatusCode.Incompatible or StatusCode.Resource or StatusCode.Overflow)
        {
            RemoveAttempt(scope, id, asset);
            var rejected = new AnalysisPublication(request.OperationId, "rejected", Error:
                failure.Code == StatusCode.Resource ? "超过 10 万行或当前处理限额，请缩小表格。" : failure.Message);
            SaveOperation(catalog, operation with { Publication = rejected }, operationRevision);
            return rejected;
        }

        var fact = result.Rows.Single();
        int count = int.Parse(fact["scannedRows"]!, CultureInfo.InvariantCulture);
        int affected = int.Parse(fact["affectedRows"]!, CultureInfo.InvariantCulture);
        scope = Reauthorize(scope);
        scope.Identity.Require("analysis.write", id);
        scope.Identity.Require("analysis.read", id);
        RequireSource(scope, current.Input.Iceberg);
        if (affected > 0)
        {
            var issues = JsonSerializer.Deserialize<RecordIssue[]>(fact["issues"]!, Json)!.Select(issue =>
            {
                var field = draft.Model.Fields[issue.Column];
                return new AnalysisIssue(issue.Row, field.FieldId, IssueText(field.Role));
            }).ToArray();
            var rejected = new AnalysisPublication(request.OperationId, "rejected", count, affected, issues);
            RemoveAttempt(scope, id, asset);
            SaveOperation(catalog, operation with { Publication = rejected }, operationRevision);
            return rejected;
        }

        string schemaText = fact["schema"]!;
        using var schema = JsonDocument.Parse(schemaText);
        string digest = await FileHash(file, cancellation);
        if (!OperatingSystem.IsWindows()) File.SetUnixFileMode(file, UnixFileMode.UserRead);
        string releaseId = Version(expected + 1);
        var measures = Measures(draft.Model);
        var startingReport = DefaultReport(current.Name, releaseId, draft.Model, measures);
        var release = new AnalysisRelease(releaseId, Version(expected), draft.Model, measures,
            count, DateTimeOffset.UtcNow, current.Input.DataAsOf, Hash(schemaText), current.Input.Iceberg?.SnapshotId,
            current.Input.Label, startingReport);
        var physical = new AnalysisAsset(release, asset, digest, schema.RootElement.Clone(), request.OperationId,
            result.ExecutionId, current.Input.Iceberg?.RunReceipt ?? current.Input.Fingerprint, current.Input.Iceberg);
        var next = current with { Asset = physical, Report = current.Report ?? startingReport };

        scope = Reauthorize(scope);
        scope.Identity.Require("analysis.write", id);
        RequireSource(scope, current.Input.Iceberg);
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

    private static void SaveOperation(Catalog catalog, OperationState state, ulong expected) =>
        catalog.CompareExchange(CatalogKind.Audit, state.Id, expected, JsonSerializer.Serialize(state, Json));

    private static async Task<string> FileHash(string path, CancellationToken cancellation)
    {
        HostFiles.NoLinks(path);
        await using var input = File.OpenRead(path);
        return Convert.ToHexStringLower(await SHA256.HashDataAsync(input, cancellation));
    }

    private static void RemoveAttempt(HostPolicy scope, string id, string asset)
    {
        string path = new AnalysisFileScope(id, Asset: asset).AssetPath(scope)!;
        HostFiles.NoLinks(path);
        string parent = Path.GetDirectoryName(path)!;
        if (!Directory.Exists(parent)) return;
        // Only the attempt reserved by this operation. A catalog-visible release
        // is found and returned before this cleanup is reached.
        foreach (var file in Directory.EnumerateFiles(parent, Path.GetFileName(path) + "*", SearchOption.TopDirectoryOnly))
        {
            HostFiles.NoLinks(file);
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
        return measures.ToArray();
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
