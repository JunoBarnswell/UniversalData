using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.DataProtection;
using UniversalData.Hosting;

namespace UniversalData.Management;

internal sealed partial class AnalysisWorkspace(HostPolicy host, IdentityDirectory identities,
    WorkerSupervisor worker, IDataProtectionProvider protection)
{
    private static readonly JsonSerializerOptions Json = HostPolicy.JsonOptions;
    private const int MaxColumns = 64;
    private const int MaxUploadBytes = 50 * 1024 * 1024;
    private static readonly string[] RawTypes =
        ["utf8", "bool", "int64", "uint64", "decimal128", "float64", "date32", "timestamp", "datetime"];
    private static readonly string[] ApprovedSqlDrivers =
        ["ud:sqlite", "ud:postgresql", "ud:mysql", "ud:mariadb", "ud:odbc", "ud:oracle_oci"];
    private readonly IDataProtector contexts = protection.CreateProtector("UniversalData.Analysis.Query.v1");
    private readonly IDataProtector cursors = protection.CreateProtector("UniversalData.Analysis.Cursor.v1");

    private sealed record DatasetState(string Id, int Version, string DatasetId, string Owner, string Name,
        AnalysisInput Input, AnalysisDraft? Draft = null, AnalysisAsset? Asset = null,
        AnalysisSavedReport? Report = null);
    private sealed record OperationState(string Id, int Version, string Fingerprint, string Attempt, ulong ExpectedRevision,
        AnalysisPublication Publication, string[]? Assets = null);
    private sealed record NativeRows(string ExecutionId, Dictionary<string, string?>[] Rows);
    private sealed record RecordIssue(int Row, int Column);
    private sealed record SourceChoice(string SourceId, string? DatasetId = null, string? ExpectedRevision = null);
    private sealed record Target(string Id, ulong Revision);

    internal void Map(RouteGroupBuilder api)
    {
        var group = api.MapGroup("/analysis");
        group.MapGet("/sources", (HttpContext context) => Sources(Scope(context)));
        group.MapGet("/datasets", (HttpContext context) => List(Scope(context)));
        group.MapGet("/datasets/{id}", (string id, HttpContext context) => Open(Scope(context), id));
        group.MapPost("/datasets", (SourceChoice choice, HttpContext context) =>
            Create(Scope(context), choice, context.RequestAborted));
        group.MapPost("/datasets/upload", async Task<IResult> (HttpContext context) =>
            Results.Ok(await Upload(Scope(context), context.Request, context.RequestAborted)));
        group.MapGet("/datasets/{id}/preview", (string id, string? sheet, HttpContext context) =>
            Preview(Scope(context), id, sheet, context.RequestAborted));
        group.MapPut("/datasets/{id}/draft", (string id, AnalysisDraftWrite request, HttpContext context) =>
            SaveDraft(Scope(context), id, request, context.RequestAborted));
        group.MapPost("/datasets/{id}/publish", (string id, AnalysisPublish request, HttpContext context) =>
            Publish(Scope(context), id, request, context.RequestAborted));
        group.MapGet("/datasets/{id}/operations/{operationId}", (string id, string operationId, HttpContext context) =>
            Operation(Scope(context), id, operationId));
        group.MapGet("/datasets/{id}/releases/{releaseId}", (string id, string releaseId, HttpContext context) =>
            Asset(Scope(context), id, releaseId).Release);
        group.MapPost("/datasets/{id}/query", (string id, AnalysisQuery request, HttpContext context) =>
            Query(Scope(context), id, request, context.RequestAborted));
        group.MapPost("/datasets/{id}/drill", (string id, AnalysisDrill request, HttpContext context) =>
            Drill(Scope(context), id, request, context.RequestAborted));
        group.MapPost("/datasets/{id}/export", (string id, AnalysisExport request, HttpContext context) =>
            Export(Scope(context), id, request, context.RequestAborted));
        group.MapPut("/datasets/{id}/report", (string id, AnalysisReportWrite request, HttpContext context) =>
            SaveReport(Scope(context), id, request));
        MapTeamDashboards(group);
        MapAnalysisRefresh(group);
    }

    private HostPolicy Scope(HttpContext context)
    {
        context.Response.Headers.CacheControl = "no-store";
        return host.Bind(context.Identity(identities));
    }

    private HostPolicy Reauthorize(HostPolicy scope)
    {
        var actor = identities.Find(scope.Identity.Subject, scope.Identity.Stamp);
        if (actor is null || actor.Tenant != scope.Tenant)
            throw Failure(StatusCode.Auth, "当前权限已变更，请重新登录。");
        return host.Bind(actor);
    }

    private AnalysisIcebergSource[] PublishedSources(HostPolicy scope) =>
        host.Tenants!.GetValueOrDefault(scope.Tenant)?.AnalysisSources ?? [];

    private AnalysisSqlSource[] PublishedSqlSources(HostPolicy scope) =>
        host.Tenants!.GetValueOrDefault(scope.Tenant)?.AnalysisSqlSources ?? [];

    private AnalysisSource[] Sources(HostPolicy scope)
    {
        scope.Identity.Require("analysis.read", "*");
        var iceberg = PublishedSources(scope).Where(source => CanRead(scope, source))
            .Select(source => new AnalysisSource(source.Id, source.Name, "iceberg", source.SnapshotId));
        var sql = PublishedSqlSources(scope).Where(source => CanRead(scope, source))
            .Select(source => new AnalysisSource(source.Id, source.Name, "sql",
                Hash(SqlAccessShape(source))[..24]));
        return iceberg.Concat(sql).ToArray();
    }

    private static bool CanRead(HostPolicy scope, AnalysisIcebergSource source)
    {
        bool Allowed(AnalysisConnection connection, ConnectionAccess action) =>
            scope.Authorize(scope.Tenant, connection.Resource, action);
        bool CanUseCredential(AnalysisConnection connection) =>
            connection.CredentialReference is null || scope.CredentialFiles.ContainsKey(connection.CredentialReference);
        return HostIdentity.ValidId(source.Id) && source.Id != "sample" &&
            scope.Identity.Allows("analysis.source", source.Id) &&
            long.TryParse(source.SnapshotId, NumberStyles.None, CultureInfo.InvariantCulture, out var snapshot) &&
            snapshot > 0 && !string.IsNullOrWhiteSpace(source.RunReceipt) &&
            source.Catalog.Driver == "http" && source.Storage.Driver is "file" or "s3" &&
            Allowed(source.Catalog, ConnectionAccess.Open) && Allowed(source.Catalog, ConnectionAccess.Network) &&
            Allowed(source.Storage, ConnectionAccess.Open) && Allowed(source.Storage, ConnectionAccess.Read) &&
            (source.Storage.Driver != "s3" || Allowed(source.Storage, ConnectionAccess.Network)) &&
            CanUseCredential(source.Catalog) && CanUseCredential(source.Storage);
    }

    private static bool CanRead(HostPolicy scope, AnalysisSqlSource source)
    {
        string driver = source.Connection.Driver;
        string resource = source.Connection.Resource;
        string? credential = source.Connection.CredentialReference;
        if (!ApprovedSqlDrivers.Contains(driver, StringComparer.Ordinal) ||
            !HostIdentity.ValidId(source.Id) || source.Id == "sample" || string.IsNullOrWhiteSpace(source.Name) ||
            string.IsNullOrWhiteSpace(resource) || source.Table is not { Length: > 0 and <= 512 } ||
            !scope.Identity.Allows("analysis.source", source.Id) ||
            credential is not null && !scope.CredentialFiles.ContainsKey(credential) ||
            credential is null && driver != "ud:sqlite")
            return false;
        if (source.Schema is { } schema && schema.ValueKind != JsonValueKind.Object)
            return false;
        if (driver == "ud:sqlite" && !Path.IsPathFullyQualified(resource))
            return false;
        if (!scope.Authorize(scope.Tenant, resource, ConnectionAccess.Open) ||
            !scope.Authorize(scope.Tenant, resource, ConnectionAccess.Discover) ||
            !scope.Authorize(scope.Tenant, resource, ConnectionAccess.Read))
            return false;
        if (driver != "ud:sqlite" && !scope.Authorize(scope.Tenant, resource, ConnectionAccess.Network))
            return false;
        return ValidSqlIdentifier(source.Table);
    }

    private static bool ValidSqlIdentifier(string value) => !string.IsNullOrWhiteSpace(value) && value.Length <= 512 &&
        value.All(character => char.IsAsciiLetterOrDigit(character) || character is '_' or '.' or '$' or '"' or '`' or '[' or ']' or ' ');

    private static string AccessShape(AnalysisIcebergSource source) =>
        Hash(JsonSerializer.Serialize(new
        {
            source.Catalog, source.Storage, source.Namespace, source.Table, source.Warehouse, source.RowFilter, source.Schema
        }, Json));

    private static string SqlAccessShape(AnalysisSqlSource source) =>
        Hash(JsonSerializer.Serialize(new
        {
            source.Id, source.Name, source.Connection, source.Table, source.AllowSharing, source.Schema
        }, Json));

    private void RequireSource(HostPolicy scope, AnalysisIcebergSource? source)
    {
        if (source is null) return;
        if (!CanRead(scope, source) || !PublishedSources(scope).Any(current =>
                current.Id == source.Id && CanRead(scope, current) && AccessShape(current) == AccessShape(source)))
            throw Failure(StatusCode.Auth, "当前身份已无法读取此报表的数据来源。");
    }

    private void RequireSource(HostPolicy scope, AnalysisSqlSource? source)
    {
        if (source is null) return;
        if (!CanRead(scope, source) || !PublishedSqlSources(scope).Any(current =>
                current.Id == source.Id && CanRead(scope, current) && SqlAccessShape(current) == SqlAccessShape(source)))
            throw Failure(StatusCode.Auth, "当前身份已无法读取此报表的数据来源。");
    }

    private void RequireSource(HostPolicy scope, AnalysisInput input)
    {
        if (input.Kind == "iceberg")
        {
            if (input.Iceberg is null) throw Failure(StatusCode.Auth, "分析数据来源无效。");
            RequireSource(scope, input.Iceberg);
        }
        else if (input.Kind == "sql")
        {
            if (input.Sql is null) throw Failure(StatusCode.Auth, "分析数据来源无效。");
            RequireSource(scope, input.Sql);
        }
        else if (input.Iceberg is not null || input.Sql is not null)
            throw Failure(StatusCode.Auth, "分析数据来源类型无效。");
    }

    private void RequireSource(HostPolicy scope, AnalysisAsset asset)
    {
        RequireSource(scope, asset.Source);
        RequireSource(scope, asset.Sql);
    }

    /// <summary>
    /// Validates that an asset may be bound to a shared/team view.  Only an
    /// explicitly host-approved SQL source can leave its owner's workspace;
    /// the current catalog binding and source definition are checked again so
    /// an old asset cannot be rebound to a changed source.
    /// </summary>
    private void RequireDistributable(HostPolicy owner, string id, AnalysisSqlSource? source)
    {
        owner.Identity.Require("analysis.read", id);
        if (source is null || !source.AllowSharing)
            throw Failure(StatusCode.Auth, "只有明确允许共享的 SQL 数据源可以加入团队报表。");
        var approved = PublishedSqlSources(owner).SingleOrDefault(current =>
            current.Id == source.Id && current.AllowSharing && CanRead(owner, current) &&
            SqlAccessShape(current) == SqlAccessShape(source));
        if (approved is null)
            throw Failure(StatusCode.Auth, "当前身份已无法验证可共享的数据来源。");
    }

    private void RequireDistributable(HostPolicy owner, string id, AnalysisAsset asset) =>
        RequireDistributable(owner, id, asset.Sql);

    private static string Hash(string value) => Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(value)));
    private static string Prefix(HostPolicy scope) => "analysis/" + Hash(scope.Identity.Subject)[..24] + "/";
    private static string Key(HostPolicy scope, string id) => Prefix(scope) + GuidKey(id);
    private static string OperationKey(HostPolicy scope, string id, string operation) =>
        "analysis-op/" + Hash(Key(scope, id))[..24] + "/" + GuidKey(operation);

    private static string GuidKey(string value)
    {
        if (!Guid.TryParseExact(value, "N", out var id) || id == Guid.Empty || id.ToString("N") != value)
            throw Failure(StatusCode.Invalid, "分析标识无效。");
        return value;
    }
    private static ulong Revision(string value)
    {
        if (!ulong.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var revision) || revision == ulong.MaxValue)
            throw Failure(StatusCode.Invalid, "版本标识无效。");
        return revision;
    }
    private static string Version(ulong value) => value.ToString(CultureInfo.InvariantCulture);

    private static UniversalDataException Failure(StatusCode code, string message) =>
        new((int)code, message, new(ErrorSource.Native, code, message));

    private static T Read<T>(string? text) where T : class => text is not null
        ? JsonSerializer.Deserialize<T>(text, Json) ?? throw new InvalidDataException("Analysis record is invalid")
        : throw Failure(StatusCode.Invalid, "找不到指定的分析或版本。");

    private static DatasetState State(Catalog catalog, HostPolicy scope, string id, ulong revision)
    {
        var state = Read<DatasetState>(catalog.Get(CatalogKind.Audit, Key(scope, id), revision));
        if (state.Owner != scope.Identity.Subject || state.DatasetId != id)
            throw Failure(StatusCode.Auth, "当前身份无法访问此分析。");
        return state;
    }

    private static AnalysisDataset View(DatasetState state, ulong revision) =>
        new(state.DatasetId, state.Name, Version(revision), state.Input.Label, state.Input.Kind,
            state.Draft?.Source.Sheet, state.Draft?.Model, state.Asset?.Release.Id, state.Report);

    private AnalysisDataset[] List(HostPolicy scope)
    {
        scope.Identity.Require("analysis.read", "*");
        using var catalog = scope.OpenCatalog();
        using var results = JsonDocument.Parse(catalog.Query(JsonSerializer.Serialize(new
        {
            kind = (int)CatalogKind.Audit, afterId = Prefix(scope), latestOnly = true, limit = 1000
        }, Json)));
        var values = new List<AnalysisDataset>();
        foreach (var row in results.RootElement.EnumerateArray())
        {
            string key = row.GetProperty("id").GetString()!;
            if (!key.StartsWith(Prefix(scope), StringComparison.Ordinal)) break;
            string id = key[Prefix(scope).Length..];
            if (!scope.Identity.Allows("analysis.read", id)) continue;
            ulong revision = row.GetProperty("revision").GetUInt64();
            values.Add(View(State(catalog, scope, id, revision), revision));
        }
        return values.ToArray();
    }

    private AnalysisDataset Open(HostPolicy scope, string id)
    {
        scope.Identity.Require("analysis.read", id);
        using var catalog = scope.OpenCatalog();
        ulong revision = catalog.LatestRevision(CatalogKind.Audit, Key(scope, id));
        return View(State(catalog, scope, id, revision), revision);
    }

    private static Target AuthorizeTarget(HostPolicy scope, string? id, string? expected)
    {
        if (id is null)
        {
            if (expected is not null) throw Failure(StatusCode.Invalid, "请选择要更新的分析。");
            scope.Identity.Require("analysis.write", "*");
            scope.Identity.Require("analysis.read", "*");
            return new(Guid.NewGuid().ToString("N"), 0);
        }
        if (expected is null) throw Failure(StatusCode.Invalid, "请提供最新草稿版本。");
        scope.Identity.Require("analysis.write", id);
        scope.Identity.Require("analysis.read", id);
        using var catalog = scope.OpenCatalog();
        var revision = Revision(expected);
        RequireCurrent(catalog, Key(scope, id), revision);
        _ = State(catalog, scope, id, revision);
        return new(id, revision);
    }

    private AnalysisDataset StoreSource(HostPolicy scope, Target target, AnalysisInput input)
    {
        string id = target.Id;
        using var catalog = scope.OpenCatalog();
        DatasetState next = target.Revision == 0
            ? new(Key(scope, id), ProductInfo.Protocol, id, scope.Identity.Subject, input.Label, input)
            : State(catalog, scope, id, target.Revision) with { Input = input, Draft = null };
        using var lease = LockDataset(catalog, next.Id);
        CommitDataset(catalog, next, target.Revision, lease);
        return View(next, target.Revision + 1);
    }

    private async Task<AnalysisDataset> Create(HostPolicy scope, SourceChoice choice, CancellationToken cancellation)
    {
        var target = AuthorizeTarget(scope, choice.DatasetId, choice.ExpectedRevision);
        if (choice.SourceId == "sample")
        {
            const string sample = "订单号,下单日期,地区,商品类别,实付金额,币种\n" +
                "0001,2026-11-02,华东,家具,100.00,CNY\n" +
                "0001,2026-11-02,华东,用品,50.00,CNY\n" +
                "0002,2026-11-06,华南,家具,300.00,CNY\n";
            using var stream = new MemoryStream(Encoding.UTF8.GetBytes(sample));
            return await StoreUpload(scope, target, stream, "订单样例", "csv", cancellation);
        }
        var iceberg = PublishedSources(scope).Where(item => item.Id == choice.SourceId && CanRead(scope, item)).ToArray();
        var sql = PublishedSqlSources(scope).Where(item => item.Id == choice.SourceId && CanRead(scope, item)).ToArray();
        if (iceberg.Length + sql.Length != 1)
            throw Failure(StatusCode.Auth, "当前身份无法读取所选数据。");
        if (iceberg is [var table])
            return StoreSource(scope, target,
                new("iceberg", table.Name, null, Hash(JsonSerializer.Serialize(table, Json)), table.DataAsOf, table));
        var database = sql[0];
        return StoreSource(scope, target,
            new("sql", database.Name, null, SqlAccessShape(database),
                DateTimeOffset.UtcNow, Sql: database));
    }

    private Task<AnalysisDataset> Upload(HostPolicy scope, HttpRequest request, CancellationToken cancellation)
    {
        var target = AuthorizeTarget(scope, request.Query["datasetId"].FirstOrDefault(),
            request.Query["expectedRevision"].FirstOrDefault());
        if (request.ContentLength > MaxUploadBytes)
            throw Failure(StatusCode.Resource, "表格大小超过 50 MiB。");
        string name = Path.GetFileName(Uri.UnescapeDataString(request.Headers["X-File-Name"].ToString()));
        string extension = Path.GetExtension(name).ToLowerInvariant();
        if (extension is not (".csv" or ".xlsx"))
            throw Failure(StatusCode.Invalid, "请选择 CSV 或单工作表 Excel 文件。");
        return StoreUpload(scope, target, request.Body, Name(Path.GetFileNameWithoutExtension(name)), extension[1..], cancellation);
    }

    private async Task<AnalysisDataset> StoreUpload(HostPolicy scope, Target upload, Stream body, string label, string kind,
        CancellationToken cancellation)
    {
        string id = upload.Id;
        string file = "input-" + Guid.NewGuid().ToString("N") + "." + kind;
        var files = new AnalysisFileScope(id, Input: file);
        string destination = files.InputPath(scope)!;
        HostFiles.NoLinks(destination);
        string directory = Path.GetDirectoryName(destination)!;
        if (OperatingSystem.IsWindows()) Directory.CreateDirectory(directory);
        else Directory.CreateDirectory(directory, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        bool catalogStarted = false;
        try
        {
            using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            await using (var target = new FileStream(destination, FileMode.CreateNew, FileAccess.Write,
                             FileShare.None, 65536, FileOptions.Asynchronous))
            {
                long length = 0;
                byte[] buffer = new byte[65536];
                int read;
                while ((read = await body.ReadAsync(buffer, cancellation)) != 0)
                {
                    length += read;
                    if (length > MaxUploadBytes)
                        throw Failure(StatusCode.Resource, "表格大小超过 50 MiB。");
                    hash.AppendData(buffer, 0, read);
                    await target.WriteAsync(buffer.AsMemory(0, read), cancellation);
                }
                if (length == 0)
                    throw Failure(StatusCode.Invalid, "文件为空。");
                await target.FlushAsync(cancellation);
                target.Flush(flushToDisk: true);
            }
            if (!OperatingSystem.IsWindows())
                File.SetUnixFileMode(destination, UnixFileMode.UserRead);
            scope = Reauthorize(scope);
            scope.Identity.Require("analysis.write", upload.Revision == 0 ? "*" : id);
            scope.Identity.Require("analysis.read", upload.Revision == 0 ? "*" : id);
            catalogStarted = true;
            return StoreSource(scope, upload, new(kind, label, file, Convert.ToHexStringLower(hash.GetHashAndReset()), DateTimeOffset.UtcNow));
        }
        finally
        {
            if (!catalogStarted)
            {
                File.Delete(destination);
                if (upload.Revision == 0) Directory.Delete(directory);
            }
        }
    }

    private static string Name(string value)
    {
        string text = value.Trim();
        if (text.Length is < 1 or > 128 || text.Any(char.IsControl))
            throw Failure(StatusCode.Invalid, "请填写 1 到 128 字的名称。");
        return text;
    }

    private async Task<AnalysisPreview> Preview(HostPolicy scope, string id, string? selected, CancellationToken cancellation)
    {
        scope.Identity.Require("analysis.read", id);
        using var catalog = scope.OpenCatalog();
        var state = State(catalog, scope, id, catalog.LatestRevision(CatalogKind.Audit, Key(scope, id)));
        RequireSource(scope, state.Input);
        bool chooseSheet = state.Input.Kind == "xlsx" && selected is null && state.Draft?.Source.Sheet is null;
        var schema = await Inspect(scope, state, selected ?? state.Draft?.Source.Sheet, cancellation);
        if (chooseSheet && schema.Sheets.Length > 1)
            return new([], [], schema.Sheets, null, null);

        var files = new AnalysisFileScope(id, Input: state.Input.FileId);
        var plan = new AnalysisPlan(id);
        var source = plan.Source(state.Input, schema, files.InputPath(scope), stream: true);
        var sample = await Run(scope, files, plan.Sample(source), 32, cancellation,
            "无法预览表格。请检查一行唯一表头、字段类型和公式，Excel 可另存为值后再上传。", sample: true);
        scope = Reauthorize(scope);
        scope.Identity.Require("analysis.read", id);
        RequireSource(scope, state.Input);

        var fields = schema.Columns.Select(column => Suggest(column, sample.Rows)).ToArray();
        var rows = sample.Rows.Select(row => schema.Columns.ToDictionary(column => column.Id,
            column => row.GetValueOrDefault(column.Name), StringComparer.Ordinal)).ToArray();
        return new(fields, rows, schema.Sheets, schema.Sheet, SuggestModel(fields, rows));
    }

    private async Task<AnalysisSourceSchema> Inspect(HostPolicy scope, DatasetState state, string? sheet,
        CancellationToken cancellation)
    {
        var input = state.Input;
        if (input.Iceberg is { } iceberg)
            return new(ReadColumns(iceberg.Schema, file: false), iceberg.Schema, AnalysisPlan.Document(new { }), [], null);
        if (input.Sql is { } sql)
            return await InspectSql(scope, state, sql, cancellation);
        if (sheet is { Length: > 128 } || sheet?.Any(char.IsControl) == true)
            throw Failure(StatusCode.Invalid, "工作表名称无效。");

        var files = new AnalysisFileScope(state.DatasetId, Input: input.FileId);
        string path = files.InputPath(scope)!;
        var payload = AnalysisPlan.Document(new
        {
            connection = "input",
            configuration = new
            {
                format = input.Kind,
                options = input.Kind == "csv" ? (object)new { header = true, nullMarker = "" }
                    : new { header = true, sheet, formulaPolicy = "null", maxDecompressedBytes = 150 * 1024 * 1024 }
            }
        });
        var response = await InspectWorker(scope, files, "file", new AnalysisPlan(state.DatasetId).Empty(path), payload,
            cancellation, "无法识别表格。请提供一行唯一表头的 CSV 或 Excel 明细表。");
        var schema = response.GetProperty("schema");
        var metadata = response.GetProperty("metadata");
        var columns = ReadColumns(schema, file: true);
        string[] sheets = metadata.TryGetProperty("sheets", out var sheetList)
            ? sheetList.EnumerateArray().Select(item => item.GetString()!).ToArray() : [];
        string? selected = metadata.TryGetProperty("selectedSheet", out var choice) ? choice.GetString() : null;
        var options = input.Kind == "csv" ? metadata.GetProperty("options") : AnalysisPlan.Document(new { });
        // CSV columns remain lexical strings. Excel-native dates remain typed.
        var readSchema = AnalysisPlan.Document(new
        {
            version = ProductInfo.Protocol,
            fields = columns.Select(column => new { name = column.Name, nullable = true,
                type = input.Kind == "xlsx" && column.Type == "date32" ? "date32" : "utf8" })
        });
        return new(columns, readSchema, options, sheets, selected);
    }

    private async Task<AnalysisSourceSchema> InspectSql(HostPolicy scope, DatasetState state,
        AnalysisSqlSource source, CancellationToken cancellation)
    {
        JsonElement schema;
        if (source.Schema is { } cached)
            schema = cached.Clone();
        else
        {
            var payload = AnalysisPlan.Document(new { connection = "input", table = source.Table });
            var response = await InspectWorker(scope, null, "schema", new AnalysisPlan(state.DatasetId).Schema(source),
                payload, cancellation, "无法识别数据库表结构，请检查来源表或视图。");
            schema = response.Clone();
        }
        var columns = ReadColumns(schema, file: false);
        return new(columns, schema, AnalysisPlan.Document(new { }), [], null);
    }

    private static AnalysisColumn[] ReadColumns(JsonElement schema, bool file)
    {
        if (!schema.TryGetProperty("fields", out var fields) || fields.ValueKind != JsonValueKind.Array ||
            fields.GetArrayLength() is < 1 or > MaxColumns)
            throw Failure(StatusCode.Schema, "表格需要 1 到 64 列。");
        var columns = new List<AnalysisColumn>();
        foreach (var field in fields.EnumerateArray())
        {
            string label = field.GetProperty("name").GetString()!;
            _ = Name(label);
            string type = field.GetProperty("type").GetString()!;
            if (!file && !RawTypes.Contains(type))
                throw Failure(StatusCode.Schema, "所选数据包含暂未支持的列类型。");
            string identity = !file && field.TryGetProperty("identity", out var native) &&
                Guid.TryParseExact(native.GetString(), "N", out var fieldId) && fieldId != Guid.Empty
                ? fieldId.ToString("N") : Hash(label)[..32];
            columns.Add(new("f" + identity, label, type));
        }
        if (columns.Select(column => column.Name.Trim()).Distinct(StringComparer.OrdinalIgnoreCase).Count() != columns.Count ||
            columns.Select(column => column.Id).Distinct(StringComparer.Ordinal).Count() != columns.Count)
            throw Failure(StatusCode.Schema, "表头中有重复的字段名称。");
        return columns.ToArray();
    }

    private static AnalysisField Suggest(AnalysisColumn column, Dictionary<string, string?>[] sample)
    {
        string label = column.Name.Replace("_", "", StringComparison.Ordinal).Replace(" ", "", StringComparison.Ordinal).ToLowerInvariant();
        string[] values = sample.Select(row => row.GetValueOrDefault(column.Name))
            .Where(value => !string.IsNullOrWhiteSpace(value)).Select(value => value!).ToArray();
        (string role, string reason) = label switch
        {
            "订单号" or "订单编号" or "orderid" or "ordernumber" => ("identifier", "订单标识，用于按订单号去重。"),
            "实付金额" or "行金额" or "paidamount" or "lineamount" or "amount" => ("amount", "可确认币种和金额粒度后求和。"),
            "币种" or "货币" or "currency" or "currencycode" => ("currency", "用于核对单一币种。"),
            "地区" or "省份" or "订单状态" or "商品类别" or "region" or "category" or "status" => ("category", "可用于分类汇总和筛选。"),
            "下单日期" or "下单时间" or "订单日期" or "orderdate" or "orderat" => ("date", "可按日期筛选和汇总。"),
            "手机号" or "身份证号" or "phone" or "mobile" or "email" => ("omit", "默认停用该字段。"),
            _ when column.Type is "date32" or "timestamp" => ("date", "来源声明为日期或时间戳。"),
            _ when values.Length >= 4 && values.Distinct(StringComparer.Ordinal).Count() <= values.Length / 2 &&
                values.All(value => value.Length <= 64 && !decimal.TryParse(value, CultureInfo.InvariantCulture, out _))
                => ("category", "样本中有重复类别。"),
            _ => ("text", "保留用于查看明细。")
        };
        return new(column.Id, column.Name, column.Type, role, role == "text" || role == "amount" ? "review" : "suggested", reason);
    }

    private static AnalysisModel SuggestModel(AnalysisField[] fields, Dictionary<string, string?>[] rows)
    {
        HashSet<string> unique = new(StringComparer.Ordinal);
        var rules = fields.Select(field => new AnalysisRule(field.Id, field.Label,
            field.SemanticRole is "date" or "amount" or "identifier" or "currency" && !unique.Add(field.SemanticRole)
                ? "text" : field.SemanticRole)).ToArray();
        var date = fields.FirstOrDefault(field => field.SemanticRole == "date");
        string format = "iso";
        if (date is not null)
        {
            string[] dates = rows.Select(row => row.GetValueOrDefault(date.Id)).Where(value => !string.IsNullOrEmpty(value))
                .Select(value => value!).ToArray();
            if (date.PhysicalType == "timestamp") format = "timestamp";
            else if (dates.Any(value => !DateOnly.TryParseExact(value, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out _)))
                format = "";
        }
        var order = rules.Any(rule => rule.Role == "identifier");
        var currency = rules.FirstOrDefault(rule => rule.Role == "currency");
        string?[] units = currency is null ? [] : rows.Select(row => row.GetValueOrDefault(currency.FieldId))
            .Where(value => !string.IsNullOrEmpty(value)).Distinct(StringComparer.Ordinal).Take(2).ToArray();
        string? suggestion = units is [{ Length: 3 } unit] && unit.All(character => character is >= 'A' and <= 'Z') ? unit : null;
        return new(order ? "一条订单商品明细" : "一条记录", rules, format, Currency: suggestion);
    }

    private async Task<AnalysisDataset> SaveDraft(HostPolicy scope, string id, AnalysisDraftWrite request,
        CancellationToken cancellation)
    {
        scope.Identity.Require("analysis.write", id);
        using var catalog = scope.OpenCatalog();
        ulong expected = Revision(request.ExpectedRevision);
        RequireCurrent(catalog, Key(scope, id), expected);
        var state = State(catalog, scope, id, expected);
        RequireSource(scope, state.Input);
        var source = state.Draft is { } existing && existing.Source.Sheet == request.Sheet
            ? existing.Source : await Inspect(scope, state, request.Sheet, cancellation);
        if (source.Sheets.Length > 1 && request.Sheet is null)
            throw Failure(StatusCode.Invalid, "请先选择一个工作表。");
        var model = BindModel(source, request.Model);
        scope = Reauthorize(scope);
        scope.Identity.Require("analysis.write", id);
        RequireSource(scope, state.Input);
        var next = state with { Name = Name(request.Name), Draft = new(model, source) };
        using var lease = LockDataset(catalog, state.Id);
        CommitDataset(catalog, next, expected, lease);
        return View(next, expected + 1);
    }

    private static AnalysisModel BindModel(AnalysisSourceSchema source, AnalysisModel model, bool confirmed = false)
    {
        const string uniqueRoles = "date identifier currency";
        if (model.Fields.Length != source.Columns.Length || model.Fields.Select(rule => rule.FieldId).Distinct().Count() != model.Fields.Length ||
            model.Fields.Any(rule => !source.Columns.Any(column => column.Id == rule.FieldId) ||
                rule.Role is not ("text" or "category" or "date" or "amount" or "identifier" or "currency" or "omit")) ||
            model.Fields.All(rule => rule.Role == "omit") ||
            uniqueRoles.Split(' ').Any(role => model.Fields.Count(rule => rule.Role == role) > 1))
            throw Failure(StatusCode.Schema, "字段设置无效。每种日期、订单号和币种只能选用一列，金额字段可按币种确认多列。");
        if (model.OffsetMinutes is < -840 or > 840 ||
            (model.DateFormat is not ("" or "iso" or "dmy" or "mdy" or "timestamp") ||
            confirmed && model.DateFormat == "" && model.Fields.Any(rule => rule.Role == "date")))
            throw Failure(StatusCode.Invalid, "请确认日期格式和时区。");
        if (model.AmountFormat is not ("point" or "pointGrouped" or "comma" or "commaGrouped"))
            throw Failure(StatusCode.Invalid, "请选择金额格式。");
        bool amount = model.Fields.Any(rule => rule.Role == "amount");
        bool currencyColumn = model.Fields.Any(rule => rule.Role == "currency");
        if (confirmed && (amount || currencyColumn) && (model.Currency is not { Length: 3 } currency ||
                currency.Any(character => character is < 'A' or > 'Z')))
            throw Failure(StatusCode.Invalid, "请填写三位币种代码，例如 CNY。");
        if (confirmed && amount && (!model.ConfirmLineAmount || !currencyColumn && !model.ConfirmSingleCurrency))
            throw Failure(StatusCode.Invalid, "请确认金额属于每行，并确认整张表使用单一币种。");
        var ordered = source.Columns.Select(column => model.Fields.Single(rule => rule.FieldId == column.Id))
            .Select(rule => rule with { Label = Name(rule.Label) }).ToArray();
        if (ordered.Where(rule => rule.Role != "omit").Select(rule => rule.Label).Distinct(StringComparer.OrdinalIgnoreCase).Count() !=
            ordered.Count(rule => rule.Role != "omit"))
            throw Failure(StatusCode.Schema, "显示名称需要各不相同。");
        AnalysisMetricBinder.Validate(model);
        return model with { Grain = Name(model.Grain), Fields = ordered, DateFormat = confirmed && model.DateFormat == "" ? "iso" : model.DateFormat,
            Currency = amount || currencyColumn ? model.Currency : null };
    }

    private static DatasetLease LockDataset(Catalog catalog, string key) =>
        new(catalog, catalog.TryAcquireLease(CatalogKind.Audit, key, Guid.NewGuid().ToString("N"), 120000)
            ?? throw Failure(StatusCode.Busy, "这份分析正在更新，请稍后重试。"));

    private static void CommitDataset(Catalog catalog, DatasetState state, ulong expected, DatasetLease lease) =>
        catalog.CompareExchange(CatalogKind.Audit, state.Id, expected, JsonSerializer.Serialize(state, Json), lease.Guard);

    private static void RequireCurrent(Catalog catalog, string key, ulong expected)
    {
        if (expected == 0 || catalog.LatestRevision(CatalogKind.Audit, key) != expected)
            throw Failure(StatusCode.Fenced, "分析已更新，请重新打开最新草稿。");
    }

    private async Task<JsonElement> InspectWorker(HostPolicy scope, AnalysisFileScope? files, string mode, string package,
        JsonElement payload, CancellationToken cancellation, string error)
    {
        try
        {
            return await worker.InspectAsync(new(ProductInfo.Protocol, files?.ScratchTicket ?? Guid.NewGuid().ToString("N"), mode,
                scope, scope.Identity, package, Payload: payload, AnalysisFiles: files), cancellation);
        }
        catch (UniversalDataException failure) when (failure.Code is StatusCode.Schema or StatusCode.Quality or
            StatusCode.Parse or StatusCode.Incompatible or StatusCode.Unsupported or StatusCode.Overflow)
        {
            throw Failure(failure.Code, error);
        }
        finally
        {
            // A supervised timeout can terminate the Worker before its own cleanup runs.
            if (files?.ScratchPath(scope) is { } scratch && Directory.Exists(scratch))
            {
                HostFiles.NoLinks(scratch);
                Directory.Delete(scratch, recursive: true);
            }
        }
    }

    private async Task<NativeRows> Run(HostPolicy scope, AnalysisFileScope files, string package, int limit,
        CancellationToken cancellation, string error, bool sample = false)
    {
        var data = await InspectWorker(scope, files, "analysis", package, AnalysisPlan.Document(new { maxRows = limit, sample }),
            cancellation, error);
        return data.Deserialize<NativeRows>(Json) ?? throw new InvalidDataException("Missing Native analysis result");
    }
}
