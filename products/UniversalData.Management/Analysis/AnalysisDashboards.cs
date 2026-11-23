using System.Text.Json;
using Microsoft.AspNetCore.DataProtection;
using UniversalData.Hosting;

namespace UniversalData.Management;

internal sealed partial class AnalysisWorkspace
{
    private const string BoardPrefix = "bi-board/";
    private readonly IDataProtector boardTokens = protection.CreateProtector("UniversalData.Analysis.Dashboard.v1");
    private static string MemberKey(string subject) => "s" + Hash(subject);

    private void MapTeamDashboards(RouteGroupBuilder group)
    {
        var boards = group.MapGroup("/dashboards");
        boards.MapGet("", (string? after, HttpContext c) => ListBoards(Scope(c), after));
        boards.MapGet("/{id}", (string id, HttpContext c) => GetBoard(Scope(c), id));
        boards.MapPost("", (AnalysisDashboardCreate request, HttpContext c) => CreateBoard(Scope(c), request));
        boards.MapPut("/{id}/draft", (string id, AnalysisDashboardDraft request, HttpContext c) =>
            EditBoard(Scope(c), id, request.ExpectedRevision, (owner, state, _) =>
                state with { Draft = Definition(owner, state.DatasetId, request.Name, request.Cards) }));
        boards.MapPost("/{id}/publish", (string id, AnalysisRevision request, HttpContext c) =>
            EditBoard(Scope(c), id, request.ExpectedRevision, (owner, state, next) =>
            {
                var definition = Definition(owner, state.DatasetId, state.Draft.Name, state.Draft.Cards);
                if (state.Members.Count != 0)
                    RequireDistributable(owner, state.DatasetId, Asset(owner, state.DatasetId, definition.Release.Id));
                return state with { Draft = definition, Published = definition, PublishedRevision = Version(next) };
            }));
        boards.MapPut("/{id}/members", (string id, AnalysisDashboardAccessWrite request, HttpContext c) =>
            EditBoard(Scope(c), id, request.ExpectedRevision, (owner, state, _) => ShareBoard(owner, state, request)));
        boards.MapPost("/{id}/query", (string id, AnalysisDashboardQuery request, HttpContext c) =>
            QueryBoard(Scope(c), id, request, c.RequestAborted));
        boards.MapPost("/{id}/drill", (string id, AnalysisDrill request, HttpContext c) =>
            DrillBoard(Scope(c), id, request, c.RequestAborted));
        boards.MapPost("/{id}/export", (string id, AnalysisExport request, HttpContext c) =>
            ExportBoard(Scope(c), id, request, c.RequestAborted));
    }

    private DashboardDefinition Definition(HostPolicy owner, string dataset, string name, AnalysisDashboardCard[] cards)
    {
        string releaseId = Open(owner, dataset).PublishedReleaseId ?? throw Failure(StatusCode.Invalid, "请先发布数据集。");
        var asset = Asset(owner, dataset, releaseId);
        var release = asset.Release;
        if (cards.Length is < 1 or > 12 || cards.Any(card => card is null || !HostIdentity.ValidId(card.Id) ||
                card.Dimensions is null || card.Dimensions.Any(d => d is null)) ||
            cards.Select(card => card.Id).Distinct().Count() != cards.Length)
            throw Failure(StatusCode.Invalid, "看板需要 1 至 12 张具有独立标识的卡片。");
        foreach (var card in cards)
        {
            _ = Name(card.Title);
            _ = BindQuery(release, new(release.Id, card.Dimensions, [card.MeasureId]));
            if (card.Chart is not ("kpi" or "table" or "bar" or "line") ||
                card.Chart == "kpi" && card.Dimensions.Length != 0 ||
                card.Chart is "line" or "bar" && card.Dimensions.Length != 1 ||
                card.Chart == "line" && card.Dimensions[0].Level is not ("month" or "day") ||
                card.Comparison is not ("none" or "yoy" or "mom") ||
                card.Comparison != "none" && !release.Model.Fields.Any(field => field.Role == "date"))
                throw Failure(StatusCode.Invalid, "卡片类型、维度或期间比较设置无效。");
        }
        return new(Name(name), cards, release, asset.Sql);
    }

    private AnalysisDashboard CreateBoard(HostPolicy owner, AnalysisDashboardCreate request)
    {
        owner.Identity.Require("analysis.write", request.DatasetId);
        var draft = Definition(owner, request.DatasetId, request.Name, request.Cards);
        string key = BoardPrefix + Guid.NewGuid().ToString("N");
        var state = new DashboardState(key, ProductInfo.Protocol, owner.Identity.Subject,
            MemberKey(owner.Identity.Subject), request.DatasetId, draft, null, null, []);
        using var catalog = owner.OpenCatalog();
        catalog.CompareExchange(CatalogKind.Audit, key, 0, JsonSerializer.Serialize(state, Json));
        return BoardView(owner, state, 1);
    }

    private AnalysisDashboard EditBoard(HostPolicy owner, string id, string expected,
        Func<HostPolicy, DashboardState, ulong, DashboardState> update)
    {
        string key = BoardPrefix + GuidKey(id);
        using var catalog = owner.OpenCatalog();
        var state = Read<DashboardState>(catalog.GetConfirmed(CatalogKind.Audit, key));
        if (state.Owner != owner.Identity.Subject)
            throw Failure(StatusCode.Auth, "仅作者可更改此看板。");
        owner.Identity.Require("analysis.write", state.DatasetId);
        ulong revision = Revision(expected);
        RequireCurrent(catalog, key, revision);
        state = update(owner, state, revision + 1);
        catalog.CompareExchange(CatalogKind.Audit, key, revision, JsonSerializer.Serialize(state, Json));
        return BoardView(owner, state, revision + 1);
    }

    private DashboardState ShareBoard(HostPolicy owner, DashboardState state, AnalysisDashboardAccessWrite request)
    {
        if (request.Members.Length > 200 || request.Members.Any(m => m is null || !HostIdentity.ValidId(m.Subject)) ||
            request.Members.Select(m => m.Subject).Distinct().Count() != request.Members.Length)
            throw Failure(StatusCode.Invalid, "同一成员只能出现一次，最多可分享给 200 人。");
        if (request.Members.Length > 0)
        {
            var published = state.Published ?? throw Failure(StatusCode.Invalid, "请先发布看板。");
            RequireDistributable(owner, state.DatasetId, ResolveBoardAsset(owner, state, published, null));
        }
        var recipients = identities.FindInTenant(owner.Tenant, request.Members.Select(m => m.Subject));
        if (recipients.Count != request.Members.Length || recipients.ContainsKey(state.Owner))
            throw Failure(StatusCode.Invalid, "请指定同租户的有效成员。");
        return state with { Members = request.Members.ToDictionary(m => MemberKey(m.Subject)) };
    }

    private AnalysisDashboard GetBoard(HostPolicy actor, string id)
    {
        using var catalog = actor.OpenCatalog();
        string key = BoardPrefix + GuidKey(id);
        ulong revision = catalog.LatestRevision(CatalogKind.Audit, key);
        if (revision == 0) throw Failure(StatusCode.Auth, "当前身份无法读取该看板。");
        var state = Read<DashboardState>(catalog.Get(CatalogKind.Audit, key, revision));
        var view = BoardView(actor, state, revision);
        var owner = BoardOwner(actor, state);
        var definition = view.Owner ? state.Draft : state.Published!;
        var asset = ResolveBoardAsset(owner, state, definition, null);
        if (!view.Owner) RequireDistributable(owner, state.DatasetId, asset);
        return view with { ReleaseId = asset.Release.Id, DataAsOf = asset.Release.DataAsOf };
    }

    private static AnalysisDashboard BoardView(HostPolicy actor, DashboardState state, ulong revision)
    {
        bool owner = state.Owner == actor.Identity.Subject;
        if (owner) actor.Identity.Require("analysis.read", state.DatasetId);
        var member = state.Members.GetValueOrDefault(MemberKey(actor.Identity.Subject));
        if (!owner && (member is null || state.Published is null))
            throw Failure(StatusCode.Auth, "当前身份无法读取该看板。");
        var definition = owner ? state.Draft : state.Published!;
        return new(state.Id[BoardPrefix.Length..], definition.Name, state.DatasetId, Version(revision),
            state.PublishedRevision, owner, definition.Cards, owner ? state.Members.Values.ToArray() : [],
            owner || member!.Details, owner || member!.Export, definition.Release.Model.Fields.Where(f => f.Role != "omit").ToArray(),
            definition.Release.Measures, definition.Release.DataAsOf, definition.Release.Id);
    }

    private AnalysisPage<AnalysisDashboard> ListBoards(HostPolicy actor, string? after)
    {
        const int size = 40;
        string cursor = after is null ? BoardPrefix : BoardPrefix + GuidKey(after);
        using var catalog = actor.OpenCatalog();
        using var page = JsonDocument.Parse(catalog.Query(JsonSerializer.Serialize(new
        {
            dashboardReader = MemberKey(actor.Identity.Subject), afterId = cursor, limit = size + 1
        }, Json)));
        var rows = page.RootElement;
        var items = new List<AnalysisDashboard>();
        var states = rows.EnumerateArray().Take(size).Select(row => (
            State: row.GetProperty("document").Deserialize<DashboardState>(Json)!,
            Revision: row.GetProperty("revision").GetUInt64())).ToArray();
        var publishers = identities.FindInTenant(actor.Tenant, states.Select(entry => entry.State.Owner)
            .Where(owner => owner != actor.Identity.Subject));
        foreach (var (state, revision) in states)
        {
            if (state.Owner == actor.Identity.Subject && !actor.Identity.Allows("analysis.read", state.DatasetId)) continue;
            var view = BoardView(actor, state, revision);
            if (!view.Owner)
            {
                if (!publishers.TryGetValue(state.Owner, out var publisher)) continue;
                try { RequireDistributable(host.Bind(publisher), state.DatasetId, state.Published!.Source); }
                catch (UniversalDataException e) when (e.Code == StatusCode.Auth) { continue; }
            }
            items.Add(view);
        }
        return new(items.ToArray(), rows.GetArrayLength() > size ?
            rows[size - 1].GetProperty("id").GetString()![BoardPrefix.Length..] : null);
    }

    private HostPolicy BoardOwner(HostPolicy actor, DashboardState state)
    {
        if (actor.Identity.Subject == state.Owner) return actor;
        var owner = identities.Find(state.Owner);
        if (owner?.Tenant != actor.Tenant) throw Failure(StatusCode.Auth, "发布者已失去访问资格。");
        return host.Bind(owner);
    }

    private AnalysisAsset ResolveBoardAsset(HostPolicy owner, DashboardState state, DashboardDefinition definition, string? requested)
    {
        string id = requested ?? Open(owner, state.DatasetId).PublishedReleaseId ?? definition.Release.Id;
        var asset = Asset(owner, state.DatasetId, id);
        if (asset.Release.ModelRevisionId == definition.Release.ModelRevisionId &&
            Revision(id) >= Revision(definition.Release.Id)) return asset;
        if (requested is not null) throw Failure(StatusCode.Fenced, "数据版本与看板发布的模型不一致。");
        return Asset(owner, state.DatasetId, definition.Release.Id);
    }
}
