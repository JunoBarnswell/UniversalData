using System.Globalization;
using System.Text.Json;
using Microsoft.AspNetCore.DataProtection;
using UniversalData.Hosting;

namespace UniversalData.Management;

internal sealed partial class AnalysisWorkspace
{
    private async Task<AnalysisDashboardResult> QueryBoard(HostPolicy actor, string id, AnalysisDashboardQuery request,
        CancellationToken cancellation)
    {
        string key = BoardPrefix + GuidKey(id);
        using var catalog = actor.OpenCatalog();
        ulong head = catalog.LatestRevision(CatalogKind.Audit, key);
        if (head == 0) throw Failure(StatusCode.Auth, "当前身份无法读取该看板。");
        var state = Read<DashboardState>(catalog.Get(CatalogKind.Audit, key, head));
        var view = BoardView(actor, state, head);
        bool preview = view.Owner && request.Revision == Version(head) && request.Revision != state.PublishedRevision;
        if (!preview && (state.Published is null || request.Revision != state.PublishedRevision))
            throw Failure(StatusCode.Fenced, "看板版本已变化，请重新打开。");
        var definition = preview ? state.Draft : state.Published!;
        var owner = BoardOwner(actor, state);
        var asset = ResolveBoardAsset(owner, state, definition, request.ReleaseId);
        if (!view.Owner) RequireDistributable(owner, state.DatasetId, asset);
        _ = BindQuery(asset.Release, new(asset.Release.Id, [], Filters: request.Filters));
        // Recheck member authorization before any per-card context is signed.
        // A final check below still fences a revocation that happens while the
        // shared Native computation is running.
        if (!view.Owner) VerifyBoardAccess(actor, id, asset, request.Revision, false, false);
        var periods = definition.Cards.Select(card => card.Comparison).Distinct()
            .ToDictionary(comparison => comparison, comparison => ComparisonFilters(request.Filters, comparison));
        var results = new Dictionary<(string, string), AnalysisResult>();
        var reports = new List<DashboardReport>();
        int groupingIndex = 0;
        foreach (var grouping in definition.Cards.GroupBy(card => JsonSerializer.Serialize(card.Dimensions, Json)))
        {
            foreach (string period in grouping.Select(c => c.Comparison).Append("none").Distinct())
            {
                var cards = grouping.Where(card => period == "none" || card.Comparison == period).ToArray();
                var query = BindQuery(asset.Release, new(asset.Release.Id, cards[0].Dimensions,
                    cards.Select(c => c.MeasureId).Distinct().ToArray(), periods.GetValueOrDefault(period, request.Filters)));
                var binding = new QueryBinding(state.DatasetId, owner.Identity.Subject, owner.Tenant, query);
                string context = contexts.Protect(JsonSerializer.Serialize(binding, Json));
                reports.Add(new("group-" + groupingIndex.ToString(CultureInfo.InvariantCulture) + ":" + period,
                    binding, context, cards, query, period));
            }
            groupingIndex++;
        }
        // All report requests use the same immutable release and board filters.
        // AnalysisPlan.QueryBatch fans out one Parquet scan to the required
        // dimension/period aggregates. Contexts remain per-card and are only
        // signed after the authorization check above.
        var computed = await ReadReports(owner, asset,
            reports.Select(report => new ReportRequest(report.Key, report.Binding, report.Context)).ToArray(),
            cancellation);
        // Recheck after the shared computation and before signing any returned
        // card context. A member revoked while Native was scanning must not
        // receive even an otherwise unusable token.
        VerifyBoardAccess(actor, id, asset, request.Revision, false, false);
        foreach (var report in reports)
        {
            var result = computed[report.Key];
            foreach (var card in report.Cards)
            {
                // Each card exports only its configured measure.
                var selected = report.Query with { Measures = [card.MeasureId],
                    SortMeasure = card.Chart == "bar" ? card.MeasureId : null,
                    Descending = card.Chart == "bar" };
                var inner = contexts.Protect(JsonSerializer.Serialize(report.Binding with { Query = selected }, Json));
                var token = new DashboardReadToken(actor.Tenant, actor.Identity.Subject, id, state.DatasetId,
                    request.Revision, asset.Release.Id, inner);
                results[(card.Id, report.Period)] = CardResult(result, card,
                    boardTokens.Protect(JsonSerializer.Serialize(token, Json)));
            }
        }
        var output = definition.Cards.Select(card =>
        {
            var current = results[(card.Id, "none")];
            var previous = card.Comparison == "none" ? null : results[(card.Id, card.Comparison)];
            decimal? delta = null, rate = null;
            if (!current.Empty && previous is { Empty: false } &&
                decimal.TryParse(current.Total[card.MeasureId], NumberStyles.Float, CultureInfo.InvariantCulture, out var value) &&
                decimal.TryParse(previous.Total[card.MeasureId], NumberStyles.Float, CultureInfo.InvariantCulture, out var baseline))
            {
                delta = value - baseline;
                if (baseline != 0) rate = delta / baseline;
            }
            return new AnalysisDashboardCardResult(card.Id, current, previous, delta?.ToString(CultureInfo.InvariantCulture),
                rate?.ToString(CultureInfo.InvariantCulture));
        }).ToArray();
        return new(request.Revision, asset.Release.Id, asset.Release.DataAsOf, output);
    }

    private static AnalysisResult CardResult(AnalysisResult source, AnalysisDashboardCard card, string token)
    {
        Dictionary<string, string?> ProjectMeasure(Dictionary<string, string?> values) =>
            new() { [card.MeasureId] = values[card.MeasureId] };
        var groups = source.Groups.Select(group =>
        {
            var cells = group.Cells.Select(cell => cell with { Values = ProjectMeasure(cell.Values) });
            if (card.Chart == "bar")
                cells = cells.OrderByDescending(cell => decimal.TryParse(cell.Values[card.MeasureId], NumberStyles.Float,
                    CultureInfo.InvariantCulture, out var rank) ? (decimal?)rank : null);
            return group with { Cells = cells.ToArray() };
        });
        return source with { Context = token, Measures = source.Measures.Where(m => m.Id == card.MeasureId).ToArray(),
            Total = ProjectMeasure(source.Total), Groups = groups.ToArray() };
    }

    private static AnalysisFilter[] ComparisonFilters(AnalysisFilter[] filters, string comparison)
    {
        if (comparison == "none") return filters;
        var start = filters.SingleOrDefault(f => f.Operator == "from");
        var end = filters.SingleOrDefault(f => f.Operator == "before");
        if (start is null || end?.FieldId != start.FieldId || !DateOnly.TryParseExact(start.Value,
            "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var month) || month.Day != 1 ||
            month.Year is < 2 or > 9998 || month.AddMonths(1).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) != end.Value)
            throw Failure(StatusCode.Invalid, "同比或环比需要选择一个完整自然月。");
        var previous = comparison == "yoy" ? month.AddYears(-1) : month.AddMonths(-1);
        return filters.Select(f => f == start ? f with { Value = previous.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) } :
            f == end ? f with { Value = previous.AddMonths(1).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) } : f).ToArray();
    }

    private HostPolicy VerifyBoardAccess(HostPolicy actor, string id, AnalysisAsset asset, string revision,
        bool needDetails, bool needExport)
    {
        actor = Reauthorize(actor);
        using var catalog = actor.OpenCatalog();
        string key = BoardPrefix + GuidKey(id);
        var state = Read<DashboardState>(catalog.GetConfirmed(CatalogKind.Audit, key));
        var owner = BoardOwner(actor, state);
        owner.Identity.Require("analysis.read", state.DatasetId);
        if (state.Owner == actor.Identity.Subject) return owner;
        var member = state.Members.GetValueOrDefault(MemberKey(actor.Identity.Subject));
        if (member is null || state.PublishedRevision != revision ||
            needDetails && !member.Details || needExport && !member.Export)
            throw Failure(StatusCode.Auth, "当前授权不能执行此看板操作。");
        RequireDistributable(owner, state.DatasetId, asset);
        return owner;
    }

    private (DashboardReadToken Token, AnalysisAsset Asset, HostPolicy Owner) BoardContext(
        HostPolicy actor, string id, string value, bool details, bool export)
    {
        var token = Unprotect<DashboardReadToken>(boardTokens, value);
        if (token.Reader != actor.Identity.Subject || token.Tenant != actor.Tenant || token.BoardId != GuidKey(id))
            throw Failure(StatusCode.Auth, "查询上下文属于其他看板或成员。");
        using var catalog = actor.OpenCatalog();
        var state = Read<DashboardState>(catalog.GetConfirmed(CatalogKind.Audit, BoardPrefix + id));
        var owner = BoardOwner(actor, state);
        var asset = Asset(owner, token.DatasetId, token.ReleaseId);
        VerifyBoardAccess(actor, id, asset, token.Revision, details, export);
        return (token, asset, owner);
    }

    private async Task<AnalysisDetails> DrillBoard(HostPolicy actor, string id, AnalysisDrill request, CancellationToken cancel)
    {
        var (token, asset, owner) = BoardContext(actor, id, request.Context, true, false);
        var result = await Drill(owner, token.DatasetId, request with { Context = token.Context }, cancel);
        VerifyBoardAccess(actor, id, asset, token.Revision, true, false);
        return result;
    }

    private async Task<IResult> ExportBoard(HostPolicy actor, string id, AnalysisExport request, CancellationToken cancel)
    {
        var (token, asset, owner) = BoardContext(actor, id, request.Context, request.Details, true);
        var result = await Export(owner, token.DatasetId, request with { Context = token.Context }, cancel);
        VerifyBoardAccess(actor, id, asset, token.Revision, request.Details, true);
        return result;
    }
}
