using System.Text.Json;
using UniversalData.Hosting;

namespace UniversalData.Management;

internal sealed partial class AnalysisWorkspace
{
    private const string SchedulePrefix = "bi-schedule/";
    private static string RefreshKey(string id, string operation) => "bi-refresh/" + GuidKey(id) + "/" + GuidKey(operation);

    private void MapAnalysisRefresh(RouteGroupBuilder group)
    {
        group.MapGet("/datasets/{id}/refresh-schedule", (string id, HttpContext c) =>
        {
            var actor = Scope(c);
            _ = Open(actor, id);
            using var catalog = actor.OpenCatalog();
            var (state, revision) = GetSchedule(catalog, actor, id);
            return ScheduleView(state, revision);
        });
        group.MapPut("/datasets/{id}/refresh-schedule", (string id, AnalysisRefreshScheduleWrite request, HttpContext c) =>
            SaveSchedule(Scope(c), id, request));
        group.MapPost("/datasets/{id}/refresh", (string id, AnalysisRefreshRequest request, HttpContext c) =>
            QueueRefresh(Scope(c), id, request.OperationId));
        group.MapGet("/datasets/{id}/refreshes", (string id, string? after, HttpContext c) =>
            RefreshHistory(Scope(c), id, after));
    }

    private static (RefreshState State, ulong Revision) GetSchedule(Catalog catalog, HostPolicy actor, string id)
    {
        string key = SchedulePrefix + GuidKey(id);
        ulong revision = catalog.LatestRevision(CatalogKind.Audit, key);
        var state = revision == 0 ? new RefreshState(key, ProductInfo.Protocol, id, actor.Identity.Subject,
            actor.Identity.Stamp, false, 60, null) : Read<RefreshState>(catalog.Get(CatalogKind.Audit, key, revision));
        if (state.Subject != actor.Identity.Subject) throw Failure(StatusCode.Auth, "当前身份无法访问此刷新计划。");
        return (state, revision);
    }

    private static AnalysisRefreshSchedule ScheduleView(RefreshState state, ulong revision) =>
        new(Version(revision), state.Enabled, state.IntervalMinutes,
            state.NextDueMs is long due ? DateTimeOffset.FromUnixTimeMilliseconds(due) : null);

    private static long? NextRefresh(int minutes, long anchor, long now)
    {
        var schedule = new JobSchedule("refresh", JobFrequency.Interval, DateTimeOffset.FromUnixTimeMilliseconds(anchor),
            IntervalSeconds: checked((uint)minutes * 60));
        return NativeJobScheduler.Next(schedule, now);
    }

    private AnalysisRefreshSchedule SaveSchedule(HostPolicy actor, string id, AnalysisRefreshScheduleWrite request)
    {
        actor.Identity.Require("analysis.write", id);
        var dataset = Open(actor, id);
        if (request.Enabled && (dataset.Kind != "sql" || dataset.PublishedReleaseId is null))
            throw Failure(StatusCode.Invalid, "请先发布数据库数据集。");
        if (request.IntervalMinutes is < 1 or > 1440)
            throw Failure(StatusCode.Invalid, "刷新间隔须为 1 至 1440 分钟。");
        using var catalog = actor.OpenCatalog();
        var (state, revision) = GetSchedule(catalog, actor, id);
        if (Revision(request.ExpectedRevision) != revision)
            throw Failure(StatusCode.Fenced, "刷新计划已变化。");
        long now = catalog.WorkerClock;
        state = state with { Enabled = request.Enabled, IntervalMinutes = request.IntervalMinutes, Stamp = actor.Identity.Stamp,
            NextDueMs = request.Enabled ? NextRefresh(request.IntervalMinutes, now, now) : null };
        catalog.CompareExchange(CatalogKind.Audit, state.Id, revision, JsonSerializer.Serialize(state, Json));
        return ScheduleView(state, revision + 1);
    }

    private AnalysisRefreshReceipt QueueRefresh(HostPolicy actor, string id, string operation)
    {
        actor.Identity.Require("analysis.write", id);
        var dataset = Open(actor, id);
        if (dataset.Kind != "sql" || dataset.PublishedReleaseId is null)
            throw Failure(StatusCode.Invalid, "请先发布数据库数据集。");
        using var catalog = actor.OpenCatalog();
        string key = RefreshKey(id, operation);
        if (catalog.GetConfirmed(CatalogKind.Audit, key) is string prior)
            return Read<RefreshHistoryRecord>(prior).Receipt;
        var (state, revision) = GetSchedule(catalog, actor, id);
        if (state.Active is { } previously)
        {
            if (previously.OperationId == operation) return Queued(previously);
            throw Failure(StatusCode.Busy, "该数据集已有刷新任务。");
        }
        var ticket = new AnalysisRefreshTicket(operation, dataset.Revision, dataset.PublishedReleaseId,
            DateTimeOffset.FromUnixTimeMilliseconds(catalog.WorkerClock));
        state = state with { Active = ticket, Stamp = actor.Identity.Stamp };
        catalog.CompareExchange(CatalogKind.Audit, state.Id, revision, JsonSerializer.Serialize(state, Json));
        return Queued(ticket);
    }

    private static AnalysisRefreshReceipt Queued(AnalysisRefreshTicket ticket) =>
        new(ticket.OperationId, "checking", ticket.StartedAt, null, null, null);

    private AnalysisPage<AnalysisRefreshReceipt> RefreshHistory(HostPolicy actor, string id, string? after)
    {
        _ = Open(actor, id);
        using var catalog = actor.OpenCatalog();
        string prefix = "bi-refresh/" + GuidKey(id) + "/";
        var (schedule, _) = GetSchedule(catalog, actor, id);
        var items = new List<AnalysisRefreshReceipt>();
        bool activeCursor = after is not null && schedule.Active?.OperationId == after;
        if (after is null && schedule.Active is { } active && catalog.GetConfirmed(CatalogKind.Audit, RefreshKey(id, active.OperationId)) is null)
            items.Add(Queued(active));
        string cursor = after is null || activeCursor ? prefix : RefreshKey(id, after);
        using var rows = JsonDocument.Parse(catalog.Query(JsonSerializer.Serialize(new { refreshHistory = prefix, afterId = cursor, limit = 41 }, Json)));
        var receipts = rows.RootElement.EnumerateArray().Select(row => row.GetProperty("document")
            .Deserialize<RefreshHistoryRecord>(Json)!.Receipt).ToArray();
        items.AddRange(receipts);
        return new(items.Take(40).ToArray(), items.Count > 40 ? items[39].OperationId : null);
    }

    internal async Task DispatchRefreshes(string tenant, ClusterLeadership authority, ClusterLeadership.Term? term,
        ILogger logger, CancellationToken stopping)
    {
        using var pin = authority.Pin(term);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(stopping, term?.Lost ?? CancellationToken.None);
        var cancel = linked.Token;
        var system = host.Bind(HostIdentity.Operator(tenant));
        using var catalog = system.OpenCatalog();
        long now = catalog.WorkerClock;
        using var due = JsonDocument.Parse(catalog.Query(JsonSerializer.Serialize(new { refreshDueMs = now, limit = 64 }, Json)));
        var running = due.RootElement.EnumerateArray().Select(row =>
            (State: row.GetProperty("document").Deserialize<RefreshState>(Json)!, Revision: row.GetProperty("revision").GetUInt64())).ToArray();
        await Parallel.ForEachAsync(running, new ParallelOptions { CancellationToken = cancel,
            MaxDegreeOfParallelism = checked((int)host.MaxConcurrency) }, async (entry, token) =>
        {
            try { await RunRefresh(system, entry.State, entry.Revision, now, token); }
            catch (UniversalDataException e) when (e.Code is StatusCode.Fenced or StatusCode.Busy) { }
            catch (UniversalDataException e)
            {
                authority.CatalogUnavailable(e);
                logger.LogWarning("BI refresh {Dataset} could not be committed ({Status})", entry.State.DatasetId, e.Code);
            }
        });
    }

    private async Task RunRefresh(HostPolicy system, RefreshState state, ulong revision, long now, CancellationToken cancel)
    {
        using var catalog = system.OpenCatalog();
        var identity = identities.Find(state.Subject, state.Stamp);
        HostPolicy? actor = identity?.Tenant == system.Tenant ? host.Bind(identity) : null;
        string id = state.DatasetId;
        var ticket = state.Active;
        if (state.Enabled && state.NextDueMs is long due && due <= now)
        {
            string occurrence = Hash(state.Id + "/" + due.ToString(System.Globalization.CultureInfo.InvariantCulture))[..32];
            var started = DateTimeOffset.FromUnixTimeMilliseconds(due);
            if (ticket is not null || now - due >= state.IntervalMinutes * 60000L || actor is null)
            {
                RecordRefresh(catalog, id, new(occurrence, "missed", started, DateTimeOffset.UtcNow,
                    actor is null ? "调度身份已失效。" : "已错过调度时刻或前一轮仍在运行。", null));
            }
            else
            {
                try
                {
                    var dataset = Open(actor, id);
                    var release = dataset.PublishedReleaseId ?? throw Failure(StatusCode.Invalid, "数据集没有已发布版本。");
                    ticket = new(occurrence, dataset.Revision, release, started);
                }
                catch (UniversalDataException e) when (e.Code is StatusCode.Auth or StatusCode.Invalid)
                {
                    RecordRefresh(catalog, id, new(occurrence, "rejected", started, DateTimeOffset.UtcNow, e.Message, null));
                }
            }
            state = state with { NextDueMs = NextRefresh(state.IntervalMinutes, due, now), Active = ticket };
            catalog.CompareExchange(CatalogKind.Audit, state.Id, revision++, JsonSerializer.Serialize(state, Json));
        }
        if (ticket is null) return;
        string history = RefreshKey(id, ticket.OperationId);
        if (catalog.GetConfirmed(CatalogKind.Audit, history) is null)
        {
            AnalysisRefreshReceipt receipt;
            try
            {
                if (actor is null) throw Failure(StatusCode.Auth, "执行身份已失效。");
                var operation = await Publish(actor, id, new(ticket.OperationId, ticket.ExpectedRevision, ticket.ExpectedRelease),
                    cancel, refresh: true);
                receipt = new(ticket.OperationId, operation.State, ticket.StartedAt, DateTimeOffset.UtcNow,
                    operation.Error ?? operation.Issues?.FirstOrDefault()?.Message, operation.Release?.Id);
            }
            catch (UniversalDataException e) when (e.Code is not (StatusCode.Busy or StatusCode.Transient or StatusCode.CommitUnknown))
            {
                receipt = new(ticket.OperationId, "rejected", ticket.StartedAt, DateTimeOffset.UtcNow, e.Message, null);
            }
            RecordRefresh(catalog, id, receipt);
        }
        var latest = catalog.LatestRevision(CatalogKind.Audit, state.Id);
        state = Read<RefreshState>(catalog.Get(CatalogKind.Audit, state.Id, latest));
        if (state.Active?.OperationId == ticket.OperationId)
            catalog.CompareExchange(CatalogKind.Audit, state.Id, latest, JsonSerializer.Serialize(state with { Active = null }, Json));
    }

    private static void RecordRefresh(Catalog catalog, string id, AnalysisRefreshReceipt receipt)
    {
        string key = RefreshKey(id, receipt.OperationId);
        if (catalog.GetConfirmed(CatalogKind.Audit, key) is null)
            catalog.CompareExchange(CatalogKind.Audit, key, 0, JsonSerializer.Serialize(new RefreshHistoryRecord(key, ProductInfo.Protocol, receipt), Json));
    }
}
