namespace UniversalData;

public sealed partial class EtlManagementClient
{
    private static string BoardPath(string id) => "/api/analysis/dashboards/" + Segment(id);
    private static string After(string? cursor) => cursor is null ? "" : "?after=" + Segment(cursor);

    public ValueTask<Result<AnalysisPage<AnalysisDashboard>>> AnalysisDashboardsAsync(string? after = null,
        CancellationToken cancellation = default) =>
        Send<AnalysisPage<AnalysisDashboard>>(HttpMethod.Get, "/api/analysis/dashboards" + After(after), null, cancellation);

    public ValueTask<Result<AnalysisDashboard>> AnalysisDashboardAsync(string id,
        CancellationToken cancellation = default) =>
        Send<AnalysisDashboard>(HttpMethod.Get, BoardPath(id), null, cancellation);

    public ValueTask<Result<AnalysisDashboard>> CreateDashboardAsync(AnalysisDashboardCreate request,
        CancellationToken cancellation = default) =>
        Send<AnalysisDashboard>(HttpMethod.Post, "/api/analysis/dashboards", request, cancellation, mutation: true);

    public ValueTask<Result<AnalysisDashboard>> SaveDashboardDraftAsync(string id, AnalysisDashboardDraft request,
        CancellationToken cancellation = default) =>
        Send<AnalysisDashboard>(HttpMethod.Put, BoardPath(id) + "/draft", request, cancellation, mutation: true);

    public ValueTask<Result<AnalysisDashboard>> PublishDashboardAsync(string id, AnalysisRevision request,
        CancellationToken cancellation = default) =>
        Send<AnalysisDashboard>(HttpMethod.Post, BoardPath(id) + "/publish", request, cancellation, mutation: true);

    public ValueTask<Result<AnalysisDashboard>> ShareDashboardAsync(string id, AnalysisDashboardAccessWrite request,
        CancellationToken cancellation = default) =>
        Send<AnalysisDashboard>(HttpMethod.Put, BoardPath(id) + "/members", request, cancellation, mutation: true);

    public ValueTask<Result<AnalysisDashboardResult>> QueryDashboardAsync(string id, AnalysisDashboardQuery request,
        CancellationToken cancellation = default) =>
        Send<AnalysisDashboardResult>(HttpMethod.Post, BoardPath(id) + "/query", request, cancellation);

    public ValueTask<Result<AnalysisDetails>> DrillDashboardAsync(string id, AnalysisDrill request,
        CancellationToken cancellation = default) =>
        Send<AnalysisDetails>(HttpMethod.Post, BoardPath(id) + "/drill", request, cancellation);

    public ValueTask<Result<byte[]>> ExportDashboardAsync(string id, AnalysisExport request,
        CancellationToken cancellation = default) =>
        Send<byte[]>(HttpMethod.Post, BoardPath(id) + "/export", request, cancellation,
            read: async (response, token) => await response.ReadAsByteArrayAsync(token).ConfigureAwait(false));

    public ValueTask<Result<AnalysisRefreshSchedule>> AnalysisRefreshScheduleAsync(string id,
        CancellationToken cancellation = default) =>
        Send<AnalysisRefreshSchedule>(HttpMethod.Get, AnalysisPath(id) + "/refresh-schedule", null, cancellation);

    public ValueTask<Result<AnalysisRefreshSchedule>> SaveAnalysisRefreshScheduleAsync(string id, AnalysisRefreshScheduleWrite request,
        CancellationToken cancellation = default) =>
        Send<AnalysisRefreshSchedule>(HttpMethod.Put, AnalysisPath(id) + "/refresh-schedule", request, cancellation, mutation: true);

    public ValueTask<Result<AnalysisRefreshReceipt>> RefreshAnalysisAsync(string id, AnalysisRefreshRequest request,
        CancellationToken cancellation = default) =>
        Send<AnalysisRefreshReceipt>(HttpMethod.Post, AnalysisPath(id) + "/refresh", request, cancellation, mutationId: request.OperationId);

    public ValueTask<Result<AnalysisPage<AnalysisRefreshReceipt>>> AnalysisRefreshHistoryAsync(string id, string? after = null,
        CancellationToken cancellation = default) =>
        Send<AnalysisPage<AnalysisRefreshReceipt>>(HttpMethod.Get, AnalysisPath(id) + "/refreshes" + After(after), null, cancellation);
}
