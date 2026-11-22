namespace UniversalData;

public sealed partial class EtlManagementClient
{
    public ValueTask<Result<AnalysisSource[]>> AnalysisSourcesAsync(CancellationToken cancellation = default) =>
        Send<AnalysisSource[]>(HttpMethod.Get, "/api/analysis/sources", null, cancellation);

    public ValueTask<Result<AnalysisDataset[]>> AnalysisDatasetsAsync(CancellationToken cancellation = default) =>
        Send<AnalysisDataset[]>(HttpMethod.Get, "/api/analysis/datasets", null, cancellation);

    public ValueTask<Result<AnalysisDataset>> OpenAnalysisAsync(string id, CancellationToken cancellation = default) =>
        Send<AnalysisDataset>(HttpMethod.Get, AnalysisPath(id), null, cancellation);

    public ValueTask<Result<AnalysisDataset>> SelectAnalysisSourceAsync(string sourceId, string? datasetId = null,
        string? expectedRevision = null, CancellationToken cancellation = default) =>
        Send<AnalysisDataset>(HttpMethod.Post, "/api/analysis/datasets", new { sourceId, datasetId, expectedRevision }, cancellation, mutation: true);

    /// <summary>Uploads a table to the authenticated workspace. The request disposes the supplied stream.</summary>
    public async ValueTask<Result<AnalysisDataset>> UploadAnalysisAsync(Stream table, string fileName, string? datasetId = null,
        string? expectedRevision = null, CancellationToken cancellation = default)
    {
        using var content = new StreamContent(table);
        content.Headers.Add("X-File-Name", Segment(fileName));
        content.Headers.ContentType = new("application/octet-stream");
        string path = "/api/analysis/datasets/upload";
        if (datasetId is not null)
            path += "?datasetId=" + Segment(datasetId) + "&expectedRevision=" + Segment(expectedRevision ?? "");
        return await Send<AnalysisDataset>(HttpMethod.Post, path, content, cancellation, mutation: true).ConfigureAwait(false);
    }

    public ValueTask<Result<AnalysisPreview>> PreviewAnalysisAsync(string id, string? sheet = null,
        CancellationToken cancellation = default) =>
        Send<AnalysisPreview>(HttpMethod.Get, AnalysisPath(id) + "/preview" +
            (sheet is null ? "" : "?sheet=" + Segment(sheet)), null, cancellation);

    public ValueTask<Result<AnalysisDataset>> SaveAnalysisDraftAsync(string id, AnalysisDraftWrite draft,
        CancellationToken cancellation = default) =>
        Send<AnalysisDataset>(HttpMethod.Put, AnalysisPath(id) + "/draft", draft, cancellation, mutation: true);

    public ValueTask<Result<AnalysisPublication>> PublishAnalysisAsync(string id, AnalysisPublish operation,
        CancellationToken cancellation = default) =>
        Send<AnalysisPublication>(HttpMethod.Post, AnalysisPath(id) + "/publish", operation, cancellation, mutationId: operation.OperationId);

    public ValueTask<Result<AnalysisPublication>> AnalysisOperationAsync(string id, string operationId,
        CancellationToken cancellation = default) =>
        Send<AnalysisPublication>(HttpMethod.Get, AnalysisPath(id) + "/operations/" + Segment(operationId), null, cancellation);

    public ValueTask<Result<AnalysisRelease>> AnalysisReleaseAsync(string id, string releaseId,
        CancellationToken cancellation = default) =>
        Send<AnalysisRelease>(HttpMethod.Get, AnalysisPath(id) + "/releases/" + Segment(releaseId), null, cancellation);

    public ValueTask<Result<AnalysisResult>> QueryAnalysisAsync(string id, AnalysisQuery query,
        CancellationToken cancellation = default) =>
        Send<AnalysisResult>(HttpMethod.Post, AnalysisPath(id) + "/query", query, cancellation);

    public ValueTask<Result<AnalysisDetails>> DrillAnalysisAsync(string id, AnalysisDrill drill,
        CancellationToken cancellation = default) =>
        Send<AnalysisDetails>(HttpMethod.Post, AnalysisPath(id) + "/drill", drill, cancellation);

    public ValueTask<Result<byte[]>> ExportAnalysisAsync(string id, AnalysisExport export,
        CancellationToken cancellation = default) =>
        Send<byte[]>(HttpMethod.Post, AnalysisPath(id) + "/export", export, cancellation,
            read: async (content, token) => await content.ReadAsByteArrayAsync(token).ConfigureAwait(false));

    public ValueTask<Result<AnalysisDataset>> SaveAnalysisReportAsync(string id, AnalysisReportWrite report,
        CancellationToken cancellation = default) =>
        Send<AnalysisDataset>(HttpMethod.Put, AnalysisPath(id) + "/report", report, cancellation, mutation: true);

    private static string AnalysisPath(string id) => "/api/analysis/datasets/" + Segment(id);
}
