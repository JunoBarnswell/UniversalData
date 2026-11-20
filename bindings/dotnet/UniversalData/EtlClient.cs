using System.Text.Json;

namespace UniversalData;

public sealed class EtlClient(EtlClientOptions options)
{
    public ValueTask<Result<EtlRun>> StartAsync(Package package, string parameters = "{}",
        CancellationToken cancellation = default) => Start(package, parameters, "", Guid.NewGuid().ToString("N"),
            false, cancellation);

    public ValueTask<Result<EtlRun>> ResumeAsync(Package package, string executionId, string journalTicket,
        string parameters = "{}", CancellationToken cancellation = default) =>
        Start(package, parameters, executionId, journalTicket, true, cancellation);

    public async ValueTask<Result<JsonElement>> PreviewAsync(Package package, JsonElement pipeline, string component,
        uint sourceRows = 1000, uint sampleRows = 32, string parameters = "{}",
        CancellationToken cancellation = default)
    {
        var start = WorkerSession.Start(options.WorkerPath);
        if (!start.IsSuccess)
            return new(start.Code, default, start.Error);
        await using var worker = start.Value!;
        var result = (await worker.InvokeAsync("etl.preview", new
        {
            package = package.ToJson(), options.PolicyFile, options.Tenant,
            pipeline, component, sourceRows, sampleRows, parameters
        }, cancellation: cancellation).ConfigureAwait(false)).Response;
        return new(result.Code, result.Value.GetValueOrDefault(), result.Error);
    }

    public ValueTask<Result<EtlRun>> StartNamedAsync(Package package, string parameters, string id,
        CancellationToken cancellation = default) =>
        Start(package, parameters, id, Guid.NewGuid().ToString("N"), false, cancellation);

    public ValueTask<Result<JsonElement>> ExplainAsync(Package package, string parameters = "{}",
        CancellationToken cancellation = default) => InspectAsync("inspect", package, parameters, cancellation: cancellation);
    public ValueTask<Result<JsonElement>> ComponentCatalogAsync(Package package,
        CancellationToken cancellation = default) => InspectAsync("catalog", package, "{}", cancellation: cancellation);
    public ValueTask<Result<JsonElement>> InstantiateAsync(Package package,
        CancellationToken cancellation = default) => InspectAsync("instantiate", package, "{}", cancellation: cancellation);
    public ValueTask<Result<JsonElement>> DiscoverSchemaAsync(Package package, string connection, string table,
        CancellationToken cancellation = default) => InspectAsync("discover", package, "{}", connection, table, cancellation);

    internal async ValueTask<Result<JsonElement>> InspectAsync(string mode, Package package, string parameters,
        string? connection = null, string? table = null, CancellationToken cancellation = default)
    {
        var start = WorkerSession.Start(options.WorkerPath);
        if (!start.IsSuccess)
            return new(start.Code, default, start.Error);
        await using var worker = start.Value!;
        var response = (await worker.InvokeAsync("etl." + mode, new
        {
            package = package.ToJson(), options.PolicyFile, options.Tenant, parameters, connection, table
        }, cancellation: cancellation).ConfigureAwait(false)).Response;
        return new(response.Code, response.Value.GetValueOrDefault(), response.Error);
    }

    private async ValueTask<Result<EtlRun>> Start(Package package, string parameters, string id, string ticket,
        bool resume, CancellationToken cancellation)
    {
        var start = WorkerSession.Start(options.WorkerPath);
        if (!start.IsSuccess)
            return new(start.Code, null, start.Error);
        var worker = start.Value!;
        var response = (await worker.InvokeAsync("etl.start", new
        {
            package = package.ToJson(), options.PolicyFile, options.Tenant, id, ticket, resume, parameters
        }, mayWrite: true, cancellation: cancellation).ConfigureAwait(false)).Response;
        if (response.Code == StatusCode.Ok && response.Value is { } opened &&
            opened.TryGetProperty("id", out var identity) && identity.GetString() is { } executionId)
            return Result<EtlRun>.Success(new(worker, executionId, ticket));
        await worker.DisposeAsync().ConfigureAwait(false);
        return response.Code == StatusCode.Ok
            ? new(StatusCode.CommitUnknown, null, new(ErrorSource.Worker, StatusCode.Protocol, "Missing execution identity"))
            : new(response.Code, null, response.Error);
    }
}
