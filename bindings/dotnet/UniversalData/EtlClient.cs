using System.Text.Json;

namespace UniversalData;

/// <summary>Execute and inspect Native Packages using a DataClient Worker lease.</summary>
public sealed class EtlClient(DataClient client)
{
    public Task<EtlRun> StartAsync(Package package, string parameters = "{}",
        CancellationToken cancellationToken = default) =>
        StartNamedAsync(package, parameters, Guid.NewGuid().ToString("N"), cancellationToken);

    public Task<EtlRun> StartNamedAsync(Package package, string parameters, string id,
        CancellationToken cancellationToken = default) =>
        StartAsync((_, _) => Task.FromResult(package), [], parameters, id, Guid.NewGuid().ToString("N"),
            resume: false, cancellationToken);

    public Task<EtlRun> ResumeAsync(Package package, string executionId, string journalTicket,
        string parameters = "{}", CancellationToken cancellationToken = default) =>
        StartAsync((_, _) => Task.FromResult(package), [], parameters, executionId, journalTicket,
            resume: true, cancellationToken);

    public async Task<JsonElement> PreviewAsync(Package package, JsonElement pipeline, string component,
        uint sourceRows = 1000, uint sampleRows = 32, string parameters = "{}",
        CancellationToken cancellationToken = default)
    {
        await using var worker = await client.OpenWorkerAsync(cancellationToken, dedicated: true).ConfigureAwait(false);
        return await PreviewAsync(worker, package, pipeline, component, sourceRows, sampleRows, parameters,
            [], cancellationToken).ConfigureAwait(false);
    }

    public Task<JsonElement> ExplainAsync(Package package, string parameters = "{}",
        CancellationToken cancellationToken = default) =>
        InspectAsync("inspect", package, parameters, null, null, cancellationToken);

    public Task<JsonElement> ComponentCatalogAsync(Package package, CancellationToken cancellationToken = default) =>
        ComponentCatalogAsync(package, "{}", cancellationToken);

    public Task<JsonElement> ComponentCatalogAsync(Package package, string parameters,
        CancellationToken cancellationToken = default) => InspectAsync("catalog", package, parameters, null, null, cancellationToken);

    public Task<JsonElement> InstantiateAsync(Package package, CancellationToken cancellationToken = default) =>
        InspectAsync("instantiate", package, "{}", null, null, cancellationToken);

    public Task<JsonElement> DiscoverSchemaAsync(Package package, string connection, string table,
        CancellationToken cancellationToken = default) =>
        DiscoverSchemaAsync(package, connection, table, "{}", cancellationToken);

    public Task<JsonElement> DiscoverSchemaAsync(Package package, string connection, string table, string parameters,
        CancellationToken cancellationToken = default) => InspectAsync("discover", package, parameters, connection, table, cancellationToken);

    public Task<JsonElement> TestMessageConnectionAsync(Package package, string connection,
        CancellationToken cancellationToken = default) =>
        InspectAsync("messageConnection", package, "{}", connection, null, cancellationToken);

    private async Task<JsonElement> InspectAsync(string operation, Package package, string parameters,
        string? connection, string? table, CancellationToken cancellationToken)
    {
        await using var worker = await client.OpenWorkerAsync(cancellationToken, dedicated: true).ConfigureAwait(false);
        return await InspectAsync(worker, operation, package, [], parameters, connection, table,
            cancellationToken).ConfigureAwait(false);
    }

    internal static async Task<JsonElement> InspectAsync(WorkerSession worker, string operation, Package package,
        SdkSourceBinding[] sources, string parameters, string? connection, string? table,
        CancellationToken cancellationToken)
    {
        var response = (await worker.InvokeAsync("etl." + operation, new
        {
            package = package.ToJson(), parameters, connection, table, sources
        }, cancellation: cancellationToken).ConfigureAwait(false)).Response;
        return Read(response, "Native ETL inspection failed");
    }

    internal static async Task<JsonElement> PreviewAsync(WorkerSession worker, Package package, JsonElement pipeline,
        string component, uint sourceRows, uint sampleRows, string parameters, SdkSourceBinding[] sources,
        CancellationToken cancellationToken)
    {
        var response = (await worker.InvokeAsync("etl.preview", new
        {
            package = package.ToJson(), pipeline, component, sourceRows, sampleRows, parameters, sources
        }, cancellation: cancellationToken).ConfigureAwait(false)).Response;
        return Read(response, "Native ETL preview failed");
    }

    internal async Task<EtlRun> StartAsync(Func<WorkerSession, CancellationToken, Task<Package>> compile,
        SdkSourceBinding[] sources, string parameters, string id, string ticket, bool resume,
        CancellationToken cancellationToken, bool readResults = false, uint cpuSlots = 1)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        if (!Guid.TryParseExact(ticket, "N", out _))
            throw new ArgumentException("Invalid Native Journal ticket", nameof(ticket));
        if (cpuSlots is 0 or > 64) throw new ArgumentOutOfRangeException(nameof(cpuSlots));
        var worker = await client.RentJobWorkerAsync(cpuSlots, readResults, cancellationToken).ConfigureAwait(false);
        try
        {
            var package = await compile(worker, cancellationToken).ConfigureAwait(false);
            var response = (await worker.InvokeAsync("etl.start", new
            {
                package = package.ToJson(), id, ticket, resume, parameters, sources, readResults
            }, mayWrite: true, cancellation: cancellationToken).ConfigureAwait(false)).Response;
            Check(response, "Native ETL start failed", new(id, JournalTicket: ticket));
            if (response.Value is not { } opened || !opened.TryGetProperty("id", out var identity) ||
                identity.GetString() != id)
                throw new DataExecutionException((int)StatusCode.CommitUnknown, "Worker returned another execution identity",
                    new(id, JournalTicket: ticket), new(ErrorSource.Worker, StatusCode.Protocol, "ETL execution identity differs"));
            return new(worker, id, ticket, response.Sequence, readResults, cpuSlots == 1 ? client.ReturnJobWorkerAsync : null);
        }
        catch
        {
            if (cpuSlots == 1) await client.ReturnJobWorkerAsync(worker, readResults, complete: false).ConfigureAwait(false);
            else await worker.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    internal static JsonElement Read(WorkerResponseFrame response, string message)
    {
        Check(response, message);
        return response.Value ?? throw new UniversalDataException((int)StatusCode.Protocol,
            "Worker returned no ETL result");
    }

    internal static void Check(WorkerResponseFrame response, string message, DataRunReference? run = null,
        ExecutionMetrics? metrics = null)
    {
        if (response.Code == StatusCode.Ok) return;
        var description = response.Error is { Message.Length: > 0 } detail
            ? $"{message} ({response.Code}): {detail.Message}" : $"{message} ({response.Code})";
        if (run is not null)
        {
            var code = response.Code is StatusCode.WorkerLost or StatusCode.Protocol ? StatusCode.CommitUnknown :
                response.Code;
            throw new DataExecutionException((int)code, description, run, response.Error) { Metrics = metrics };
        }
        throw new UniversalDataException((int)response.Code, description, response.Error);
    }
}
