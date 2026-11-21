using System.Text.Json;

namespace UniversalData;

public sealed partial class DataClient
{
    public Task<EtlRun> StartAsync(Package package, string parameters = "{}",
        CancellationToken cancellation = default) =>
        StartNamedAsync(package, parameters, Guid.NewGuid().ToString("N"), cancellation);

    public Task<EtlRun> StartNamedAsync(Package package, string parameters, string id,
        CancellationToken cancellation = default) =>
        StartPackageAsync(package, parameters, Sources.Snapshot(), false, cancellation,
            new(id, Guid.NewGuid().ToString("N")));

    public Task<EtlRun> ResumeAsync(Package package, DataRunReference execution,
        string parameters = "{}", CancellationToken cancellation = default)
    {
        ArgumentNullException.ThrowIfNull(execution);
        if (execution.JournalTicket is null) throw new ArgumentException("The Native Journal ticket is required", nameof(execution));
        return StartPackageAsync(package, parameters, Sources.Snapshot(), false, cancellation,
            execution, resume: true);
    }

    public Task<EtlRun> ResumeAsync(Package package, string executionId, string journalTicket,
        string parameters = "{}", CancellationToken cancellation = default) =>
        ResumeAsync(package, new DataRunReference(executionId, journalTicket), parameters, cancellation);

    public async Task<Package> PrepareAsync(Package package, CancellationToken cancellation = default)
    {
        ArgumentNullException.ThrowIfNull(package);
        await using var worker = await OpenWorkerAsync(cancellation, dedicated: true).ConfigureAwait(false);
        var response = (await worker.InvokeAsync("etl.instantiate", new { package = package.ToJson() },
            cancellation: cancellation).ConfigureAwait(false)).Response;
        JsonElement document = Read(response, "Native ETL package preparation failed");
        var result = Package.FromJson(document.GetRawText());
        return result.Value ?? throw new UniversalDataException((int)result.Code,
            "Native ETL package preparation returned an invalid package", result.Error);
    }

    public Task<Package> PrepareAsync(DataJob job, CancellationToken cancellation = default)
    {
        ArgumentNullException.ThrowIfNull(job);
        return PrepareAsync(Compile(job), cancellation);
    }

    public Task<JobValidationResult> ValidateAsync(DataJob job, CancellationToken cancellation = default) =>
        ValidateAsync(Compile(job), cancellation);

    public Task<JobValidationResult> ValidateAsync(DataJob job, string parameters,
        CancellationToken cancellation = default) => ValidateAsync(Compile(job), parameters, cancellation);

    public Task<JobValidationResult> ValidateAsync(Package package, CancellationToken cancellation = default) =>
        ValidateAsync(package, "{}", cancellation);

    public async Task<JobValidationResult> ValidateAsync(Package package, string parameters,
        CancellationToken cancellation = default)
    {
        JsonElement inspected = await ExplainAsync(package, parameters, cancellation).ConfigureAwait(false);
        return JobValidationResult.Read(inspected);
    }

    public Task<JsonElement> ExplainAsync(Package package, string parameters = "{}",
        CancellationToken cancellation = default) =>
        InspectAsync("inspect", package, Sources.Snapshot(), parameters, null, null, cancellation);

    public Task<JsonElement> ComponentCatalogAsync(Package package, CancellationToken cancellation = default) =>
        ComponentCatalogAsync(package, "{}", cancellation);

    public Task<JsonElement> ComponentCatalogAsync(Package package, string parameters,
        CancellationToken cancellation = default) =>
        InspectAsync("catalog", package, Sources.Snapshot(), parameters, null, null, cancellation);

    public Task<JsonElement> InstantiateAsync(Package package, CancellationToken cancellation = default) =>
        InspectAsync("instantiate", package, Sources.Snapshot(), "{}", null, null, cancellation);

    public Task<JsonElement> DiscoverSchemaAsync(Package package, string connection, string table,
        CancellationToken cancellation = default) =>
        DiscoverSchemaAsync(package, connection, table, "{}", cancellation);

    public Task<JsonElement> DiscoverSchemaAsync(Package package, string connection, string table,
        string parameters, CancellationToken cancellation = default) =>
        InspectAsync("discover", package, Sources.Snapshot(), parameters, connection, table, cancellation);

    public Task<JsonElement> TestMessageConnectionAsync(Package package, string connection,
        CancellationToken cancellation = default) =>
        InspectAsync("messageConnection", package, Sources.Snapshot(), "{}", connection, null, cancellation);

    public Task<JsonElement> PreviewAsync(DataJob job, uint sourceRows = 1000, uint sampleRows = 32,
        CancellationToken cancellation = default) =>
        PreviewAsync(Compile(job), sourceRows, sampleRows, cancellation);

    public Task<JsonElement> PreviewAsync(DataJob job, string parameters, uint sourceRows = 1000,
        uint sampleRows = 32, CancellationToken cancellation = default) =>
        PreviewAsync(Compile(job), parameters, sourceRows, sampleRows, cancellation);

    public Task<JsonElement> PreviewAsync(Package package, uint sourceRows = 1000, uint sampleRows = 32,
        CancellationToken cancellation = default) => PreviewAsync(package, "{}", sourceRows, sampleRows, cancellation);

    public Task<JsonElement> PreviewAsync(Package package, string parameters, uint sourceRows = 1000, uint sampleRows = 32,
        CancellationToken cancellation = default)
    {
        JsonElement pipeline = SelectPreviewPipeline(package.Definition);
        string component = pipeline.GetProperty("components").EnumerateArray()
            .First(node => IsPreviewSource(node.GetProperty("component").GetString()))
            .GetProperty("id").GetString()!;
        return PreviewAsync(package, pipeline, component, sourceRows, sampleRows, parameters, cancellation);
    }

    public async Task<JsonElement> PreviewAsync(Package package, JsonElement pipeline, string component,
        uint sourceRows = 1000, uint sampleRows = 32, string parameters = "{}",
        CancellationToken cancellation = default)
    {
        if (sourceRows is 0 or > 10000 || sampleRows is 0 or > 32 || sampleRows > sourceRows)
            throw new ArgumentOutOfRangeException(nameof(sourceRows));
        await using var worker = await OpenWorkerAsync(cancellation, dedicated: true).ConfigureAwait(false);
        return await PreviewAsync(worker, package, pipeline, component, sourceRows, sampleRows, parameters,
            Sources.Snapshot().Values.ToArray(), cancellation).ConfigureAwait(false);
    }

    public async ValueTask<Result<JobPublicationReceipt>> PublishAsync(DataJob job,
        EtlManagementClient management, JobPublicationOptions? options = null,
        CancellationToken cancellation = default)
    {
        ArgumentNullException.ThrowIfNull(job);
        return await PublishAsync(Compile(job, publish: true), management, options, cancellation).ConfigureAwait(false);
    }

    public async ValueTask<Result<JobPublicationReceipt>> PublishAsync(Package package,
        EtlManagementClient management, JobPublicationOptions? options = null,
        CancellationToken cancellation = default)
    {
        ArgumentNullException.ThrowIfNull(package);
        ArgumentNullException.ThrowIfNull(management);
        options ??= new();
        using var document = JsonDocument.Parse(package.ToJson());
        var request = new JobPublicationRequest(options.EffectiveRequestId, document.RootElement.Clone(),
            options.EffectiveExpectedPackageRevision, options.Schedules, options.Run, options.Parameters,
            options.Environment);
        return await management.PublishJobAsync(request, cancellation).ConfigureAwait(false);
    }

    public ValueTask<Result<JsonElement>> EnqueueAsync(EtlManagementClient management,
        ExecutionSubmission request, CancellationToken cancellation = default)
    {
        ArgumentNullException.ThrowIfNull(management);
        return management.StartExecutionAsync(request, cancellation);
    }

    public ValueTask<Result<EtlExecutionStatus>> StatusAsync(EtlManagementClient management,
        string executionId, CancellationToken cancellation = default)
    {
        ArgumentNullException.ThrowIfNull(management);
        return management.ExecutionAsync(executionId, cancellation);
    }

    public ValueTask<Result> StopAsync(EtlManagementClient management, string executionId,
        CancellationToken cancellation = default) => management.StopExecutionAsync(executionId, cancellation);

    public ValueTask<Result> CancelAsync(EtlManagementClient management, string executionId,
        CancellationToken cancellation = default) => management.CancelExecutionAsync(executionId, cancellation);

    public ValueTask<Result<EtlExecutionStatus>> ResumeAsync(EtlManagementClient management,
        string executionId, CancellationToken cancellation = default) =>
        management.ResumeExecutionAsync(executionId, cancellation);

    internal Task<EtlRun> StartPackageAsync(Package package, string parameters,
        IReadOnlyDictionary<string, SdkSourceBinding> inventory,
        bool readResults, CancellationToken cancellation, DataRunReference execution, bool resume = false)
    {
        ArgumentNullException.ThrowIfNull(package);
        cancellation.ThrowIfCancellationRequested();
        var root = package.Definition;
        uint slots = root.TryGetProperty("policy", out var policy) &&
            policy.TryGetProperty("maxConcurrency", out var parallelism) && parallelism.ValueKind == JsonValueKind.Number
                ? parallelism.GetUInt32() : 1;
        var sources = BindJobSources(root, inventory);
        return StartPackageWorkerAsync(package, parameters, sources, readResults, cancellation, execution,
            resume, slots);
    }

    private async Task<EtlRun> StartPackageWorkerAsync(Package package, string parameters,
        SdkSourceBinding[] sources, bool readResults, CancellationToken cancellation,
        DataRunReference execution, bool resume, uint slots)
    {
        string id = execution.RunId;
        string ticket = execution.JournalTicket;
        if (!Guid.TryParseExact(ticket, "N", out _))
            throw new ArgumentException("Invalid Native Journal ticket", nameof(execution));
        if (slots is 0 or > 64) throw new ArgumentOutOfRangeException(nameof(slots));
        var worker = await RentJobWorkerAsync(slots, readResults, cancellation).ConfigureAwait(false);
        try
        {
            var response = (await worker.InvokeAsync("etl.start", new
            {
                package = package.ToJson(), id, ticket, resume, parameters, sources, readResults
            }, mayWrite: true, cancellation: cancellation).ConfigureAwait(false)).Response;
            Check(response, "Native ETL start failed", execution);
            if (response.Value is not { } opened || !opened.TryGetProperty("id", out var identity) ||
                identity.GetString() != id)
                throw new DataExecutionException((int)StatusCode.CommitUnknown,
                    "Worker returned another execution identity", execution,
                    new(ErrorSource.Worker, StatusCode.Protocol, "ETL execution identity differs"));
            return new(worker, id, ticket, response.Sequence, readResults,
                slots == 1 ? ReturnJobWorkerAsync : null);
        }
        catch
        {
            if (slots == 1) await ReturnJobWorkerAsync(worker, readResults, complete: false).ConfigureAwait(false);
            else await worker.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    internal static async Task<JsonElement> InspectAsync(WorkerSession worker, string operation, Package package,
        SdkSourceBinding[] sources, string parameters, string? connection, string? table,
        CancellationToken cancellation)
    {
        var response = (await worker.InvokeAsync("etl." + operation, new
        {
            package = package.ToJson(), parameters, connection, table, sources
        }, cancellation: cancellation).ConfigureAwait(false)).Response;
        return Read(response, "Native ETL inspection failed");
    }

    private async Task<JsonElement> InspectAsync(string operation, Package package,
        IReadOnlyDictionary<string, SdkSourceBinding> sources,
        string parameters, string? connection, string? table, CancellationToken cancellation)
    {
        await using var worker = await OpenWorkerAsync(cancellation, dedicated: true).ConfigureAwait(false);
        return await InspectAsync(worker, operation, package, sources.Values.ToArray(), parameters, connection, table, cancellation)
            .ConfigureAwait(false);
    }

    internal static async Task<JsonElement> PreviewAsync(WorkerSession worker, Package package, JsonElement pipeline,
        string component, uint sourceRows, uint sampleRows, string parameters, SdkSourceBinding[] sources,
        CancellationToken cancellation)
    {
        var response = (await worker.InvokeAsync("etl.preview", new
        {
            package = package.ToJson(), pipeline, component, sourceRows, sampleRows, parameters, sources
        }, cancellation: cancellation).ConfigureAwait(false)).Response;
        return Read(response, "Native ETL preview failed");
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

    private static JsonElement SelectPreviewPipeline(JsonElement definition)
    {
        if (!definition.TryGetProperty("dataFlows", out var flows) || flows.ValueKind != JsonValueKind.Array)
            throw new UniversalDataException((int)StatusCode.Unsupported, "Package has no data-flow source to preview");
        foreach (var flow in flows.EnumerateArray())
            if (flow.GetProperty("components").EnumerateArray().Any(node =>
                    IsPreviewSource(node.GetProperty("component").GetString())))
                return flow.Clone();
        throw new UniversalDataException((int)StatusCode.Unsupported, "Package has no previewable source");
    }

    private static bool IsPreviewSource(string? component) => component is "sql.source" or "iceberg.source" or
        "mq.source" or "postgresql.cdc.source" or "mysql.cdc.source" or "mariadb.cdc.source" or
        "sqlserver.cdc.source" or "oracle.cdc.source";
}
