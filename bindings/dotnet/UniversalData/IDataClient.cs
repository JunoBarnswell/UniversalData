using System.Text.Json;

namespace UniversalData;

public interface IDataClient : IAsyncDisposable
{
    DataSourceRegistry Sources { get; }

    Task<DataJobResult> RunAsync<TInput>(DataJob<TInput> job, TInput input, CancellationToken cancellation = default);
    Task<DataJobResult> RunAsync(DataJob<object> job, CancellationToken cancellation = default);
    IAsyncEnumerable<TRow> RunAsync<TInput, TRow>(DataJobQuery<TInput, TRow> job, TInput input,
        CancellationToken cancellation = default);
    IAsyncEnumerable<TRow> RunAsync<TRow>(DataJobQuery<object, TRow> job,
        CancellationToken cancellation = default);

    Package Compile(DataJob job, bool publish = false);
    Task<DataJobResult> RunAsync(Package package, string parameters = "{}",
        CancellationToken cancellationToken = default);

    Task<EtlRun> StartAsync(DataJob<object> job, CancellationToken cancellation = default);
    Task<EtlRun> StartAsync<TInput>(DataJob<TInput> job, TInput input,
        CancellationToken cancellation = default);
    Task<EtlRun> StartAsync<TRow>(DataJobQuery<object, TRow> job,
        CancellationToken cancellation = default);
    Task<EtlRun> StartAsync<TInput, TRow>(DataJobQuery<TInput, TRow> job, TInput input,
        CancellationToken cancellation = default);

    Task<EtlRun> StartAsync(Package package, string parameters = "{}",
        CancellationToken cancellation = default);
    Task<EtlRun> StartNamedAsync(Package package, string parameters, string id,
        CancellationToken cancellation = default);

    Task<EtlRun> ResumeAsync(DataJob<object> job, DataRunReference execution,
        CancellationToken cancellation = default);
    Task<EtlRun> ResumeAsync<TInput>(DataJob<TInput> job, TInput input, DataRunReference execution,
        CancellationToken cancellation = default);
    Task<Package> PrepareAsync(DataJob job, CancellationToken cancellation = default);
    Task<EtlRun> ResumeAsync(Package package, DataRunReference execution,
        string parameters = "{}", CancellationToken cancellation = default);
    Task<EtlRun> ResumeAsync(Package package, string executionId, string journalTicket,
        string parameters = "{}", CancellationToken cancellation = default);

    Task<JobValidationResult> ValidateAsync(DataJob job, CancellationToken cancellation = default);
    Task<JobValidationResult> ValidateAsync(DataJob job, string parameters, CancellationToken cancellation = default);
    Task<JobValidationResult> ValidateAsync(Package package, CancellationToken cancellation = default);
    Task<JobValidationResult> ValidateAsync(Package package, string parameters, CancellationToken cancellation = default);

    Task<JsonElement> ExplainAsync(Package package, string parameters = "{}",
        CancellationToken cancellation = default);
    Task<JsonElement> ComponentCatalogAsync(Package package, CancellationToken cancellation = default);
    Task<JsonElement> ComponentCatalogAsync(Package package, string parameters,
        CancellationToken cancellation = default);
    Task<JsonElement> InstantiateAsync(Package package, CancellationToken cancellation = default);
    Task<JsonElement> DiscoverSchemaAsync(Package package, string connection, string table,
        CancellationToken cancellation = default);
    Task<JsonElement> DiscoverSchemaAsync(Package package, string connection, string table,
        string parameters, CancellationToken cancellation = default);
    Task<JsonElement> TestMessageConnectionAsync(Package package, string connection,
        CancellationToken cancellation = default);

    Task<JsonElement> PreviewAsync(DataJob job, uint sourceRows = 1000, uint sampleRows = 32,
        CancellationToken cancellation = default);
    Task<JsonElement> PreviewAsync(DataJob job, string parameters, uint sourceRows = 1000, uint sampleRows = 32,
        CancellationToken cancellation = default);
    Task<Package> PrepareAsync(Package package, CancellationToken cancellation = default);
    Task<JsonElement> PreviewAsync(Package package, uint sourceRows = 1000, uint sampleRows = 32,
        CancellationToken cancellation = default);
    Task<JsonElement> PreviewAsync(Package package, string parameters, uint sourceRows = 1000, uint sampleRows = 32,
        CancellationToken cancellation = default);
    Task<JsonElement> PreviewAsync(Package package, JsonElement pipeline, string component,
        uint sourceRows = 1000, uint sampleRows = 32, string parameters = "{}",
        CancellationToken cancellation = default);

    ValueTask<Result<JobPublicationReceipt>> PublishAsync(DataJob job,
        EtlManagementClient management, JobPublicationOptions? options = null,
        CancellationToken cancellation = default);
    ValueTask<Result<JobPublicationReceipt>> PublishAsync(Package package,
        EtlManagementClient management, JobPublicationOptions? options = null,
        CancellationToken cancellation = default);
    ValueTask<Result<JsonElement>> EnqueueAsync(EtlManagementClient management,
        ExecutionSubmission request, CancellationToken cancellation = default);
    ValueTask<Result<EtlExecutionStatus>> StatusAsync(EtlManagementClient management,
        string executionId, CancellationToken cancellation = default);
    ValueTask<Result> StopAsync(EtlManagementClient management, string executionId,
        CancellationToken cancellation = default);
    ValueTask<Result> CancelAsync(EtlManagementClient management, string executionId,
        CancellationToken cancellation = default);
    ValueTask<Result<EtlExecutionStatus>> ResumeAsync(EtlManagementClient management,
        string executionId, CancellationToken cancellation = default);
}
