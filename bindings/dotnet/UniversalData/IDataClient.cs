using System.Text.Json;

namespace UniversalData;

public interface IDataClient : IAsyncDisposable
{
    Task<DataJobResult> RunAsync<TInput>(DataJob<TInput> job, TInput input, CancellationToken cancellation = default);
    IAsyncEnumerable<TRow> RunAsync<TInput, TRow>(DataJobQuery<TInput, TRow> job, TInput input,
        CancellationToken cancellation = default);
    Package Compile(DataJob job, bool publish = false);
    Task<DataJobResult> RunAsync(Package package, string parameters = "{}",
        CancellationToken cancellation = default);
    Task<EtlRun> StartAsync(Package package, string parameters = "{}",
        CancellationToken cancellation = default);
    Task<Package> PrepareAsync(DataJob job, CancellationToken cancellation = default);
    Task<EtlRun> ResumeAsync(Package package, DataRunReference execution,
        string parameters = "{}", CancellationToken cancellation = default);
    Task<JobValidationResult> ValidateAsync(DataJob job, CancellationToken cancellation = default);
    Task<JobValidationResult> ValidateAsync(DataJob job, string parameters, CancellationToken cancellation = default);
    Task<JobValidationResult> ValidateAsync(Package package, CancellationToken cancellation = default);
    Task<JobValidationResult> ValidateAsync(Package package, string parameters, CancellationToken cancellation = default);
    Task<JsonElement> PreviewAsync(DataJob job, uint sourceRows = 1000, uint sampleRows = 32,
        CancellationToken cancellation = default);
    Task<JsonElement> PreviewAsync(DataJob job, string parameters, uint sourceRows = 1000, uint sampleRows = 32,
        CancellationToken cancellation = default);
    Task<Package> PrepareAsync(Package package, CancellationToken cancellation = default);
    Task<JsonElement> PreviewAsync(Package package, uint sourceRows = 1000, uint sampleRows = 32,
        CancellationToken cancellation = default);
    Task<JsonElement> PreviewAsync(Package package, string parameters, uint sourceRows = 1000, uint sampleRows = 32,
        CancellationToken cancellation = default);
}
