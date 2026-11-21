namespace UniversalData;

public interface IDataClient : IAsyncDisposable
{
    Task<DataJobResult> RunAsync<TInput>(DataJob<TInput> job, TInput input, CancellationToken cancellation = default);
    IAsyncEnumerable<TRow> RunAsync<TInput, TRow>(DataJobQuery<TInput, TRow> job, TInput input,
        CancellationToken cancellation = default);
}
