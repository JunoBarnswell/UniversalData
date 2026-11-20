using System.Text.Json;

namespace UniversalData;

public sealed class EtlRun : IAsyncDisposable
{
    private readonly WorkerSession worker;
    internal EtlRun(WorkerSession worker, string id, string journalTicket)
    {
        this.worker = worker;
        Id = id;
        JournalTicket = journalTicket;
    }

    public string Id { get; }
    public string JournalTicket { get; }
    public int WorkerProcessId => worker.ProcessId;

    public async ValueTask<Result<JsonElement>> ProgressAsync(CancellationToken cancellation = default)
    {
        var response = (await worker.InvokeAsync("etl.progress", cancellation: cancellation).ConfigureAwait(false)).Response;
        return new(response.Code, response.Value.GetValueOrDefault(), response.Error);
    }

    public async ValueTask<Result<ExecutionMetrics>> WaitAsync(CancellationToken cancellation = default)
    {
        while (true)
        {
            if (cancellation.IsCancellationRequested)
                return new(StatusCode.Cancelled, null, new(ErrorSource.Worker, StatusCode.Cancelled, "Wait cancelled"));
            var response = (await worker.InvokeAsync("etl.wait", mayWrite: true).ConfigureAwait(false)).Response;
            if (response.Code is StatusCode.WorkerLost or StatusCode.Protocol)
                return new(StatusCode.CommitUnknown, null, response.Error);
            if (response.Code != StatusCode.Busy)
                return new(response.Code, response.Value?.Deserialize<ExecutionMetrics>(WorkerProtocol.Json),
                    response.Error);
        }
    }

    public async ValueTask<Result<JsonElement>> DiagnosticsAsync(CancellationToken cancellation = default)
    {
        var response = (await worker.InvokeAsync("etl.diagnostics", cancellation: cancellation)
            .ConfigureAwait(false)).Response;
        return new(response.Code, response.Value.GetValueOrDefault(), response.Error);
    }

    public async ValueTask<Result> CancelAsync(CancellationToken cancellation = default) =>
        (await worker.InvokeAsync("etl.cancel", mayWrite: true, cancellation: cancellation).ConfigureAwait(false))
            .Response.Result;

    public ValueTask DisposeAsync() => worker.DisposeAsync();
}
