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

    public async Task<JsonElement> ProgressAsync(CancellationToken cancellationToken = default) =>
        EtlClient.Read(await RequestAsync("etl.progress", false, cancellationToken).ConfigureAwait(false),
            "Native ETL progress is unavailable");

    /// <summary>Cancels only the caller's wait. Use CancelAsync to stop the Native execution.</summary>
    public async Task<ExecutionMetrics> WaitAsync(CancellationToken cancellationToken = default)
    {
        while (true)
        {
            var response = await RequestAsync("etl.wait", true, cancellationToken).ConfigureAwait(false);
            if (response.Code == StatusCode.Busy) continue;
            var metrics = response.Value?.Deserialize<ExecutionMetrics>(WorkerProtocol.Json);
            EtlClient.Check(response, "Native ETL execution failed", new(Id, JournalTicket: JournalTicket), metrics);
            return metrics
                ?? throw new DataExecutionException((int)StatusCode.CommitUnknown, "Native ETL completion was not reported",
                    new(Id, JournalTicket: JournalTicket), new(ErrorSource.Worker, StatusCode.Protocol, "Missing ETL metrics"));
        }
    }

    public async Task<JsonElement> DiagnosticsAsync(CancellationToken cancellationToken = default) =>
        EtlClient.Read(await RequestAsync("etl.diagnostics", false, cancellationToken).ConfigureAwait(false),
            "Native ETL diagnostics are unavailable");

    public async Task CancelAsync(CancellationToken cancellationToken = default) =>
        EtlClient.Check(await RequestAsync("etl.cancel", true, cancellationToken).ConfigureAwait(false),
            "Native ETL cancellation could not be confirmed", new(Id, JournalTicket: JournalTicket));

    private async Task<WorkerResponseFrame> RequestAsync(string operation, bool mayWrite,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        // A transport read owns the next response. The caller may stop waiting without closing the channel.
        var exchange = worker.InvokeAsync(operation, mayWrite: mayWrite).AsTask();
        return (await exchange.WaitAsync(cancellationToken).ConfigureAwait(false)).Response;
    }

    public ValueTask DisposeAsync() => worker.DisposeAsync();
}
