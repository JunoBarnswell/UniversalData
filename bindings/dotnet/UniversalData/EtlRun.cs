using System.Text.Json;
using System.Runtime.CompilerServices;

namespace UniversalData;

public sealed class EtlRun : IAsyncDisposable
{
    private readonly WorkerSession worker;
    private readonly bool readResults;
    private readonly object identity;
    private readonly Func<WorkerSession, bool, bool, ValueTask>? release;
    private int complete, disposed;

    internal EtlRun(WorkerSession worker, string id, string journalTicket, long startSequence, bool readResults = false,
        Func<WorkerSession, bool, bool, ValueTask>? release = null)
    {
        this.worker = worker;
        Id = id;
        JournalTicket = journalTicket;
        this.readResults = readResults;
        this.release = release;
        identity = new { id, startSequence };
    }

    public string Id { get; }
    public string JournalTicket { get; }
    public int WorkerProcessId => worker.ProcessId;

    public async Task<DataRunProgress> ProgressAsync(CancellationToken cancellationToken = default) =>
        DataRunProgress.Read(DataClient.Read(await RequestAsync("etl.progress", false, cancellationToken)
            .ConfigureAwait(false), "Native ETL progress is unavailable"));

    /// <summary>Cancels only the caller's wait. Use CancelAsync to stop the Native execution.</summary>
    public async Task<ExecutionMetrics> WaitAsync(CancellationToken cancellationToken = default)
    {
        while (true)
        {
            var response = await RequestAsync("etl.wait", true, cancellationToken).ConfigureAwait(false);
            if (response.Code == StatusCode.Busy) continue;
            var metrics = response.Value?.Deserialize<ExecutionMetrics>(WorkerProtocol.Json);
            DataClient.Check(response, "Native ETL execution failed", new(Id, JournalTicket: JournalTicket), metrics);
            if (metrics is null)
                throw new DataExecutionException((int)StatusCode.CommitUnknown, "Native ETL completion was not reported",
                    new(Id, JournalTicket: JournalTicket), new(ErrorSource.Worker, StatusCode.Protocol, "Missing ETL metrics"));
            Volatile.Write(ref complete, 1);
            return metrics;
        }
    }

    /// <summary>Read one Native result batch at a time, then wait for terminal execution status.</summary>
    public async IAsyncEnumerable<T> ReadRowsAsync<T>(
        [EnumeratorCancellation] CancellationToken cancellation = default)
    {
        if (!readResults)
            throw new UniversalDataException((int)StatusCode.Invalid,
                "This ETL run was not started with result reading enabled");
        while (true)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) != 0, this);
            cancellation.ThrowIfCancellationRequested();
            var response = await worker.InvokeAsync("etl.next", identity, cancellation: cancellation).ConfigureAwait(false);
            if (response.Response.Code == StatusCode.Busy)
            {
                response.Batch?.Dispose();
                continue;
            }
            if (response.Response.Code == StatusCode.End)
            {
                response.Batch?.Dispose();
                Volatile.Write(ref complete, 1);
                yield break;
            }
            using (response.Batch)
            {
                cancellation.ThrowIfCancellationRequested();
                var metrics = response.Response.Value?.ValueKind == JsonValueKind.Object && response.Response.Code != StatusCode.Ok
                    ? response.Response.Value?.Deserialize<ExecutionMetrics>(WorkerProtocol.Json) : null;
                DataClient.Check(response.Response, "Native ETL result read failed",
                    new(Id, JournalTicket: JournalTicket), metrics);
                if (response.Batch is null)
                    throw new DataExecutionException((int)StatusCode.Protocol,
                        "Native ETL result read returned no batch",
                        new(Id, JournalTicket: JournalTicket),
                        new(ErrorSource.Worker, StatusCode.Protocol, "Missing result batch"));
                using var batch = Batch.FromWire(response.Batch.Bytes, response.Batch.Length);
                foreach (var row in batch.EnumerateEntities<T>())
                    yield return row;
            }
        }
    }

    public async Task<DataRunDiagnostic> DiagnosticsAsync(CancellationToken cancellationToken = default) =>
        DataRunDiagnostic.Read(DataClient.Read(await RequestAsync("etl.diagnostics", false, cancellationToken)
            .ConfigureAwait(false), "Native ETL diagnostics are unavailable"));

    /// <summary>Requests a graceful Native drain. The caller remains attached until the run reaches terminal state.</summary>
    public async Task StopAsync(CancellationToken cancellationToken = default) =>
        DataClient.Check(await RequestAsync("etl.stop", true, cancellationToken).ConfigureAwait(false),
            "Native ETL stop could not be confirmed", new(Id, JournalTicket: JournalTicket));

    public async Task CancelAsync(CancellationToken cancellationToken = default) =>
        DataClient.Check(await RequestAsync("etl.cancel", true, cancellationToken).ConfigureAwait(false),
            "Native ETL cancellation could not be confirmed", new(Id, JournalTicket: JournalTicket));

    private async Task<WorkerResponseFrame> RequestAsync(string operation, bool mayWrite,
        CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) != 0, this);
        cancellationToken.ThrowIfCancellationRequested();
        // A transport read owns the next response. The caller may stop waiting without closing the channel.
        var exchange = worker.InvokeAsync(operation, identity, mayWrite: mayWrite).AsTask();
        return (await exchange.WaitAsync(cancellationToken).ConfigureAwait(false)).Response;
    }

    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0) return ValueTask.CompletedTask;
        return release is null ? worker.DisposeAsync() : release(worker, readResults, Volatile.Read(ref complete) != 0);
    }
}
