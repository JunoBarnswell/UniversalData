using System.IO.Pipes;
using System.Text.Json;

namespace UniversalData;

internal sealed class WorkerSession(NamedPipeClientStream pipe, SdkWorkerGrant grant, SdkHostConfiguration host)
    : IAsyncDisposable
{
    private readonly SemaphoreSlim operations = new(1, 1);
    private long sequence;
    private int closed;

    internal int ProcessId => grant.ProcessId;

    internal static async Task<WorkerSession> AttachAsync(SdkWorkerGrant grant, SdkHostConfiguration host,
        CancellationToken cancellation)
    {
        if (grant.ProcessId <= 0 || !SdkSessionProtocol.ValidEndpoint(grant.Endpoint) ||
            !Guid.TryParseExact(grant.SessionId, "N", out _) || grant.Capability is not { Length: 64 })
            throw SdkSessionHostClient.Failure(StatusCode.Protocol, "SDK Worker grant identity is invalid");
        var pipe = SdkSessionProtocol.Connect(grant.Endpoint);
        var session = new WorkerSession(pipe, grant, host);
        try
        {
            await pipe.ConnectAsync(cancellation).ConfigureAwait(false);
            await WorkerProtocol.WriteAsync(pipe, new SdkPipeAdmission(ProductInfo.WorkerRevision, grant.Capability,
                "attach", SessionId: grant.SessionId),
                null, cancellation).ConfigureAwait(false);
            var admitted = await WorkerProtocol.ReadAsync<SdkHostStatus>(pipe, cancellation).ConfigureAwait(false);
            using (admitted.Batch) SdkSessionHostClient.Check(admitted.Message, admitted.Batch is null);
            return session;
        }
        catch
        {
            await session.TerminateAsync().ConfigureAwait(false);
            throw;
        }
    }

    internal async ValueTask<(WorkerResponseFrame Response, WorkerBatchBuffer? Batch)> InvokeAsync(
        string operation, object? values = null, bool mayWrite = false, CancellationToken cancellation = default)
    {
        try { await operations.WaitAsync(cancellation).ConfigureAwait(false); }
        catch (OperationCanceledException) { return Failure(StatusCode.Cancelled, false); }
        bool sent = false;
        bool timedOut = false;
        try
        {
            if (Volatile.Read(ref closed) != 0) return Failure(StatusCode.WorkerLost, false);
            if (cancellation.IsCancellationRequested) return Failure(StatusCode.Cancelled, false);
            long id = checked(++sequence);
            var request = new WorkerRequestFrame(ProductInfo.WorkerRevision, grant.SessionId, id, operation,
                JsonSerializer.SerializeToElement(values ?? new { }, WorkerProtocol.Json));
            using var timeout = new CancellationTokenSource(host.Options.ExecutionTimeout);
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellation, timeout.Token);
            sent = true;
            await WorkerProtocol.WriteAsync(pipe, request, null, deadline.Token).ConfigureAwait(false);
            var response = WorkerProtocol.ReadAsync<WorkerResponseFrame>(pipe).AsTask();
            using var waiting = CancellationTokenSource.CreateLinkedTokenSource(deadline.Token);
            var interrupted = Task.Delay(Timeout.InfiniteTimeSpan, waiting.Token);
            if (await Task.WhenAny(response, interrupted).ConfigureAwait(false) != response && !response.IsCompleted)
            {
                timedOut = timeout.IsCancellationRequested && !cancellation.IsCancellationRequested;
                using var grace = new CancellationTokenSource(host.Options.CancellationGrace);
                await WorkerProtocol.WriteAsync(pipe, request with { Operation = "sdk.cancel", Values = defaultValue },
                    null, grace.Token).ConfigureAwait(false);
                try { await response.WaitAsync(grace.Token).ConfigureAwait(false); }
                catch (OperationCanceledException) when (grace.IsCancellationRequested)
                {
                    await TerminateAsync().ConfigureAwait(false);
                    try { (await response.ConfigureAwait(false)).Batch?.Dispose(); }
                    catch (Exception error) when (IsTransport(error)) { }
                    return Failure(timedOut ? StatusCode.Timeout : StatusCode.Cancelled, mayWrite);
                }
            }
            await waiting.CancelAsync().ConfigureAwait(false);
            var result = await response.ConfigureAwait(false);
            if (result.Message.Revision != ProductInfo.WorkerRevision || result.Message.SessionId != grant.SessionId ||
                result.Message.Sequence != id ||
                result.Message.Kind != "result" || result.Batch is not null)
            {
                result.Batch?.Dispose();
                throw new InvalidDataException("SDK Worker response identity or shape differs");
            }
            if (timedOut && result.Message.Code == StatusCode.Cancelled)
                return (result.Message with { Code = StatusCode.Timeout,
                    Error = new OperationError(ErrorSource.Worker, StatusCode.Timeout, "SDK execution deadline elapsed") }, null);
            return result;
        }
        catch (Exception error) when (IsTransport(error))
        {
            var cause = error is OperationCanceledException ? (timedOut || !cancellation.IsCancellationRequested
                ? StatusCode.Timeout : StatusCode.Cancelled) :
                error is InvalidDataException or JsonException ? StatusCode.Protocol : StatusCode.WorkerLost;
            await TerminateAsync().ConfigureAwait(false);
            return Failure(cause, sent && mayWrite);
        }
        finally { operations.Release(); }
    }

    private static readonly JsonElement defaultValue = JsonSerializer.SerializeToElement<object?>(null, WorkerProtocol.Json);

    private (WorkerResponseFrame, WorkerBatchBuffer?) Failure(StatusCode cause, bool uncertain)
    {
        var error = new OperationError(ErrorSource.Worker, cause, cause switch
        {
            StatusCode.Cancelled => "SDK Worker operation cancelled",
            StatusCode.Timeout => "SDK Worker execution deadline elapsed",
            StatusCode.Protocol => "Invalid SDK Worker protocol message",
            _ => "SDK Worker process stopped responding"
        });
        return (new(ProductInfo.WorkerRevision, grant.SessionId, sequence, "result",
            uncertain ? StatusCode.CommitUnknown : cause, Error: error), null);
    }

    private static bool IsTransport(Exception error) => error is IOException or InvalidDataException or JsonException or
        OperationCanceledException or InvalidOperationException;

    private async Task TerminateAsync()
    {
        if (Interlocked.Exchange(ref closed, 1) != 0) return;
        await pipe.DisposeAsync().ConfigureAwait(false);
        try { await CloseAsync(poison: true).ConfigureAwait(false); }
        catch (Exception error) when (IsTransport(error) || error is UniversalDataException) { }
    }

    private async Task CloseAsync(bool poison = false)
    {
        using var deadline = new CancellationTokenSource(host.Options.CancellationGrace + TimeSpan.FromSeconds(10));
        await using var endpoint = SdkSessionProtocol.Connect(host.Endpoint);
        await endpoint.ConnectAsync(deadline.Token).ConfigureAwait(false);
        await WorkerProtocol.WriteAsync(endpoint, new SdkPipeAdmission(ProductInfo.WorkerRevision,
            host.Capability, "session.close", grant.Capability, SessionId: grant.SessionId, Poison: poison), null,
            deadline.Token).ConfigureAwait(false);
        var response = await WorkerProtocol.ReadAsync<SdkHostStatus>(endpoint, deadline.Token).ConfigureAwait(false);
        using (response.Batch) SdkSessionHostClient.Check(response.Message, response.Batch is null);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref closed, 1) != 0) return;
        await pipe.DisposeAsync().ConfigureAwait(false);
        try { await CloseAsync().ConfigureAwait(false); }
        catch (Exception error) when (IsTransport(error) || error is UniversalDataException) { }
    }
}
