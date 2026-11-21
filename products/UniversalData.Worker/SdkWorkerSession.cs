using System.Collections.Concurrent;
using System.IO.Pipes;
using System.Text.Json;
using System.Threading.Channels;
using UniversalData;
using UniversalData.Hosting;

namespace UniversalData.Worker;

internal sealed class SdkWorkerSession(HostPolicy policy, SdkWorkerConfiguration configuration,
    SdkSessionAssignment assignment, ConnectionPool pool, int cpu)
{
    internal async Task RunAsync(NamedPipeServerStream pipe, CancellationTokenSource lifetime)
    {
        var inbox = Channel.CreateBounded<Invocation>(new BoundedChannelOptions(1)
        { SingleReader = true, SingleWriter = true });
        var requests = new ConcurrentDictionary<long, CancellationTokenSource>();
        try
        {
            using (var startup = CancellationTokenSource.CreateLinkedTokenSource(lifetime.Token))
            {
                startup.CancelAfter(TimeSpan.FromSeconds(15));
                await pipe.WaitForConnectionAsync(startup.Token);
                var admission = await WorkerProtocol.ReadAsync<SdkPipeAdmission>(pipe, startup.Token);
                using var batch = admission.Batch;
                if (batch is not null || !SdkSessionProtocol.Authenticate(admission.Message, assignment.Capability) ||
                    admission.Message.Operation != "attach" || admission.Message.SessionId != assignment.SessionId)
                    throw new InvalidDataException("Incompatible SDK session attachment");
                await WorkerProtocol.WriteAsync(pipe, new SdkHostStatus(ProductInfo.WorkerRevision, StatusCode.Ok), null, startup.Token);
            }
            var pump = ReceiveAsync(pipe, inbox.Writer, requests, lifetime, assignment.SessionId);
            try
            {
                var environment = new SdkExecutionEnvironment(policy.Tenant,
                    (resource, access) => policy.Authorize(policy.Tenant, resource, access),
                    Pool: pool,
                    ConnectionWaitMilliseconds: (uint)Math.Min(3600000, Math.Ceiling(configuration.Options.QueueTimeout.TotalMilliseconds)),
                    Resolve: (resource, reference) => policy.Resolve(policy.Tenant, resource, reference),
                    CanBorrow: (source, revision) => CanBorrow(configuration.SourceStateFile, source, revision));
                var sessionPolicy = policy with { MemoryLimit = configuration.Options.MemoryPerSession,
                    MaxConcurrency = configuration.Cooperative ? 1u : policy.MaxConcurrency };
                using var etl = new SdkEtlSession(sessionPolicy, environment, cpu);
                await foreach (var invocation in inbox.Reader.ReadAllAsync(lifetime.Token))
                {
                    var token = invocation.Cancellation.Token;
                    using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
                    timeout.CancelAfter(configuration.Options.ExecutionTimeout);
                    try
                    {
                        var reply = await DispatchAsync(invocation.Frame, etl, timeout.Token, token);
                        using (reply.Batch)
                            await WorkerProtocol.WriteAsync(pipe, reply.Message, reply.Batch, lifetime.Token);
                    }
                    finally
                    {
                        requests.TryRemove(invocation.Frame.Sequence, out _);
                        invocation.Cancellation.Dispose();
                    }
                }
            }
            finally
            {
                await lifetime.CancelAsync();
                await pump;
            }
        }
        catch (Exception error) when (error is IOException or InvalidDataException or JsonException or OperationCanceledException) { }
        finally
        {
            foreach (var request in requests.Values) request.Dispose();
        }
    }

    private static async Task ReceiveAsync(Stream pipe, ChannelWriter<Invocation> output,
        ConcurrentDictionary<long, CancellationTokenSource> requests, CancellationTokenSource lifetime, string sessionId)
    {
        long previous = 0;
        try
        {
            while (!lifetime.IsCancellationRequested)
            {
                var frame = await WorkerProtocol.ReadAsync<WorkerRequestFrame>(pipe, lifetime.Token);
                using var batch = frame.Batch;
                var request = frame.Message;
                if (batch is not null || request.Revision != ProductInfo.WorkerRevision || request.SessionId != sessionId)
                    throw new InvalidDataException("Incompatible SDK Worker request");
                if (request.Operation == "sdk.cancel")
                {
                    if (request.Sequence <= 0 || request.Sequence > previous)
                        throw new InvalidDataException("SDK Worker cancellation sequence differs");
                    if (requests.TryGetValue(request.Sequence, out var running))
                    {
                        try { _ = running.CancelAsync(); }
                        catch (ObjectDisposedException) { }
                    }
                    continue;
                }
                if (request.Sequence != checked(previous + 1)) throw new InvalidDataException("SDK Worker request sequence differs");
                previous = request.Sequence;
                var cancellation = CancellationTokenSource.CreateLinkedTokenSource(lifetime.Token);
                requests.TryAdd(request.Sequence, cancellation);
                if (!output.TryWrite(new(request, cancellation))) throw new InvalidDataException("SDK Worker request overlap");
            }
        }
        catch (Exception error) when (error is IOException or InvalidDataException or JsonException or OperationCanceledException)
        {
            // Client EOF owns the lifetime of exactly this execution child.
        }
        finally
        {
            output.TryComplete();
            _ = lifetime.CancelAsync();
        }
    }

    private static async ValueTask<(WorkerResponseFrame Message, WorkerBatchBuffer? Batch)> DispatchAsync(
        WorkerRequestFrame request, SdkEtlSession etl,
        CancellationToken cancellation, CancellationToken caller)
    {
        try
        {
            return await etl.InvokeAsync(request, cancellation);
        }
        catch (UniversalDataException error)
        {
            return Reply(request, error.Code, error.Detail);
        }
        catch (OperationCanceledException)
        {
            return Reply(request, caller.IsCancellationRequested ? StatusCode.Cancelled : StatusCode.Timeout);
        }
        catch (Exception error) when (error is not OutOfMemoryException)
        {
            var code = error switch
            {
                DllNotFoundException or FileNotFoundException => StatusCode.DriverUnavailable,
                UnauthorizedAccessException => StatusCode.Auth,
                InvalidDataException or JsonException => StatusCode.Protocol,
                IOException => StatusCode.Io,
                _ => StatusCode.Invalid
            };
            return Reply(request, code);
        }
    }

    private static (WorkerResponseFrame, WorkerBatchBuffer?) Reply(WorkerRequestFrame request, StatusCode code,
        OperationError? error = null) =>
        (new(ProductInfo.WorkerRevision, request.SessionId, request.Sequence, "result", code, Error: error), null);

    private static bool CanBorrow(string statePath, string source, ulong revision)
    {
        try
        {
            using var snapshot = new FileStream(statePath, FileMode.Open, FileAccess.Read,
                FileShare.ReadWrite | FileShare.Delete, 4096, FileOptions.SequentialScan);
            return JsonSerializer.Deserialize<Dictionary<string, SdkSourceRevision>>(snapshot, WorkerProtocol.Json)
                ?.TryGetValue(source, out var current) == true && current.Enabled && current.Revision == revision;
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException or JsonException) { return false; }
    }

    private sealed record Invocation(WorkerRequestFrame Frame, CancellationTokenSource Cancellation);
}
