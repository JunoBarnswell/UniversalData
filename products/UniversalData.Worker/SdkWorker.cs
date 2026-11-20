using System.Collections.Concurrent;
using System.IO.Pipes;
using System.Text.Json;
using System.Threading.Channels;
using UniversalData;
using UniversalData.Hosting;

namespace UniversalData.Worker;

internal static class SdkWorker
{
    internal static async Task<int> RunAsync(HostPolicy policy, SdkSessionConfiguration configuration)
    {
        Console.SetOut(TextWriter.Null);
        using var lifetime = new CancellationTokenSource();
        using var finished = new CancellationTokenSource();
        _ = Task.Run(() => WatchGuardianAsync(lifetime, finished.Token));
        _ = EnforceExitAsync(lifetime.Token, finished.Token, configuration.Options.CancellationGrace);
        var inbox = Channel.CreateBounded<Invocation>(new BoundedChannelOptions(1)
        { SingleReader = true, SingleWriter = true });
        var requests = new ConcurrentDictionary<long, CancellationTokenSource>();
        try
        {
            await using var pipe = SdkSessionProtocol.Listen(configuration.Endpoint);
            using (var startup = CancellationTokenSource.CreateLinkedTokenSource(lifetime.Token))
            {
                startup.CancelAfter(TimeSpan.FromSeconds(15));
                await pipe.WaitForConnectionAsync(startup.Token);
                var admission = await WorkerProtocol.ReadAsync<SdkPipeAdmission>(pipe, startup.Token);
                using var batch = admission.Batch;
                if (batch is not null || !SdkSessionProtocol.Authenticate(admission.Message, configuration.Capability))
                    return (int)StatusCode.Auth;
                await WorkerProtocol.WriteAsync(pipe, new SdkHostStatus(ProductInfo.WorkerRevision, StatusCode.Ok), null, startup.Token);
            }
            var pump = ReceiveAsync(pipe, inbox.Writer, requests, lifetime);
            try
            {
                var environment = new SdkExecutionEnvironment(policy.Tenant, Path.Combine(policy.DataDirectory, "sdk-runs"),
                    configuration.Options.MaxBranchesPerSession, configuration.MaxConnectionsPerSource,
                    (resource, access) => policy.Authorize(policy.Tenant, resource, access),
                    (resource, reference) => policy.Resolve(policy.Tenant, resource, reference),
                    (source, revision) => CanBorrow(configuration.SourceStateFile, source, revision),
                    (source, token) => ReserveAsync(configuration, source, token));
                using var etl = new SdkEtlSession(policy, _ => environment);
                using var modules = new SdkModuleCatalog(environment.Authorize);
                await foreach (var invocation in inbox.Reader.ReadAllAsync(lifetime.Token))
                {
                    var token = invocation.Cancellation.Token;
                    using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
                    timeout.CancelAfter(configuration.Options.ExecutionTimeout);
                    try
                    {
                        var reply = await DispatchAsync(invocation.Frame, environment, modules, etl, timeout.Token, token);
                        using (reply.Batch)
                            await WorkerProtocol.WriteAsync(pipe, reply.Message, reply.Batch, lifetime.Token);
                    }
                    finally
                    {
                        requests.TryRemove(invocation.Frame.Sequence, out _);
                        invocation.Cancellation.Dispose();
                    }
                }
                return 0;
            }
            finally
            {
                await lifetime.CancelAsync();
                await pump;
            }
        }
        catch (OperationCanceledException) { return 0; }
        catch (Exception error) when (error is IOException or InvalidDataException or JsonException)
        { return (int)StatusCode.Protocol; }
        finally
        {
            await finished.CancelAsync();
            foreach (var request in requests.Values) request.Dispose();
        }
    }

    private static async Task ReceiveAsync(Stream pipe, ChannelWriter<Invocation> output,
        ConcurrentDictionary<long, CancellationTokenSource> requests, CancellationTokenSource lifetime)
    {
        long previous = 0;
        try
        {
            while (!lifetime.IsCancellationRequested)
            {
                var frame = await WorkerProtocol.ReadAsync<WorkerRequestFrame>(pipe, lifetime.Token);
                using var batch = frame.Batch;
                var request = frame.Message;
                if (batch is not null || request.Revision != ProductInfo.WorkerRevision)
                    throw new InvalidDataException("Incompatible SDK Worker request");
                if (request.Operation == "sdk.cancel")
                {
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

    private static async Task<(WorkerResponseFrame Message, WorkerBatchBuffer? Batch)> DispatchAsync(
        WorkerRequestFrame request, SdkExecutionEnvironment environment, SdkModuleCatalog modules, SdkEtlSession etl,
        CancellationToken cancellation, CancellationToken caller)
    {
        DataRunReference? reference = null;
        try
        {
            switch (request.Operation)
            {
                case "sdk.run":
                    var invocation = request.Values.Deserialize<SdkInvocation>(WorkerProtocol.Json)
                        ?? throw new InvalidDataException("Missing SDK invocation");
                    reference = new(invocation.RunId);
                    return Reply(request, StatusCode.Ok, await SdkBusinessRuntime.RunAsync(invocation, environment, modules, cancellation));
                case "sdk.recover":
                    var recovery = request.Values.Deserialize<SdkRecoveryInvocation>(WorkerProtocol.Json)
                        ?? throw new InvalidDataException("Missing SDK recovery invocation");
                    reference = recovery.Reference;
                    var result = await SdkBusinessRuntime.RecoverAsync(recovery, environment, cancellation);
                    return Reply(request, StatusCode.Ok, JsonSerializer.SerializeToElement(result, WorkerProtocol.Json));
                default:
                    if (request.Operation.StartsWith("etl.", StringComparison.Ordinal)) return await etl.InvokeAsync(request, cancellation);
                    return Reply(request, StatusCode.Protocol);
            }
        }
        catch (DataBusinessException error)
        {
            return Reply(request, StatusCode.ManagedComponent, business: new(error.RemoteType, error.Message), reference: error.Reference);
        }
        catch (DataExecutionException error)
        {
            return Reply(request, error.Code, error: error.Detail, reference: error.Reference);
        }
        catch (UniversalDataException error)
        {
            return Reply(request, error.Code, error: error.Detail, reference: reference);
        }
        catch (OperationCanceledException)
        {
            return Reply(request, caller.IsCancellationRequested ? StatusCode.Cancelled : StatusCode.Timeout, reference: reference);
        }
        catch (Exception error) when (error is not OutOfMemoryException)
        {
            if (reference is not null && request.Operation == "sdk.run")
                return Reply(request, StatusCode.ManagedComponent, business: new(error.GetType().FullName ?? error.GetType().Name, error.Message), reference: reference);
            var code = error switch
            {
                DllNotFoundException or FileNotFoundException => StatusCode.DriverUnavailable,
                UnauthorizedAccessException => StatusCode.Auth,
                InvalidDataException or JsonException => StatusCode.Protocol,
                IOException => StatusCode.Io,
                _ => StatusCode.Invalid
            };
            return Reply(request, code, reference: reference);
        }
    }

    private static (WorkerResponseFrame, WorkerBatchBuffer?) Reply(WorkerRequestFrame request, StatusCode code,
        JsonElement? value = null, OperationError? error = null, SdkRemoteFailure? business = null, DataRunReference? reference = null) =>
        (new(ProductInfo.WorkerRevision, request.Sequence, "result", code, value, error, BusinessError: business, RecoveryReference: reference), null);

    private static bool CanBorrow(string statePath, string source, ulong revision)
    {
        try
        {
            using var snapshot = new FileStream(statePath, FileMode.Open, FileAccess.Read,
                FileShare.ReadWrite | FileShare.Delete, 4096, FileOptions.SequentialScan);
            return JsonSerializer.Deserialize<Dictionary<string, SdkSourceRevision>>(snapshot, WorkerProtocol.Json)
                ?.TryGetValue(source, out var current) == true && current.Enabled && current.Revision >= revision;
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException or JsonException) { return false; }
    }

    private static async Task<IAsyncDisposable> ReserveAsync(SdkSessionConfiguration configuration, string source,
        CancellationToken cancellation)
    {
        var pipe = SdkSessionProtocol.Connect(configuration.HostEndpoint);
        try
        {
            await pipe.ConnectAsync(cancellation);
            await WorkerProtocol.WriteAsync(pipe, new SdkPipeAdmission(ProductInfo.WorkerRevision,
                configuration.Capability, "source.reserve", source), null, cancellation);
            var response = await WorkerProtocol.ReadAsync<SdkHostStatus>(pipe, cancellation);
            using (response.Batch) SdkSessionHostClient.Check(response.Message, response.Batch is null);
            return pipe;
        }
        catch { await pipe.DisposeAsync(); throw; }
    }

    private static async Task WatchGuardianAsync(CancellationTokenSource lifetime, CancellationToken finished)
    {
        try { await Console.OpenStandardInput().ReadExactlyAsync(new byte[1], finished); }
        catch (IOException) { }
        catch (OperationCanceledException) when (finished.IsCancellationRequested) { }
        if (!finished.IsCancellationRequested) _ = lifetime.CancelAsync();
    }

    private static async Task EnforceExitAsync(CancellationToken lifetime, CancellationToken finished, TimeSpan grace)
    {
        using var ended = CancellationTokenSource.CreateLinkedTokenSource(lifetime, finished);
        try { await Task.Delay(Timeout.InfiniteTimeSpan, ended.Token); }
        catch (OperationCanceledException) { }
        if (finished.IsCancellationRequested) return;
        try { await Task.Delay(grace, finished); }
        catch (OperationCanceledException) { return; }
        Environment.Exit(1);
    }

    private sealed record Invocation(WorkerRequestFrame Frame, CancellationTokenSource Cancellation);
}
