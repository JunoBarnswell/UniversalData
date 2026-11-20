using System.Text.Json;

namespace UniversalData;

public sealed class DataSession : IAsyncDisposable
{
    private readonly DataClient client;
    private readonly WorkerSession worker;
    private int closed;

    internal DataSession(DataClient client, WorkerSession worker) { this.client = client; this.worker = worker; }

    public int ProcessId => worker.ProcessId;

    public async Task RunAsync(Func<DataContext, CancellationToken, Task> method, CancellationToken cancellation = default) =>
        _ = await InvokeAsync(client.DescribeMethod(method, false, true), null, cancellation).ConfigureAwait(false);

    public async Task RunAsync<TArgument>(Func<DataContext, TArgument, CancellationToken, Task> method,
        TArgument argument, CancellationToken cancellation = default) =>
        _ = await InvokeAsync(client.DescribeMethod(method, true, true), Serialize(argument), cancellation).ConfigureAwait(false);

    public Task<TResult> RunAsync<TResult>(Func<DataContext, CancellationToken, Task<TResult>> method,
        CancellationToken cancellation = default) => InvokeAsync<TResult>(client.DescribeMethod(method, false, true), null, cancellation);

    public Task<TResult> RunAsync<TArgument, TResult>(Func<DataContext, TArgument, CancellationToken, Task<TResult>> method,
        TArgument argument, CancellationToken cancellation = default) =>
        InvokeAsync<TResult>(client.DescribeMethod(method, true, true), Serialize(argument), cancellation);

    internal Task<DataSourceProbe> ProbeAsync(string name, CancellationToken cancellation) =>
        InvokeAsync<DataSourceProbe>(new(SdkBusinessRuntime.BuiltinModule, typeof(SdkBusinessRuntime).FullName!, "probe", [], true),
            Serialize(name), cancellation);

    internal async Task<DataRecoveryResult> RecoverAsync(DataRunReference reference, CancellationToken cancellation)
    {
        var result = await SendAsync("sdk.recover", new SdkRecoveryInvocation(reference, client.Sources.Snapshot()),
            reference, cancellation).ConfigureAwait(false);
        return result.Deserialize<DataRecoveryResult>(WorkerProtocol.Json)
            ?? throw new InvalidDataException("Worker omitted the recovery result");
    }

    private async Task<TResult> InvokeAsync<TResult>(SdkMethod method, JsonElement? argument, CancellationToken cancellation) =>
        (await InvokeAsync(method, argument, cancellation).ConfigureAwait(false)).Deserialize<TResult>(WorkerProtocol.Json)!;

    private Task<JsonElement> InvokeAsync(SdkMethod method, JsonElement? argument, CancellationToken cancellation)
    {
        var reference = new DataRunReference(Guid.NewGuid().ToString("N"));
        return SendAsync("sdk.run", new SdkInvocation(reference.RunId, method, argument, client.Sources.Snapshot(),
            client.Modules, client.DefaultSource), reference, cancellation);
    }

    private async Task<JsonElement> SendAsync(string operation, object payload, DataRunReference reference,
        CancellationToken cancellation)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref closed) != 0, this);
        cancellation.ThrowIfCancellationRequested();
        var result = await worker.InvokeAsync(operation, payload, mayWrite: true, cancellation: cancellation).ConfigureAwait(false);
        result.Batch?.Dispose();
        var response = result.Response;
        if (response.BusinessError is { } business)
            throw new DataBusinessException(business.Type, business.Message, response.RecoveryReference ?? reference);
        if (response.Code != StatusCode.Ok)
            throw new DataExecutionException((int)response.Code, response.Error?.Message ?? "Worker operation failed",
                response.RecoveryReference ?? reference, response.Error);
        return response.Value ?? Serialize<object?>(null);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref closed, 1) == 0) await worker.DisposeAsync().ConfigureAwait(false);
    }

    private static JsonElement Serialize<T>(T value) => JsonSerializer.SerializeToElement(value, WorkerProtocol.Json);
}
