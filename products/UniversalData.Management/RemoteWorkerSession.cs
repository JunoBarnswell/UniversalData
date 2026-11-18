using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text.Json;
using System.Threading.Channels;
using UniversalData.Hosting;

namespace UniversalData.Management;

public sealed class RemoteWorkerSession : IAsyncDisposable
{
    private readonly WorkerNodeChannel channel;
    private readonly WorkerNodeFrame lease;
    private readonly TaskCompletionSource closed;
    private readonly CancellationTokenSource lifetime;
    private readonly Task pump;
    private string? renewalProbe;
    private readonly TaskCompletionSource<int> exit = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly Channel<string> reports = Channel.CreateBounded<string>(new BoundedChannelOptions(4) { SingleWriter = true });
    private readonly ConcurrentDictionary<string, TaskCompletionSource<JsonElement>> controls = new(StringComparer.Ordinal);

    internal RemoteWorkerSession(WorkerNodeChannel channel, WorkerNodeFrame lease,
        TaskCompletionSource closed, CancellationToken disconnected)
    {
        this.channel = channel;
        this.lease = lease;
        this.closed = closed;
        lifetime = CancellationTokenSource.CreateLinkedTokenSource(disconnected);
        pump = ReadReports();
    }

    public async Task<string?> ReadAsync(CancellationToken cancellation)
    {
        while (await reports.Reader.WaitToReadAsync(cancellation))
            if (reports.Reader.TryRead(out var value))
                return value;
        return null;
    }

    public Task SendAsync(string frame, CancellationToken cancellation)
    {
        if (frame is not ("start" or "cancel"))
            throw new ArgumentException("Invalid Worker pipe command", nameof(frame));
        return Send(lease.Reply(frame), cancellation);
    }

    public Task CancelAsync(CancellationToken cancellation) => SendAsync("cancel", cancellation);

    // Called only after the dispatch owner durably renews the corresponding catalog lease.
    public Task RenewAsync(CancellationToken cancellation)
    {
        var probe = Volatile.Read(ref renewalProbe);
        return probe is null ? Task.CompletedTask : Send(lease.Reply("renew", correlation: probe), cancellation);
    }

    public Task<int> WaitForExitAsync(CancellationToken cancellation) => exit.Task.WaitAsync(cancellation);

    public async Task<JsonElement> SendControlAsync(string action, JsonElement? command, CancellationToken cancellation)
    {
        var id = Guid.NewGuid().ToString("N");
        var answer = new TaskCompletionSource<JsonElement>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!controls.TryAdd(id, answer))
            throw new InvalidOperationException("Duplicate Worker control correlation");
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellation, lifetime.Token);
        deadline.CancelAfter(TimeSpan.FromSeconds(6));
        try
        {
            var data = JsonSerializer.SerializeToElement(new { action, command }, HostPolicy.JsonOptions);
            await Send(lease.Reply("control", data, id), deadline.Token);
            var response = await answer.Task.WaitAsync(deadline.Token);
            var result = response.GetProperty("result").GetInt32();
            if (result != 0)
                throw new UniversalDataException(result, "Remote Worker control failed");
            return response.GetProperty("value").Clone();
        }
        finally { controls.TryRemove(id, out _); }
    }

    private async Task Send(WorkerNodeFrame frame, CancellationToken cancellation)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellation, lifetime.Token);
        await channel.SendAsync(frame, deadline.Token);
    }

    private async Task ReadReports()
    {
        Exception? failure = null;
        try
        {
            while (await channel.ReadAsync(lifetime.Token) is { } frame)
            {
                if (!frame.SameLease(lease))
                    throw new InvalidDataException("Remote Worker report differs from its lease");
                switch (frame.Kind)
                {
                    case "report" when frame.Data is { ValueKind: JsonValueKind.Object } report:
                        await reports.Writer.WriteAsync(report.GetRawText(), lifetime.Token);
                        break;
                    case "control-result" when frame.Data is { ValueKind: JsonValueKind.Object } response:
                        if (frame.Correlation is { } id && controls.TryGetValue(id, out var pending))
                            pending.TrySetResult(response);
                        break;
                    case "renew-probe" when Guid.TryParseExact(frame.Correlation, "N", out _):
                        Volatile.Write(ref renewalProbe, frame.Correlation);
                        break;
                    case "exit" when frame.Data is { ValueKind: JsonValueKind.Number } status:
                        exit.TrySetResult(status.GetInt32());
                        return;
                    default:
                        throw new InvalidDataException("Invalid remote Worker report kind");
                }
            }
        }
        catch (Exception error) when (error is IOException or WebSocketException or JsonException or OperationCanceledException or InvalidOperationException or FormatException)
        {
            failure = new IOException("Remote Worker connection was lost", error);
        }
        finally
        {
            if (!exit.Task.IsCompleted)
            {
                failure ??= new IOException("Remote Worker did not report its exit");
                exit.TrySetException(failure);
            }
            reports.Writer.TryComplete(failure);
            await lifetime.CancelAsync();
            closed.TrySetResult();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await lifetime.CancelAsync();
        channel.Abort();
        await pump;
        lifetime.Dispose();
    }
}
