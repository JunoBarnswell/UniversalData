using System.Buffers;
using System.Net.WebSockets;
using System.Text.Json;

namespace UniversalData.Hosting;

public sealed class WorkerNodeChannel(WebSocket socket) : IDisposable
{
    public const string RegistrationPath = "/internal/worker-nodes/register";
    public const string ExecutionPath = "/internal/worker-nodes/execute";
    public static readonly TimeSpan LeaseDuration = TimeSpan.FromSeconds(20);
    private const int MaxFrameBytes = 64 * 1024 * 1024;
    private readonly SemaphoreSlim writer = new(1, 1);

    public async Task SendAsync(WorkerNodeFrame frame, CancellationToken cancellation)
    {
        var data = JsonSerializer.SerializeToUtf8Bytes(frame, HostPolicy.JsonOptions);
        if (data.Length > MaxFrameBytes)
            throw new InvalidDataException("Worker node frame is too large");
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
        deadline.CancelAfter(TimeSpan.FromSeconds(8));
        await writer.WaitAsync(deadline.Token);
        try { await socket.SendAsync(data.AsMemory(), WebSocketMessageType.Text, true, deadline.Token); }
        finally { writer.Release(); }
    }

    public async Task<WorkerNodeFrame?> ReadAsync(CancellationToken cancellation, int maximum = MaxFrameBytes)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(16 * 1024);
        try
        {
            using var message = new MemoryStream();
            ValueWebSocketReceiveResult result;
            do
            {
                result = await socket.ReceiveAsync(buffer.AsMemory(), cancellation);
                if (result.MessageType == WebSocketMessageType.Close)
                    return null;
                if (result.MessageType != WebSocketMessageType.Text || message.Length + result.Count > maximum)
                    throw new InvalidDataException("Invalid Worker node frame");
                message.Write(buffer, 0, result.Count);
            } while (!result.EndOfMessage);
            var frame = JsonSerializer.Deserialize<WorkerNodeFrame>(message.GetBuffer().AsSpan(0, (int)message.Length), HostPolicy.JsonOptions);
            if (frame is null || frame.WorkerRevision != ProductInfo.WorkerRevision || !HostIdentity.ValidId(frame.NodeId))
                throw new InvalidDataException("Incompatible Worker node frame");
            return frame;
        }
        finally { ArrayPool<byte>.Shared.Return(buffer, clearArray: true); }
    }

    public void Abort() => socket.Abort();
    public void Dispose()
    {
        socket.Abort();
        socket.Dispose();
    }
}
