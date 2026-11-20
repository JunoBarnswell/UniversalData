using System.Buffers;
using System.Buffers.Binary;
using System.Text.Json;
using System.Text;

namespace UniversalData;

/// <summary>Bounded framing shared by the service and its owned execution process.</summary>
public static class WorkerProtocol
{
    internal static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    internal const int MaximumControlBytes = 64 * 1024 * 1024;
    internal const int MaximumBatchBytes = 64 * 1024 * 1024;

    internal static async Task<int> ReadProcessIdAsync(TextReader output, CancellationToken cancellation)
    {
        var message = await ReadLine(output, 1024, cancellation).ConfigureAwait(false)
            ?? throw new IOException("Worker exited before reporting process readiness");
        using var document = JsonDocument.Parse(message);
        var hello = document.RootElement;
        if (hello.GetProperty("kind").GetString() != "hello" ||
            hello.GetProperty("version").GetInt32() != ProductInfo.Protocol ||
            hello.GetProperty("contractRevision").GetInt32() != ProductInfo.WorkerRevision ||
            hello.GetProperty("processId").GetInt32() is not (> 0 and var pid))
            throw new InvalidDataException("Incompatible Worker process handshake");
        return pid;
    }

    internal static async ValueTask WriteAsync<T>(Stream output, T control, WorkerBatchBuffer? batch,
        CancellationToken cancellation = default)
    {
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(control, Json);
        int batchLength = batch?.Length ?? 0;
        if (payload.Length > MaximumControlBytes || batchLength > MaximumBatchBytes)
            throw new InvalidDataException("Worker message exceeds its configured bound");
        var length = new byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(length, payload.Length);
        BinaryPrimitives.WriteInt32LittleEndian(length.AsSpan(4), batchLength);
        await output.WriteAsync(length, cancellation).ConfigureAwait(false);
        await output.WriteAsync(payload, cancellation).ConfigureAwait(false);
        if (batchLength != 0)
            await output.WriteAsync(batch!.Memory, cancellation).ConfigureAwait(false);
        await output.FlushAsync(cancellation).ConfigureAwait(false);
    }

    internal static async ValueTask<(T Message, WorkerBatchBuffer? Batch)> ReadAsync<T>(Stream input,
        CancellationToken cancellation = default) where T : class
    {
        var length = new byte[8];
        await input.ReadExactlyAsync(length, cancellation).ConfigureAwait(false);
        int controlLength = BinaryPrimitives.ReadInt32LittleEndian(length);
        int batchLength = BinaryPrimitives.ReadInt32LittleEndian(length.AsSpan(4));
        if (controlLength is <= 0 or > MaximumControlBytes || batchLength is < 0 or > MaximumBatchBytes)
            throw new InvalidDataException("Invalid Worker message length");
        byte[] payload = GC.AllocateUninitializedArray<byte>(controlLength);
        await input.ReadExactlyAsync(payload, cancellation).ConfigureAwait(false);
        T control = JsonSerializer.Deserialize<T>(payload, Json)
            ?? throw new InvalidDataException("Missing Worker message");
        if (batchLength == 0)
            return (control, null);
        var batch = new WorkerBatchBuffer(batchLength);
        try
        {
            await input.ReadExactlyAsync(batch.Memory, cancellation).ConfigureAwait(false);
            return (control, batch);
        }
        catch { batch.Dispose(); throw; }
    }


    public static async ValueTask<string?> ReadLine(TextReader reader, int maximumCharacters,
        CancellationToken cancellation = default)
    {
        if (maximumCharacters < 1) throw new ArgumentOutOfRangeException(nameof(maximumCharacters));
        var text = new StringBuilder(Math.Min(maximumCharacters, 4096));
        char[] buffer = ArrayPool<char>.Shared.Rent(1);
        try {
            while (true) {
                int count = await reader.ReadAsync(buffer.AsMemory(0, 1), cancellation);
                if (count == 0) return text.Length == 0 ? null : text.ToString();
                if (buffer[0] == '\n') {
                    if (text.Length != 0 && text[^1] == '\r') text.Length--;
                    return text.ToString();
                }
                if (text.Length == maximumCharacters) throw new InvalidDataException("Worker frame exceeds configured limit");
                text.Append(buffer[0]);
            }
        } finally { ArrayPool<char>.Shared.Return(buffer); }
    }
    public static async Task Discard(TextReader reader, CancellationToken cancellation)
    {
        char[] buffer = ArrayPool<char>.Shared.Rent(4096);
        try { while (await reader.ReadAsync(buffer.AsMemory(), cancellation) != 0) { } }
        finally { ArrayPool<char>.Shared.Return(buffer); }
    }
}
