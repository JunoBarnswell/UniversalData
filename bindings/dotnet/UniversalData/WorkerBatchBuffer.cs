using System.Buffers;

namespace UniversalData;

internal sealed class WorkerBatchBuffer : IDisposable
{
    private const int MaximumPooledLength = 1024 * 1024;
    private byte[]? buffer;

    internal WorkerBatchBuffer(int length)
    {
        Length = length;
        buffer = length <= MaximumPooledLength ? ArrayPool<byte>.Shared.Rent(length) :
            GC.AllocateUninitializedArray<byte>(length);
    }

    internal int Length { get; }
    internal byte[] Bytes => buffer ?? throw new ObjectDisposedException(nameof(WorkerBatchBuffer));
    internal Memory<byte> Memory => Bytes.AsMemory(0, Length);

    public void Dispose()
    {
        byte[]? bytes = Interlocked.Exchange(ref buffer, null);
        if (bytes is not null && Length <= MaximumPooledLength)
            ArrayPool<byte>.Shared.Return(bytes, clearArray: true);
    }
}
