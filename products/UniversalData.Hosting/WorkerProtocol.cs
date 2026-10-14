using System.Buffers;
using System.Text;

namespace UniversalData.Hosting;

/// <summary>Bounded framing shared by the service and its owned execution process.</summary>
public static class WorkerProtocol
{
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
