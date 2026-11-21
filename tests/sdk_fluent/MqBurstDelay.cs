using System.Diagnostics;
using System.Text.Json;
using UniversalData;

namespace FluentAcceptance;

internal sealed class MqBurstDelay : IManagedComponent
{
    private static readonly TimeSpan BusinessDelay = TimeSpan.FromSeconds(5);
    private readonly CancellationTokenSource stop = new();
    private string timingFile = "";

    public void Open(ManagedComponentContext context, string configuration, string[] inputs, string[] outputs)
    {
        using var config = JsonDocument.Parse(configuration);
        timingFile = config.RootElement.GetProperty("timingFile").GetString()!;
    }

    public StatusCode Step(ManagedComponentContext context, ManagedComponentEvent input)
    {
        if (input.Kind == ComponentEventKind.Poll) return StatusCode.End;
        if (input.Kind != ComponentEventKind.Data || input.Batch is not { } batch) return StatusCode.Ok;

        int rows = checked((int)batch.Rows);
        int active = 0, peak = 0;
        double minimum = double.PositiveInfinity;
        var samples = new object();
        Parallel.ForAsync(0, rows, new ParallelOptions { MaxDegreeOfParallelism = rows, CancellationToken = stop.Token },
            async (_, cancellation) =>
            {
                long entered = Stopwatch.GetTimestamp();
                lock (samples) peak = Math.Max(peak, ++active);
                TimeSpan elapsed;
                while ((elapsed = Stopwatch.GetElapsedTime(entered)) < BusinessDelay)
                    await Task.Delay((int)Math.Ceiling((BusinessDelay - elapsed).TotalMilliseconds), cancellation);
                lock (samples)
                {
                    active--;
                    minimum = Math.Min(minimum, elapsed.TotalMilliseconds);
                }
            }).GetAwaiter().GetResult();
        context.CheckCancellation();
        File.AppendAllText(timingFile, JsonSerializer.Serialize(new { Rows = rows, PeakInFlight = peak,
            MinimumDelayMs = minimum }) + "\n");
        context.Emit(0, batch);
        return StatusCode.Ok;
    }

    public void Cancel() => stop.Cancel();

    public void Dispose() => stop.Dispose();
}
