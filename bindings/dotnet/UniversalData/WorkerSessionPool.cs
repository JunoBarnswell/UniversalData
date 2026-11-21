using System.Diagnostics;

namespace UniversalData;

internal sealed class WorkerSessionPool : IAsyncDisposable
{
    private static readonly TimeSpan IdleTime = TimeSpan.FromMilliseconds(250);
    private readonly object gate = new();
    private readonly List<(WorkerSession Worker, bool ReturnsRows, long Returned)> idle = [];
    private readonly Timer timer;
    private readonly int capacity;
    private int allocated;
    private int opening;
    private bool disposed;

    internal WorkerSessionPool(int capacity)
    {
        this.capacity = capacity;
        timer = new(_ => RetireIdle(), null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
    }

    internal async Task<WorkerSession> RentAsync(Func<CancellationToken, Task<WorkerSession>> open,
        bool returnsRows, CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        lock (gate)
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            int selected = idle.Count - 1;
            while (selected >= 0 && idle[selected].ReturnsRows != returnsRows) selected--;
            if (selected < 0 && allocated >= capacity) selected = idle.Count - 1;
            if (selected >= 0)
            {
                var worker = idle[selected].Worker;
                idle.RemoveAt(selected);
                return worker;
            }
            allocated++;
            opening++;
        }
        try { return await open(cancellation).ConfigureAwait(false); }
        catch { lock (gate) allocated--; throw; }
        finally { lock (gate) opening--; }
    }

    internal ValueTask ReturnAsync(WorkerSession worker, bool returnsRows, bool complete)
    {
        lock (gate)
        {
            // A queued admission needs the underlying slot to be released.
            if (complete && !disposed && opening == 0)
            {
                if (idle.Count == 0) timer.Change(IdleTime, IdleTime);
                idle.Add((worker, returnsRows, Stopwatch.GetTimestamp()));
                return ValueTask.CompletedTask;
            }
            allocated--;
        }
        return worker.DisposeAsync();
    }

    private void RetireIdle()
    {
        List<WorkerSession>? expired = null;
        lock (gate)
        {
            if (disposed) return;
            for (int index = 0; index < idle.Count && Stopwatch.GetElapsedTime(idle[index].Returned) >= IdleTime; index++)
                (expired ??= []).Add(idle[index].Worker);
            if (expired is not null)
            {
                idle.RemoveRange(0, expired.Count);
                allocated -= expired.Count;
            }
            if (idle.Count == 0) timer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        }
        if (expired is not null)
            foreach (var worker in expired) _ = worker.DisposeAsync();
    }

    public async ValueTask DisposeAsync()
    {
        WorkerSession[] remaining;
        lock (gate)
        {
            if (disposed) return;
            disposed = true;
            remaining = idle.Select(item => item.Worker).ToArray();
            idle.Clear();
        }
        await timer.DisposeAsync().ConfigureAwait(false);
        await Task.WhenAll(remaining.Select(worker => worker.DisposeAsync().AsTask())).ConfigureAwait(false);
    }
}
