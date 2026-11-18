namespace UniversalData.Management;

internal sealed class WorkerStartBarrier(int count, Action opened)
{
    private readonly TaskCompletionSource<bool> readiness = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int remaining = count;

    internal async Task ReadyAsync(CancellationToken cancellation)
    {
        if (Interlocked.Decrement(ref remaining) == 0 && !readiness.Task.IsCompleted)
        {
            try { opened(); readiness.TrySetResult(true); }
            catch { Fail(); throw; }
        }
        if (!await readiness.Task.WaitAsync(cancellation))
            throw new OperationCanceledException("A Worker did not reach the start barrier");
    }

    internal void Fail() => readiness.TrySetResult(false);
}
