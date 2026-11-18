namespace UniversalData.Management;

internal sealed class WorkerReservation(WorkerPlacement[] placements, Action release) : IDisposable
{
    internal WorkerPlacement[] Placements { get; } = placements;
    private Action? releaseOnce = release;
    public void Dispose() => Interlocked.Exchange(ref releaseOnce, null)?.Invoke();
}
