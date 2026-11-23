namespace UniversalData.Management;

/// <summary>Renewable catalog lease held while a dataset publication mutates state.</summary>
internal sealed class DatasetLease : IDisposable
{
    private const uint DurationMilliseconds = 120000;
    private const int RenewalMilliseconds = 60000;
    private readonly Catalog catalog;
    private readonly object sync = new();
    private readonly CancellationTokenSource lifetime = new();
    private readonly Task renewal;
    private CatalogLease guard;
    private bool disposed;

    internal DatasetLease(Catalog catalog, CatalogLease guard)
    {
        this.catalog = catalog;
        this.guard = guard;
        renewal = RenewLoop();
    }

    internal CatalogLease Guard
    {
        get { lock (sync) return guard; }
    }

    private async Task RenewLoop()
    {
        try
        {
            while (true)
            {
                await Task.Delay(RenewalMilliseconds, lifetime.Token).ConfigureAwait(false);
                lock (sync)
                {
                    if (disposed) return;
                    guard = catalog.RenewLease(guard, DurationMilliseconds);
                }
            }
        }
        catch (OperationCanceledException) when (lifetime.IsCancellationRequested) { }
    }

    public void Dispose()
    {
        CatalogLease current;
        lock (sync)
        {
            if (disposed) return;
            disposed = true;
            current = guard;
            lifetime.Cancel();
        }
        try { renewal.GetAwaiter().GetResult(); }
        finally
        {
            catalog.ReleaseLease(current);
            lifetime.Dispose();
        }
    }
}
