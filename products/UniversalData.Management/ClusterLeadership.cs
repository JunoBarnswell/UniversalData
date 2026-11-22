using System.Diagnostics;
using UniversalData.Hosting;

namespace UniversalData.Management;

public sealed class ClusterLeadership : BackgroundService
{
    private const uint DurationMilliseconds = 12000;
    private readonly ClusterCatalog? catalog;
    private readonly string owner;
    private readonly ILogger<ClusterLeadership> logger;
    private Term? current;

    internal ClusterLeadership(ClusterCatalog? catalog, string node, ILogger<ClusterLeadership> logger)
    {
        this.catalog = catalog;
        this.logger = logger;
        owner = node + "." + Guid.NewGuid().ToString("N");
        if (catalog is not null)
            catalog.Authority = () => Current?.Lease;
    }

    internal bool Enabled => catalog is not null;
    internal Term? Current
    {
        get
        {
            var term = Volatile.Read(ref current);
            return term?.Valid == true ? term : null;
        }
    }
    internal bool Ready => !Enabled || Current is not null;

    internal IDisposable? Pin(Term? term) => term is null ? null : catalog?.Pin(term.Lease);
    internal void RequireCurrentAuthority(CancellationToken cancellation)
    {
        cancellation.ThrowIfCancellationRequested();
        if (catalog is not null && (Current is not { } term ||
            catalog.PinnedAuthority is not { } pinned || pinned.Generation != term.Lease.Generation))
            throw new OperationCanceledException("Management leadership is no longer confirmed", cancellation);
    }
    internal void CatalogUnavailable(UniversalDataException error)
    {
        if (catalog is null || error.Detail?.Source != ErrorSource.Catalog ||
            error.Code is not (StatusCode.Transient or StatusCode.Io or StatusCode.Timeout or StatusCode.CommitUnknown))
            return;
        var term = Volatile.Read(ref current);
        if (term is null || catalog.PinnedAuthority is { } pinned && pinned.Generation != term.Lease.Generation)
            return;
        if (ReferenceEquals(Interlocked.CompareExchange(ref current, null, term), term))
        {
            term.Revoke();
            logger.LogWarning("Catalog confirmation was lost ({Status}).", error.Code);
        }
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        catalog is null ? Task.CompletedTask : Task.Factory.StartNew(
            () => Elect(catalog,stoppingToken),CancellationToken.None,TaskCreationOptions.LongRunning,TaskScheduler.Default);

    private void Elect(ClusterCatalog catalog,CancellationToken stoppingToken)
    {
        Term? term = null;
        try
        {
            do
            {
                try
                {
                    if (term is { Valid: false })
                    {
                        Interlocked.CompareExchange(ref current, null, term);
                        term.Revoke();
                        term = null;
                    }
                    bool initializing = term is null;
                    long issued = Stopwatch.GetTimestamp();
                    using var authority = catalog.Open(ClusterCatalog.SystemTenant, election: true);
                    var lease = term is null
                        ? authority.TryAcquireLease(CatalogKind.Audit, ClusterCatalog.LeadershipId, owner, DurationMilliseconds)
                        : authority.RenewLease(term.Lease, DurationMilliseconds);
                    if (lease is null) continue;
                    term ??= new Term(lease);
                    if (!term.Confirm(lease, issued))
                        throw new IOException("Leadership confirmation exceeded its lease window");
                    if (initializing)
                    {
                        ClusterHostState.EnsureInitialized(catalog);
                        if (!term.Valid)
                            throw new IOException("Leadership confirmation expired during initialization");
                        Volatile.Write(ref current, term);
                    }
                }
                catch (Exception error) when (error is UniversalDataException or IOException or InvalidDataException or
                    System.Security.Cryptography.CryptographicException)
                {
                    if (term is not null)
                    {
                        Interlocked.CompareExchange(ref current, null, term);
                        term.Revoke();
                        term = null;
                        logger.LogWarning("Cluster leadership was lost ({Reason}).", error is UniversalDataException native
                            ? native.Code.ToString() : error.GetType().Name);
                    }
                }
            } while (!stoppingToken.WaitHandle.WaitOne(TimeSpan.FromSeconds(3)));
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        finally
        {
            Volatile.Write(ref current, null);
            if (term is { } active)
            {
                active.Revoke();
                try
                {
                    using var authority = catalog.Open(ClusterCatalog.SystemTenant, election: true);
                    authority.ReleaseLease(active.Lease);
                }
                catch (UniversalDataException) { }
            }
        }
    }

    internal sealed class Term(CatalogLease lease)
    {
        private readonly CancellationTokenSource lost = new();
        private CatalogLease currentLease = lease;
        private long deadline;
        internal CatalogLease Lease => Volatile.Read(ref currentLease);
        internal CancellationToken Lost => lost.Token;
        internal bool Valid => !lost.IsCancellationRequested && Stopwatch.GetTimestamp() < Interlocked.Read(ref deadline);
        internal bool Confirm(CatalogLease value, long issued)
        {
            var remaining = TimeSpan.FromMilliseconds(DurationMilliseconds) - Stopwatch.GetElapsedTime(issued);
            if (lost.IsCancellationRequested || remaining <= TimeSpan.Zero)
                return false;
            Volatile.Write(ref currentLease, value);
            Interlocked.Exchange(ref deadline, issued + Stopwatch.Frequency * (long)DurationMilliseconds / 1000);
            lost.CancelAfter(remaining);
            return !lost.IsCancellationRequested;
        }
        internal void Revoke() => lost.Cancel();
    }
}
