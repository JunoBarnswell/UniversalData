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
    internal bool Ready => !Enabled || Current is { Ready: true };

    internal IDisposable? Pin(Term? term) => term is null ? null : catalog?.Pin(term.Lease);
    internal void CatalogUnavailable(UniversalDataException error)
    {
        if (Enabled && error.Detail?.Source == ErrorSource.Catalog && error.Code is StatusCode.Transient or StatusCode.Io)
        {
            if (Interlocked.Exchange(ref current,null) is { } lost)
            {
                lost.Revoke();
                logger.LogWarning("Catalog confirmation was lost ({Status}).",error.Code);
            }
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
                    if (term is { Valid:false }) term = null;
                    long issued = Stopwatch.GetTimestamp();
                    using var authority = catalog.Open(ClusterCatalog.SystemTenant, election: true);
                    var lease = term is null
                        ? authority.TryAcquireLease(CatalogKind.Audit, ClusterCatalog.LeadershipId, owner, DurationMilliseconds)
                        : authority.RenewLease(term.Lease, DurationMilliseconds);
                    if (lease is null) continue;
                    if (Stopwatch.GetElapsedTime(issued) >= TimeSpan.FromMilliseconds(DurationMilliseconds))
                        throw new IOException("Leadership confirmation exceeded its lease window");
                    term ??= new Term(lease);
                    term.Confirm(lease, issued);
                    Volatile.Write(ref current, term);
                    using (Pin(term))
                    {
                        ClusterHostState.EnsureInitialized(catalog);
                    }
                    term.Ready = true;
                }
                catch (Exception error) when (error is UniversalDataException or IOException or InvalidDataException or
                    System.Security.Cryptography.CryptographicException)
                {
                    if (term is not null)
                    {
                        Volatile.Write(ref current, null);
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
        internal volatile bool Ready;
        internal void Confirm(CatalogLease value, long issued)
        {
            Volatile.Write(ref currentLease, value);
            Interlocked.Exchange(ref deadline, issued + Stopwatch.Frequency * (long)DurationMilliseconds / 1000);
            lost.CancelAfter(TimeSpan.FromMilliseconds(DurationMilliseconds) - Stopwatch.GetElapsedTime(issued));
        }
        internal void Revoke() => lost.Cancel();
    }
}
