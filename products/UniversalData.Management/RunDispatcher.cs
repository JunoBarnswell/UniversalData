using System.Collections.Concurrent;
using UniversalData.Hosting;

namespace UniversalData.Management;

/// <summary>Tenant-separated dispatch over one Native execution and Catalog contract.</summary>
public sealed class RunDispatcher(HostPolicy root, ProductConfiguration configuration, IdentityDirectory identities,
    WorkerNodeDirectory remoteNodes, WorkerReservations reservations, WorkerSupervisor supervisor,
    ClusterLeadership leadership, ILogger<RunDispatcher> logger) : BackgroundService
{
    private readonly ConcurrentDictionary<string,TenantDispatcher> tenants = new(StringComparer.Ordinal);
    public TenantDispatcher For(HostIdentity identity) => tenants.GetOrAdd(identity.Tenant,tenant =>
        new(root.Bind(HostIdentity.Operator(tenant)),root,configuration,identities,remoteNodes,reservations,supervisor,leadership,logger));
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        Directory.CreateDirectory(root.DataDirectory);
        var dispatchers = (root.Tenants?.Keys.AsEnumerable() ?? []).Order(StringComparer.Ordinal)
            .Select(tenant => For(HostIdentity.Operator(tenant))).ToArray();
        if (dispatchers.Length == 0) return;
        int first = 0;
        using var cadence = new PeriodicTimer(TimeSpan.FromMilliseconds(250));
        try
        {
            do
            {
                var term = leadership.Current;
                if (leadership.Enabled && term is not { Ready: true }) continue;
                for (int offset = 0; offset < dispatchers.Length; offset++)
                    dispatchers[(first + offset) % dispatchers.Length].Tick(term,stoppingToken);
                first = (first + 1) % dispatchers.Length;
            } while (await cadence.WaitForNextTickAsync(stoppingToken));
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        finally
        {
            await Task.WhenAll(dispatchers.Select(dispatcher => dispatcher.WaitForMonitors()));
        }
    }
}
