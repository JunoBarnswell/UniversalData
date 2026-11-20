using System.Collections.Concurrent;
using UniversalData.Hosting;

namespace UniversalData.Management;

/// <summary>Tenant-separated dispatch over one Native execution and Catalog contract.</summary>
public sealed class RunDispatcher(HostPolicy root, ProductConfiguration configuration, IdentityDirectory identities,
    WorkerNodeDirectory remoteNodes, WorkerReservations reservations) : BackgroundService
{
    private readonly ConcurrentDictionary<string,TenantDispatcher> tenants = new(StringComparer.Ordinal);
    public TenantDispatcher For(HostIdentity identity) => tenants.GetOrAdd(identity.Tenant,tenant =>
        new(root.Bind(HostIdentity.Operator(tenant)),root,configuration,identities,remoteNodes,reservations));
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        Directory.CreateDirectory(root.DataDirectory);
        foreach (var tenant in root.Tenants is null ? Array.Empty<string>() : root.Tenants.Keys.AsEnumerable()) For(HostIdentity.Operator(tenant));
        await Task.WhenAll(tenants.Values.Select(dispatcher => dispatcher.Run(stoppingToken)));
    }
}
