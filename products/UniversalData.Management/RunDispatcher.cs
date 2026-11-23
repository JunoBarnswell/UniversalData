using System.Collections.Concurrent;
using UniversalData.Hosting;

namespace UniversalData.Management;

/// <summary>Tenant-separated dispatch over one Native execution and Catalog contract.</summary>
public sealed class RunDispatcher(HostPolicy root, ProductConfiguration configuration, IdentityDirectory identities,
    WorkerNodeDirectory remoteNodes, WorkerReservations reservations, WorkerSupervisor supervisor,
    ClusterLeadership leadership, ILogger<RunDispatcher> logger, IServiceProvider services) : BackgroundService
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
        var analysis = services.GetRequiredService<AnalysisWorkspace>();
        var refreshTasks = new Dictionary<string, Task>(StringComparer.Ordinal);
        long nextCheck = 0;
        using var cadence = new PeriodicTimer(TimeSpan.FromMilliseconds(250));
        try
        {
            do
            {
                var term = leadership.Current;
                if (leadership.Enabled && term is null) continue;
                for (int offset = 0; offset < dispatchers.Length; offset++)
                    dispatchers[(first + offset) % dispatchers.Length].Tick(term,stoppingToken);
                first = (first + 1) % dispatchers.Length;
                if (Environment.TickCount64 >= nextCheck)
                {
                    nextCheck = Environment.TickCount64 + 2000;
                    foreach (string tenant in root.Tenants!.Keys)
                    {
                        if (refreshTasks.TryGetValue(tenant, out var task))
                        {
                            if (!task.IsCompleted) continue;
                            try { await task; }
                            catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested) { }
                            catch (Exception e) when (e is UniversalDataException or IOException)
                            { logger.LogWarning("BI refresh dispatch for {Tenant} failed ({Failure})", tenant, e.GetType().Name); }
                        }
                        refreshTasks[tenant] = Task.Run(() => analysis.DispatchRefreshes(tenant, leadership, term, logger, stoppingToken), stoppingToken);
                    }
                }
            } while (await cadence.WaitForNextTickAsync(stoppingToken));
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        finally
        {
            await Task.WhenAll(dispatchers.Select(dispatcher => dispatcher.WaitForMonitors()));
            try { await Task.WhenAll(refreshTasks.Values); }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        }
    }
}
