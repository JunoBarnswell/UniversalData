using UniversalData.Hosting;

namespace UniversalData.Management;

/// <summary>One physical capacity inventory shared by every tenant in this Management process.</summary>
public sealed class WorkerReservations(HostPolicy policy, ProductConfiguration configuration, WorkerNodeDirectory directory,
    WorkerSupervisor supervisor)
{
    private readonly object gate = new();
    private readonly Dictionary<(string Node, bool Remote), (uint Cpu, uint Processes, ulong Memory)> usage = new();
    public bool Ready => supervisor.Ready;

    public WorkerNodeStatus[] Status(string tenant)
    {
        var local = Local(tenant);
        lock (gate)
        {
            return [Describe(local, false, supervisor.Ready ? "Online" : "Offline", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()),
                .. directory.Status(tenant).Select(entry => Describe(entry.Node, true, entry.State,
                    entry.HeartbeatMs)).OrderBy(entry => entry.NodeId, StringComparer.Ordinal)];
        }
    }

    private RemoteWorkerNode Local(string tenant) => new(configuration.NodeId, supervisor.MaxCpu,
        supervisor.MaxProcesses, supervisor.MaxMemoryBytes, [tenant], Capacity: supervisor.Status());

    private WorkerNodeStatus Describe(RemoteWorkerNode node, bool remote, string state, long heartbeat)
    {
        var current = usage.GetValueOrDefault((node.NodeId, remote));
        var capacity = node.Capacity;
        return new(node.NodeId, remote, state == "Online", heartbeat, node.MaxProcesses,
            Math.Max(current.Processes,(capacity?.Running ?? 0) + (capacity?.Draining ?? 0)),
            node.MaxCpu, Math.Max(current.Cpu,capacity?.UsedCpu ?? 0), node.MaxMemoryBytes,
            Math.Max(current.Memory,capacity?.UsedMemoryBytes ?? 0), state, capacity);
    }

    internal string Rejection(string tenant, bool remote, System.Text.Json.JsonElement? package,
        WorkerPlacement? journal, bool recovery)
    {
        if (!configuration.Cluster) return "WorkerCapacity";
        var eligible = directory.Nodes.Where(node => node.AllowedTenants.Contains(tenant,StringComparer.Ordinal));
        if (recovery && journal is { Remote: true } && !eligible.Any(node => node.NodeId == journal.NodeId))
            return "JournalOwnerUnavailable";
        if (!remote) return "WorkerIsolation";
        if (!eligible.Any()) return "NoOnlineWorker";
        if (package is { } document && !eligible.Any(node => Supports(node,tenant,document))) return "WorkerCapabilities";
        return "WorkerCapacity";
    }

    private static bool Supports(RemoteWorkerNode node,string tenant,System.Text.Json.JsonElement document) =>
        node.Inventory is not null && node.Inventory.TryGetValue(tenant,out var inventory) && inventory.Supports(document);

    internal WorkerPlacement[]? PlanHandoff(string tenant, System.Text.Json.JsonElement package,
        KafkaElasticGroup[] additions, WorkerPlacement[] retiring)
    {
        var nodes = directory.Nodes.Where(node => node.AllowedTenants.Contains(tenant,StringComparer.Ordinal) &&
            Supports(node,tenant,package)).ToArray();
        if (nodes.Length == 0) return null;

        var reserved = new WorkerPlacement[additions.Length];
        lock (gate)
        {
            var free = new Dictionary<string,(uint Processes,uint Cpu,ulong Memory)>(StringComparer.Ordinal);
            foreach (var node in nodes)
            {
                var old = retiring.Where(place => place.NodeId == node.NodeId && place.Remote).ToArray();
                uint restoredCpu = old.Aggregate(0U,(total,place) => total + place.Budget.CpuSlots);
                ulong restoredMemory = old.Aggregate(0UL,(total,place) => total + place.Budget.ProcessMemoryBytes);
                var used = usage.GetValueOrDefault((node.NodeId,true));
                var available = node.Capacity;
                free[node.NodeId] = (
                    Math.Min(node.MaxProcesses,Math.Min(node.MaxProcesses - Math.Min(node.MaxProcesses,used.Processes),
                        available?.AvailableProcesses ?? 0) + (uint)old.Length),
                    Math.Min(node.MaxCpu,Math.Min(node.MaxCpu - Math.Min(node.MaxCpu,used.Cpu),available?.AvailableCpu ?? 0) + restoredCpu),
                    Math.Min(node.MaxMemoryBytes,Math.Min(node.MaxMemoryBytes - Math.Min(node.MaxMemoryBytes,used.Memory),
                        available?.AvailableMemoryBytes ?? 0) + restoredMemory));
            }
            foreach (int index in Enumerable.Range(0,additions.Length).OrderByDescending(i => additions[i].Partitions.Length))
            {
                var budget = Budget(additions[index].Parallelism);
                var candidate = nodes.Where(node => free[node.NodeId] is var left && left.Processes > 0 &&
                    left.Cpu >= budget.CpuSlots && left.Memory >= budget.ProcessMemoryBytes)
                    .OrderByDescending(node => free[node.NodeId].Cpu).FirstOrDefault();
                if (candidate is null) return null;
                var remaining = free[candidate.NodeId];
                free[candidate.NodeId] = (remaining.Processes-1,remaining.Cpu-budget.CpuSlots,
                    remaining.Memory-budget.ProcessMemoryBytes);
                reserved[index] = new(candidate.NodeId,true,budget);
            }
        }
        return reserved;
    }

    private WorkerBudget Budget(WorkerParallelism? plan) => new(plan?.ThreadsPerWorker ?? 1, policy.MemoryLimit,
        checked(policy.MemoryLimit + WorkerSupervisor.ProcessOverhead),plan?.ConsumerLanesPerWorker ?? 1);

    internal WorkerReservation? TryReserve(string tenant, WorkerParallelism? plan, bool remote, int count,
        IReadOnlyList<WorkerPlacement?>? pinned = null, WorkerPlacement? excluded = null,
        System.Text.Json.JsonElement? package = null)
    {
        var budget = Budget(plan);
        (RemoteWorkerNode Node, bool Remote)[] nodes = [.. (configuration.Cluster
            ? Enumerable.Empty<(RemoteWorkerNode, bool)>() : [(Local(tenant), false)]), .. (remote
            ? directory.Nodes.Where(node => node.AllowedTenants.Contains(tenant, StringComparer.Ordinal) &&
                (package is null || Supports(node, tenant, package.Value))).Select(node => (node, true))
            : Enumerable.Empty<(RemoteWorkerNode, bool)>())];
        var placements = new WorkerPlacement[count];
        var acquired = new List<WorkerPlacement>(count);
        lock (gate)
        {
            var order = Enumerable.Range(0, count).OrderBy(index => pinned?[index] is null);
            foreach (int index in order)
            {
                var preferred = pinned?[index];
                var size = preferred?.Budget ?? excluded?.Budget ?? budget;
                if (size.NativeMemoryBytes > policy.MemoryLimit)
                {
                    Release(acquired);
                    return null;
                }
                foreach (var candidate in nodes.OrderBy(node => usage.GetValueOrDefault((node.Node.NodeId, node.Remote)).Processes))
                {
                    var node = candidate.Node;
                    if (excluded is not null && excluded.NodeId == node.NodeId && excluded.Remote == candidate.Remote ||
                        preferred is not null && (preferred.NodeId != node.NodeId || preferred.Remote != candidate.Remote))
                        continue;
                    var key = (node.NodeId, candidate.Remote);
                    var current = usage.GetValueOrDefault(key);
                    if (node.Capacity is { } inventory && (inventory.AvailableProcesses == 0 ||
                        size.CpuSlots > inventory.AvailableCpu || size.ProcessMemoryBytes > inventory.AvailableMemoryBytes))
                        continue;
                    if (current.Processes >= node.MaxProcesses || current.Cpu > node.MaxCpu ||
                        size.CpuSlots > node.MaxCpu - current.Cpu || current.Memory > node.MaxMemoryBytes ||
                        size.ProcessMemoryBytes > node.MaxMemoryBytes - current.Memory)
                        continue;
                    usage[key] = (current.Cpu + size.CpuSlots, current.Processes + 1, current.Memory + size.ProcessMemoryBytes);
                    placements[index] = new WorkerPlacement(node.NodeId, candidate.Remote, size);
                    acquired.Add(placements[index]);
                    break;
                }
                if (placements[index] is not null)
                    continue;
                Release(acquired);
                return null;
            }
        }
        return new WorkerReservation(placements, () => { lock (gate) Release(acquired); });
    }

    private void Release(List<WorkerPlacement> placements)
    {
        foreach (var item in placements)
        {
            var key = (item.NodeId, item.Remote);
            var current = usage[key];
            if (current.Processes == 1)
                usage.Remove(key);
            else
                usage[key] = (current.Cpu - item.Budget.CpuSlots, current.Processes - 1,
                    current.Memory - item.Budget.ProcessMemoryBytes);
        }
    }
}
