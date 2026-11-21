using UniversalData.Hosting;
using UniversalData;

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
        if (additions.Length == 0) return [];
        var nodes = directory.Nodes.Where(node => node.AllowedTenants.Contains(tenant,StringComparer.Ordinal) &&
            Supports(node,tenant,package)).ToArray();
        if (nodes.Length == 0) return null;
        lock (gate)
        {
            var snapshots = nodes.Select(node => PlacementNode(node, true, true, retiring)).ToArray();
            var budgets = additions.Select(addition => PlacementBudget(Budget(addition.Parallelism))).ToArray();
            var planned = NativeWorkerPlacement.Plan(budgets[0], policy.MemoryLimit, snapshots, additions.Length,
                budgets: budgets);
            if (planned is null) return null;
            return planned.Select(value => new WorkerPlacement(value.NodeId, value.Remote,
                new(value.Budget.CpuSlots, value.Budget.NativeMemoryBytes, value.Budget.ProcessMemoryBytes,
                    value.Budget.ConsumerLanes))).ToArray();
        }
    }

    private WorkerBudget Budget(WorkerParallelism? plan) => new(plan?.ThreadsPerWorker ?? 1, policy.MemoryLimit,
        checked(policy.MemoryLimit + WorkerSupervisor.ProcessOverhead),plan?.ConsumerLanesPerWorker ?? 1);

    private NativePlacementNode PlacementNode(RemoteWorkerNode node, bool remote, bool eligible,
        IReadOnlyList<WorkerPlacement>? releasing = null)
    {
        var released = releasing?.Where(place => place.Remote && place.NodeId == node.NodeId).ToArray() ?? [];
        uint releasedCpu = released.Aggregate(0U, (total, place) => checked(total + place.Budget.CpuSlots));
        ulong releasedMemory = released.Aggregate(0UL, (total, place) => checked(total + place.Budget.ProcessMemoryBytes));
        var current = usage.GetValueOrDefault((node.NodeId, remote));
        uint usedCpu = current.Cpu > releasedCpu ? current.Cpu - releasedCpu : 0;
        uint usedProcesses = current.Processes > (uint)released.Length ? current.Processes - (uint)released.Length : 0;
        ulong usedMemory = current.Memory > releasedMemory ? current.Memory - releasedMemory : 0;
        usedCpu = Math.Min(usedCpu, node.MaxCpu);
        usedProcesses = Math.Min(usedProcesses, node.MaxProcesses);
        usedMemory = Math.Min(usedMemory, node.MaxMemoryBytes);
        var capacity = node.Capacity;
        uint availableProcesses = Math.Min(node.MaxProcesses,
            capacity?.AvailableProcesses ?? (node.MaxProcesses - usedProcesses));
        uint availableCpu = Math.Min(node.MaxCpu, capacity?.AvailableCpu ?? (node.MaxCpu - usedCpu));
        ulong availableMemory = Math.Min(node.MaxMemoryBytes,
            capacity?.AvailableMemoryBytes ?? (node.MaxMemoryBytes - usedMemory));
        if (released.Length != 0)
        {
            availableProcesses = Math.Min(node.MaxProcesses, checked(availableProcesses + (uint)released.Length));
            availableCpu = Math.Min(node.MaxCpu, checked(availableCpu + releasedCpu));
            availableMemory = Math.Min(node.MaxMemoryBytes, checked(availableMemory + releasedMemory));
        }
        return new(node.NodeId, remote, remote || supervisor.Ready, eligible, true, node.MaxCpu, node.MaxProcesses,
            usedCpu, usedProcesses, availableCpu, availableProcesses, node.MaxMemoryBytes,
            usedMemory, availableMemory);
    }

    private static NativePlacementBudget PlacementBudget(WorkerBudget budget) =>
        new(budget.CpuSlots, budget.ConsumerLanes, budget.NativeMemoryBytes, budget.ProcessMemoryBytes);

    internal WorkerReservation? TryReserve(string tenant, WorkerParallelism? plan, bool remote, int count,
        IReadOnlyList<WorkerPlacement?>? pinned = null, WorkerPlacement? excluded = null,
        System.Text.Json.JsonElement? package = null)
    {
        if (count == 0) return new WorkerReservation([], static () => { });
        if (count < 0) throw new ArgumentOutOfRangeException(nameof(count));
        var budget = Budget(plan);
        (RemoteWorkerNode Node, bool Remote)[] nodes = [.. (configuration.Cluster
            ? Enumerable.Empty<(RemoteWorkerNode, bool)>() : [(Local(tenant), false)]), .. (remote
            ? directory.Nodes.Where(node => node.AllowedTenants.Contains(tenant, StringComparer.Ordinal) &&
                (package is null || Supports(node, tenant, package.Value))).Select(node => (node, true))
            : Enumerable.Empty<(RemoteWorkerNode, bool)>())];
        if (nodes.Length == 0) return null;
        lock (gate)
        {
            var snapshots = nodes.Select(candidate => PlacementNode(candidate.Node, candidate.Remote, true)).ToArray();
            var nativePins = pinned?.Select(pin => pin is null ? (NativePlacementPin?)null : new NativePlacementPin(pin.NodeId, pin.Remote,
                PlacementBudget(pin.Budget))).ToArray();
            var nativeExcluded = excluded is { } value
                ? new NativePlacementPin(value.NodeId, value.Remote, PlacementBudget(value.Budget)) : (NativePlacementPin?)null;
            var planned = NativeWorkerPlacement.Plan(PlacementBudget(budget), policy.MemoryLimit, snapshots, count,
                nativePins, nativeExcluded);
            if (planned is null) return null;
            var placements = planned.Select(value => new WorkerPlacement(value.NodeId, value.Remote,
                new(value.Budget.CpuSlots, value.Budget.NativeMemoryBytes, value.Budget.ProcessMemoryBytes,
                    value.Budget.ConsumerLanes))).ToArray();
            foreach (var placement in placements)
            {
                var key = (placement.NodeId, placement.Remote);
                var current = usage.GetValueOrDefault(key);
                usage[key] = (checked(current.Cpu + placement.Budget.CpuSlots),
                    checked(current.Processes + 1), checked(current.Memory + placement.Budget.ProcessMemoryBytes));
            }
            return new WorkerReservation(placements, () => { lock (gate) Release(placements.ToList()); });
        }
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
