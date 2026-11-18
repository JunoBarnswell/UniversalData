using UniversalData.Hosting;

namespace UniversalData.Management;

/// <summary>One physical capacity inventory shared by every tenant in this Management process.</summary>
public sealed class WorkerReservations(HostPolicy policy, ProductConfiguration configuration, WorkerNodeDirectory directory)
{
    private readonly object gate = new();
    private readonly Dictionary<(string Node, bool Remote), (uint Cpu, uint Processes, ulong Memory)> usage = new();
    private const ulong ClrAndProcessOverhead = 268435456;

    internal WorkerReservation? TryReserve(string tenant, WorkerParallelism? plan, bool remote, int count,
        IReadOnlyList<WorkerPlacement?>? pinned = null, WorkerPlacement? excluded = null)
    {
        var budget = new WorkerBudget(plan?.ThreadsPerWorker ?? 1, policy.MemoryLimit,
            checked(policy.MemoryLimit + ClrAndProcessOverhead), plan?.ConsumerLanesPerWorker ?? 1);
        ulong hostMemory = checked((ulong)Math.Max(0, GC.GetGCMemoryInfo().TotalAvailableMemoryBytes));
        var local = new RemoteWorkerNode(configuration.NodeId, Math.Min(policy.MaxConcurrency, (uint)Environment.ProcessorCount),
            policy.MaxConcurrency, hostMemory, [tenant]);
        (RemoteWorkerNode Node, bool Remote)[] nodes = [(local, false), .. (remote
            ? directory.Nodes.Where(node => node.AllowedTenants.Contains(tenant, StringComparer.Ordinal)).Select(node => (node, true))
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
