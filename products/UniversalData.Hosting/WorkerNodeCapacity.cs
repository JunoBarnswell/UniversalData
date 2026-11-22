namespace UniversalData.Hosting;

public sealed record WorkerNodeCapacity(uint MaxCpu, uint MaxProcesses, ulong MaxMemoryBytes, string[] Tenants,
    Dictionary<string, WorkerNodeInventory> Inventory)
{
    public bool Valid => MaxCpu > 0 && MaxProcesses is > 0 and <= 64 && MaxMemoryBytes > 0 &&
        Tenants is { Length: > 0 and <= 128 } && Tenants.All(HostIdentity.ValidId) &&
        Tenants.Distinct(StringComparer.Ordinal).Count() == Tenants.Length && Inventory.Count == Tenants.Length &&
        Tenants.All(tenant => Inventory.TryGetValue(tenant, out var inventory) && inventory.Valid);
}

public sealed record WorkerNodeExecution(string MemberId, ulong Generation, string Lease, int ProcessId,
    bool Started, ulong Produced, bool CanAttach);

public sealed record WorkerNodeHeartbeat(WorkerCapacityStatus Capacity, WorkerNodeExecution[] Executions);
