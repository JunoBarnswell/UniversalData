namespace UniversalData.Hosting;

/// <summary>Immutable execution reservation carried on the private Worker request.</summary>
public sealed record WorkerBudget(uint CpuSlots, ulong NativeMemoryBytes, ulong ProcessMemoryBytes, uint ConsumerLanes);
