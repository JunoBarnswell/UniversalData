namespace UniversalData.Hosting;

internal readonly record struct WorkerNodeCapacityBudget(uint Processes, uint Cpu, ulong Memory);
