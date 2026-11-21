namespace UniversalData.Hosting;

internal sealed record WorkerNodeResourceRecord(WorkerProcessIdentity Owner, WorkerProcessIdentity? Worker,
    uint Cpu, ulong Memory);
