using System.Text.Json;

namespace UniversalData.Hosting;

/// <summary>Physical reservations shared by the supervisors using one node data directory.</summary>
internal sealed class WorkerNodeResources
{
    private readonly string directory;
    private readonly string gate;
    private readonly string demand;
    private readonly WorkerNodeCapacityBudget capacity;
    private readonly WorkerProcessIdentity owner;

    internal WorkerNodeResources(string nodeDirectory, uint maxProcesses, uint maxConnectionsPerSource)
    {
        directory = Path.Combine(Path.GetFullPath(nodeDirectory), "worker-reservations");
        gate = Path.Combine(directory, "admission.lock");
        demand = Path.Combine(directory, "admission-demand");
        HostFiles.NoLinks(directory);
        if (OperatingSystem.IsWindows()) Directory.CreateDirectory(directory);
        else Directory.CreateDirectory(directory, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        SourceAdmission = WorkerSourceAdmission.Open(directory, maxConnectionsPerSource);
        owner = WorkerProcessIdentity.Read(Environment.ProcessId)
            ?? throw new InvalidOperationException("Current process identity is unavailable");
        capacity = new(maxProcesses, Math.Min(maxProcesses, (uint)Environment.ProcessorCount),
            (ulong)Math.Max(0, GC.GetGCMemoryInfo().TotalAvailableMemoryBytes));
    }

    internal WorkerSourceAdmission SourceAdmission { get; }

    internal bool HasWaitingRequest
    {
        get
        {
            var age = DateTime.UtcNow - File.GetLastWriteTimeUtc(demand);
            return age >= TimeSpan.Zero && age <= TimeSpan.FromSeconds(1);
        }
    }

    internal WorkerNodeCapacityBudget Available()
    {
        using var admission = TryLock();
        return admission is null ? default : ReadAvailable();
    }

    internal WorkerNodeResourceLease? TryReserve(WorkerBudget budget, bool requested)
    {
        using var admission = TryLock();
        if (admission is null) return null;
        var free = ReadAvailable();
        if (free.Processes == 0 || free.Cpu < budget.CpuSlots || free.Memory < budget.ProcessMemoryBytes)
        {
            if (requested && !HasWaitingRequest)
                HostFiles.WritePrivate(demand, ReadOnlySpan<byte>.Empty, overwrite: true);
            return null;
        }
        string path = Path.Combine(directory, Guid.NewGuid().ToString("N") + ".json");
        var record = new WorkerNodeResourceRecord(owner, null, budget.CpuSlots, budget.ProcessMemoryBytes);
        HostFiles.WritePrivate(path, JsonSerializer.SerializeToUtf8Bytes(record));
        return new WorkerNodeResourceLease(path, record);
    }

    private FileStream? TryLock()
    {
        HostFiles.NoLinks(gate);
        var options = new FileStreamOptions { Mode = FileMode.OpenOrCreate, Access = FileAccess.ReadWrite, Share = FileShare.None };
        if (!OperatingSystem.IsWindows()) options.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
        try { return new FileStream(gate, options); }
        catch (IOException) { return null; }
    }

    private WorkerNodeCapacityBudget ReadAvailable()
    {
        var free = capacity;
        foreach (string path in Directory.EnumerateFiles(directory, "*.json"))
        {
            HostFiles.NoLinks(path);
            WorkerNodeResourceRecord record;
            try
            {
                using var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
                record = JsonSerializer.Deserialize<WorkerNodeResourceRecord>(file)
                    ?? throw new InvalidDataException("Invalid Worker node reservation");
            }
            catch (FileNotFoundException) { continue; }
            if (!(record.Worker ?? record.Owner).IsAlive())
            {
                File.Delete(path);
                continue;
            }
            free = new(free.Processes == 0 ? 0 : free.Processes - 1,
                free.Cpu - Math.Min(free.Cpu, record.Cpu), free.Memory - Math.Min(free.Memory, record.Memory));
        }
        return free;
    }

}
