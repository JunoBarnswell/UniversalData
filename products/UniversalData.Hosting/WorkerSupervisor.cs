using System.Diagnostics;
using System.Text.Json;

namespace UniversalData.Hosting;

/// <summary>Owns physical process capacity and the inherited pipe from launch until observed exit.</summary>
public sealed class WorkerSupervisor : IAsyncDisposable
{
    internal const ulong ProcessOverhead = 268435456;
    private readonly Lock gate = new();
    private readonly List<Slot> slots = [];
    private readonly string path;
    private readonly string? settingsPath;
    private readonly WorkerNodeResources? nodeResources;
    private readonly bool enforceOrdinaryLimits;
    private readonly CancellationTokenSource lifetime = new();
    private readonly Task upkeep;
    private WorkerSupervisorSettings settings;
    private TaskCompletionSource changed = Signal();
    private int generation = 1;
    private int failures;
    private long retryAfter;
    private volatile string? configError;
    private string? loadedSettings;
    private bool closing;

    public WorkerSupervisor(string workerPath, uint maxProcesses, uint maxCpu, ulong maxMemoryBytes,
        ulong nativeMemoryBytes, string? configurationPath = null, bool enforceOrdinaryLimits = false,
        int initialPrewarm = 1, string? nodeDirectory = null, uint nodeMaxProcesses = 0, uint nodeMaxConnectionsPerSource = 8)
    {
        path = Path.GetFullPath(workerPath);
        settingsPath = configurationPath is null ? null : Path.GetFullPath(configurationPath);
        this.enforceOrdinaryLimits = enforceOrdinaryLimits;
        nodeResources = nodeDirectory is null ? null : new(nodeDirectory,
            nodeMaxProcesses == 0 ? maxProcesses : nodeMaxProcesses, nodeMaxConnectionsPerSource);
        MaxProcesses = maxProcesses;
        MaxCpu = maxCpu;
        MaxMemoryBytes = maxMemoryBytes;
        OrdinaryBudget = new(1, nativeMemoryBytes, checked(nativeMemoryBytes + ProcessOverhead), 1);
        settings = new(Prewarm: initialPrewarm);
        if (settingsPath is not null)
            settings = ReadSettings(out loadedSettings);
        settings.Validate(maxProcesses);
        upkeep = MaintainAsync();
    }

    public uint MaxProcesses { get; }
    public uint MaxCpu { get; }
    public ulong MaxMemoryBytes { get; }
    internal WorkerBudget OrdinaryBudget { get; }
    public bool Ready => File.Exists(path) && MaxProcesses > 0 && !lifetime.IsCancellationRequested;

    public WorkerCapacityStatus Status()
    {
        lock (gate)
        {
            var (processes, cpu, memory) = Usage();
            var idle = slots.Where(slot => !slot.Finished && !slot.Borrowed && !slot.Draining && slot.ProcessId != 0)
                .Select(slot => slot.ProcessId).ToArray();
            uint reusable = (uint)slots.Count(slot => !slot.Finished && !slot.Borrowed && !slot.Draining);
            var shared = nodeResources?.Available();
            uint freeProcesses = Math.Min(MaxProcesses - processes, shared?.Processes ?? uint.MaxValue);
            uint freeCpu = Math.Min(MaxCpu - cpu, shared?.Cpu ?? uint.MaxValue);
            ulong freeMemory = Math.Min(MaxMemoryBytes - memory, shared?.Memory ?? ulong.MaxValue);
            return new((uint)slots.Count(slot => !slot.Draining && slot.ProcessId == 0),
                idle,
                (uint)slots.Count(slot => !slot.Finished && slot.Borrowed && !slot.Draining && slot.ProcessId != 0),
                (uint)slots.Count(slot => !slot.Finished && slot.Draining), processes, cpu, memory,
                freeProcesses + reusable, freeCpu + reusable,
                freeMemory + reusable * OrdinaryBudget.ProcessMemoryBytes,
                configError ?? (closing ? "Draining" : Stopwatch.GetTimestamp() < retryAfter
                    ? "WorkerRestartBackoff" : null));
        }
    }

    internal async Task<Lease> RentAsync(WorkerRequest request, WorkerBudget charge, CancellationToken cancellation)
    {
        request = request with { SourceAdmission = nodeResources?.SourceAdmission };
        if (charge.CpuSlots == 0 || charge.ProcessMemoryBytes == 0 || charge.CpuSlots > MaxCpu ||
            charge.ProcessMemoryBytes > MaxMemoryBytes || request.Budget is { } governed && governed != charge)
            throw new UniversalDataException((int)StatusCode.Resource, "Worker request exceeds node capacity");
        Slot slot;
        while (true)
        {
            cancellation.ThrowIfCancellationRequested();
            Task next;
            lock (gate)
            {
                if (closing)
                    throw new UniversalDataException((int)StatusCode.Busy, "Worker supervisor is draining");

                bool ordinary = !request.Isolated && request.Mode != "compile" &&
                    request.Budget is null && charge == OrdinaryBudget;
                var warm = ordinary ? slots.Find(candidate => candidate.Generation == generation &&
                    !candidate.Borrowed && !candidate.Draining && !candidate.Finished) : null;
                var available = warm ?? (Stopwatch.GetTimestamp() >= retryAfter ? TryOpen(charge, request) : null);
                if (available is not null)
                {
                    slot = available;
                    slot.Borrowed = true;
                    Notify();
                    break;
                }

                if (!Fits(charge))
                    Retire(slots.Find(candidate => !candidate.Draining && !candidate.Finished &&
                        (!candidate.Borrowed || candidate.BeginIdleRetirement?.Invoke(TimeSpan.Zero) == true)));
                next = changed.Task;
            }
            await next.WaitAsync(cancellation);
        }
        try
        {
            var process = await slot.Ready.Task.WaitAsync(cancellation);
            await process.StandardInput.WriteLineAsync(JsonSerializer.Serialize(request, HostPolicy.JsonOptions).AsMemory(), cancellation);
            await process.StandardInput.FlushAsync(cancellation);
            return new Lease(this, slot, process);
        }
        catch
        {
            await Release(slot);
            throw;
        }
    }

    public Task<JsonElement> InspectAsync(WorkerRequest request, CancellationToken cancellation) =>
        WorkerLauncher.Inspect(this, request, cancellation);

    private WorkerSupervisorSettings ReadSettings(out string fingerprint)
    {
        HostFiles.NoLinks(settingsPath!);
        fingerprint = File.ReadAllText(settingsPath!);
        return JsonSerializer.Deserialize<WorkerSupervisorSettings>(fingerprint, HostPolicy.JsonOptions)
            ?? throw new InvalidDataException("Worker supervisor settings are empty");
    }

    private async Task MaintainAsync()
    {
        using var cadence = new PeriodicTimer(TimeSpan.FromMilliseconds(250));
        try
        {
            do
            {
                if (settingsPath is not null)
                {
                    try
                    {
                        var next = ReadSettings(out string fingerprint);
                        next.Validate(MaxProcesses);
                        lock (gate)
                        {
                            if (loadedSettings != fingerprint)
                            {
                                loadedSettings = fingerprint;
                                if (next != settings)
                                {
                                    settings = next;
                                    generation++;
                                }
                            }
                            configError = null;
                        }
                    }
                    catch (Exception error) when (error is IOException or UnauthorizedAccessException or JsonException or UniversalDataException)
                    { configError = "InvalidSupervisorConfiguration"; }
                }

                lock (gate)
                {
                    if (closing)
                        return;
                    long now = Stopwatch.GetTimestamp();
                    bool pressure = nodeResources?.HasWaitingRequest == true;
                    bool newReady = slots.Any(slot => slot.Generation == generation && slot.ProcessId != 0 && !slot.Draining);
                    int warm = 0;
                    foreach (var slot in slots)
                    {
                        if (slot.Draining)
                            continue;
                        if (slot.Borrowed)
                        {
                            var retention = pressure ? TimeSpan.Zero : TimeSpan.FromSeconds(settings.IdleSeconds);
                            if (slot.BeginIdleRetirement?.Invoke(retention) == true)
                                Retire(slot);
                            continue;
                        }
                        if (pressure)
                        {
                            Retire(slot);
                            continue;
                        }
                        if (slot.Generation != generation)
                        {
                            if (newReady || settings.Prewarm == 0 || !Fits(OrdinaryBudget))
                                Retire(slot);
                        }
                        else if (++warm > settings.Prewarm &&
                            Stopwatch.GetElapsedTime(slot.ReadyAt, now) >= TimeSpan.FromSeconds(settings.IdleSeconds))
                            Retire(slot);
                    }
                    if (!pressure && warm < settings.Prewarm && now >= retryAfter)
                        TryOpen(OrdinaryBudget, null);
                    Notify();
                }
            } while (await cadence.WaitForNextTickAsync(lifetime.Token));
        }
        catch (OperationCanceledException) when (lifetime.IsCancellationRequested) { }
    }

    private (uint Processes, uint Cpu, ulong Memory) Usage()
    {
        uint processes = 0, cpu = 0;
        ulong memory = 0;
        foreach (var slot in slots)
        {
            if (slot.Finished)
                continue;
            processes++;
            cpu += slot.Charge.CpuSlots;
            memory += slot.Charge.ProcessMemoryBytes;
        }
        return (processes, cpu, memory);
    }

    private bool Fits(WorkerBudget budget)
    {
        var (processes, cpu, memory) = Usage();
        return processes < MaxProcesses && budget.CpuSlots <= MaxCpu - cpu &&
            budget.ProcessMemoryBytes <= MaxMemoryBytes - memory;
    }

    private Slot? TryOpen(WorkerBudget charge, WorkerRequest? request)
    {
        if (!Fits(charge)) return null;
        var reservation = nodeResources?.TryReserve(charge, request is not null);
        if (nodeResources is not null && reservation is null) return null;
        var slot = new Slot(generation, charge, request?.Isolated == true, reservation);
        slots.Add(slot);
        slot.Observer = Task.Run(() => Observe(slot, request));
        return slot;
    }

    private async Task Observe(Slot slot, WorkerRequest? request)
    {
        Process? process = null;
        Task? stderr = null;
        try
        {
            using var startup = CancellationTokenSource.CreateLinkedTokenSource(slot.Retiring.Token);
            startup.CancelAfter(TimeSpan.FromSeconds(15));
            var budget = request?.Budget ?? (enforceOrdinaryLimits ? slot.Charge : null);
            process = WorkerLauncher.Launch(path, slot.Key, budget, request);
            slot.Process = process;
            stderr = WorkerProtocol.Discard(process.StandardError, CancellationToken.None);
            int pid = await WorkerLauncher.WaitReady(process, startup.Token);
            slot.Reservation?.Bind(pid);
            lock (gate)
            {
                slot.ProcessId = pid;
                slot.ReadyAt = Stopwatch.GetTimestamp();
                slot.Ready.TrySetResult(process);
                Notify();
            }
            await process.WaitForExitAsync(slot.Retiring.Token);
            lock (gate)
            {
                if (!slot.Borrowed && !slot.Draining)
                    Backoff();
                else if (process.ExitCode == 0)
                    failures = 0;
            }
        }
        catch (Exception error) when (error is not OutOfMemoryException)
        {
            slot.Ready.TrySetException(error);
            if (!slot.Retiring.IsCancellationRequested)
                lock (gate) Backoff();
            if (process is not null && !process.HasExited)
            {
                await WorkerLauncher.Terminate(process, slot.Key, slot.Isolated);
                await process.WaitForExitAsync();
            }
        }
        finally
        {
            try { if (stderr is not null) await stderr; }
            catch (IOException) { }
            try { slot.Reservation?.Dispose(); }
            finally
            {
                lock (gate)
                {
                    slot.Finished = true;
                    if (!slot.Borrowed)
                        Forget(slot);
                    Notify();
                }
            }
        }
    }

    private void Backoff()
    {
        double seconds = Math.Min(30, 1 << Math.Min(failures++, 5));
        retryAfter = Stopwatch.GetTimestamp() + (long)(seconds * Stopwatch.Frequency);
    }

    private void Retire(Slot? slot)
    {
        if (slot is null || slot.Retiring.IsCancellationRequested || slot.Finished)
            return;
        slot.Draining = true;
        slot.Retiring.Cancel();
    }

    private async ValueTask Release(Slot slot)
    {
        lock (gate)
            Retire(slot);
        try { await slot.Observer; }
        finally
        {
            lock (gate)
            {
                slot.Borrowed = false;
                if (slot.Finished)
                    Forget(slot);
                Notify();
            }
        }
    }

    private void Forget(Slot slot)
    {
        slots.Remove(slot);
        slot.Process?.Dispose();
        slot.Retiring.Dispose();
    }

    private static TaskCompletionSource Signal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
    private void Notify()
    {
        changed.TrySetResult();
        changed = Signal();
    }

    public async ValueTask DisposeAsync()
    {
        Task[] children;
        lock (gate)
        {
            if (closing)
                return;
            closing = true;
            lifetime.Cancel();
            foreach (var slot in slots)
                Retire(slot);
            children = slots.Select(slot => slot.Observer).ToArray();
            Notify();
        }
        try { await Task.WhenAll([upkeep, .. children]); }
        finally { lifetime.Dispose(); }
    }

    internal sealed class Slot(int generation, WorkerBudget charge, bool isolated, WorkerNodeResourceLease? reservation)
    {
        internal string Key { get; } = Guid.NewGuid().ToString("N");
        internal int Generation { get; } = generation;
        internal WorkerBudget Charge { get; } = charge;
        internal bool Isolated { get; } = isolated;
        internal WorkerNodeResourceLease? Reservation { get; } = reservation;
        internal CancellationTokenSource Retiring { get; } = new();
        internal TaskCompletionSource<Process> Ready { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal Task Observer { get; set; } = Task.CompletedTask;
        internal Process? Process { get; set; }
        internal int ProcessId { get; set; }
        internal long ReadyAt { get; set; }
        internal bool Borrowed { get; set; }
        internal Func<TimeSpan, bool>? BeginIdleRetirement { get; set; }
        internal bool Draining { get; set; }
        internal bool Finished { get; set; }
    }

    internal sealed class Lease(WorkerSupervisor owner, Slot slot, Process process) : IAsyncDisposable
    {
        private readonly SemaphoreSlim commands = new(1, 1);
        private Task? drain;
        internal Process Process { get; } = process;
        internal int ProcessId => slot.ProcessId;
        internal Task TerminateAsync() => WorkerLauncher.Terminate(Process, slot.Key, slot.Isolated);

        internal void OnIdleRetirement(Func<TimeSpan, bool> begin)
        {
            lock (owner.gate) slot.BeginIdleRetirement = begin;
        }
        internal async Task SendAsync(string command, CancellationToken cancellation)
        {
            await commands.WaitAsync(cancellation);
            try
            {
                await Process.StandardInput.WriteLineAsync(command.AsMemory(), cancellation);
                await Process.StandardInput.FlushAsync(cancellation);
            }
            finally { commands.Release(); }
        }

        internal Task DrainAsync(uint seconds, bool requestStop = false)
        {
            lock (owner.gate)
            {
                slot.Draining = true;
                return drain ??= AwaitDrain(seconds, requestStop);
            }
        }

        private async Task AwaitDrain(uint seconds, bool requestStop)
        {
            if (Process.HasExited) return;
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(seconds));
            try
            {
                if (requestStop) await SendAsync("stop", deadline.Token);
                await Process.WaitForExitAsync(deadline.Token);
            }
            catch (Exception error) when (error is IOException or OperationCanceledException or InvalidOperationException)
            {
                if (!Process.HasExited) await TerminateAsync();
            }
        }
        public async ValueTask DisposeAsync()
        {
            try
            {
                if (Volatile.Read(ref drain) is { } pending)
                    await pending;
            }
            finally { await owner.Release(slot); }
        }
    }
}
