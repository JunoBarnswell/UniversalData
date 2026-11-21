using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.Pipes;
using System.Text.Json;

namespace UniversalData.Hosting;

/// <summary>Owns the SDK admission pipe and the physical Worker children behind it.</summary>
internal sealed class SdkSessionHost : IAsyncDisposable
{
    private readonly SdkHostConfiguration configuration;
    private readonly HostPolicy policy;
    private readonly string stateDirectory;
    private readonly string statePath;
    private readonly WorkerSupervisor supervisor;
    private readonly bool sharedSessions;
    private readonly SemaphoreSlim active;
    private readonly SemaphoreSlim waiting;
    private readonly SemaphoreSlim launches = new(1, 1);
    private readonly SemaphoreSlim updates = new(1, 1);
    private readonly ConcurrentDictionary<string, Child> children = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, Session> sessions = new(StringComparer.Ordinal);
    private Dictionary<string, SdkSourceRevision> revisions = new(StringComparer.Ordinal);
    private int disposing;

    private SdkSessionHost(string workerPath, SdkHostConfiguration configuration)
    {
        this.configuration = configuration;
        var options = configuration.Options;
        if (!HostIdentity.ValidId(configuration.Tenant) || !Path.IsPathFullyQualified(configuration.PolicyFile) ||
            options.MaxConcurrentSessions is < 1 or > 64 || options.MaxSessionsPerWorker is < 1 or > 64 ||
            options.MaxSessionsPerWorker > options.MaxConcurrentSessions ||
            options.MaxBranchesPerSession is < 1 or > 64 || options.QueueCapacity is < 1 or > 65536 ||
            options.MaxConnectionsPerSource is < 1 or > 64 || options.QueueTimeout <= TimeSpan.Zero ||
            options.QueueTimeout > TimeSpan.FromDays(7) || options.ExecutionTimeout <= TimeSpan.Zero ||
            options.ExecutionTimeout > TimeSpan.FromDays(7) || options.CancellationGrace <= TimeSpan.Zero ||
            options.CancellationGrace > TimeSpan.FromMinutes(5) || options.MemoryPerSession == 0)
            throw Error(StatusCode.Invalid, "Invalid SDK Worker host limits");

        var root = HostPolicy.Load(configuration.PolicyFile);
        maxJobParallelism = Math.Min(root.MaxConcurrency, (uint)Math.Min(Environment.ProcessorCount,
            Math.Min(options.MaxConcurrentSessions, options.MaxBranchesPerSession)));
        sharedSessions = options.MaxSessionsPerWorker > 1;
        var identity = HostIdentity.Operator(configuration.Tenant);
        var tenant = root.Tenants is null
            ? root with { DataDirectory = Path.Combine(root.DataDirectory, "tenants", identity.Tenant), Tenant = identity.Tenant, Identity = identity }
            : root.Bind(identity);
        if (options.MaxConcurrentSessions > root.MaxConcurrency || options.MemoryPerSession > root.MemoryLimit ||
            options.ExecutionTimeout > TimeSpan.FromSeconds(root.ExecutionTimeoutSeconds) ||
            options.CancellationGrace > TimeSpan.FromSeconds(root.CancellationGraceSeconds))
            throw Error(StatusCode.Resource, "SDK Worker budgets exceed the host policy");

        ulong sessionMemory = checked(options.MemoryPerSession + WorkerSupervisor.ProcessOverhead);
        ulong hostMemory = checked(sessionMemory * (uint)options.MaxConcurrentSessions);
        if (options.RequireIsolation && (options.MaxConcurrentSessions < 2 || options.MaxConnectionsPerSource < 2 ||
            Environment.ProcessorCount < options.MaxConcurrentSessions ||
            (ulong)GC.GetGCMemoryInfo().TotalAvailableMemoryBytes < hostMemory))
            throw Error(StatusCode.Resource, "The host cannot supply the requested independent Worker budgets");
        policy = tenant with { MaxConcurrency = 1, MemoryLimit = options.MemoryPerSession,
            MaxConnectionsPerSource = Math.Min(tenant.MaxConnectionsPerSource, (uint)options.MaxConnectionsPerSource),
            ExecutionTimeoutSeconds = checked((uint)Math.Ceiling(options.ExecutionTimeout.TotalSeconds)),
            CancellationGraceSeconds = checked((uint)Math.Ceiling(options.CancellationGrace.TotalSeconds)) };

        stateDirectory = Path.Combine(policy.DataDirectory, "sdk-hosts", Guid.NewGuid().ToString("N"));
        HostFiles.NoLinks(Path.GetDirectoryName(stateDirectory)!);
        Directory.CreateDirectory(stateDirectory);
        if (!OperatingSystem.IsWindows())
            File.SetUnixFileMode(stateDirectory, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        statePath = Path.Combine(stateDirectory, "sources.json");
        HostFiles.WritePrivate(statePath, "{}"u8);
        active = new(options.MaxConcurrentSessions, options.MaxConcurrentSessions);
        waiting = new(options.QueueCapacity, options.QueueCapacity);

        // Charge each shared child for every logical session it can host.  The node
        // reservation is shared by Management and all SDK hosts on this node.
        supervisor = new WorkerSupervisor(workerPath, (uint)options.MaxConcurrentSessions,
            (uint)options.MaxConcurrentSessions, hostMemory, options.MemoryPerSession,
            enforceOrdinaryLimits: options.RequireIsolation,
            initialPrewarm: sharedSessions ? 0 : options.RequireIsolation ? options.MaxConcurrentSessions : 1,
            nodeDirectory: root.DataDirectory, nodeMaxProcesses: root.MaxConcurrency,
            nodeMaxConnectionsPerSource: root.MaxConnectionsPerSource);
    }

    internal static async Task<int> RunAsync(string workerPath)
    {
        var input = Console.OpenStandardInput();
        var output = Console.OpenStandardOutput();
        Console.SetOut(TextWriter.Null);
        try
        {
            var frame = await WorkerProtocol.ReadAsync<SdkHostConfiguration>(input);
            using var batch = frame.Batch;
            if (batch is not null || frame.Message.Revision != ProductInfo.WorkerRevision)
                throw Error(StatusCode.Incompatible, "Incompatible SDK Worker host protocol");
            await using var host = new SdkSessionHost(workerPath, frame.Message);
            if (!host.supervisor.Ready) throw Error(StatusCode.DriverUnavailable, "SDK Worker deployment is missing");
            using var stopping = new CancellationTokenSource();
            if (frame.Message.Options.RequireIsolation && !host.sharedSessions)
                await host.CheckIsolationAsync(stopping.Token);
            _ = CancelAfterRead(input, stopping, stopping.Token);
            try { await host.AcceptAsync(output, stopping.Token); }
            finally { await stopping.CancelAsync(); }
            return 0;
        }
        catch (Exception error) when (Expected(error))
        {
            await TryReportAsync(output, Status(error));
            return (int)Status(error).Code;
        }
    }

    private async Task CheckIsolationAsync(CancellationToken stopping)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(stopping);
        deadline.CancelAfter(TimeSpan.FromSeconds(15));
        try
        {
            await Task.WhenAll(Enumerable.Range(0, configuration.Options.MaxConcurrentSessions).Select(_ => WarmAsync()));
        }
        catch (OperationCanceledException) when (!stopping.IsCancellationRequested)
        { throw Error(StatusCode.Resource, "The reserved SDK Worker processes did not report readiness"); }

        async Task WarmAsync()
        {
            var child = await Child.StartAsync(this, cooperative: false, reusable: true, cpuSlots: 1, deadline.Token);
            children[child.Capability] = child;
            child.AllowIdleRetirement();
        }
    }

    private async Task AcceptAsync(Stream output, CancellationToken stopping)
    {
        var work = new HashSet<Task>();
        NamedPipeServerStream? listener = SdkSessionProtocol.Listen(configuration.Endpoint,
            NamedPipeServerStream.MaxAllowedServerInstances);
        try
        {
            await WorkerProtocol.WriteAsync(output, Ok(), null, stopping);
            while (!stopping.IsCancellationRequested)
            {
                await listener.WaitForConnectionAsync(stopping);
                work.RemoveWhere(item => item.IsCompletedSuccessfully);
                var admitted = listener;
                listener = SdkSessionProtocol.Listen(configuration.Endpoint, NamedPipeServerStream.MaxAllowedServerInstances);
                work.Add(ServeAsync(admitted, stopping));
            }
        }
        catch (OperationCanceledException) when (stopping.IsCancellationRequested) { }
        finally
        {
            listener?.Dispose();
            await Task.WhenAll(work);
        }
    }

    private async Task ServeAsync(NamedPipeServerStream pipe, CancellationToken stopping)
    {
        await using var connection = pipe;
        try
        {
            using var handshake = CancellationTokenSource.CreateLinkedTokenSource(stopping);
            handshake.CancelAfter(TimeSpan.FromSeconds(5));
            var frame = await WorkerProtocol.ReadAsync<SdkPipeAdmission>(pipe, handshake.Token);
            using var batch = frame.Batch;
            if (batch is not null) throw Error(StatusCode.Protocol, "Invalid SDK host admission frame");
            var request = frame.Message;
            if (SdkSessionProtocol.Authenticate(request, configuration.Capability))
            {
                switch (request.Operation)
                {
                    case "open": await OpenAsync(pipe, request.Dedicated, request.CpuSlots, stopping); return;
                    case "source.update": await UpdateAsync(pipe, request, handshake.Token); return;
                    case "session.close": await CloseAsync(pipe, request, handshake.Token); return;
                }
            }
            throw Error(StatusCode.Auth, "SDK host capability is outside its operation scope");
        }
        catch (Exception error) when (Expected(error)) { await TryReportAsync(pipe, Status(error)); }
    }

    private readonly uint maxJobParallelism;

    private async Task OpenAsync(NamedPipeServerStream pipe, bool dedicated, uint cpuSlots, CancellationToken stopping)
    {
        uint allowed = dedicated ? maxJobParallelism : 1;
        if (cpuSlots == 0 || cpuSlots > allowed)
            throw Error(StatusCode.Resource, $"Job parallelism must be between 1 and {allowed}");
        using var queue = CancellationTokenSource.CreateLinkedTokenSource(stopping);
        queue.CancelAfter(configuration.Options.QueueTimeout);
        using var sent = new CancellationTokenSource();
        var abandoned = CancelAfterRead(pipe, queue, sent.Token);
        bool admitted = false;
        bool transferred = false;
        Session? session = null;
        try
        {
            if (!active.Wait(0))
            {
                if (!waiting.Wait(0)) throw Error(StatusCode.Busy, "SDK Worker queue is full");
                try { await active.WaitAsync(queue.Token); }
                finally { waiting.Release(); }
            }
            admitted = true;
            var child = await GetChildAsync(dedicated, cpuSlots, queue.Token);
            try
            {
                session = await child.OpenSessionAsync(queue.Token);
                if (!sessions.TryAdd(session.Capability, session))
                    throw Error(StatusCode.Resource, "SDK Worker session capability collision");
                transferred = true;
                session.StartObserver();
                await WorkerProtocol.WriteAsync(pipe, Ok(new(child.ProcessId, session.Endpoint,
                    session.Capability, session.SessionId, child.Cooperative)), null, queue.Token);
                await sent.CancelAsync();
                session = null; // Session now owns the active slot.
            }
            catch
            {
                if (session is not null) await session.AbortAsync();
                throw;
            }
        }
        catch (OperationCanceledException) when (!stopping.IsCancellationRequested)
        { throw Error(StatusCode.Timeout, "SDK Worker queue deadline elapsed"); }
        finally
        {
            await sent.CancelAsync();
            await abandoned;
            if (admitted && !transferred) active.Release();
        }
    }

    private async Task<Child> GetChildAsync(bool dedicated, uint cpuSlots, CancellationToken cancellation)
    {
        bool cooperative = !dedicated && sharedSessions;
        bool reusable = !dedicated;
        await launches.WaitAsync(cancellation);
        try
        {
            if (reusable)
            {
                foreach (var child in children.Values)
                    if (child.Reusable && child.Cooperative == cooperative && child.ReserveSlot()) return child;
            }
            var created = await Child.StartAsync(this, cooperative, reusable, cpuSlots, cancellation);
            children[created.Capability] = created;
            if (!created.ReserveSlot())
                throw Error(StatusCode.Resource, "SDK Worker child has no session capacity");
            created.AllowIdleRetirement();
            return created;
        }
        finally { launches.Release(); }
    }

    private async Task UpdateAsync(Stream pipe, SdkPipeAdmission request, CancellationToken cancellation)
    {
        if (request.SourceName is not { } name || !HostIdentity.ValidId(name) ||
            request.SourceRevision is not { Revision: > 0 } value)
            throw Error(StatusCode.Invalid, "Invalid SDK source revision");
        await updates.WaitAsync(cancellation);
        try
        {
            var current = Volatile.Read(ref revisions);
            if (current.TryGetValue(name, out var prior) && (value.Revision < prior.Revision ||
                value.Revision == prior.Revision && value != prior))
                throw Error(StatusCode.Fenced, "SDK source revision changed");
            if (prior != value)
            {
                var next = new Dictionary<string, SdkSourceRevision>(current, StringComparer.Ordinal) { [name] = value };
                HostFiles.WritePrivate(statePath, JsonSerializer.SerializeToUtf8Bytes(next, WorkerProtocol.Json), overwrite: true);
                Volatile.Write(ref revisions, next);
            }
        }
        finally { updates.Release(); }
        await WorkerProtocol.WriteAsync(pipe, Ok(), null, cancellation);
    }

    private async Task CloseAsync(Stream pipe, SdkPipeAdmission request, CancellationToken cancellation)
    {
        if (request.SourceName is not { } capability || !sessions.TryGetValue(capability, out var session) ||
            request.SessionId != session.SessionId)
            throw Error(StatusCode.Auth, "SDK session capability is invalid");
        await session.CloseAsync(request.Poison, cancellation);
        await WorkerProtocol.WriteAsync(pipe, Ok(), null, cancellation);
    }

    private async Task ReleaseSessionAsync(Session session)
    {
        bool owned = sessions.TryRemove(session.Capability, out _);
        try { await session.Child.SessionReleasedAsync(session); }
        finally { if (owned) active.Release(); }
    }

    private void RemoveChild(Child child)
    {
        if (children.TryGetValue(child.Capability, out var current) && ReferenceEquals(current, child))
            children.TryRemove(child.Capability, out _);
    }

    private sealed class Child : IAsyncDisposable
    {
        internal readonly SdkSessionHost Host;
        private readonly WorkerSupervisor.Lease lease;
        private readonly int capacity;
        private readonly ConcurrentDictionary<string, Session> sessions = new(StringComparer.Ordinal);
        private readonly object gate = new();
        private readonly CancellationTokenSource lifetime = new();
        private int active;
        private int poisoned;
        private int retiring;
        private int leaseDisposed;
        private long idleSince;

        private Child(SdkSessionHost host, WorkerSupervisor.Lease lease, SdkWorkerConfiguration configuration,
            int capacity, bool cooperative, bool reusable)
        {
            Host = host;
            this.lease = lease;
            this.capacity = capacity;
            Cooperative = cooperative;
            Reusable = reusable;
            Configuration = configuration;
            Token = lifetime.Token;
            idleSince = Stopwatch.GetTimestamp();
            ProcessTask = ObserveProcessAsync();
        }

        internal string Capability => Configuration.Capability;
        internal string Endpoint => Configuration.Endpoint;
        internal int ProcessId => lease.ProcessId;
        internal bool Cooperative { get; }
        internal bool Reusable { get; }
        internal SdkWorkerConfiguration Configuration { get; }
        internal CancellationToken Token { get; }
        internal Task ProcessTask { get; }

        internal static async Task<Child> StartAsync(SdkSessionHost host, bool cooperative, bool reusable,
            uint cpuSlots, CancellationToken cancellation)
        {
            int capacity = cooperative ? host.configuration.Options.MaxSessionsPerWorker : 1;
            uint slots = cooperative ? (uint)capacity : cpuSlots;
            ulong memory = checked(host.configuration.Options.MemoryPerSession * (ulong)capacity);
            var worker = new SdkWorkerConfiguration(host.configuration.Options,
                SdkSessionProtocol.CreateEndpoint(), SdkSessionProtocol.CreateCapability(),
                host.statePath, capacity, cooperative);
            var childPolicy = host.policy with { MaxConcurrency = slots, MemoryLimit = memory };
            var budget = new WorkerBudget(slots, memory, checked(memory + WorkerSupervisor.ProcessOverhead), slots);
            var request = new WorkerRequest(ProductInfo.Protocol, Guid.NewGuid().ToString("N"), "sdk.worker",
                childPolicy, childPolicy.Identity, Payload: JsonSerializer.SerializeToElement(worker, WorkerProtocol.Json),
                Budget: slots > 1 && host.configuration.Options.RequireIsolation ? budget : null);
            WorkerSupervisor.Lease? lease = null;
            try
            {
                lease = await host.supervisor.RentAsync(request, slots == 1 ? host.supervisor.OrdinaryBudget : budget,
                    cancellation);
                await VerifyReadyAsync(worker, cancellation);
                var child = new Child(host, lease, worker, capacity, cooperative, reusable);
                lease = null;
                return child;
            }
            finally
            {
                if (lease is not null) await lease.DisposeAsync();
            }
        }

        private static async Task VerifyReadyAsync(SdkWorkerConfiguration worker, CancellationToken cancellation)
        {
            await using var owner = SdkSessionProtocol.Connect(worker.Endpoint);
            await owner.ConnectAsync(cancellation);
            await WorkerProtocol.WriteAsync(owner, new SdkSessionAssignment(ProductInfo.WorkerRevision,
                worker.Capability, "", "", "", ReadyProbe: true), null, cancellation);
            var reply = await WorkerProtocol.ReadAsync<SdkHostStatus>(owner, cancellation);
            using (reply.Batch) SdkSessionHostClient.Check(reply.Message, reply.Batch is null);
        }

        internal bool ReserveSlot()
        {
            lock (gate)
            {
                if (retiring != 0 || poisoned != 0 || active >= capacity) return false;
                active++;
                return true;
            }
        }

        internal async Task<Session> OpenSessionAsync(CancellationToken cancellation)
        {
            string sessionId = Guid.NewGuid().ToString("N");
            string endpoint = SdkSessionProtocol.CreateEndpoint();
            string capability = SdkSessionProtocol.CreateCapability();
            var owner = SdkSessionProtocol.Connect(Endpoint);
            try
            {
                await owner.ConnectAsync(cancellation);
                await WorkerProtocol.WriteAsync(owner, new SdkSessionAssignment(ProductInfo.WorkerRevision,
                    Capability, sessionId, endpoint, capability), null, cancellation);
                var reply = await WorkerProtocol.ReadAsync<SdkHostStatus>(owner, cancellation);
                using (reply.Batch) SdkSessionHostClient.Check(reply.Message, reply.Batch is null);
                var session = new Session(this, owner, sessionId, endpoint, capability);
                sessions[capability] = session;
                return session;
            }
            catch
            {
                await owner.DisposeAsync();
                ReleaseReservation();
                MarkPoisoned();
                throw;
            }
        }

        internal async Task SessionReleasedAsync(Session session)
        {
            sessions.TryRemove(session.Capability, out _);
            ReleaseReservation();
            if (!Reusable || Volatile.Read(ref poisoned) != 0) await RetireIfIdleAsync();
        }

        internal void MarkPoisoned()
        {
            Interlocked.Exchange(ref poisoned, 1);
            _ = RetireIfIdleAsync();
        }

        internal async Task RetireIfIdleAsync()
        {
            bool retire;
            lock (gate) retire = active == 0 && (poisoned != 0 || !Reusable) &&
                Interlocked.Exchange(ref retiring, 1) == 0;
            if (!retire) return;
            Host.RemoveChild(this);
            try { await lease.TerminateAsync(); }
            catch (Exception error) when (error is IOException or InvalidOperationException) { }
        }

        internal void AllowIdleRetirement()
        {
            if (Reusable) lease.OnIdleRetirement(BeginIdleRetirement);
        }

        private bool BeginIdleRetirement(TimeSpan idleFor)
        {
            lock (gate)
                return active == 0 && idleSince != 0 && Stopwatch.GetElapsedTime(idleSince) >= idleFor &&
                    Interlocked.Exchange(ref retiring, 1) == 0;
        }

        private void ReleaseReservation()
        {
            lock (gate)
                if (active > 0 && --active == 0) idleSince = Stopwatch.GetTimestamp();
        }

        private async Task ObserveProcessAsync()
        {
            try { await lease.Process.WaitForExitAsync(); }
            catch (InvalidOperationException) { }
            finally
            {
                lifetime.Cancel();
                foreach (var session in sessions.Values) await session.ProcessExitedAsync();
                if (Interlocked.Exchange(ref leaseDisposed, 1) == 0)
                    await lease.DisposeAsync();
                Host.RemoveChild(this);
                lifetime.Dispose();
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref retiring, 1) != 0)
            {
                await ProcessTask;
                return;
            }
            Host.RemoveChild(this);
            foreach (var session in sessions.Values) await session.AbortAsync();
            try { await lease.TerminateAsync(); }
            catch (Exception error) when (error is IOException or InvalidOperationException) { }
            await ProcessTask;
            if (Interlocked.Exchange(ref leaseDisposed, 1) == 0) await lease.DisposeAsync();
        }
    }

    private sealed class Session
    {
        private readonly NamedPipeClientStream owner;
        private readonly TaskCompletionSource finished = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource released = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int releaseStarted;
        private int closed;

        internal Session(Child child, NamedPipeClientStream owner, string sessionId, string endpoint, string capability)
        {
            Child = child;
            this.owner = owner;
            SessionId = sessionId;
            Endpoint = endpoint;
            Capability = capability;
        }

        internal Child Child { get; }
        internal string SessionId { get; }
        internal string Endpoint { get; }
        internal string Capability { get; }
        internal void StartObserver() => _ = ObserveAsync();

        private async Task ObserveAsync()
        {
            try
            {
                var reply = await WorkerProtocol.ReadAsync<SdkHostStatus>(owner);
                using (reply.Batch)
                {
                    if (reply.Message.Revision != ProductInfo.WorkerRevision || reply.Batch is not null ||
                        reply.Message.Code != StatusCode.Ok)
                        throw new InvalidDataException("Invalid SDK Worker session completion");
                }
            }
            catch (Exception error) when (error is IOException or InvalidDataException or JsonException or
                InvalidOperationException or OperationCanceledException) { Child.MarkPoisoned(); }
            finally
            {
                finished.TrySetResult();
                await ReleaseAsync();
            }
        }

        internal async Task CloseAsync(bool poison, CancellationToken cancellation)
        {
            if (Interlocked.Exchange(ref closed, 1) == 0)
            {
                if (poison) Child.MarkPoisoned();
                // Normal disposal already closes the client's direct pipe.  Keep the
                // assignment pipe open until the Worker reports completion so its
                // per-process session slot cannot be reused prematurely.
                if (poison) await owner.DisposeAsync();
            }
            if (!poison)
            {
                try
                {
                    await finished.Task.WaitAsync(cancellation);
                    await ReleaseAsync();
                }
                catch (OperationCanceledException)
                {
                    Child.MarkPoisoned();
                    await owner.DisposeAsync();
                }
            }
            else await ReleaseAsync();
        }

        internal async Task AbortAsync()
        {
            Child.MarkPoisoned();
            if (Interlocked.Exchange(ref closed, 1) == 0) await owner.DisposeAsync();
            await ReleaseAsync();
        }

        internal Task ProcessExitedAsync() => ReleaseAsync();

        private async Task ReleaseAsync()
        {
            if (Interlocked.Exchange(ref releaseStarted, 1) == 0)
            {
                try { await Child.Host.ReleaseSessionAsync(this); }
                finally { released.TrySetResult(); }
            }
            else await released.Task;
        }
    }

    private static async Task CancelAfterRead(Stream stream, CancellationTokenSource cancellation, CancellationToken watching)
    {
        try
        {
            await stream.ReadExactlyAsync(new byte[1], watching);
            if (!watching.IsCancellationRequested) await cancellation.CancelAsync();
        }
        catch (IOException) { if (!watching.IsCancellationRequested) await cancellation.CancelAsync(); }
        catch (OperationCanceledException) when (watching.IsCancellationRequested) { }
    }

    private static bool Expected(Exception error) => error is UniversalDataException or IOException or JsonException or
        InvalidOperationException or ArgumentException or UnauthorizedAccessException or OperationCanceledException or
        System.ComponentModel.Win32Exception or OverflowException or PlatformNotSupportedException or InvalidDataException;

    private static UniversalDataException Error(StatusCode code, string message) => SdkSessionHostClient.Failure(code, message);
    private static SdkHostStatus Ok(SdkWorkerGrant? grant = null) => new(ProductInfo.WorkerRevision, StatusCode.Ok, Grant: grant);

    private static SdkHostStatus Status(Exception error)
    {
        var code = error switch
        {
            UniversalDataException native => native.Code,
            UnauthorizedAccessException => StatusCode.Auth,
            OperationCanceledException => StatusCode.Cancelled,
            PlatformNotSupportedException => StatusCode.Unsupported,
            ArgumentException or OverflowException => StatusCode.Invalid,
            InvalidDataException or JsonException => StatusCode.Protocol,
            _ => StatusCode.WorkerLost
        };
        var detail = (error as UniversalDataException)?.Detail ?? new(ErrorSource.Worker, code,
            "SDK Worker host request failed");
        return new(ProductInfo.WorkerRevision, code, detail);
    }

    private static async Task TryReportAsync(Stream stream, SdkHostStatus status)
    {
        try
        {
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(1));
            await WorkerProtocol.WriteAsync(stream, status, null, deadline.Token);
        }
        catch (Exception error) when (Expected(error)) { }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref disposing, 1) != 0) return;
        foreach (var child in children.Values.ToArray()) await child.DisposeAsync();
        await supervisor.DisposeAsync();
        File.Delete(statePath);
        Directory.Delete(stateDirectory);
        launches.Dispose();
        updates.Dispose();
        waiting.Dispose();
        active.Dispose();
    }
}
