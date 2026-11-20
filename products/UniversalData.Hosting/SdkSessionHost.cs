using System.Collections.Concurrent;
using System.IO.Pipes;
using System.Text.Json;

namespace UniversalData.Hosting;

internal sealed class SdkSessionHost : IAsyncDisposable
{
    private readonly SdkHostConfiguration configuration;
    private readonly HostPolicy policy;
    private readonly string stateDirectory;
    private readonly string statePath;
    private readonly WorkerSupervisor supervisor;
    private readonly SemaphoreSlim active;
    private readonly SemaphoreSlim waiting;
    private readonly SemaphoreSlim updates = new(1, 1);
    private readonly ConcurrentDictionary<string, Child> children = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, SemaphoreSlim> sources = new(StringComparer.Ordinal);
    private Dictionary<string, SdkSourceRevision> revisions = new(StringComparer.Ordinal);

    private SdkSessionHost(string workerPath, SdkHostConfiguration configuration)
    {
        this.configuration = configuration;
        var options = configuration.Options;
        if (!HostIdentity.ValidId(configuration.Tenant) || !Path.IsPathFullyQualified(configuration.PolicyFile) ||
            options.MaxConcurrentSessions is < 1 or > 64 || options.MaxBranchesPerSession is < 1 or > 64 ||
            options.QueueCapacity is < 1 or > 65536 || options.MaxConnectionsPerSource is < 1 or > 64 ||
            options.QueueTimeout <= TimeSpan.Zero || options.QueueTimeout > TimeSpan.FromDays(7) ||
            options.ExecutionTimeout <= TimeSpan.Zero || options.ExecutionTimeout > TimeSpan.FromDays(7) ||
            options.CancellationGrace <= TimeSpan.Zero || options.CancellationGrace > TimeSpan.FromMinutes(5) ||
            options.MemoryPerSession == 0)
            throw Error(StatusCode.Invalid, "Invalid SDK Worker host limits");

        var root = HostPolicy.Load(configuration.PolicyFile);
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
        supervisor = new WorkerSupervisor(workerPath, (uint)options.MaxConcurrentSessions, (uint)options.MaxConcurrentSessions,
            hostMemory, options.MemoryPerSession, enforceOrdinaryLimits: options.RequireIsolation,
            initialPrewarm: options.RequireIsolation ? 2 : 1);
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
            if (frame.Message.Options.RequireIsolation)
                await host.CheckIsolationAsync(stopping.Token);
            _ = CancelAfterRead(input, stopping, stopping.Token);
            try
            {
                await host.AcceptAsync(output, stopping.Token);
            }
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
            using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(50));
            do
            {
                if (supervisor.Status().PrewarmedPids.Length >= 2) return;
            } while (await timer.WaitForNextTickAsync(deadline.Token));
        }
        catch (OperationCanceledException) when (!stopping.IsCancellationRequested)
        {
            throw Error(StatusCode.Resource, "Two independently governed SDK Worker processes did not report readiness");
        }
    }

    private async Task AcceptAsync(Stream output, CancellationToken stopping)
    {
        var work = new HashSet<Task>();
        NamedPipeServerStream? listener = SdkSessionProtocol.Listen(configuration.Endpoint, NamedPipeServerStream.MaxAllowedServerInstances);
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
                    case "open": await OpenAsync(pipe, stopping); return;
                    case "source.update": await UpdateAsync(pipe, request, handshake.Token); return;
                    case "session.abort":
                        if (request.SourceName is { } token && children.TryGetValue(token, out var owner))
                        {
                            handshake.CancelAfter(TimeSpan.FromSeconds(10));
                            await owner.StopAsync(handshake.Token);
                        }
                        await WorkerProtocol.WriteAsync(pipe, Ok(), null, handshake.Token);
                        return;
                }
            }
            if (request is { Operation: "source.reserve", Revision: ProductInfo.WorkerRevision } &&
                request.Capability is { Length: 64 } && children.TryGetValue(request.Capability, out var child))
            {
                await ReserveAsync(pipe, request.SourceName, child, stopping);
                return;
            }
            throw Error(StatusCode.Auth, "SDK host capability is outside its operation scope");
        }
        catch (Exception error) when (Expected(error)) { await TryReportAsync(pipe, Status(error)); }
    }

    private async Task OpenAsync(NamedPipeServerStream pipe, CancellationToken stopping)
    {
        using var queue = CancellationTokenSource.CreateLinkedTokenSource(stopping);
        queue.CancelAfter(configuration.Options.QueueTimeout);
        using var sent = new CancellationTokenSource();
        var abandoned = CancelAfterRead(pipe, queue, sent.Token);
        bool admitted = false;
        try
        {
            if (!active.Wait(0))
            {
                if (!waiting.Wait(0)) throw Error(StatusCode.Busy, "SDK Worker queue is full");
                try { await active.WaitAsync(queue.Token); }
                finally { waiting.Release(); }
            }
            admitted = true;
            string token = SdkSessionProtocol.CreateCapability();
            var request = new SdkSessionConfiguration(configuration.Options, SdkSessionProtocol.CreateEndpoint(), token,
                configuration.Endpoint, statePath);
            var start = new WorkerRequest(ProductInfo.Protocol, Guid.NewGuid().ToString("N"), "sdk.session", policy, policy.Identity,
                Payload: JsonSerializer.SerializeToElement(request, WorkerProtocol.Json));
            using var child = new Child(stopping, request.MaxConnectionsPerSource);
            var lease = await supervisor.RentAsync(start, supervisor.OrdinaryBudget, queue.Token);
            bool registered = children.TryAdd(token, child);
            try
            {
                if (!registered) throw Error(StatusCode.Resource, "SDK Worker capability collision");
                await WorkerProtocol.WriteAsync(pipe, Ok(new(lease.ProcessId, request.Endpoint, token)), null, queue.Token);
                await sent.CancelAsync();
                await abandoned;
                await pipe.DisposeAsync();
                try { await lease.Process.WaitForExitAsync(child.Token); }
                catch (OperationCanceledException) when (stopping.IsCancellationRequested)
                { await lease.DrainAsync(policy.CancellationGraceSeconds, requestStop: true); }
                catch (OperationCanceledException) when (child.IsCancellationRequested)
                {
                    await lease.TerminateAsync();
                    await lease.Process.WaitForExitAsync();
                }
            }
            finally
            {
                try { await lease.DisposeAsync(); }
                finally
                {
                    if (registered) children.TryRemove(token, out _);
                    await child.CancelAsync();
                    child.Exited.TrySetResult();
                }
            }
        }
        catch (OperationCanceledException) when (!stopping.IsCancellationRequested)
        { throw Error(StatusCode.Timeout, "SDK Worker queue deadline elapsed"); }
        finally
        {
            await sent.CancelAsync();
            await abandoned;
            if (admitted) active.Release();
        }
    }

    private async Task UpdateAsync(Stream pipe, SdkPipeAdmission request, CancellationToken cancellation)
    {
        if (request.SourceName is not { } name || !HostIdentity.ValidId(name) || request.SourceRevision is not { Revision: > 0 } value)
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

    private async Task ReserveAsync(Stream pipe, string? name, Child child, CancellationToken stopping)
    {
        if (name is null || !Enabled(name))
            throw Error(StatusCode.Auth, "SDK source is disabled or unavailable");
        var slots = sources.GetOrAdd(name, _ => new SemaphoreSlim(configuration.Options.MaxConnectionsPerSource,
            configuration.Options.MaxConnectionsPerSource));
        var local = child.SourceSeats(name);
        using var scope = CancellationTokenSource.CreateLinkedTokenSource(child.Token, stopping);
        var disconnected = CancelAfterRead(pipe, scope, scope.Token);
        try
        {
            await local.WaitAsync(scope.Token);
            try
            {
                await slots.WaitAsync(scope.Token);
                try
                {
                    if (!Enabled(name)) throw Error(StatusCode.Auth, "SDK source was disabled while awaiting a connection");
                    await WorkerProtocol.WriteAsync(pipe, Ok(), null, scope.Token);
                    await disconnected;
                }
                finally { slots.Release(); }
            }
            finally { local.Release(); }
        }
        finally
        {
            await scope.CancelAsync();
            await disconnected;
        }
    }

    private bool Enabled(string name) => Volatile.Read(ref revisions).TryGetValue(name, out var value) && value.Enabled;

    private sealed class Child : IDisposable
    {
        private readonly CancellationTokenSource cancellation;
        private readonly int capacity;
        private readonly ConcurrentDictionary<string, SemaphoreSlim> sources = new(StringComparer.Ordinal);

        internal Child(CancellationToken stopping, int capacity)
        {
            cancellation = CancellationTokenSource.CreateLinkedTokenSource(stopping);
            Token = cancellation.Token;
            this.capacity = capacity;
        }

        internal CancellationToken Token { get; }
        internal bool IsCancellationRequested => Token.IsCancellationRequested;
        internal TaskCompletionSource Exited { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public void Dispose() => cancellation.Dispose();
        internal Task CancelAsync() => cancellation.CancelAsync();
        internal SemaphoreSlim SourceSeats(string name) => sources.GetOrAdd(name, _ => new(capacity, capacity));

        internal async Task StopAsync(CancellationToken deadline)
        {
            try { await CancelAsync(); }
            catch (ObjectDisposedException) { }
            await Exited.Task.WaitAsync(deadline);
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
        System.ComponentModel.Win32Exception or OverflowException or PlatformNotSupportedException;

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
        var detail = (error as UniversalDataException)?.Detail ?? new(ErrorSource.Worker, code, "SDK Worker host request failed");
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
        await supervisor.DisposeAsync();
        File.Delete(statePath);
        Directory.Delete(stateDirectory);
        foreach (var slots in sources.Values) slots.Dispose();
        updates.Dispose();
        waiting.Dispose();
        active.Dispose();
    }
}
