using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.Security;
using System.Net.WebSockets;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using UniversalData.Hosting;

namespace UniversalData.Worker;

internal sealed class WorkerNodeAgent(
    WorkerNodeAgentConfiguration configuration, HostPolicy policy, WorkerNodeCapacity capacity,
    X509Certificate2 certificate, WorkerSupervisor supervisor)
{

    private readonly HostPolicy nodePolicy = policy;
    private readonly WorkerSupervisor processSupervisor = supervisor;
    private readonly string boot = Guid.NewGuid().ToString("N");
    private readonly ConcurrentDictionary<string, AgentExecution> active = new(StringComparer.Ordinal);
    private ulong lastTerm;

    internal static async Task<int> RunAsync(string configPath)
    {
        using var shutdown = new CancellationTokenSource();
        ConsoleCancelEventHandler stop = (_, eventArgs) => { eventArgs.Cancel = true; shutdown.Cancel(); };
        Console.CancelKeyPress += stop;
        using var terminate = OperatingSystem.IsWindows() ? null : PosixSignalRegistration.Create(PosixSignal.SIGTERM, context =>
        {
            context.Cancel = true;
            shutdown.Cancel();
        });
        try
        {
            configPath = Path.GetFullPath(configPath);
            HostFiles.NoLinks(configPath);
            var config = JsonSerializer.Deserialize<WorkerNodeAgentConfiguration>(
                await File.ReadAllTextAsync(configPath, shutdown.Token), HostPolicy.JsonOptions)
                ?? throw new InvalidDataException("Missing Worker node agent configuration");
            if (!HostIdentity.ValidId(config.NodeId) || config.ManagementEndpoints is not { Length: > 0 and <= 16 } ||
                config.ManagementEndpoints.Any(endpoint => !endpoint.Url.IsAbsoluteUri || endpoint.Url.Scheme != "wss" ||
                    endpoint.Url.UserInfo.Length != 0 || endpoint.Url.AbsolutePath != "/" ||
                    endpoint.Url.Query.Length != 0 || endpoint.Url.Fragment.Length != 0 ||
                    !WorkerNodeCertificate.ValidPin(endpoint.CertificateSha256)) ||
                config.ManagementEndpoints.Select(endpoint => endpoint.Url).Distinct().Count() != config.ManagementEndpoints.Length)
                throw new InvalidDataException("Invalid Worker node agent endpoints");
            var policy = HostPolicy.Load(Path.GetFullPath(config.PolicyPath));
            if (policy.Tenants is null || config.Tenants.Any(tenant => !policy.Tenants.ContainsKey(tenant)))
                throw new InvalidDataException("Worker node agent tenant is outside its policy");
            var inventory = config.Tenants.ToDictionary(tenant => tenant, tenant => Inventory(policy.Bind(HostIdentity.Operator(tenant))), StringComparer.Ordinal);
            var capacity = new WorkerNodeCapacity(config.MaxCpu, config.MaxProcesses, config.MaxMemoryBytes, config.Tenants, inventory);
            if (!capacity.Valid || capacity.MaxCpu > (uint)Environment.ProcessorCount || capacity.MaxProcesses > policy.MaxConcurrency)
                throw new InvalidDataException("Invalid Worker node agent capacity");
            var certPath = Path.GetFullPath(config.CertificatePath);
            HostFiles.NoLinks(certPath);
            string? password = null;
            if (config.CertificatePasswordFile is { } passwordFile)
            {
                passwordFile = Path.GetFullPath(passwordFile);
                HostFiles.NoLinks(passwordFile);
                password = (await File.ReadAllTextAsync(passwordFile, shutdown.Token)).TrimEnd('\r', '\n');
            }
            using var certificate = X509CertificateLoader.LoadPkcs12FromFile(certPath, password, X509KeyStorageFlags.DefaultKeySet);
            if (!certificate.HasPrivateKey)
                throw new InvalidDataException("Worker certificate lacks a private key");
            var workerPath = typeof(WorkerNodeAgent).Assembly.Location;
            await using var supervisor = new WorkerSupervisor(workerPath, capacity.MaxProcesses, capacity.MaxCpu,
                capacity.MaxMemoryBytes, policy.MemoryLimit, config.SupervisorPath);
            var agent = new WorkerNodeAgent(config, policy, capacity, certificate, supervisor);
            await agent.ListenAsync(shutdown.Token);
            return 0;
        }
        catch (OperationCanceledException) when (shutdown.IsCancellationRequested) { return 0; }
        finally { Console.CancelKeyPress -= stop; }
    }

    private static WorkerNodeInventory Inventory(HostPolicy policy)
    {
        var plugins = policy.PluginRoots.Where(Directory.Exists).SelectMany(path => Directory.EnumerateFiles(path, "ud_connector_*"))
            .Where(path => policy.Authorize(policy.Tenant, path, ConnectionAccess.Execute)).Distinct(StringComparer.Ordinal).Take(256).ToArray();
        string assemblies = Path.Combine(policy.DataDirectory, "assemblies");
        var hashes = Directory.Exists(assemblies) ? Directory.EnumerateFiles(assemblies, "*.dll")
            .Select(Path.GetFileNameWithoutExtension).Where(hash => hash is { Length: 64 } && hash.All(Uri.IsHexDigit)).Cast<string>() : [];
        return new(plugins, policy.CredentialFiles.Keys.Take(256).ToArray(),
            hashes.Concat(policy.TrustedAssemblyHashes ?? []).Distinct(StringComparer.OrdinalIgnoreCase).Take(256).ToArray(),
            policy.FileRoots.Concat(policy.ReadOnlyRoots).Take(256).ToArray(),
            (policy.Resources?.Keys ?? Enumerable.Empty<string>()).Where(Path.IsPathFullyQualified).Take(256).ToArray(),
            (policy.TrustedAssemblyHashes ?? []).Take(256).ToArray(),OperatingSystem.IsWindows() ? "windows" : "linux",
            OperatingSystem.IsLinux() && File.Exists("/usr/bin/bwrap") && File.Exists("/usr/bin/systemd-run"));
    }

    private async Task<WorkerNodeChannel> ConnectAsync(WorkerManagementEndpoint endpoint, string path, CancellationToken cancellation)
    {
        var socket = new ClientWebSocket();
        socket.Options.ClientCertificates.Add(certificate);
        socket.Options.KeepAliveInterval = TimeSpan.FromSeconds(5);
        socket.Options.KeepAliveTimeout = TimeSpan.FromSeconds(10);
        socket.Options.RemoteCertificateValidationCallback = (_, supplied, _, errors) =>
            errors == SslPolicyErrors.None && supplied is X509Certificate2 actual &&
            WorkerNodeCertificate.Matches(actual, endpoint.CertificateSha256);
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
        deadline.CancelAfter(TimeSpan.FromSeconds(8));
        try
        {
            await socket.ConnectAsync(new Uri(endpoint.Url, path), deadline.Token);
            return new WorkerNodeChannel(socket);
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    private async Task ListenAsync(CancellationToken cancellation)
    {
        int candidate = 0;
        try
        {
            while (!cancellation.IsCancellationRequested)
            {
                var endpoint = configuration.ManagementEndpoints[candidate++ % configuration.ManagementEndpoints.Length];
                using var registration = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
                try { await RegisterAsync(endpoint, registration, cancellation); }
                catch (Exception error) when (ConnectionError(error)) { }
                finally { await registration.CancelAsync(); }
                await Task.Delay(TimeSpan.FromMilliseconds(500), cancellation);
            }
        }
        finally
        {
            var running = active.Values.ToArray();
            foreach (var execution in running) execution.Stop();
            await Task.WhenAll(running.Select(execution => execution.Completed));
        }
    }

    private async Task RegisterAsync(WorkerManagementEndpoint endpoint, CancellationTokenSource registration,
        CancellationToken shutdown)
    {
        using var channel = await ConnectAsync(endpoint, WorkerNodeChannel.RegistrationPath, registration.Token);
        var hello = WorkerNodeFrame.Registration("register", configuration.NodeId, boot,
            data: JsonSerializer.SerializeToElement(capacity, HostPolicy.JsonOptions));
        await channel.SendAsync(hello, registration.Token);
        using var handshake = CancellationTokenSource.CreateLinkedTokenSource(registration.Token);
        handshake.CancelAfter(TimeSpan.FromSeconds(5));
        var welcome = await channel.ReadAsync(handshake.Token, 4096);
        if (welcome is null || welcome.Kind != "registered" ||
            !welcome.SameLease(hello with { LeadershipGeneration = welcome.LeadershipGeneration }) ||
            welcome.LeadershipGeneration < lastTerm)
            throw new InvalidDataException("Worker node registration failed");
        lastTerm = welcome.LeadershipGeneration;
        var heartbeat = KeepAlive(channel, welcome, registration.Token);
        try
        {
            while (await channel.ReadAsync(registration.Token, 4096) is { } instruction)
            {
                if (instruction.Kind is not ("launch" or "recover") || instruction.NodeId != configuration.NodeId ||
                    instruction.AgentBootId != boot || instruction.LeadershipGeneration != lastTerm || !instruction.ValidLease)
                    throw new InvalidDataException("Worker lease instruction is invalid");
                if (instruction.Kind == "launch")
                {
                    if (active.Count >= capacity.MaxProcesses * 2 || active.ContainsKey(instruction.MemberId))
                        continue;
                    var run = new AgentExecution(this, instruction, shutdown);
                    if (!active.TryAdd(instruction.MemberId, run))
                    {
                        run.Stop();
                        continue;
                    }
                    run.Attach(endpoint, instruction, registration.Token);
                }
                else if (active.TryGetValue(instruction.MemberId, out var run))
                    run.Attach(endpoint, instruction, registration.Token);
            }
        }
        finally
        {
            await registration.CancelAsync();
            await heartbeat;
        }
    }

    private async Task KeepAlive(WorkerNodeChannel channel, WorkerNodeFrame welcome, CancellationToken cancellation)
    {
        using var clock = new PeriodicTimer(TimeSpan.FromSeconds(5));
        try
        {
            do
            {
                var state = new WorkerNodeHeartbeat(processSupervisor.Status(),
                    active.Values.Select(run => run.Status).OfType<WorkerNodeExecution>().ToArray());
                await channel.SendAsync(welcome.Reply("heartbeat", JsonSerializer.SerializeToElement(state, HostPolicy.JsonOptions)), cancellation);
            } while (await clock.WaitForNextTickAsync(cancellation));
        }
        catch (Exception error) when (ConnectionError(error)) { channel.Abort(); }
    }

    private static bool ConnectionError(Exception error) => error is IOException or WebSocketException or
        HttpRequestException or JsonException or OperationCanceledException or InvalidOperationException;

    private sealed class AgentExecution
    {
        private readonly WorkerNodeAgent node;
        private readonly WorkerNodeFrame original;
        private readonly CancellationTokenSource expiry = new(WorkerNodeChannel.LeaseDuration);
        private readonly CancellationTokenSource lifetime;
        private readonly SemaphoreSlim sessions = new(1,1);
        private readonly Lock gate = new();
        private readonly List<long> offsets = [];
        private readonly string spool;
        private TaskCompletionSource changed = Signal();
        private CancellationTokenSource? attached;
        private ulong term;
        private int processId;
        private int started;
        private int drainRequested;
        private int? exitCode;
        private WorkerRequest? request;
        private WorkerSupervisor.Lease? worker;
        private string controlToken = "";
        private Task? execution;

        internal AgentExecution(WorkerNodeAgent node, WorkerNodeFrame original, CancellationToken shutdown)
        {
            this.node = node;
            this.original = original;
            lifetime = CancellationTokenSource.CreateLinkedTokenSource(shutdown, expiry.Token);
            spool = Path.Combine(node.nodePolicy.DataDirectory, "worker-reports", node.boot, original.Lease + ".jsonl");
            Completed = CompleteAsync();
        }

        internal Task Completed { get; }
        internal void Stop()
        {
            lock (gate)
                if (!lifetime.IsCancellationRequested) lifetime.Cancel();
        }

        internal WorkerNodeExecution? Status
        {
            get
            {
                lock (gate)
                    return processId == 0 || lifetime.IsCancellationRequested ? null :
                        new(original.MemberId, original.Generation, original.Lease, processId, started != 0, (ulong)offsets.Count);
            }
        }

        internal void Attach(WorkerManagementEndpoint endpoint, WorkerNodeFrame frame, CancellationToken registration)
        {
            lock (gate)
            {
                if (lifetime.IsCancellationRequested || frame.Generation != original.Generation || frame.Lease != original.Lease ||
                    frame.LeadershipGeneration < term || frame.Sequence > (ulong)offsets.Count ||
                    frame.Kind == "recover" && processId == 0)
                    return;
                term = frame.LeadershipGeneration;
                attached?.Cancel();
                var session = CancellationTokenSource.CreateLinkedTokenSource(lifetime.Token, registration);
                attached = session;
                _ = ExchangeAsync(endpoint, frame, session);
            }
        }

        private async Task ExchangeAsync(WorkerManagementEndpoint endpoint, WorkerNodeFrame frame, CancellationTokenSource session)
        {
            bool entered = false;
            try
            {
                await sessions.WaitAsync(session.Token);
                entered = true;
                using var channel = await node.ConnectAsync(endpoint, WorkerNodeChannel.ExecutionPath, session.Token);
                await channel.SendAsync(frame.Reply("attach"), session.Token);
                if (frame.Kind == "launch")
                {
                    var message = await channel.ReadAsync(session.Token);
                    if (message is null || message.Kind != "request" || !message.SameLease(frame))
                        throw new InvalidDataException("Remote Worker request differs from its lease");
                    var launch = message.Data?.Deserialize<WorkerNodeLaunch>(HostPolicy.JsonOptions)
                        ?? throw new InvalidDataException("Remote Worker launch is missing");
                    await InitializeAsync(launch, session.Token);
                }
                var commands = CommandsAsync(channel, frame, session.Token);
                var reports = ReplayAsync(channel, frame, session.Token);
                try
                {
                    var first = await Task.WhenAny(commands, reports);
                    await first;
                    if (first == reports) await commands;
                }
                finally
                {
                    await session.CancelAsync();
                    channel.Abort();
                    try { await Task.WhenAll(commands, reports); }
                    catch (Exception error) when (ConnectionError(error) || error is UniversalDataException) { }
                }
            }
            catch (Exception error) when (ConnectionError(error) || error is UniversalDataException or UnauthorizedAccessException)
            {
                if (error is not OperationCanceledException)
                    Console.Error.WriteLine($"Worker session disconnected ({error.GetType().Name})");
            }
            finally
            {
                lock (gate)
                    if (ReferenceEquals(attached, session)) attached = null;
                if (entered) sessions.Release();
                session.Dispose();
            }
        }

        private async Task InitializeAsync(WorkerNodeLaunch launch, CancellationToken cancellation)
        {
            controlToken = Convert.ToHexString(RandomNumberGenerator.GetBytes(32));
            try
            {
                request = node.Bind(launch, controlToken);
                worker = await node.processSupervisor.RentAsync(request, launch.Budget, cancellation);
                lock (gate)
                {
                    processId = worker.ProcessId;
                    execution = ObserveAsync(worker, request);
                }
            }
            catch (Exception error) when (error is not OutOfMemoryException)
            {
                var result = error is UniversalDataException native ? native.Status : (int)StatusCode.Io;
                await AppendAsync(JsonSerializer.Serialize(new { version = ProductInfo.Protocol, ticket = launch.Ticket,
                    kind = "failed", result, phase = "worker.node.launch", errorType = "RemoteWorkerLaunch" }, HostPolicy.JsonOptions));
                lock (gate)
                {
                    exitCode = 1;
                    Notify();
                }
            }
        }

        private async Task ObserveAsync(WorkerSupervisor.Lease process, WorkerRequest bound)
        {
            int code = 1;
            try
            {
                while (await WorkerProtocol.ReadLine(process.Process.StandardOutput, 67108864, lifetime.Token) is { } text)
                {
                    using var document = JsonDocument.Parse(text);
                    var data = document.RootElement;
                    if (data.GetProperty("version").GetInt32() != ProductInfo.Protocol ||
                        data.GetProperty("ticket").GetString() != bound.Ticket ||
                        data.GetProperty("kind").GetString() is not ("ready" or "catalog" or "terminal" or "failed"))
                        throw new InvalidDataException("Invalid Worker report on the inherited pipe");
                    await AppendAsync(text);
                }
                await process.Process.WaitForExitAsync(lifetime.Token);
                code = process.Process.ExitCode;
            }
            catch (Exception error) when (ConnectionError(error) || error is UnauthorizedAccessException)
            {
                if (!process.Process.HasExited) await process.DrainAsync(bound,controlToken);
            }
            finally
            {
                lock (gate)
                {
                    worker = null;
                    exitCode = code;
                    Notify();
                }
                await process.DisposeAsync();
            }
        }

        private Task AppendAsync(string text)
        {
            long at;
            // The complete report is flushed before its position becomes visible to any connection.
            var directory = Path.GetDirectoryName(spool)!;
            if (!Directory.Exists(directory))
            {
                HostFiles.NoLinks(directory);
                if (OperatingSystem.IsWindows()) Directory.CreateDirectory(directory);
                else Directory.CreateDirectory(directory, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
            }
            var options = new FileStreamOptions { Mode = FileMode.Append, Access = FileAccess.Write, Share = FileShare.Read };
            if (!OperatingSystem.IsWindows()) options.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
            using (var file = new FileStream(spool, options))
            {
                at = file.Position;
                if (at >= 1024L * 1024 * 1024) throw new IOException("Worker report retention limit reached");
                using var writer = new StreamWriter(file, new System.Text.UTF8Encoding(false), leaveOpen: true);
                writer.WriteLine(text);
                writer.Flush();
                file.Flush(true);
            }
            lock (gate)
            {
                offsets.Add(at);
                Notify();
            }
            return Task.CompletedTask;
        }

        private async Task ReplayAsync(WorkerNodeChannel channel, WorkerNodeFrame frame, CancellationToken cancellation)
        {
            ulong sequence = frame.Sequence;
            while (!cancellation.IsCancellationRequested)
            {
                Task next;
                int? exit;
                long offset;
                lock (gate)
                {
                    next = changed.Task;
                    exit = exitCode;
                    offset = sequence < (ulong)offsets.Count ? offsets[checked((int)sequence)] : -1;
                }
                if (offset >= 0)
                {
                    using var file = new FileStream(spool, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                    file.Position = offset;
                    using var reader = new StreamReader(file, System.Text.Encoding.UTF8);
                    string text = await WorkerProtocol.ReadLine(reader, 67108864, cancellation)
                        ?? throw new InvalidDataException("Worker report is missing from durable storage");
                    using var report = JsonDocument.Parse(text);
                    await channel.SendAsync(frame.Reply("report", report.RootElement) with { Sequence = ++sequence }, cancellation);
                }
                else if (exit is { } code)
                {
                    await channel.SendAsync(frame.Reply("exit", JsonSerializer.SerializeToElement(code)) with { Sequence = sequence }, cancellation);
                    return;
                }
                else await next.WaitAsync(cancellation);
            }
        }

        private async Task CommandsAsync(WorkerNodeChannel channel, WorkerNodeFrame lease, CancellationToken cancellation)
        {
            string probe = "";
            long probeIssued = 0;
            bool confirmed = false;
            async Task ProbeAsync()
            {
                probe = Guid.NewGuid().ToString("N");
                probeIssued = Stopwatch.GetTimestamp();
                await channel.SendAsync(lease.Reply("renew-probe", correlation: probe), cancellation);
            }
            await ProbeAsync();
            while (await channel.ReadAsync(cancellation, 262144) is { } frame)
            {
                if (!frame.SameLease(lease))
                    throw new InvalidDataException("Remote Worker command differs from its lease");
                if (frame.Kind == "committed")
                {
                    lock (gate)
                    {
                        if (exitCode is null)
                            throw new InvalidDataException("Worker exit has not been reported");
                    }
                    Stop();
                    return;
                }
                if (frame.Kind == "renew")
                {
                    if (frame.Correlation == probe && !expiry.IsCancellationRequested)
                    {
                        var remaining = WorkerNodeChannel.LeaseDuration - Stopwatch.GetElapsedTime(probeIssued);
                        if (remaining <= TimeSpan.Zero) throw new IOException("Worker node lease expired");
                        expiry.CancelAfter(remaining);
                        confirmed = true;
                        await ProbeAsync();
                    }
                    continue;
                }

                WorkerSupervisor.Lease? process;
                WorkerRequest? bound;
                lock (gate)
                {
                    process = worker;
                    bound = request;
                }
                if (frame.Kind == "start" && confirmed && Volatile.Read(ref started) == 0 && process is not null)
                {
                    Volatile.Write(ref started, 1);
                    await process.Process.StandardInput.WriteLineAsync("start".AsMemory(), cancellation);
                    await process.Process.StandardInput.FlushAsync(cancellation);
                }
                else if (frame.Kind is "cancel" or "stop" && process is not null)
                {
                    await process.Process.StandardInput.WriteLineAsync(frame.Kind.AsMemory(), cancellation);
                    await process.Process.StandardInput.FlushAsync(cancellation);
                    if (Interlocked.Exchange(ref drainRequested,1) == 0 && bound is not null)
                        _ = process.EnforceDrainDeadline(bound.Policy.CancellationGraceSeconds);
                }
                else if (frame.Kind == "control" && bound is not null && frame.Data is { } data &&
                    Guid.TryParseExact(frame.Correlation, "N", out _))
                {
                    string action = data.GetProperty("action").GetString()!;
                    if (action is not ("cancel" or "stop" or "metrics" or "snapshot" or "debug"))
                        throw new InvalidDataException("Invalid Worker control action");
                    int result = 0;
                    JsonElement? value = null;
                    try { value = await WorkerControl.Send(bound.Policy.ControlDirectory(bound.Ticket), bound.Ticket,
                        controlToken, action, data.GetProperty("command"), cancellation); }
                    catch (UniversalDataException error) { result = error.Status; }
                    catch (Exception error) when (ConnectionError(error)) { result = (int)StatusCode.Io; }
                    await channel.SendAsync(lease.Reply("control-result", JsonSerializer.SerializeToElement(
                        new { result, value }), frame.Correlation), cancellation);
                }
                else if (frame.Kind is not ("cancel" or "stop") && (frame.Kind != "start" || !confirmed))
                    throw new InvalidDataException("Invalid remote Worker command");
            }
        }

        private async Task CompleteAsync()
        {
            try { await Task.Delay(Timeout.InfiniteTimeSpan, lifetime.Token); }
            catch (OperationCanceledException) { }
            await sessions.WaitAsync();
            try
            {
                if (execution is { } ended) await ended;
                if (request is { } bound)
                {
                    var vault = new ParameterVault(bound.Policy);
                    vault.RemoveLocal(bound.ControlReference);
                    vault.RemoveLocal(bound.ParametersReference);
                }
                if (File.Exists(spool)) File.Delete(spool);
            }
            finally
            {
                node.active.TryRemove(new KeyValuePair<string,AgentExecution>(original.MemberId,this));
                lifetime.Dispose();
                expiry.Dispose();
                sessions.Release();
            }
        }

        private static TaskCompletionSource Signal() => new(TaskCreationOptions.RunContinuationsAsynchronously);
        private void Notify()
        {
            changed.TrySetResult();
            changed = Signal();
        }
    }

    private WorkerRequest Bind(WorkerNodeLaunch launch, string controlToken)
    {
        if (!Guid.TryParseExact(launch.Ticket, "N", out _) ||
            !capacity.Tenants.Contains(launch.Tenant, StringComparer.Ordinal) ||
            !HostIdentity.ValidId(launch.Subject) || launch.Grants.Length > 1024 || WorkerNodeLaunch.SamplesRows(launch.Debug) ||
            launch.Budget.CpuSlots > nodePolicy.MaxConcurrency || launch.Budget.NativeMemoryBytes > nodePolicy.MemoryLimit ||
            launch.Shard is { } shard && (shard.Members == 0 || shard.Member >= shard.Members ||
                shard.Lanes != launch.Budget.ConsumerLanes))
            throw new UniversalDataException((int)StatusCode.Auth, "Worker launch is outside its node scope");
        var identity = new HostIdentity(launch.Subject, launch.Tenant, "", launch.Grants);
        using var package = JsonDocument.Parse(launch.Package);
        identity.Require("package.execute", package.RootElement.GetProperty("id").GetString()!);
        if (launch.DeploymentId is { } deployment)
            identity.Require("deployment.execute", deployment);
        var scoped = nodePolicy.Bind(identity);
        var remoteResources = launch.Resources ?? new Dictionary<string, ConnectionAccess[]>();
        scoped = scoped with {
            MaxConcurrency = launch.Budget.CpuSlots,
            MemoryLimit = launch.Budget.NativeMemoryBytes,
            FileRoots = SharedPaths(scoped.FileRoots, launch.FileRoots),
            ReadOnlyRoots = SharedPaths(scoped.ReadOnlyRoots, launch.FileRoots.Concat(launch.ReadOnlyRoots))
                .Concat(SharedPaths(scoped.FileRoots, launch.ReadOnlyRoots)).Distinct(StringComparer.Ordinal).ToArray(),
            PluginRoots = SharedPaths(scoped.PluginRoots, launch.PluginRoots),
            NetworkOrigins = scoped.NetworkOrigins.Intersect(launch.NetworkOrigins, StringComparer.OrdinalIgnoreCase).ToArray(),
            CredentialFiles = scoped.CredentialFiles.Where(pair => launch.CredentialReferences.Contains(pair.Key, StringComparer.Ordinal)).ToDictionary(),
            Resources = scoped.Resources?.Where(pair => remoteResources.ContainsKey(pair.Key)).ToDictionary(
                pair => pair.Key, pair => pair.Value.Intersect(remoteResources[pair.Key]).ToArray())
        };
        bool isolated = launch.Isolated || scoped.RequiresIsolation(launch.Package);
        if (launch.BindingSnapshot is { } snapshot)
            foreach (var entry in snapshot.EnumerateArray())
                if (entry.GetProperty("kind").GetInt32() == (int)CatalogKind.Package)
                {
                    var nested = entry.GetProperty("document");
                    identity.Require("package.execute", nested.GetProperty("id").GetString()!);
                    isolated |= scoped.RequiresIsolation(nested.GetRawText());
                }
        if (launch.Resume && !Directory.Exists(scoped.ExecutionDirectory(launch.JournalTicket ?? launch.Ticket)))
            throw new UniversalDataException((int)StatusCode.Fenced, "Remote Worker recovery directory is unavailable");
        var vault = new ParameterVault(scoped);
        string parameters = launch.Parameters is null ? "" : vault.Store(launch.Parameters);
        string control = vault.Store(JsonSerializer.Serialize(new { token = controlToken }));
        return new WorkerRequest(ProductInfo.Protocol, launch.Ticket, "execute", scoped, identity,
            launch.Package, parameters, launch.ExecutionId, launch.Resume, launch.Revision,
            launch.EventSequence, launch.Debug, ControlReference: control, Isolated: isolated,
            BindingSnapshot: launch.BindingSnapshot, DeploymentId: launch.DeploymentId,
            DeploymentRevision: launch.DeploymentRevision, Budget: launch.GovernProcess ? launch.Budget : null, Shard: launch.Shard,
            BrokerRestore: launch.BrokerRestore, JournalTicket: launch.JournalTicket, Assignment: launch.Assignment);
    }

    private static string[] SharedPaths(IEnumerable<string> local, IEnumerable<string> remote) =>
        (from left in local from right in remote
         let shared = HostPolicy.Inside(left, right) ? left : HostPolicy.Inside(right, left) ? right : null
         where shared is not null select shared).Distinct(StringComparer.Ordinal).ToArray();
}
