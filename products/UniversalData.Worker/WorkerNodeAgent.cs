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
    Uri management, X509Certificate2 certificate, string workerPath)
{
    private readonly object reservationsLock = new();
    private readonly HashSet<string> reservations = new(StringComparer.Ordinal);
    private uint reservedCpu;
    private ulong reservedMemory;

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
            if (!Uri.TryCreate(config.ManagementUrl, UriKind.Absolute, out var management) ||
                management.Scheme != "wss" || management.UserInfo.Length != 0 || management.AbsolutePath != "/" ||
                management.Query.Length != 0 || management.Fragment.Length != 0 || !HostIdentity.ValidId(config.NodeId) ||
                !WorkerNodeCertificate.ValidPin(config.ManagementCertificateSha256))
                throw new InvalidDataException("Invalid Worker node agent endpoint");
            var capacity = new WorkerNodeCapacity(config.MaxCpu, config.MaxProcesses, config.MaxMemoryBytes, config.Tenants);
            var policy = HostPolicy.Load(Path.GetFullPath(config.PolicyPath));
            if (!capacity.Valid || capacity.MaxCpu > (uint)Environment.ProcessorCount ||
                capacity.MaxProcesses > policy.MaxConcurrency || policy.Tenants is null ||
                capacity.Tenants.Any(tenant => !policy.Tenants.ContainsKey(tenant)))
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
            var agent = new WorkerNodeAgent(config, policy, capacity, management, certificate, workerPath);
            await agent.ListenAsync(shutdown.Token);
            return 0;
        }
        catch (OperationCanceledException) when (shutdown.IsCancellationRequested) { return 0; }
        finally { Console.CancelKeyPress -= stop; }
    }

    private async Task<WorkerNodeChannel> ConnectAsync(string path, CancellationToken cancellation)
    {
        var socket = new ClientWebSocket();
        socket.Options.ClientCertificates.Add(certificate);
        socket.Options.KeepAliveInterval = TimeSpan.FromSeconds(5);
        socket.Options.KeepAliveTimeout = TimeSpan.FromSeconds(10);
        socket.Options.RemoteCertificateValidationCallback = (_, supplied, _, errors) =>
            errors == SslPolicyErrors.None && supplied is X509Certificate2 actual &&
            WorkerNodeCertificate.Matches(actual, configuration.ManagementCertificateSha256);
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
        deadline.CancelAfter(TimeSpan.FromSeconds(8));
        try
        {
            await socket.ConnectAsync(new Uri(management, path), deadline.Token);
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
        while (!cancellation.IsCancellationRequested)
        {
            using var registration = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
            var active = new Dictionary<string, Task>(StringComparer.Ordinal);
            try
            {
                using var channel = await ConnectAsync(WorkerNodeChannel.RegistrationPath, registration.Token);
                var hello = WorkerNodeFrame.Registration("register", configuration.NodeId,
                    JsonSerializer.SerializeToElement(capacity, HostPolicy.JsonOptions));
                await channel.SendAsync(hello, registration.Token);
                using var handshake = CancellationTokenSource.CreateLinkedTokenSource(registration.Token);
                handshake.CancelAfter(TimeSpan.FromSeconds(10));
                var welcome = await channel.ReadAsync(handshake.Token, 4096);
                if (welcome?.Kind != "registered" || !welcome.SameLease(hello))
                    throw new InvalidDataException("Worker node registration failed");
                while (await channel.ReadAsync(registration.Token, 4096) is { } lease)
                {
                    if (lease.Kind != "launch" || lease.NodeId != configuration.NodeId || !lease.ValidLease)
                        throw new InvalidDataException("Worker lease instruction is invalid");
                    foreach (var finished in active.Where(entry => entry.Value.IsCompleted).Select(entry => entry.Key).ToArray())
                        active.Remove(finished);
                    if (active.Count >= capacity.MaxProcesses || active.ContainsKey(lease.MemberId))
                        continue;
                    active.Add(lease.MemberId, ExecuteAsync(lease, registration.Token));
                }
            }
            catch (Exception error) when (error is IOException or WebSocketException or HttpRequestException or
                JsonException or OperationCanceledException or InvalidOperationException) { }
            finally
            {
                await registration.CancelAsync();
                await Task.WhenAll(active.Values);
            }
            await Task.Delay(TimeSpan.FromSeconds(2), cancellation);
        }
    }

    private async Task ExecuteAsync(WorkerNodeFrame lease, CancellationToken registration)
    {
        using var expiry = new CancellationTokenSource(WorkerNodeChannel.LeaseDuration);
        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(registration, expiry.Token);
        WorkerNodeChannel? channel = null;
        WorkerRequest? request = null;
        Process? child = null;
        WorkerBudget? reservation = null;
        string ticket = "";
        string controlToken = "";
        try
        {
            channel = await ConnectAsync(WorkerNodeChannel.ExecutionPath, lifetime.Token);
            await channel.SendAsync(lease.Reply("attach"), lifetime.Token);
            var frame = await channel.ReadAsync(lifetime.Token);
            if (frame is null || frame.Kind != "request" || !frame.SameLease(lease))
                throw new InvalidDataException("Remote Worker request differs from its lease");
            var launch = frame.Data?.Deserialize<WorkerNodeLaunch>(HostPolicy.JsonOptions)
                ?? throw new InvalidDataException("Remote Worker launch is missing");
            ticket = launch.Ticket;
            if (!Reserve(lease.MemberId, launch.Budget))
                throw new UniversalDataException((int)StatusCode.Busy, "Worker node has no execution capacity");
            reservation = launch.Budget;
            controlToken = Convert.ToHexString(RandomNumberGenerator.GetBytes(32));
            request = Bind(launch, controlToken);
            child = WorkerLauncher.Launch(workerPath, request);
            _ = WorkerProtocol.Discard(child.StandardError, lifetime.Token);
            await child.StandardInput.WriteLineAsync(
                JsonSerializer.Serialize(request, HostPolicy.JsonOptions).AsMemory(), lifetime.Token)
                .WaitAsync(lifetime.Token);
            await child.StandardInput.FlushAsync(lifetime.Token);
            var commands = CommandsAsync(channel, lease, child, request, controlToken, expiry, lifetime.Token);
            var reports = ReportsAsync(channel, lease, child, request.Ticket, lifetime.Token);
            try
            {
                if (await Task.WhenAny(commands, reports) == commands)
                {
                    await commands;
                    throw new IOException("Management Worker connection closed");
                }
                await reports;
                await child.WaitForExitAsync(lifetime.Token);
                await channel.SendAsync(lease.Reply("exit",
                    JsonSerializer.SerializeToElement(child.ExitCode)), lifetime.Token);
            }
            finally
            {
                await lifetime.CancelAsync();
                channel.Abort();
                if (!child.HasExited)
                    await DrainAsync(child, request, controlToken);
                try { await Task.WhenAll(commands, reports).WaitAsync(TimeSpan.FromSeconds(2)); }
                catch (Exception error) when (error is IOException or WebSocketException or JsonException or
                    OperationCanceledException or InvalidOperationException or UniversalDataException or TimeoutException) { }
            }
        }
        catch (Exception error) when (error is not OutOfMemoryException)
        {
            if (channel is not null && child is null && Guid.TryParseExact(ticket, "N", out _) &&
                !lifetime.IsCancellationRequested)
            {
                try
                {
                    var result = error is UniversalDataException native ? native.Status : (int)StatusCode.Io;
                    await channel.SendAsync(lease.Reply("report", JsonSerializer.SerializeToElement(
                        new { version = ProductInfo.Protocol, ticket, kind = "failed", result, phase = "worker.node", errorType = "RemoteWorkerLaunch" })), lifetime.Token);
                    await channel.SendAsync(lease.Reply("exit", JsonSerializer.SerializeToElement(1)), lifetime.Token);
                }
                catch (Exception failure) when (failure is IOException or OperationCanceledException or WebSocketException or InvalidOperationException) { }
            }
        }
        finally
        {
            channel?.Dispose();
            if (child is not null && request is not null)
            {
                using (child)
                    if (!child.HasExited)
                        await DrainAsync(child, request, controlToken);
            }
            if (request is not null && request.ControlReference.Length != 0)
            {
                var directory = new ParameterVault(request.Policy).DirectoryFor(request.ControlReference);
                try
                {
                    File.Delete(Path.Combine(directory, "key"));
                    File.Delete(Path.Combine(directory, "payload"));
                    Directory.Delete(directory);
                }
                catch (IOException) { }
            }
            if (reservation is not null)
                Release(lease.MemberId, reservation);
        }
    }

    private WorkerRequest Bind(WorkerNodeLaunch launch, string controlToken)
    {
        if (!Guid.TryParseExact(launch.Ticket, "N", out _) ||
            !capacity.Tenants.Contains(launch.Tenant, StringComparer.Ordinal) ||
            !HostIdentity.ValidId(launch.Subject) || launch.Grants.Length > 1024 || WorkerNodeLaunch.SamplesRows(launch.Debug) ||
            launch.Budget.CpuSlots > policy.MaxConcurrency || launch.Budget.NativeMemoryBytes > policy.MemoryLimit ||
            launch.Shard is { } shard && (shard.Members == 0 || shard.Member >= shard.Members ||
                shard.Lanes != launch.Budget.ConsumerLanes))
            throw new UniversalDataException((int)StatusCode.Auth, "Worker launch is outside its node scope");
        var identity = new HostIdentity(launch.Subject, launch.Tenant, "", launch.Grants);
        using var package = JsonDocument.Parse(launch.Package);
        identity.Require("package.execute", package.RootElement.GetProperty("id").GetString()!);
        if (launch.DeploymentId is { } deployment)
            identity.Require("deployment.execute", deployment);
        var scoped = policy.Bind(identity);
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
        if (launch.ParametersReference.Length != 0)
            _ = vault.Resolve(launch.ParametersReference);
        string control = vault.Store(JsonSerializer.Serialize(new { token = controlToken }));
        return new WorkerRequest(ProductInfo.Protocol, launch.Ticket, "execute", scoped, identity,
            launch.Package, launch.ParametersReference, launch.ExecutionId, launch.Resume, launch.Revision,
            launch.EventSequence, launch.Debug, ControlReference: control, Isolated: isolated,
            BindingSnapshot: launch.BindingSnapshot, DeploymentId: launch.DeploymentId,
            DeploymentRevision: launch.DeploymentRevision, Budget: launch.Budget, Shard: launch.Shard,
            BrokerRestore: launch.BrokerRestore, JournalTicket: launch.JournalTicket);
    }

    private static string[] SharedPaths(IEnumerable<string> local, IEnumerable<string> remote) =>
        (from left in local from right in remote
         let shared = HostPolicy.Inside(left, right) ? left : HostPolicy.Inside(right, left) ? right : null
         where shared is not null select shared).Distinct(StringComparer.Ordinal).ToArray();

    private bool Reserve(string memberId, WorkerBudget budget)
    {
        lock (reservationsLock)
        {
            if (reservedCpu > capacity.MaxCpu || budget.CpuSlots > capacity.MaxCpu - reservedCpu ||
                reservedMemory > capacity.MaxMemoryBytes || budget.ProcessMemoryBytes > capacity.MaxMemoryBytes - reservedMemory ||
                reservations.Count >= capacity.MaxProcesses || !reservations.Add(memberId))
                return false;
            reservedCpu += budget.CpuSlots;
            reservedMemory += budget.ProcessMemoryBytes;
            return true;
        }
    }

    private void Release(string memberId, WorkerBudget budget)
    {
        lock (reservationsLock)
        {
            reservations.Remove(memberId);
            reservedCpu -= budget.CpuSlots;
            reservedMemory -= budget.ProcessMemoryBytes;
        }
    }

    private static async Task ReportsAsync(WorkerNodeChannel channel, WorkerNodeFrame lease, Process child,
        string ticket, CancellationToken cancellation)
    {
        while (await WorkerProtocol.ReadLine(child.StandardOutput, 67108864, cancellation) is { } text)
        {
            using var document = JsonDocument.Parse(text);
            var data = document.RootElement;
            if (data.GetProperty("version").GetInt32() != ProductInfo.Protocol ||
                data.GetProperty("ticket").GetString() != ticket ||
                data.GetProperty("kind").GetString() is not ("ready" or "catalog" or "terminal" or "failed"))
                throw new InvalidDataException("Invalid Worker report on the inherited pipe");
            await channel.SendAsync(lease.Reply("report", data), cancellation);
        }
    }

    private static async Task CommandsAsync(WorkerNodeChannel channel, WorkerNodeFrame lease, Process child,
        WorkerRequest request, string controlToken, CancellationTokenSource expiry, CancellationToken cancellation)
    {
        bool started = false;
        string probe = "";
        long probeIssued = 0;
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
            switch (frame.Kind)
            {
                case "renew":
                    // A buffered renewal is bounded by the local issue time of its challenge.
                    if (frame.Correlation == probe && !expiry.IsCancellationRequested)
                    {
                        var remaining = WorkerNodeChannel.LeaseDuration - Stopwatch.GetElapsedTime(probeIssued);
                        if (remaining <= TimeSpan.Zero)
                            throw new IOException("Worker node lease expired");
                        expiry.CancelAfter(remaining);
                        await ProbeAsync();
                    }
                    break;
                case "start" when !started:
                case "cancel":
                    started = true;
                    await child.StandardInput.WriteLineAsync(frame.Kind.AsMemory(), cancellation);
                    await child.StandardInput.FlushAsync(cancellation);
                    break;
                case "control" when frame.Data is { } data && Guid.TryParseExact(frame.Correlation, "N", out _):
                    var action = data.GetProperty("action").GetString()!;
                    if (action is not ("cancel" or "stop" or "metrics" or "snapshot" or "debug"))
                        throw new InvalidDataException("Invalid Worker control action");
                    JsonElement? command = data.GetProperty("command");
                    int result = 0;
                    JsonElement? value = null;
                    try { value = await WorkerControl.Send(request.Policy.ControlDirectory(request.Ticket),
                        request.Ticket, controlToken, action, command, cancellation); }
                    catch (UniversalDataException error) { result = error.Status; }
                    catch (Exception error) when (error is IOException or OperationCanceledException) { result = (int)StatusCode.Io; }
                    await channel.SendAsync(lease.Reply("control-result", JsonSerializer.SerializeToElement(
                        new { result, value }), frame.Correlation), cancellation);
                    break;
                default:
                    throw new InvalidDataException("Invalid remote Worker command");
            }
        }
    }

    private static async Task DrainAsync(Process child, WorkerRequest request, string controlToken)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(Math.Min(5, request.Policy.CancellationGraceSeconds)));
        try
        {
            await WorkerControl.Send(request.Policy.ControlDirectory(request.Ticket), request.Ticket, controlToken,
                "stop", cancellation: deadline.Token);
            await child.WaitForExitAsync(deadline.Token);
        }
        catch (Exception error) when (error is IOException or OperationCanceledException or System.Net.Sockets.SocketException or
                                      UniversalDataException or InvalidOperationException)
        {
            if (!child.HasExited)
            {
                await WorkerLauncher.Terminate(child, request.Ticket, request.Isolated);
                using var killed = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                await child.WaitForExitAsync(killed.Token);
            }
        }
    }
}
