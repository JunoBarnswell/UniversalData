using System.Collections.Concurrent;
using System.Text.Json;
using UniversalData.Hosting;

namespace UniversalData.Management;

public sealed class WorkerNodeDirectory
{
    private readonly ConcurrentDictionary<string, Registration> registered = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, Registration> history = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, Pending> pending = new(StringComparer.Ordinal);

    public IReadOnlyList<RemoteWorkerNode> Nodes => registered.Values.Where(entry => entry.Online).Select(entry => entry.Node)
        .OrderBy(node => node.NodeId, StringComparer.Ordinal).ToArray();

    internal IEnumerable<(RemoteWorkerNode Node, bool Online, long HeartbeatMs)> Status(string tenant) =>
        history.Values.Where(value => value.Node.AllowedTenants.Contains(tenant, StringComparer.Ordinal))
            .Select(value => (value.Node, value.Online && registered.ContainsKey(value.Node.NodeId), value.Heartbeat));

    public async Task<RemoteWorkerSession> StartAsync(WorkerPlacement placement, WorkerRequest request, string memberId,
        ulong generation, CancellationToken cancellation)
    {
        string nodeId = placement.NodeId;
        var launch = WorkerNodeLaunch.From(request, placement.Budget);
        if (!registered.TryGetValue(nodeId, out var registration) ||
            !registration.Online || !registration.Node.AllowedTenants.Contains(launch.Tenant, StringComparer.Ordinal) ||
            launch.Budget.CpuSlots > registration.Node.MaxCpu ||
            launch.Budget.ProcessMemoryBytes > registration.Node.MaxMemoryBytes)
            throw new InvalidOperationException("Worker node is unavailable for this reservation");

        var lease = new WorkerNodeFrame(ProductInfo.WorkerRevision, "launch", nodeId, memberId, generation,
            Guid.NewGuid().ToString("N"));
        if (!lease.ValidLease)
            throw new ArgumentException("Invalid remote Worker membership");
        var handoff = new Pending(lease, registration);
        if (!pending.TryAdd(lease.Lease, handoff))
            throw new InvalidOperationException("Duplicate Worker handoff");
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellation, registration.Disconnected);
        deadline.CancelAfter(TimeSpan.FromSeconds(15));
        try
        {
            await registration.Channel.SendAsync(lease, deadline.Token);
            var channel = await handoff.Attached.Task.WaitAsync(deadline.Token);
            await channel.SendAsync(lease.Reply("request", JsonSerializer.SerializeToElement(launch, HostPolicy.JsonOptions)), deadline.Token);
            return new RemoteWorkerSession(channel, lease, handoff.Completed, registration.Disconnected);
        }
        catch
        {
            handoff.Completed.TrySetResult();
            throw;
        }
        finally { pending.TryRemove(lease.Lease, out _); }
    }

    internal async Task RegisterAsync(RemoteWorkerNode node, WorkerNodeChannel channel, CancellationToken cancellation)
    {
        using var disconnected = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
        var registration = new Registration(node, channel, disconnected.Token);
        if (!registered.TryAdd(node.NodeId, registration))
            throw new InvalidOperationException("Worker node is already connected");
        history[node.NodeId] = registration;
        try
        {
            var registeredFrame = WorkerNodeFrame.Registration("registered", node.NodeId);
            await channel.SendAsync(registeredFrame, disconnected.Token);
            while (!disconnected.IsCancellationRequested)
            {
                using var active = CancellationTokenSource.CreateLinkedTokenSource(disconnected.Token);
                active.CancelAfter(TimeSpan.FromSeconds(20));
                var frame = await channel.ReadAsync(active.Token, 4096);
                if (frame is null)
                    break;
                if (frame.Kind != "heartbeat" || !frame.SameLease(registeredFrame))
                    throw new InvalidDataException("Invalid Worker node heartbeat");
                registration.Touch();
            }
        }
        finally
        {
            await disconnected.CancelAsync();
            registered.TryRemove(new KeyValuePair<string, Registration>(node.NodeId, registration));
        }
    }

    internal async Task AttachAsync(WorkerNodeFrame frame, WorkerNodeChannel channel, CancellationToken cancellation)
    {
        if (!pending.TryGetValue(frame.Lease, out var handoff) || !frame.SameLease(handoff.Lease) ||
            !registered.TryGetValue(frame.NodeId, out var current) || !ReferenceEquals(current, handoff.Registration) ||
            !handoff.Attached.TrySetResult(channel))
            throw new InvalidDataException("Worker handoff is unavailable");
        await handoff.Completed.Task.WaitAsync(cancellation);
    }

    private sealed class Registration(RemoteWorkerNode node, WorkerNodeChannel channel, CancellationToken disconnected)
    {
        private long heartbeat = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        internal RemoteWorkerNode Node { get; } = node;
        internal WorkerNodeChannel Channel { get; } = channel;
        internal CancellationToken Disconnected { get; } = disconnected;
        internal long Heartbeat => Interlocked.Read(ref heartbeat);
        internal bool Online => !Disconnected.IsCancellationRequested &&
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - Heartbeat <= 20000;
        internal void Touch() => Interlocked.Exchange(ref heartbeat, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
    }
    private sealed record Pending(WorkerNodeFrame Lease, Registration Registration)
    {
        internal TaskCompletionSource<WorkerNodeChannel> Attached { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
