using System.Collections.Concurrent;
using System.Text.Json;
using UniversalData.Hosting;

namespace UniversalData.Management;

public sealed class WorkerNodeDirectory
{
    private readonly ConcurrentDictionary<string, Registration> registered = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, Pending> pending = new(StringComparer.Ordinal);

    public IReadOnlyList<RemoteWorkerNode> Nodes => registered.Values.Select(entry => entry.Node)
        .OrderBy(node => node.NodeId, StringComparer.Ordinal).ToArray();

    public async Task<RemoteWorkerSession> StartAsync(string nodeId, WorkerRequest request, string memberId,
        ulong generation, CancellationToken cancellation)
    {
        var launch = WorkerNodeLaunch.From(request);
        if (!registered.TryGetValue(nodeId, out var registration) ||
            !registration.Node.AllowedTenants.Contains(launch.Tenant, StringComparer.Ordinal) ||
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
        try
        {
            await channel.SendAsync(WorkerNodeFrame.Registration("registered", node.NodeId), disconnected.Token);
            if (await channel.ReadAsync(disconnected.Token, 4096) is not null)
                throw new InvalidDataException("Invalid Worker node registration message");
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

    private sealed record Registration(RemoteWorkerNode Node, WorkerNodeChannel Channel, CancellationToken Disconnected);
    private sealed record Pending(WorkerNodeFrame Lease, Registration Registration)
    {
        internal TaskCompletionSource<WorkerNodeChannel> Attached { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
