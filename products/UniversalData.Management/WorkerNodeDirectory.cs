using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using UniversalData.Hosting;

namespace UniversalData.Management;

public sealed class WorkerNodeDirectory(ClusterLeadership leadership)
{
    private readonly ConcurrentDictionary<string, Registration> registered = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, Registration> history = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, Pending> pending = new(StringComparer.Ordinal);

    public IReadOnlyList<RemoteWorkerNode> Nodes => registered.Values.Where(entry => entry.State == "Online")
        .Select(entry => entry.Node with { Capacity = entry.Heartbeat?.Capacity })
        .OrderBy(node => node.NodeId, StringComparer.Ordinal).ToArray();

    internal IEnumerable<(RemoteWorkerNode Node, string State, long HeartbeatMs)> Status(string tenant) =>
        history.Values.Where(value => value.Node.AllowedTenants.Contains(tenant, StringComparer.Ordinal))
            .Select(value => (value.Node with { Capacity = value.Heartbeat?.Capacity }, value.State, value.HeartbeatMs));

    internal bool Matches(RunState state) => MatchingRegistration(state) is not null;

    private Registration? MatchingRegistration(RunState state)
    {
        if (state.Placement is not { Remote: true } placement || !registered.TryGetValue(placement.NodeId, out var node) ||
            node.State != "Online" || node.Boot != state.AgentBootId)
            return null;
        return node.Heartbeat?.Executions.Any(execution => execution.MemberId == state.Id &&
            execution.Generation == state.TaskGeneration && execution.Lease == state.DispatchLeaseId &&
            execution.ProcessId == state.ProcessId && execution.Produced >= state.ReportSequence &&
            execution.CanAttach) == true ? node : null;
    }

    internal bool JournalOwnerAvailable(RunState state) =>
        state.Placement is { Remote: true } placement && state.HomeNodeId == placement.NodeId &&
        registered.TryGetValue(placement.NodeId, out var registration) && registration.State == "Online" &&
        (state.WorkerExitConfirmed || registration.Boot == state.AgentBootId) &&
        registration.Heartbeat?.Executions.All(execution => execution.MemberId != state.Id) == true;

    public async Task<RemoteWorkerSession> StartAsync(WorkerPlacement placement, WorkerRequest request, string memberId,
        ulong generation, CancellationToken cancellation, string? recoveryAgentBootId = null)
    {
        var launch = WorkerNodeLaunch.From(request, placement.Budget, recoveryAgentBootId);
        if (!registered.TryGetValue(placement.NodeId, out var registration) ||
            registration.State != "Online" || !registration.Node.AllowedTenants.Contains(launch.Tenant, StringComparer.Ordinal) ||
            recoveryAgentBootId is not null && registration.Boot != recoveryAgentBootId ||
            launch.Budget.CpuSlots > registration.Node.MaxCpu ||
            launch.Budget.ProcessMemoryBytes > registration.Node.MaxMemoryBytes)
            throw new IOException("Worker node is unavailable for this reservation");
        var lease = new WorkerNodeFrame(ProductInfo.WorkerRevision, "launch", placement.NodeId, memberId, generation,
            Guid.NewGuid().ToString("N"), LeadershipGeneration: registration.Term, AgentBootId: registration.Boot);
        return await OpenSession(registration, lease, launch, cancellation);
    }

    internal Task<RemoteWorkerSession> RecoverAsync(RunState state, CancellationToken cancellation)
    {
        var registration = MatchingRegistration(state) ??
            throw new IOException("Worker no longer reports the execution to be recovered");
        var frame = new WorkerNodeFrame(ProductInfo.WorkerRevision,"recover",registration.Node.NodeId,state.Id,
            state.TaskGeneration,state.DispatchLeaseId!, LeadershipGeneration:registration.Term,
            AgentBootId:registration.Boot, Sequence:state.ReportSequence);
        return OpenSession(registration,frame,null,cancellation);
    }

    private async Task<RemoteWorkerSession> OpenSession(Registration registration, WorkerNodeFrame frame,
        WorkerNodeLaunch? request, CancellationToken cancellation)
    {
        if (!frame.ValidLease)
            throw new ArgumentException("Invalid remote Worker membership");
        var handoff = new Pending(frame, registration);
        if (!pending.TryAdd(frame.Lease, handoff))
            throw new IOException("Worker handoff is already in progress");
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellation, registration.Disconnected);
        deadline.CancelAfter(TimeSpan.FromSeconds(10));
        try
        {
            await registration.Channel.SendAsync(frame, deadline.Token);
            var channel = await handoff.Attached.Task.WaitAsync(deadline.Token);
            if (request is not null)
                await channel.SendAsync(frame.Reply("request", JsonSerializer.SerializeToElement(request, HostPolicy.JsonOptions)), deadline.Token);
            return new RemoteWorkerSession(channel, frame, handoff.Completed, registration.Disconnected);
        }
        catch
        {
            handoff.Completed.TrySetResult();
            throw;
        }
        finally { pending.TryRemove(frame.Lease, out _); }
    }

    internal async Task RegisterAsync(RemoteWorkerNode node, string boot, WorkerNodeChannel channel, CancellationToken cancellation)
    {
        var term = leadership.Current;
        if (leadership.Enabled && term is null)
            throw new IOException("Management has no leadership term");
        using var disconnected = CancellationTokenSource.CreateLinkedTokenSource(cancellation, term?.Lost ?? CancellationToken.None);
        var registration = new Registration(node, boot, term?.Lease.Generation ?? 0, channel, disconnected.Token);
        if (!registered.TryAdd(node.NodeId, registration))
            throw new InvalidOperationException("Worker node is already connected");
        history[node.NodeId] = registration;
        try
        {
            var welcome = WorkerNodeFrame.Registration("registered", node.NodeId, boot, registration.Term);
            await channel.SendAsync(welcome, disconnected.Token);
            while (!disconnected.IsCancellationRequested)
            {
                using var active = CancellationTokenSource.CreateLinkedTokenSource(disconnected.Token);
                active.CancelAfter(TimeSpan.FromSeconds(20));
                var frame = await channel.ReadAsync(active.Token, 262144);
                if (frame is null) break;
                if (frame.Kind != "heartbeat" || !frame.SameLease(welcome))
                    throw new InvalidDataException("Invalid Worker node heartbeat");
                var heartbeat = frame.Data?.Deserialize<WorkerNodeHeartbeat>(HostPolicy.JsonOptions);
                if (heartbeat is null || heartbeat.Capacity.UsedProcesses > node.MaxProcesses ||
                    heartbeat.Capacity.UsedCpu > node.MaxCpu || heartbeat.Capacity.UsedMemoryBytes > node.MaxMemoryBytes ||
                    heartbeat.Capacity.AvailableCpu > node.MaxCpu || heartbeat.Capacity.AvailableProcesses > node.MaxProcesses ||
                    heartbeat.Capacity.AvailableMemoryBytes > node.MaxMemoryBytes ||
                    heartbeat.Executions.Length > node.MaxProcesses * 2 ||
                    heartbeat.Executions.Any(run => !HostIdentity.ValidId(run.MemberId) ||
                        !Guid.TryParseExact(run.Lease,"N",out _) || run.Generation == 0 || run.ProcessId <= 0))
                    throw new InvalidDataException("Worker node capacity heartbeat is invalid");
                registration.Touch(heartbeat);
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
            frame.Sequence != handoff.Lease.Sequence ||
            !registered.TryGetValue(frame.NodeId, out var current) || !ReferenceEquals(current, handoff.Registration) ||
            !handoff.Attached.TrySetResult(channel))
            throw new InvalidDataException("Worker handoff is unavailable");
        await handoff.Completed.Task.WaitAsync(cancellation);
    }

    private sealed class Registration(RemoteWorkerNode node, string boot, ulong term, WorkerNodeChannel channel,
        CancellationToken disconnected)
    {
        private long clock = Stopwatch.GetTimestamp();
        private long heartbeatMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        private volatile WorkerNodeHeartbeat? heartbeat;
        internal RemoteWorkerNode Node => node;
        internal string Boot => boot;
        internal ulong Term => term;
        internal WorkerNodeChannel Channel => channel;
        internal CancellationToken Disconnected => disconnected;
        internal WorkerNodeHeartbeat? Heartbeat => heartbeat;
        internal long HeartbeatMs => Interlocked.Read(ref heartbeatMs);
        internal string State
        {
            get
            {
                double seconds = Stopwatch.GetElapsedTime(Interlocked.Read(ref clock)).TotalSeconds;
                if (disconnected.IsCancellationRequested || seconds >= 20) return "Offline";
                return seconds >= 10 || heartbeat is null ? "Suspect" : "Online";
            }
        }
        internal void Touch(WorkerNodeHeartbeat value)
        {
            heartbeat = value;
            Interlocked.Exchange(ref clock, Stopwatch.GetTimestamp());
            Interlocked.Exchange(ref heartbeatMs, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        }
    }

    private sealed record Pending(WorkerNodeFrame Lease, Registration Registration)
    {
        internal TaskCompletionSource<WorkerNodeChannel> Attached { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
