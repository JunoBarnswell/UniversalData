using System.Collections.Concurrent;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text.Json;
using UniversalData.Hosting;

namespace UniversalData.Management;

public sealed class TenantDispatcher(HostPolicy policy, HostPolicy root, ProductConfiguration configuration, IdentityDirectory identities,
    WorkerNodeDirectory remoteNodes, WorkerReservations reservations)
{
    private readonly object gate = new();
    private readonly string owner = configuration.NodeId;
    private readonly Dictionary<string,RunState> states = new(StringComparer.Ordinal);
    private readonly Dictionary<string,JobState> jobs = new(StringComparer.Ordinal);
    private readonly Dictionary<string,DeploymentState> deployments = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string,RemoteWorkerSession> remoteWorkers = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string,Process> workers = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string,Task> monitors = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string,CatalogLease> leases = new(StringComparer.Ordinal);
    private const uint LeaseDuration = 30000;
    private static long Now => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    public RunState[] List(HostIdentity identity) { lock(gate) { Refresh();return states.Values.Where(run => run.ParentId is null && identity.Allows("execution.read",run.Id)).OrderByDescending(run => run.TimestampMs).ToArray(); } }
    public RunState? Get(HostIdentity identity,string id) { identity.Require("execution.read",id); lock(gate) { Refresh();return states.GetValueOrDefault(id) is { ParentId: null } state ? state : null; } }
    public RunState[] Members(HostIdentity identity, string id)
    {
        identity.Require("execution.read", id);
        lock (gate) {
            Refresh();
            var parent = states.GetValueOrDefault(id);
            if (parent is null || parent.ParentId is not null) throw new ArgumentException("Execution was not found");
            return parent.MemberIds?.Where(states.ContainsKey).Select(member => states[member]).ToArray() ?? [];
        }
    }
    public JobState[] Jobs(HostIdentity identity) { lock(gate) { Refresh();return jobs.Values.Where(job => identity.Allows("job.read",job.Id)).OrderBy(job => job.Id).ToArray(); } }
    public DeploymentState[] Deployments(HostIdentity identity) { lock(gate) { Refresh();return deployments.Values.Where(item => identity.Allows("deployment.read",item.Id)).OrderBy(item => item.Id).ToArray(); } }
    private RunState Get(string id) { lock(gate) { Refresh();return states.GetValueOrDefault(id) ?? throw new ArgumentException("Execution does not exist"); } }
    private string Package(string id,string revision)
    {
        using var catalog = policy.OpenCatalog();
        return catalog.Get(CatalogKind.Package,id,ulong.Parse(revision)) ?? throw new ArgumentException("Package version does not exist");
    }
    private void Write(string id,ulong revision,string document,CatalogLease? supplied = null)
    {
        using var catalog = policy.OpenCatalog();
        bool temporary = supplied is null && !leases.TryGetValue(id,out supplied);
        supplied ??= catalog.TryAcquireLease(CatalogKind.Audit,id,owner,LeaseDuration)
            ?? throw new UniversalDataException((int)StatusCode.Busy,"Catalog object is owned by another dispatcher");
        try { catalog.CompareExchange(CatalogKind.Audit,id,checked(revision-1),document,supplied); }
        finally { if (temporary) { try { catalog.ReleaseLease(supplied); } catch(UniversalDataException error) when(error.Code==StatusCode.Fenced) { } } }
    }
    private void Save(RunState value,CatalogLease? supplied = null)
    {
        Write("run/"+value.Id,value.Revision,JsonSerializer.Serialize(value with { Id="run/"+value.Id },HostPolicy.JsonOptions),supplied);
        states[value.Id] = value;
    }
    private void Save(JobState value)
    {
        Write("job/"+value.Id,value.Revision,JsonSerializer.Serialize(value with { Id="job/"+value.Id },HostPolicy.JsonOptions)); jobs[value.Id] = value;
    }
    private void Save(DeploymentState value)
    {
        Write("deployment/"+value.Id,value.Revision,JsonSerializer.Serialize(value with { Id="deployment/"+value.Id },HostPolicy.JsonOptions)); deployments[value.Id] = value;
    }
    public DeploymentState Deploy(HostIdentity identity,DeploymentRequest request)
    {
        identity.Require("deployment.write",request.Id); identity.Require("package.deploy",request.PackageId);
        _ = Package(request.PackageId,request.PackageRevision);
        lock(gate) {
            Refresh();
            using var catalog = policy.OpenCatalog(); var revision = checked(catalog.LatestRevision(CatalogKind.Deployment,request.Id) + 1);
            var projectId = "deployment-project/" + request.Id;
            catalog.Put(CatalogKind.Project,projectId,revision,JsonSerializer.Serialize(new { version = ProductInfo.Protocol,id = projectId,
                packages = new[] { new { id = request.PackageId,revision = ulong.Parse(request.PackageRevision) } } },HostPolicy.JsonOptions));
            catalog.Put(CatalogKind.Deployment,request.Id,revision,JsonSerializer.Serialize(new { version = ProductInfo.Protocol,id = request.Id,
                project = new { id = projectId,revision } },HostPolicy.JsonOptions));
            var state = new DeploymentState(ProductInfo.Protocol,request.Id,request.PackageId,request.PackageRevision,revision.ToString(System.Globalization.CultureInfo.InvariantCulture),
                true,Now,checked((deployments.GetValueOrDefault(request.Id)?.Revision ?? 0) + 1));
            Save(state); return state;
        }
    }
    public async Task<DeploymentState> ActivateDeployment(HostIdentity identity,string id,bool enabled)
    {
        identity.Require("deployment.manage",id); DeploymentState state; string[] running;
        lock(gate) {
            Refresh();
            state = deployments.GetValueOrDefault(id) ?? throw new ArgumentException("Deployment does not exist");
            state = state with { Enabled = enabled,TimestampMs = Now,Revision = checked(state.Revision + 1) }; Save(state);
            running = enabled ? [] : states.Values.Where(run => run.ParentId is null && run.DeploymentId == id && run.State == "Running").Select(run => run.Id).ToArray();
            if (!enabled) running = states.Values.Where(run => run.ParentId is null && run.DeploymentId == id && run.State is "Queued" or "Claimed" or "Running").Select(run => run.Id).ToArray();
        }
        foreach(string ticket in running) await CancelProcess(ticket); return state;
    }
    private void CheckDeployment(string? id,string? revision,string packageId,string packageRevision)
    {
        if ((id is null) != (revision is null)) throw new ArgumentException("Deployment identity and revision must be supplied together");
        if (id is not null && (!deployments.TryGetValue(id,out var state) || !state.Enabled || state.DeploymentRevision != revision || state.PackageId != packageId || state.PackageRevision != packageRevision))
            throw new ArgumentException("Deployment binding is inactive or stale");
    }
    public JobState SaveJob(HostIdentity identity,JobRequest request)
    {
        identity.Require("job.write",request.Id); identity.Require("package.execute",request.PackageId);
        if (request.Version != ProductInfo.Protocol || !HostIdentity.ValidId(request.Id) || request.EverySeconds == 0 || request.MaxConcurrency == 0 ||
            request.MaxConcurrency > policy.MaxConcurrency || request.MaxAttempts is < 1 or > 10 || request.RetryDelaySeconds > 86400) throw new ArgumentException("Invalid job policy");
        _ = WorkerParallelism.Read(Package(request.PackageId,request.PackageRevision), policy.MaxConcurrency);
        lock(gate) {
            Refresh();
            string reference;
            if (request.Parameters is not null) reference = ParametersReference(request.Parameters);
            else if (jobs.TryGetValue(request.Id,out var existing) && request.ParametersReference == existing.ParametersReference) reference = existing.ParametersReference;
            else reference = "";
            CheckDeployment(request.DeploymentId,request.DeploymentRevision,request.PackageId,request.PackageRevision);
            if (request.DeploymentId is not null) identity.Require("deployment.execute",request.DeploymentId);
            var job = new JobState(ProductInfo.Protocol,request.Id,request.PackageId,request.PackageRevision,reference,request.Enabled,request.EverySeconds,
                request.NextDueMs == 0 ? Now : request.NextDueMs,identity.Subject,identity.Stamp,request.MaxConcurrency,request.MaxAttempts,request.RetryDelaySeconds,Now,
                checked((jobs.GetValueOrDefault(request.Id)?.Revision ?? 0) + 1),request.DeploymentId,request.DeploymentRevision);
            Save(job); return job;
        }
    }
    private string ParametersReference(string parameters)
    {
        using var document = JsonDocument.Parse(parameters);
        return document.RootElement.ValueKind == JsonValueKind.Object && !document.RootElement.EnumerateObject().Any()
            ? "" : new ParameterVault(policy).Store(parameters);
    }

    private string ControlReference() => new ParameterVault(policy).Store(JsonSerializer.Serialize(new { token = Convert.ToHexString(RandomNumberGenerator.GetBytes(32)) }));
    public RunState Enqueue(HostIdentity identity,RunRequest request)
    {
        identity.Require("package.execute",request.PackageId);
        var parallelism = WorkerParallelism.Read(Package(request.PackageId,request.PackageRevision), policy.MaxConcurrency);
        if (request.Debug is not null && parallelism is { } plan &&
            (plan.WorkerProcesses != 1 || plan.ConsumerLanesPerWorker != 1 || plan.ThreadsPerWorker != 1))
            throw new ArgumentException("Debug requires a single Worker and consumer lane");
        if (request.Debug is not null) identity.Require("package.debug",request.PackageId);
        if (request.DeploymentId is not null) identity.Require("deployment.execute",request.DeploymentId);
        var reference = ParametersReference(request.Parameters);
        lock(gate) {
            Refresh();
            CheckDeployment(request.DeploymentId,request.DeploymentRevision,request.PackageId,request.PackageRevision);
            var state = new RunState(ProductInfo.Protocol,Guid.NewGuid().ToString("N"),request.PackageId,request.PackageRevision,reference,"Queued",identity.Subject,identity.Stamp,ControlReference(),
                TimestampMs:Now,Debug:request.Debug?.Clone(),DeploymentId:request.DeploymentId,DeploymentRevision:request.DeploymentRevision, Parallelism: parallelism); Save(state); return state;
        }
    }
    public RunState Resume(HostIdentity identity,string id,bool brokerRestore = false)
    {
        identity.Require("execution.resume",id);
        lock(gate) {
            var state = Get(id); identity.Require("package.execute",state.PackageId);
            using var catalog = policy.OpenCatalog();
            if (state.ParentId is not null || state.State != "RecoveryRequired" || workers.ContainsKey(id) || remoteWorkers.ContainsKey(id) || Alive(state) ||
                catalog.LeaseActive(CatalogKind.Audit,"run/"+id) ||
                state.MemberIds is null && state.ExecutionId.Length == 0 ||
                state.MemberIds is { } members && members.Any(member => catalog.LeaseActive(CatalogKind.Audit,"run/"+member) ||
                    states.TryGetValue(member, out var prior) && Alive(prior)))
                throw new ArgumentException("Execution is not ready for recovery on this node");
            if (brokerRestore) {
                string package = Package(state.PackageId, state.PackageRevision);
                bool stages = state.MemberIds is not null && WorkerParallelism.UsesKafkaStages(package);
                bool single = state.MemberIds is null && state.Parallelism is { WorkerProcesses: 1 } &&
                    state.Placement is { } placement && (placement.Remote || state.BrokerRestore) &&
                    state.ExecutionId.Length != 0 && WorkerParallelism.UsesBrokerRecovery(package);
                if (!(stages || single) || state.ParametersReference.Length != 0 || state.Debug is not null)
                    throw new ArgumentException("Execution does not support Kafka Broker recovery");
            } else if (state.BrokerRestore || state.HomeNodeId is { } home && home != owner) {
                throw new ArgumentException("Execution requires recovery on its journal node or an eligible Broker recovery");
            }
            var executionRevision = state.ExecutionId.Length == 0 ? 0 : catalog.LatestRevision(CatalogKind.Execution,state.ExecutionId);
            long safeAfter = state.MemberIds is { } group
                ? group.Select(member => states.GetValueOrDefault(member)?.RecoverySafeAfterMs ?? 0).DefaultIfEmpty().Max()
                : state.RecoverySafeAfterMs;
            state = state with { State = "Queued",Failure=null,Resume = true,ExecutionRevision = executionRevision,
                BrokerRestore=brokerRestore, JournalTicket=brokerRestore ? Guid.NewGuid().ToString("N") : state.JournalTicket,
                HomeNodeId=brokerRestore ? null : state.HomeNodeId,
                NotBeforeMs=Math.Max(Now,safeAfter),
                Subject = identity.Subject,SubjectStamp = identity.Stamp,Owner = null,ProcessId = null,Attempt=checked(state.Attempt+1),Revision = checked(state.Revision + 1),TimestampMs = Now };
            Save(state); return state;
        }
    }
    private string Token(RunState state) { using var value = JsonDocument.Parse(new ParameterVault(policy).Resolve(state.ControlReference)); return value.RootElement.GetProperty("token").GetString()!; }
    public async Task Cancel(HostIdentity identity,string id) { identity.Require("execution.cancel",id); await CancelProcess(id); }
    private async Task CancelProcess(string id)
    {
        var state = Get(id);
        if (state.ParentId is not null) throw new ArgumentException("Execution was not found");
        using(var catalog=policy.OpenCatalog()) PutControl(catalog,"cancel",state);
        if (state.State == "Running" && remoteWorkers.TryGetValue(id, out var remote)) {
            try { await remote.CancelAsync(CancellationToken.None); } catch (IOException) { }
        } else if (state.State == "Running" && Alive(state)) {
            try { await WorkerControl.Send(policy.ControlDirectory(id),id,Token(state),"cancel"); } catch (IOException) { }
            if (workers.TryGetValue(id,out var process)) {
                _ = Task.Run(async () => { await Task.Delay(TimeSpan.FromSeconds(policy.CancellationGraceSeconds));
                    try { if (!process.HasExited) await WorkerLauncher.Terminate(process,id,policy.RequiresIsolation(Package(Get(id).PackageId,Get(id).PackageRevision))); } catch (InvalidOperationException) { } });
            }
        } else lock(gate) { state = Get(id);using var catalog=policy.OpenCatalog();
            if (state.State is "Queued" or "Claimed" && !catalog.LeaseActive(CatalogKind.Audit,"run/"+id))
                Save(state with { State = "Cancelled",Revision = checked(state.Revision + 1),TimestampMs = Now }); }
    }
    public async Task<JsonElement> Debug(HostIdentity identity,string id,JsonElement? command = null)
    {
        identity.Require("execution.debug",id); var state = Get(id);
        if (state.ParentId is not null) throw new ArgumentException("Execution was not found");
        using var catalog=policy.OpenCatalog();
        if(state.Debug is not null && command is null && state.State is not ("Running" or "Claimed")) {
            var revision=catalog.LatestRevision(CatalogKind.Audit,"debug/"+id);if(revision!=0){using var snapshot=JsonDocument.Parse(catalog.Get(CatalogKind.Audit,"debug/"+id,revision)!);return snapshot.RootElement.GetProperty("snapshot").Clone();}
        }
        if (state.State != "Running" || state.Debug is null || !catalog.LeaseActive(CatalogKind.Audit,"run/"+id)) throw new ArgumentException("Execution is not an active debug session");
        if (remoteWorkers.TryGetValue(id, out var remote))
            return await remote.SendControlAsync(command is null ? "snapshot" : "debug", command, CancellationToken.None);
        if (!Alive(state)) throw new ArgumentException("Execution is not an active debug session");
        return await WorkerControl.Send(policy.ControlDirectory(id),id,Token(state),command is null ? "snapshot" : "debug",command);
    }
    public async Task Stop(HostIdentity identity,string id)
    {
        identity.Require("execution.cancel",id);var state=Get(identity,id) ?? throw new ArgumentException("Execution was not found");
        using var catalog=policy.OpenCatalog();
        if (state.State!="Running" || !catalog.LeaseActive(CatalogKind.Audit,"run/"+id)) throw new ArgumentException("Execution is not running");
        if (state.MemberIds is not null || state.Placement is { Remote: true }) {
            PutControl(catalog,"stop",state);
            return;
        }
        if (!Alive(state)) throw new ArgumentException("Execution is not running");
        await WorkerControl.Send(policy.ControlDirectory(id),id,Token(state),"stop");
    }
    public Task<JsonElement> Metrics(HostIdentity identity,string id)
    {
        identity.Require("execution.read",id);var state=Get(identity,id) ?? throw new ArgumentException("Execution was not found");
        using var catalog=policy.OpenCatalog();
        if (state.MemberIds is not null) {
            var members = Members(identity, id).Select(member => {
                ulong latest = member.ExecutionId.Length == 0 ? 0 : catalog.LatestRevision(CatalogKind.Execution, member.ExecutionId);
                return new { member.Id, member.Shard, member.State, member.Result, member.Placement,
                    metrics = latest == 0 ? (JsonElement?)null : JsonSerializer.Deserialize<JsonElement>(
                        catalog.Get(CatalogKind.Execution, member.ExecutionId, latest)!) };
            }).ToArray();
            return Task.FromResult(JsonSerializer.SerializeToElement(new { metrics = new { }, progress = new { pipelines = Array.Empty<object>() }, members }, HostPolicy.JsonOptions));
        }
        if(state.ExecutionId.Length==0)return Task.FromResult(JsonSerializer.SerializeToElement(new {metrics=new {},progress=new {pipelines=Array.Empty<object>()}},HostPolicy.JsonOptions));
        var revision=catalog.LatestRevision(CatalogKind.Execution,state.ExecutionId);if(revision==0)throw new IOException("Native execution facts are unavailable");
        using var record=JsonDocument.Parse(catalog.Get(CatalogKind.Execution,state.ExecutionId,revision)!);
        return Task.FromResult(JsonSerializer.SerializeToElement(new {metrics=record.RootElement.Clone(),progress=record.RootElement.TryGetProperty("progress",out var progress)?progress.Clone():(JsonElement?)null,diagnostics=record.RootElement.TryGetProperty("diagnostics",out var details)?details.Clone():(JsonElement?)null},HostPolicy.JsonOptions));
    }
    private static bool Alive(RunState state)
    {
        if (state.Placement is { Remote: true } || state.ProcessId is not { } id) return false;
        try { using var process = Process.GetProcessById(id); return !process.HasExited && state.ProcessStartedAtMs == new DateTimeOffset(process.StartTime.ToUniversalTime()).ToUnixTimeMilliseconds(); }
        catch(ArgumentException) { return false; }
    }
    private void Refresh()
    {
        using var catalog = policy.OpenCatalog(); string? afterId = null; ulong afterRevision = 0;
        while(true) {
            var query = new Dictionary<string,object> { ["kind"] = (int)CatalogKind.Audit,["limit"] = 100,["latestOnly"]=true };
            if (afterId is not null) { query["afterId"] = afterId; query["afterRevision"] = afterRevision; }
            using var page = JsonDocument.Parse(catalog.Query(JsonSerializer.Serialize(query,HostPolicy.JsonOptions)));
            if (page.RootElement.GetArrayLength() == 0) break;
            foreach(var entry in page.RootElement.EnumerateArray()) {
                afterId = entry.GetProperty("id").GetString()!; afterRevision = entry.GetProperty("revision").GetUInt64(); var document = entry.GetProperty("document");
                if (afterId.StartsWith("run/",StringComparison.Ordinal)) {
                    var state = document.Deserialize<RunState>(HostPolicy.JsonOptions)!;
                    if (state.RecordVersion != 2 || state.ParametersReference is null || state.Subject is null) throw new InvalidDataException("Legacy run requires explicit credential migration");
                    state = state with { Id = state.Id[4..] }; if (!states.TryGetValue(state.Id,out var prior) || prior.Revision < state.Revision) states[state.Id] = state;
                } else if (afterId.StartsWith("job/",StringComparison.Ordinal)) {
                    var job = document.Deserialize<JobState>(HostPolicy.JsonOptions)!;
                    if (job.RecordVersion != 2 || job.ParametersReference is null || job.Subject is null) throw new InvalidDataException("Legacy job requires explicit credential migration");
                    job = job with { Id = job.Id[4..] }; if (!jobs.TryGetValue(job.Id,out var prior) || prior.Revision < job.Revision) jobs[job.Id] = job;
                } else if (afterId.StartsWith("deployment/",StringComparison.Ordinal)) {
                    var state = document.Deserialize<DeploymentState>(HostPolicy.JsonOptions)!; state = state with { Id = state.Id[11..] };
                    if (!deployments.TryGetValue(state.Id,out var prior) || prior.Revision < state.Revision) deployments[state.Id] = state;
                }
            }
        }
    }
    private void Reconcile()
    {
        using var catalog=policy.OpenCatalog();
        foreach(var state in states.Values.Where(run => run.State is "Claimed" or "Running" && !workers.ContainsKey(run.Id) &&
            !remoteWorkers.ContainsKey(run.Id) && !monitors.ContainsKey(run.Id) &&
            (run.ParentId is null || !monitors.ContainsKey(run.ParentId))).ToArray()) {
            var lease=catalog.TryAcquireLease(CatalogKind.Audit,"run/"+state.Id,owner,LeaseDuration);
            if (lease is null) continue;
            try {
                Refresh();var current=states[state.Id];
                if (current.State is "Claimed" or "Running")
                    Save(current with { State="RecoveryRequired",Result=(int)StatusCode.CommitUnknown,Owner=null,
                        Revision=checked(current.Revision+1),TimestampMs=Now },lease);
            } finally { catalog.ReleaseLease(lease); }
        }
    }
    private void Schedule()
    {
        foreach(var job in jobs.Values.Where(job => job.Enabled && job.NextDueMs <= Now).ToArray()) {
            using var catalog=policy.OpenCatalog();
            var lease=catalog.TryAcquireLease(CatalogKind.Audit,"job/"+job.Id,owner,LeaseDuration);
            if (lease is null) continue;
            leases[lease.Id]=lease;
            try {
            Refresh();
            if (jobs[job.Id].Revision!=job.Revision) continue;
            if (states.Values.Count(run => run.ParentId is null && run.JobId == job.Id && run.State is "Queued" or "Claimed" or "Running") >= job.MaxConcurrency) continue;
            if (identities.Find(job.Subject,job.SubjectStamp) is not { } identity || !identity.Allows("package.execute",job.PackageId)) {
                Save(job with { Enabled = false,TimestampMs = Now,Revision = checked(job.Revision + 1) }); continue;
            }
            if (job.DeploymentId is not null && (!deployments.TryGetValue(job.DeploymentId,out var deployment) || !deployment.Enabled || deployment.DeploymentRevision != job.DeploymentRevision)) continue;
            var digest = SHA256.HashData(System.Text.Encoding.UTF8.GetBytes($"{policy.Tenant}/{job.Id}/{job.Revision}/{job.NextDueMs}"));
            var id = Convert.ToHexString(digest.AsSpan(0,16)).ToLowerInvariant();
            if (!states.ContainsKey(id)) Save(new RunState(ProductInfo.Protocol,id,job.PackageId,job.PackageRevision,job.ParametersReference,"Queued",job.Subject,job.SubjectStamp,ControlReference(),
                TimestampMs:job.NextDueMs,JobId:job.Id,DeploymentId:job.DeploymentId,DeploymentRevision:job.DeploymentRevision,
                Parallelism:WorkerParallelism.Read(Package(job.PackageId,job.PackageRevision),policy.MaxConcurrency)));
            Save(job with { NextDueMs = checked(Now + job.EverySeconds * 1000L),TimestampMs = Now,Revision = checked(job.Revision + 1) });
            } finally { leases.TryRemove(lease.Id,out _);catalog.ReleaseLease(lease); }
        }
    }
    public async Task Run(CancellationToken cancellation)
    {
        lock(gate) Refresh();
        try {
            while(!cancellation.IsCancellationRequested) {
                RunState[] queued;
                lock(gate) { Refresh();Reconcile(); Schedule(); queued = states.Values.Where(run => run.ParentId is null && run.State == "Queued" && run.NotBeforeMs <= Now).OrderBy(run => run.TimestampMs).ToArray(); }
                foreach(var state in queued) {
                    if (monitors.Count >= policy.MaxConcurrency) break;
                    if (monitors.ContainsKey(state.Id)) continue;
                    var task = Dispatch(state,cancellation); monitors[state.Id] = task;
                    _ = task.ContinueWith(_ => monitors.TryRemove(state.Id,out var ignored),TaskScheduler.Default);
                }
                await Task.Delay(250,cancellation);
            }
        } finally {
            await Task.WhenAll(monitors.Values);
        }
    }
    private async Task Dispatch(RunState run, CancellationToken cancellation)
    {
        if (run.HomeNodeId is { } home && home != owner)
            return;
        var plan = run.Parallelism;
        bool remote = plan is not null && run.ParametersReference.Length == 0 && run.Debug is null;
        if (plan is null || plan.WorkerProcesses == 1) {
            using var reservation = reservations.TryReserve(policy.Tenant, plan, remote, 1,
                run.BrokerRestore ? null : [run.Placement], run.BrokerRestore ? run.Placement : null);
            if (reservation is not null)
                await StartWorker(run.Id, reservation.Placements[0], cancellation);
            return;
        }
        await DispatchGroup(run, plan, remote, cancellation);
    }

    private async Task DispatchGroup(RunState run, WorkerParallelism plan, bool allowRemote,
        CancellationToken cancellation)
    {
        string[] memberIds = run.MemberIds ?? Enumerable.Range(0, checked((int)plan.WorkerProcesses)).Select(index =>
            Convert.ToHexString(SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(
                $"{policy.Tenant}/{run.Id}/{index}")).AsSpan(0, 16)).ToLowerInvariant()).ToArray();
        int[] pending;
        using var catalog = policy.OpenCatalog();
        lock (gate) {
            Refresh();
            pending = Enumerable.Range(0, memberIds.Length).Where(index =>
                !states.TryGetValue(memberIds[index], out var prior) || prior.State != "Succeeded").ToArray();
        }
        WorkerPlacement?[] pins;
        lock (gate) pins = pending.Select(index => run.BrokerRestore ? null : states.GetValueOrDefault(memberIds[index])?.Placement).ToArray();
        using var reservation = reservations.TryReserve(policy.Tenant, plan, allowRemote, pending.Length, pins);
        if (reservation is null)
            return;
        var lease = catalog.TryAcquireLease(CatalogKind.Audit, "run/" + run.Id, owner, LeaseDuration);
        if (lease is null)
            return;
        using var group = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
        group.CancelAfter(TimeSpan.FromSeconds(policy.ExecutionTimeoutSeconds + policy.CancellationGraceSeconds));
        using var heartbeatStop = CancellationTokenSource.CreateLinkedTokenSource(group.Token);
        var heartbeat = HoldGroupLease(lease, group, heartbeatStop.Token);
        var running = new List<Task<RunState>>(pending.Length);
        var memberClaims = new List<CatalogLease>(pending.Length);
        int handedOff = 0;
        try
        {
            lock (gate)
            {
                run = Get(run.Id);
                if (run.State != "Queued" || run.HomeNodeId is { } home && home != owner)
                    return;
                if (Control(catalog, run) == "cancel") {
                    Save(run with { State="Cancelled", Result=(int)StatusCode.Cancelled,
                        Revision=checked(run.Revision+1),TimestampMs=Now }, lease);
                    return;
                }
                CheckDeployment(run.DeploymentId, run.DeploymentRevision, run.PackageId, run.PackageRevision);
                run = PrepareMembers(catalog, lease, run, plan, memberIds, pending, reservation.Placements);
            }

            foreach (int index in pending)
            {
                var claim = catalog.TryAcquireLease(CatalogKind.Audit, "run/" + memberIds[index], owner,
                    LeaseDuration, "workers", policy.MaxConcurrency);
                if (claim is null)
                {
                    lock (gate)
                    {
                        var parent = Get(run.Id);
                        Save(parent with { State="Queued", Owner=null, Revision=checked(parent.Revision+1),
                            TimestampMs=Now, NotBeforeMs=checked(Now+1000) }, lease);
                    }
                    return;
                }
                memberClaims.Add(claim);
            }

            var barrier = new WorkerStartBarrier(pending.Length, () =>
            {
                lock (gate)
                {
                    var parent = Get(run.Id);
                    CheckDeployment(parent.DeploymentId, parent.DeploymentRevision, parent.PackageId, parent.PackageRevision);
                    if (Control(catalog, parent) is not null)
                        throw new OperationCanceledException("The run was stopped before its Workers were ready");
                    Save(parent with { State = "Running", Revision = checked(parent.Revision + 1), TimestampMs = Now }, lease);
                }
            });
            for (int slot = 0; slot < pending.Length; ++slot)
            {
                ++handedOff;
                running.Add(AwaitMember(memberIds[pending[slot]], reservation.Placements[slot], memberClaims[slot]));
            }
            while (running.Count != 0)
            {
                var completed = await Task.WhenAny(running);
                running.Remove(completed);
                var member = await completed;
                if (running.Count != 0 && member.State is "Failed" or "RecoveryRequired")
                    PutControl(catalog, "stop", run);
            }

            lock (gate) SaveGroupOutcome(catalog, lease, run, memberIds);

            async Task<RunState> AwaitMember(string id, WorkerPlacement placement, CatalogLease claim)
            {
                try { await StartWorker(id, placement, group.Token, barrier, claim); }
                finally { barrier.Fail(); }
                return Get(id);
            }
        }
        catch (Exception error) when (error is not OutOfMemoryException)
        {
            await group.CancelAsync();
            try { await Task.WhenAll(running); }
            catch (Exception remaining) when (remaining is not OutOfMemoryException) { }
            try {
                lock (gate) {
                    var current = Get(run.Id);
                    if (current.State is "Queued" or "Claimed" or "Running") {
                        bool unknown = current.State == "Running" || current.Resume;
                        Save(current with { State = unknown ? "RecoveryRequired" : "Failed",
                            Result = unknown ? (int)StatusCode.CommitUnknown : (int)StatusCode.Io,
                            Failure = new RunFailure((int)StatusCode.Io, "dispatch.group", error.GetType().Name),
                            Revision = checked(current.Revision + 1), TimestampMs = Now }, lease);
                    }
                }
            } catch (UniversalDataException fenced) when (fenced.Code == StatusCode.Fenced) { }
        }
        finally
        {
            await heartbeatStop.CancelAsync();
            await heartbeat;
            for (int slot = handedOff; slot < memberClaims.Count; ++slot)
                try { catalog.ReleaseLease(memberClaims[slot]); }
                catch (UniversalDataException fenced) when (fenced.Code == StatusCode.Fenced) { }
            try { catalog.ReleaseLease(lease); }
            catch (UniversalDataException fenced) when (fenced.Code == StatusCode.Fenced) { }
        }
    }

    private RunState PrepareMembers(Catalog catalog, CatalogLease lease, RunState run, WorkerParallelism plan,
        string[] memberIds, int[] pending, WorkerPlacement[] placements)
    {
        string? stageRunId = WorkerParallelism.UsesKafkaStages(Package(run.PackageId, run.PackageRevision)) ? run.Id : null;
        run = run with { State = "Claimed", Owner = owner, HomeNodeId = owner, MemberIds = memberIds,
            Revision = checked(run.Revision + 1), TimestampMs = Now };
        Save(run, lease);
        for (int slot = 0; slot < pending.Length; ++slot)
        {
            int index = pending[slot];
            string id = memberIds[index];
            var placement = placements[slot];
            var previous = states.GetValueOrDefault(id);
            var shard = new WorkerShard((uint)index, plan.WorkerProcesses, plan.ConsumerLanesPerWorker, stageRunId);
            var member = previous is null
                ? new RunState(ProductInfo.Protocol, id, run.PackageId, run.PackageRevision, run.ParametersReference,
                    "Queued", run.Subject, run.SubjectStamp, ControlReference(), TimestampMs: Now,
                    DeploymentId: run.DeploymentId, DeploymentRevision: run.DeploymentRevision,
                    Attempt: run.Attempt, Parallelism: plan, ParentId: run.Id,
                    Shard: shard, Placement: placement, HomeNodeId: owner)
                : previous with { State = "Queued", Subject = run.Subject, SubjectStamp = run.SubjectStamp,
                    Attempt = run.Attempt, Resume = previous.ExecutionId.Length != 0, Failure = null,
                    BrokerRestore = run.BrokerRestore && previous.ExecutionId.Length != 0,
                    JournalTicket = run.BrokerRestore && previous.ExecutionId.Length != 0
                        ? Guid.NewGuid().ToString("N") : previous.JournalTicket,
                    ExecutionRevision = previous.ExecutionId.Length == 0 ? 0 : catalog.LatestRevision(CatalogKind.Execution, previous.ExecutionId),
                    Owner = null, ProcessId = null, Shard = shard, Placement = placement, Revision = checked(previous.Revision + 1),
                    TimestampMs = Now };
            Save(member);
        }
        return run;
    }

    private void SaveGroupOutcome(Catalog catalog, CatalogLease lease, RunState run, string[] memberIds)
    {
        var parent = Get(run.Id);
        var members = memberIds.Select(id => states[id]).ToArray();
        var uncertain = members.FirstOrDefault(member => member.State is "RecoveryRequired" or "Claimed" or "Running");
        var failed = members.FirstOrDefault(member => member.State == "Failed");
        bool wasCancelled = catalog.Get(CatalogKind.Audit, "cancel/" + run.Id, parent.Attempt) is not null;
        bool wasStopped = catalog.Get(CatalogKind.Audit, "stop/" + run.Id, parent.Attempt) is not null;
        string final = uncertain is not null ? "RecoveryRequired" :
            wasCancelled ? "Cancelled" : failed is not null ? "Failed" :
            wasStopped || members.Any(member => member.State == "Stopped") ? "Stopped" :
            members.All(member => member.State == "Succeeded" && member.Result == 0) ? "Succeeded" : "Failed";
        int result = final switch {
            "RecoveryRequired" => (int)StatusCode.CommitUnknown,
            "Cancelled" => (int)StatusCode.Cancelled,
            "Failed" => failed?.Result ?? (int)StatusCode.Busy,
            _ => 0
        };
        var outcome = parent with { State = final, Result = result, Failure = uncertain?.Failure ?? failed?.Failure,
            BrokerRestore = parent.BrokerRestore && final != "Succeeded",
            Revision = checked(parent.Revision + 1), TimestampMs = Now };
        if (final == "Failed" && parent.JobId is { } jobId && jobs.TryGetValue(jobId, out var job) &&
            job.Enabled && parent.Attempt < job.MaxAttempts)
            outcome = outcome with { State="Queued", Resume=true, Attempt=checked(parent.Attempt+1),
                NotBeforeMs=checked(Now+job.RetryDelaySeconds*1000L), Owner=null };
        Save(outcome, lease);
    }

    private async Task HoldGroupLease(CatalogLease lease, CancellationTokenSource group, CancellationToken cancellation)
    {
        try
        {
            while (true)
            {
                await Task.Delay(TimeSpan.FromSeconds(5), cancellation);
                using var catalog = policy.OpenCatalog();
                lease = catalog.RenewLease(lease, LeaseDuration);
            }
        }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested) { }
        catch (Exception error) when (error is UniversalDataException or IOException or InvalidOperationException)
        {
            await group.CancelAsync();
        }
    }

    private static string? Control(Catalog catalog, RunState run)
    {
        string id = run.ParentId ?? run.Id;
        return catalog.Get(CatalogKind.Audit, "cancel/" + id, run.Attempt) is not null ? "cancel" :
            catalog.Get(CatalogKind.Audit, "stop/" + id, run.Attempt) is not null ? "stop" : null;
    }

    private static void PutControl(Catalog catalog, string action, RunState run)
    {
        string id = action + "/" + run.Id;
        catalog.Put(CatalogKind.Audit, id, run.Attempt, JsonSerializer.Serialize(
            new { version = ProductInfo.Protocol, id, attempt = run.Attempt }, HostPolicy.JsonOptions));
    }

    private async Task StartWorker(string id, WorkerPlacement placement, CancellationToken cancellation,
        WorkerStartBarrier? barrier = null, CatalogLease? reserved = null)
    {
        RunState state;
        CatalogLease lease;
        lock(gate) {
            state = Get(id);
            using var catalog=policy.OpenCatalog();
            if (state.State != "Queued") {
                if (reserved is not null) catalog.ReleaseLease(reserved);
                return;
            }
            var acquired=reserved ?? catalog.TryAcquireLease(CatalogKind.Audit,"run/"+id,owner,LeaseDuration,"workers",policy.MaxConcurrency);
            if (acquired is null) { barrier?.Fail(); return; }
            lease=acquired;leases[lease.Id]=lease;
            try {
            state=Get(id);
            if (state.State!="Queued") { barrier?.Fail();leases.TryRemove(lease.Id,out _);catalog.ReleaseLease(lease);return; }
            if (Control(catalog, state) is { } action) {
                Save(state with { State=action=="cancel"?"Cancelled":"Stopped", Result=action=="cancel"?(int)StatusCode.Cancelled:0,
                    Revision=checked(state.Revision+1),TimestampMs=Now });
                barrier?.Fail(); leases.TryRemove(lease.Id,out _);catalog.ReleaseLease(lease);return;
            }
            CheckDeployment(state.DeploymentId,state.DeploymentRevision,state.PackageId,state.PackageRevision);
            state = state with { State = "Claimed",Owner = owner,Placement = state.Parallelism is null ? null : placement,
                HomeNodeId = state.Parallelism is null ? null : owner,Revision = checked(state.Revision + 1),TimestampMs = Now }; Save(state);
            } catch (ArgumentException error) {
                try {
                    Save(state with { State=state.Resume?"RecoveryRequired":"Failed",
                        Result=state.Resume?(int)StatusCode.CommitUnknown:(int)StatusCode.Invalid,
                        Failure=new RunFailure((int)StatusCode.Invalid,"dispatch.deployment",error.GetType().Name),
                        Revision=checked(state.Revision+1),TimestampMs=Now });
                } finally { barrier?.Fail(); leases.TryRemove(lease.Id,out _);catalog.ReleaseLease(lease); }
                return;
            } catch { leases.TryRemove(lease.Id,out _);catalog.ReleaseLease(lease);throw; }
        }
        Process? process = null;
        RemoteWorkerSession? remote = null;
        bool startSent = false, isolated = false, terminalRecorded = false;
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellation); deadline.CancelAfter(TimeSpan.FromSeconds(policy.ExecutionTimeoutSeconds + policy.CancellationGraceSeconds));
        using var heartbeatStop=CancellationTokenSource.CreateLinkedTokenSource(deadline.Token);
        int cancelled=0;
        var controlState = state;
        var remoteState = new TaskCompletionSource<RemoteWorkerSession>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var pipeWriter = new SemaphoreSlim(1, 1);
        async Task SendPipe(string command, CancellationToken token) {
            if (remoteState.Task.IsCompletedSuccessfully) { await remoteState.Task.Result.SendAsync(command, token);return; }
            if (workers.TryGetValue(id, out var local)) {
                await pipeWriter.WaitAsync(token);
                try {
                    await local.StandardInput.WriteLineAsync(command.AsMemory(), token);
                    await local.StandardInput.FlushAsync(token);
                } finally { pipeWriter.Release(); }
            }
        }
        async Task HoldLease() {
            try {
                uint tick=0;
                while(true) {
                    await Task.Delay(1000,heartbeatStop.Token);
                    using var catalog=policy.OpenCatalog();
                    string? action = Control(catalog, controlState);
                    int signal = action == "cancel" ? 2 : action == "stop" ? 1 : 0;
                    if (action is not null && signal > Volatile.Read(ref cancelled)) {
                        Interlocked.Exchange(ref cancelled,signal);
                        if (action == "cancel") await SendPipe(action, heartbeatStop.Token);
                        else if (remoteState.Task.IsCompletedSuccessfully)
                            await remoteState.Task.Result.SendControlAsync(action, null, heartbeatStop.Token);
                        else if (workers.ContainsKey(id))
                            await WorkerControl.Send(policy.ControlDirectory(id), id, Token(state), action, cancellation: heartbeatStop.Token);
                        deadline.CancelAfter(TimeSpan.FromSeconds(policy.CancellationGraceSeconds));
                    }
                    if (++tick%5==0) {
                        leases[lease.Id]=catalog.RenewLease(lease,LeaseDuration);
                        if (remoteState.Task.IsCompletedSuccessfully)
                            await remoteState.Task.Result.RenewAsync(heartbeatStop.Token);
                    }
                }
            } catch(OperationCanceledException) when(heartbeatStop.IsCancellationRequested) { }
              catch(Exception error) when(error is UniversalDataException or IOException or InvalidOperationException or System.Net.WebSockets.WebSocketException) { deadline.Cancel(); }
        }
        var heartbeat=HoldLease();
        try {
            var identity = identities.Find(state.Subject,state.SubjectStamp) ?? throw new UniversalDataException((int)StatusCode.Auth,"Dispatch authority expired");
            identity.Require("package.execute",state.PackageId); if (state.Debug is not null) identity.Require("package.debug",state.PackageId);
            if (state.DeploymentId is not null) identity.Require("deployment.execute",state.DeploymentId);
            var scoped = root.Bind(identity); string package = Package(state.PackageId,state.PackageRevision);
            using var catalog = policy.OpenCatalog(); JsonElement? snapshot = null;
            if (state.DeploymentId is not null) {
                using var documents = JsonDocument.Parse(catalog.BindingSnapshot(state.DeploymentId,ulong.Parse(state.DeploymentRevision!),state.PackageId));
                foreach(var document in documents.RootElement.EnumerateArray())
                    if (document.GetProperty("kind").GetInt32() == (int)CatalogKind.Package) identity.Require("package.execute",document.GetProperty("id").GetString()!);
                snapshot = documents.RootElement.Clone();
            }
            isolated = scoped.RequiresIsolation(package) || snapshot is { } documentsSnapshot && documentsSnapshot.EnumerateArray().Any(document =>
                document.GetProperty("kind").GetInt32() == (int)CatalogKind.Package && scoped.RequiresIsolation(document.GetProperty("document").GetRawText()));
            var budget = state.Parallelism is null ? null : placement.Budget;
            if (budget is not null) scoped = scoped with { MaxConcurrency=budget.CpuSlots,MemoryLimit=budget.NativeMemoryBytes };
            ulong startingRevision = state.BrokerRestore ? catalog.LatestRevision(CatalogKind.Execution,state.ExecutionId) : state.ExecutionRevision;
            var request = new WorkerRequest(ProductInfo.Protocol,id,"execute",scoped,identity,package,state.ParametersReference,state.ExecutionId,
                state.Resume && !state.BrokerRestore,startingRevision,
                state.ExecutionId.Length == 0 ? 0 : catalog.LatestRevision(CatalogKind.Event,state.ExecutionId),state.Debug,ControlReference:state.ControlReference,Isolated:isolated,
                BindingSnapshot:snapshot,DeploymentId:state.DeploymentId,DeploymentRevision:state.DeploymentRevision is null ? 0 : ulong.Parse(state.DeploymentRevision),
                Budget: budget, Shard: state.Shard ?? (state.Parallelism is { } plan && plan.ConsumerLanesPerWorker > 1
                    ? new WorkerShard(0,1,plan.ConsumerLanesPerWorker) : null),
                BrokerRestore:state.BrokerRestore, JournalTicket:state.JournalTicket);
            if (placement.Remote) {
                remote = await remoteNodes.StartAsync(placement.NodeId, request, id, lease.Generation, deadline.Token);
                remoteWorkers[id] = remote;remoteState.TrySetResult(remote);
            } else {
                process = WorkerLauncher.Launch(configuration.WorkerPath,request);
                _ = WorkerProtocol.Discard(process.StandardError,deadline.Token);
                await process.StandardInput.WriteLineAsync(JsonSerializer.Serialize(request,HostPolicy.JsonOptions).AsMemory(), deadline.Token);
                await process.StandardInput.FlushAsync(deadline.Token);
                workers[id] = process;
            }
            var reports = new WorkerReportReader(catalog, state, token => remote is null
                ? WorkerProtocol.ReadLine(process!.StandardOutput,67108864,token) : new ValueTask<string?>(remote.ReadAsync(token)));
            var ready = await reports.ReadAsync("ready", deadline.Token);
            string nativeId = ready.GetProperty("executionId").GetString()!;
            if (!ready.TryGetProperty("controlReady",out var controlReady) || controlReady.ValueKind != JsonValueKind.True)
                throw new InvalidDataException("Worker control endpoint is not ready");
            if (!ready.TryGetProperty("historyComplete",out var history) || history.ValueKind!=JsonValueKind.True ||
                reports.NativeId is { } announced && announced != nativeId) throw new InvalidDataException("Worker Native history is incomplete");
            if (state.Resume && state.ExecutionId != nativeId) throw new InvalidDataException("Recovery changed Native identity");
            var readyRecord = ready.GetProperty("record");
            if (readyRecord.GetProperty("id").GetString() != nativeId || readyRecord.GetProperty("packageId").GetString() != state.PackageId)
                throw new InvalidDataException("Worker ready record differs from dispatch");
            catalog.Put(CatalogKind.Execution,nativeId,ready.GetProperty("revision").GetUInt64(),readyRecord.GetRawText());
            lock(gate) {
                if (Get(id).State != "Claimed" || Get(id).Owner != owner) return;
                CheckDeployment(state.DeploymentId,state.DeploymentRevision,state.PackageId,state.PackageRevision);
                if (Control(catalog, state) is not null) {
                    Interlocked.Exchange(ref cancelled,1);
                    throw new OperationCanceledException("Dispatch was cancelled before Native start");
                }
                state = Get(id) with { State = barrier is null ? "Running" : "Claimed",Failure=null,ExecutionId = nativeId,
                    ProcessId = process?.Id,ProcessStartedAtMs = process is null ? null : new DateTimeOffset(process.StartTime.ToUniversalTime()).ToUnixTimeMilliseconds(),
                    Revision = checked(Get(id).Revision + 1),TimestampMs = Now }; Save(state);
            }
            if (barrier is not null) {
                await barrier.ReadyAsync(deadline.Token);
                lock (gate) {
                    var current = Get(id);
                    state = current with { State="Running",Revision=checked(current.Revision+1),TimestampMs=Now };
                    Save(state);
                }
            }
            startSent = true; await SendPipe("start",deadline.Token);
            var terminal = await reports.ReadAsync("terminal", deadline.Token);
            if (terminal.GetProperty("executionId").GetString() != nativeId) throw new InvalidDataException("Worker terminal identity differs");
            var terminalRecord = terminal.GetProperty("record");
            if (terminalRecord.GetProperty("id").GetString() != nativeId || terminalRecord.GetProperty("packageId").GetString() != state.PackageId)
                throw new InvalidDataException("Worker terminal record differs from dispatch");
            catalog.Put(CatalogKind.Execution,nativeId,terminal.GetProperty("revision").GetUInt64(),terminalRecord.GetRawText());
            if(terminal.TryGetProperty("debug",out var debug)&&debug.ValueKind==JsonValueKind.Object) {
                string debugId="debug/"+id;ulong revision=catalog.LatestRevision(CatalogKind.Audit,debugId)+1;
                catalog.Put(CatalogKind.Audit,debugId,revision,JsonSerializer.Serialize(new {version=ProductInfo.Protocol,id=debugId,snapshot=debug},HostPolicy.JsonOptions));
            }
            int result = terminal.GetProperty("result").GetInt32(); string final = terminal.GetProperty("state").GetString()!;
            if (final is not ("Succeeded" or "Failed" or "Cancelled" or "RecoveryRequired" or "Stopped") ||
                final == "Succeeded" && result != 0) throw new InvalidDataException("Invalid Native terminal state");
            if (state.BrokerRestore && final != "Succeeded")
            {
                final = "RecoveryRequired";
                result = (int)StatusCode.CommitUnknown;
            }
            if (remote is not null) {
                int exit = await remote.WaitForExitAsync(deadline.Token);
                if (final == "Succeeded" && exit != 0)
                    throw new InvalidDataException("Remote Worker exit differs from its Native terminal state");
            }
            lock(gate) {
                var current = Get(id);
                state = current with { State = final,Result = result,
                    BrokerRestore=state.BrokerRestore && final == "RecoveryRequired",
                    RecoverySafeAfterMs=final == "RecoveryRequired" && placement.Remote
                        ? checked((long)leases.GetValueOrDefault(lease.Id,lease).UntilMilliseconds) : 0,
                    Revision = checked(current.Revision + 1),TimestampMs = Now };
                if (final == "Failed" && result != (int)StatusCode.CommitUnknown && state.JobId is { } jobId && jobs.TryGetValue(jobId,out var job) && job.Enabled && state.Attempt < job.MaxAttempts)
                    state = state with { State = "Queued",Resume = true,ExecutionRevision = catalog.LatestRevision(CatalogKind.Execution,nativeId),Attempt = checked(state.Attempt + 1),
                        NotBeforeMs = checked(Now + job.RetryDelaySeconds * 1000L),ProcessId = null,Owner = null };
                Save(state);
            }
            terminalRecorded = true;
            if (remote is null) await process!.WaitForExitAsync(deadline.Token);
        } catch(Exception error) when(error is not OutOfMemoryException) {
            barrier?.Fail();
            bool unknown = startSent || state.BrokerRestore || barrier is null && isolated && process is not null ||
                state.Resume && state.Result==(int)StatusCode.CommitUnknown;
            if (unknown && placement.Remote) {
                await heartbeatStop.CancelAsync();
                await heartbeat;
            }
            var failure=error is WorkerFailureException worker?worker.Failure:new RunFailure(error is UniversalDataException nativeError?nativeError.Status:error is IOException?(int)StatusCode.Io:(int)StatusCode.Invalid,"dispatch",error.GetType().Name);
            try { lock(gate) { var current = Get(id); if (!terminalRecorded && current.State != "Cancelled") Save(current with { State = unknown ? "RecoveryRequired" : Volatile.Read(ref cancelled)!=0 ? "Cancelled" : "Failed",
                Result = unknown ? (int)StatusCode.CommitUnknown : failure.Result,Failure=failure,
                RecoverySafeAfterMs=unknown && placement.Remote
                    ? checked((long)leases.GetValueOrDefault(lease.Id,lease).UntilMilliseconds) : 0,
                Revision = checked(current.Revision + 1),TimestampMs = Now }); } }
            catch(UniversalDataException fenced) when(fenced.Code==StatusCode.Fenced) { /* New owner will reconcile unknown effects. */ }
        } finally {
            barrier?.Fail();
            heartbeatStop.Cancel();await heartbeat;
            try {
                if (remote is not null) await remote.DisposeAsync();
                if (process is not null) {
                    using (process) {
                        if (!process.HasExited) {
                            await WorkerLauncher.Terminate(process,id,isolated);
                            using var killed = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                            await process.WaitForExitAsync(killed.Token);
                        }
                    }
                }
            } finally {
                workers.TryRemove(id,out _);
                remoteWorkers.TryRemove(id,out _);
                leases.TryRemove(lease.Id,out _);
                using var catalog=policy.OpenCatalog();
                try { catalog.ReleaseLease(lease); } catch(UniversalDataException error) when(error.Code==StatusCode.Fenced) { }
            }
        }
    }
}
