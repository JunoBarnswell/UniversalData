using System.Collections.Concurrent;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text.Json;
using UniversalData.Hosting;

namespace UniversalData.Management;

public sealed partial class TenantDispatcher(HostPolicy tenantPolicy, HostPolicy root, ProductConfiguration configuration, IdentityDirectory identityDirectory,
    WorkerNodeDirectory remoteNodes, WorkerReservations reservations, WorkerSupervisor supervisor,
    ClusterLeadership leadership, ILogger<RunDispatcher> logger)
{
    private readonly HostPolicy policy = tenantPolicy;
    private readonly IdentityDirectory identities = identityDirectory;
    private readonly object gate = new();
    private readonly string owner = configuration.Cluster ? configuration.NodeId + "." + Guid.NewGuid().ToString("N") : configuration.NodeId;
    private readonly Dictionary<string,RunState> states = new(StringComparer.Ordinal);
    private readonly Dictionary<string,JobState> jobs = new(StringComparer.Ordinal);
    private readonly Dictionary<string,DeploymentState> deployments = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string,RemoteWorkerSession> remoteWorkers = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string,WorkerSupervisor.Lease> workers = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string,Task> monitors = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string,string> queueReasons = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string,CatalogLease> leases = new(StringComparer.Ordinal);
    private RunState? queueCursor;
    private const uint LeaseDuration = 30000;
    private uint ExecutionLeaseDuration => configuration.Cluster ? 20000U : LeaseDuration;
    private static long Now => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    public RunState[] List(HostIdentity identity) { lock(gate) { Refresh();return states.Values.Where(run => run.ParentId is null && identity.Allows("execution.read",run.Id)).OrderByDescending(run => run.TimestampMs).ToArray(); } }
    public RunStateView View(RunState run) => RunStateView.From(run,
        run.State == "Queued" ? queueReasons.GetValueOrDefault(run.Id) ??
        (run.NotBeforeMs > Now ? "RetryDelay" : "WaitingForWorker") : null, ActiveProcesses(run));
    private int ActiveProcesses(RunState run)
    {
        lock (gate)
            return run.ElasticPlan is null ? 0 : run.ElasticPlan.Groups.Count(group =>
                states.TryGetValue(group.Id,out var member) && member.State == "Running" && member.ProcessId is > 0);
    }
    public RunState? Get(HostIdentity identity,string id)
    {
        identity.Require("execution.read",id);
        lock (gate)
        {
            using var catalog = policy.OpenCatalog();
            return ReadRun(catalog,id) is { ParentId:null } state ? state : null;
        }
    }
    public RunState[] Members(HostIdentity identity, string id)
    {
        identity.Require("execution.read", id);
        lock (gate) {
            using var catalog = policy.OpenCatalog();
            var parent = ReadRun(catalog,id);
            if (parent is null || parent.ParentId is not null) throw new ArgumentException("Execution was not found");
            return parent.MemberIds?.Select(member => ReadRun(catalog,member)).OfType<RunState>().ToArray() ?? [];
        }
    }
    public JobState[] Jobs(HostIdentity identity) { lock(gate) { Refresh();return jobs.Values.Where(job => identity.Allows("job.read",job.Id)).OrderBy(job => job.Id).ToArray(); } }
    public DeploymentState[] Deployments(HostIdentity identity) { lock(gate) { Refresh();return deployments.Values.Where(item => identity.Allows("deployment.read",item.Id)).OrderBy(item => item.Id).ToArray(); } }
    private RunState? ReadRun(Catalog catalog, string id)
    {
        if (!HostIdentity.ValidId(id)) throw new ArgumentException("Invalid execution id");
        string key = "run/" + id;
        var revision = catalog.LatestRevision(CatalogKind.Audit,key);
        if (revision == 0) return null;
        using var document = JsonDocument.Parse(catalog.Get(CatalogKind.Audit,key,revision)!);
        var state = ReadRun(document.RootElement);
        states[id] = state;
        return state;
    }
    private static RunState ReadRun(JsonElement document)
    {
        var state = document.Deserialize<RunState>(HostPolicy.JsonOptions)!;
        if (state.RecordVersion != 2 || state.ParametersReference is null || state.Subject is null)
            throw new InvalidDataException("Run record requires explicit credential migration");
        return state with { Id = state.Id[4..] };
    }
    private RunState Get(string id) { lock(gate) { using var catalog = policy.OpenCatalog(); return ReadRun(catalog,id) ?? throw new ArgumentException("Execution does not exist"); } }
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
        string id = Guid.NewGuid().ToString("N");
        string? fingerprint = null;
        if (policy.Cluster is { } cluster)
        {
            if (request.RequestId is not { } requestId || !HostIdentity.ValidId(requestId))
                throw new ArgumentException("Cluster execution requires a requestId");
            id = Convert.ToHexString(SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(identity.Subject + "\0" + requestId)).AsSpan(0, 16)).ToLowerInvariant();
            fingerprint = cluster.Fingerprint("execution/" + identity.Tenant + "/" + id,
                JsonSerializer.Serialize(request,HostPolicy.JsonOptions));
            lock (gate)
            {
                using var catalog = policy.OpenCatalog();
                if (ReadRun(catalog, id) is { } existing)
                    return SameRequest(existing, fingerprint);
            }
        }
        string packageText = Package(request.PackageId,request.PackageRevision);
        using var nativePackage = new NativePackage(packageText, policy.MemoryLimit);
        bool requiresJournal = nativePackage.RequiresJournal;
        var parallelism = WorkerParallelism.Read(packageText, policy.MaxConcurrency);
        CheckElastic(request.Elastic,packageText,request.Debug);
        if (request.Debug is not null && parallelism is { } plan &&
            (plan.WorkerProcesses != 1 || plan.ConsumerLanesPerWorker != 1 || plan.ThreadsPerWorker != 1))
            throw new ArgumentException("Debug requires a single Worker and consumer lane");
        if (request.Debug is not null) identity.Require("package.debug",request.PackageId);
        if (request.DeploymentId is not null) identity.Require("deployment.execute",request.DeploymentId);
        lock(gate) {
            Refresh();
            CheckDeployment(request.DeploymentId,request.DeploymentRevision,request.PackageId,request.PackageRevision);
            using var catalog = policy.OpenCatalog();
            if (fingerprint is not null && ReadRun(catalog,id) is { } existing)
                return SameRequest(existing, fingerprint);
            var state = new RunState(ProductInfo.Protocol,id,request.PackageId,request.PackageRevision,ParametersReference(request.Parameters),"Queued",identity.Subject,identity.Stamp,ControlReference(),
                TimestampMs:Now,Debug:request.Debug?.Clone(),DeploymentId:request.DeploymentId,DeploymentRevision:request.DeploymentRevision,
                Parallelism: parallelism, RequiresJournal: requiresJournal, RequestDigest: fingerprint, Elastic: request.Elastic);
            try { Save(state); return state; }
            catch (UniversalDataException error) when (fingerprint is not null && error.Code is StatusCode.Fenced or StatusCode.Busy)
            {
                if (ReadRun(catalog,id) is { } stored) return SameRequest(stored,fingerprint!);
                throw;
            }
        }
    }

    private static RunState SameRequest(RunState existing, string fingerprint) =>
        existing.RequestDigest == fingerprint ? existing :
        throw new UniversalDataException((int)StatusCode.Fenced, "Request id was already used for a different execution");
    public RunState Resume(HostIdentity identity,string id,bool brokerRestore = false)
    {
        identity.Require("execution.resume",id);
        lock(gate) {
            var state = Get(id); identity.Require("package.execute",state.PackageId);
            using var catalog = policy.OpenCatalog();
            string packageText = Package(state.PackageId, state.PackageRevision);
            using (var nativePackage = new NativePackage(packageText, policy.MemoryLimit))
                if (!nativePackage.RequiresJournal)
                    throw new ArgumentException("This stream execution has no Native Journal and cannot be resumed");
            if (state.ParentId is not null || state.State != "RecoveryRequired" || workers.ContainsKey(id) || remoteWorkers.ContainsKey(id) || Alive(state) ||
                catalog.LeaseActive(CatalogKind.Audit,"run/"+id) ||
                state.MemberIds is null && state.ExecutionId.Length == 0 ||
                state.MemberIds is { } members && members.Any(member => catalog.LeaseActive(CatalogKind.Audit,"run/"+member) ||
                    states.TryGetValue(member, out var prior) && Alive(prior)))
                throw new ArgumentException("Execution is not ready for recovery on this node");
            if (brokerRestore) {
                if (state.ElasticPlan is { } elastic)
                {
                    var replacement = elastic.Groups.Select(group => KafkaElasticGroup.Create(group.Partitions,true)).ToArray();
                    var next = state with { State="Queued",Failure=null,Subject=identity.Subject,SubjectStamp=identity.Stamp,
                        Revision=checked(state.Revision+1),Attempt=checked(state.Attempt+1),TimestampMs=Now,
                        ElasticPlan=elastic with { Groups=replacement,Next=null,Reason=null,Revision=checked(elastic.Revision+1),
                            LastChangeMs=Now,HighSamples=0,LowSinceMs=0 },
                        MemberIds=replacement.Select(group => group.Id).ToArray() };
                    Save(next);
                    return next;
                }
                bool stages = state.MemberIds is not null && WorkerParallelism.UsesKafkaStages(packageText);
                bool single = state.MemberIds is null && state.Parallelism is { WorkerProcesses: 1 } &&
                    state.Placement is { } placement && (placement.Remote || state.BrokerRestore) &&
                    state.ExecutionId.Length != 0 && WorkerParallelism.UsesBrokerRecovery(packageText);
                if (!(stages || single) || state.ParametersReference.Length != 0 || state.Debug is not null)
                    throw new ArgumentException("Execution does not support Kafka Broker recovery");
            } else if (state.BrokerRestore || state.HomeNodeId is { } home && state.Placement is not { Remote: true } && home != configuration.NodeId) {
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
                    try { await process.KillAsync(); } catch (InvalidOperationException) { } });
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
    public Task Stop(HostIdentity identity,string id)
    {
        identity.Require("execution.cancel",id);var state=Get(identity,id) ?? throw new ArgumentException("Execution was not found");
        using var catalog=policy.OpenCatalog();
        if (state.State!="Running" || !catalog.LeaseActive(CatalogKind.Audit,"run/"+id)) throw new ArgumentException("Execution is not running");
        PutControl(catalog,"stop",state);
        return Task.CompletedTask;
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
    private static long ProcessStartTime(int id)
    {
        using var process = Process.GetProcessById(id);
        return new DateTimeOffset(process.StartTime.ToUniversalTime()).ToUnixTimeMilliseconds();
    }
    private static bool Alive(RunState state)
    {
        if (state.Placement is { Remote: true } || state.ProcessId is not { } id) return false;
        try
        {
            using var process = Process.GetProcessById(id);
            if (process.HasExited || state.ProcessStartedAtMs != new DateTimeOffset(process.StartTime.ToUniversalTime()).ToUnixTimeMilliseconds())
                return false;
            if (OperatingSystem.IsLinux())
            {
                // A worker whose parent was killed may remain as an unreaped zombie.
                string record = File.ReadAllText($"/proc/{id}/stat");
                int processState = record.LastIndexOf(')') + 2;
                if (processState < record.Length && record[processState] is 'Z' or 'X')
                    return false;
            }
            return true;
        }
        catch (Exception error) when (error is ArgumentException or InvalidOperationException or FileNotFoundException or DirectoryNotFoundException)
        { return false; }
    }
    private void Refresh() => RefreshRange(null);
    private void RefreshDefinitions()
    {
        RefreshRange("deployment/");
        RefreshRange("job/");
    }
    private void RefreshRange(string? prefix)
    {
        using var catalog = policy.OpenCatalog(); string? afterId = prefix; ulong afterRevision = 0;
        while(true) {
            var query = new Dictionary<string,object> { ["kind"] = (int)CatalogKind.Audit,["limit"] = 100,["latestOnly"]=true };
            if (afterId is not null) { query["afterId"] = afterId; query["afterRevision"] = afterRevision; }
            using var page = JsonDocument.Parse(catalog.Query(JsonSerializer.Serialize(query,HostPolicy.JsonOptions)));
            if (page.RootElement.GetArrayLength() == 0) break;
            foreach(var entry in page.RootElement.EnumerateArray()) {
                afterId = entry.GetProperty("id").GetString()!;
                if (prefix is not null && !afterId.StartsWith(prefix,StringComparison.Ordinal))
                    return;
                afterRevision = entry.GetProperty("revision").GetUInt64(); var document = entry.GetProperty("document");
                if (afterId.StartsWith("run/",StringComparison.Ordinal)) {
                    var state = ReadRun(document);
                    if (!states.TryGetValue(state.Id,out var prior) || prior.Revision < state.Revision) states[state.Id] = state;
                } else if (afterId.StartsWith("job/",StringComparison.Ordinal)) {
                    var job = ReadJob(document);
                    if (!jobs.TryGetValue(job.Id, out var prior) || prior.Revision < job.Revision) jobs[job.Id] = job;
                } else if (afterId.StartsWith("deployment/",StringComparison.Ordinal)) {
                    var state = document.Deserialize<DeploymentState>(HostPolicy.JsonOptions)!; state = state with { Id = state.Id[11..] };
                    if (!deployments.TryGetValue(state.Id,out var prior) || prior.Revision < state.Revision) deployments[state.Id] = state;
                }
            }
        }
    }
    private RunState[] QueryRuns(Catalog catalog, string state, bool rootsOnly = false, ulong due = long.MaxValue, uint limit = 128, RunState? after = null)
    {
        using var page = JsonDocument.Parse(catalog.QueryRuns(state, rootsOnly, due, limit, (ulong)(after?.NotBeforeMs ?? 0),
            (ulong)(after?.TimestampMs ?? 0), after is null ? "" : "run/" + after.Id));
        var runs = new RunState[page.RootElement.GetArrayLength()];
        int index = 0;
        foreach (var row in page.RootElement.EnumerateArray())
        {
            var run = ReadRun(row.GetProperty("document"));
            states[run.Id] = runs[index++] = run;
        }
        return runs;
    }

    private void Reconcile(CancellationToken stopping)
    {
        using var catalog=policy.OpenCatalog();
        foreach(var state in QueryRuns(catalog, "Claimed").Concat(QueryRuns(catalog, "Running")).Where(run => !workers.ContainsKey(run.Id) &&
            !remoteWorkers.ContainsKey(run.Id) && !monitors.ContainsKey(run.Id) &&
            (run.ParentId is null || !monitors.ContainsKey(run.ParentId) || run.Assignment is not null)).ToArray()) {
            if (state.ParentId is null && state.Elastic is not null && state.State == "Running")
            {
                Track(state.Id,DispatchTracked(state,leadership.Current,stopping));
                continue;
            }
            if (configuration.Cluster && state.State == "Running" && state.StartAuthorized &&
                state.Owner is { } previous && state.TaskGeneration != 0 && remoteNodes.Matches(state))
            {
                var identity = identities.Find(state.Subject,state.SubjectStamp);
                if (identity?.Allows("package.execute",state.PackageId) == true &&
                    (state.DeploymentId is null || identity.Allows("deployment.execute",state.DeploymentId)))
                {
                    try
                    {
                        var prior = new CatalogLease(CatalogKind.Audit,"run/"+state.Id,previous,state.TaskGeneration,0);
                        var recovered = catalog.AdoptLease(prior,owner,ExecutionLeaseDuration);
                        var adopted = state with { Owner=owner,Revision=checked(state.Revision+1),TimestampMs=Now };
                        Save(adopted,recovered);
                        Track(adopted.Id,ContinueRemote(adopted,recovered,leadership.Current,stopping));
                        continue;
                    }
                    catch (UniversalDataException error) when (Retryable(error)) { }
                }
            }
            var lease=catalog.TryAcquireLease(CatalogKind.Audit,"run/"+state.Id,owner,ExecutionLeaseDuration);
            if (lease is null) continue;
            try {
                var current = ReadRun(catalog, state.Id);
                if (current?.State is "Claimed" or "Running")
                {
                    bool authorized = current.StartAuthorized || current.State == "Running" || current.Resume;
                    var actor = configuration.Cluster && authorized && current.ExecutionId.Length != 0 &&
                        current.ParentId is null && current.MemberIds is null && current.Placement is { Remote:true } &&
                        current.Debug is null && !current.BrokerRestore
                        ? identities.Find(current.Subject,current.SubjectStamp) : null;
                    bool restore = actor?.Allows("package.execute",current.PackageId) == true &&
                        (current.DeploymentId is null || actor.Allows("deployment.execute",current.DeploymentId)) &&
                        WorkerParallelism.UsesBrokerRecovery(Package(current.PackageId,current.PackageRevision));
                    Save(current with { State=authorized && !restore ? "RecoveryRequired" : "Queued",
                        Resume=restore || current.Resume, BrokerRestore=restore || current.BrokerRestore,
                        JournalTicket=restore ? Guid.NewGuid().ToString("N") : current.JournalTicket,
                        Attempt=restore ? checked(current.Attempt+1) : current.Attempt,
                        Result=authorized ? (int)StatusCode.CommitUnknown : 0,Owner=null,
                        ProcessId=null, Placement=authorized ? current.Placement : null,
                        HomeNodeId=authorized && !restore ? current.HomeNodeId : null,
                        Failure=authorized ? new RunFailure((int)StatusCode.CommitUnknown,"dispatch.reconcile",
                            "ExecutionPermitExpired") : null,
                        Revision=checked(current.Revision+1),TimestampMs=Now },lease);
                }
            } finally { catalog.ReleaseLease(lease); }
        }
    }
    internal void Tick(ClusterLeadership.Term? term,CancellationToken cancellation)
    {
        using var pin = leadership.Pin(term);
        try
        {
            lock (gate)
            {
                RefreshDefinitions();
                Reconcile(cancellation);
                Schedule();
                if (monitors.Count >= policy.MaxConcurrency) return;
                using var catalog = policy.OpenCatalog();
                var queued = QueryRuns(catalog,"Queued",true,(ulong)Now,limit:16,after:queueCursor);
                if (queued.Length == 0)
                {
                    queueCursor = null;
                    return;
                }
                foreach (var state in queued)
                {
                    queueCursor = state;
                    if (monitors.ContainsKey(state.Id)) continue;
                    Track(state.Id,DispatchTracked(state,term,cancellation));
                    break;
                }
            }
        }
        catch (UniversalDataException error) when (Retryable(error))
        { leadership.CatalogUnavailable(error); logger.LogDebug("Tenant scheduling is waiting for the Catalog ({Status}).",error.Code); }
    }

    internal Task WaitForMonitors() => Task.WhenAll(monitors.Values.Where(task => !task.IsFaulted));

    private void Track(string id,Task task)
    {
        monitors[id] = task;
        _ = task.ContinueWith(completed =>
        {
            if (completed.IsFaulted)
                logger.LogError("Execution monitor ended ({Failure}).", completed.Exception?.GetBaseException().GetType().Name);
            monitors.TryRemove(id,out _);
        }, TaskScheduler.Default);
    }

    private async Task ContinueRemote(RunState state,CatalogLease lease,ClusterLeadership.Term? term,CancellationToken stopping)
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(stopping,term?.Lost ?? CancellationToken.None);
        try { await StartWorker(state.Id,state.Placement!,cancellation.Token,continuation:state,reserved:lease); }
        catch (UniversalDataException error) when (Retryable(error)) { leadership.CatalogUnavailable(error); }
    }

    private static bool Retryable(UniversalDataException error) =>
        error.Code is StatusCode.Fenced or StatusCode.Busy or StatusCode.Transient or StatusCode.Io;

    private async Task DispatchTracked(RunState state, ClusterLeadership.Term? term, CancellationToken stopping)
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(stopping,term?.Lost ?? CancellationToken.None);
        try
        {
            if (state.Elastic is not null && state.State == "Running") await DispatchElastic(state,cancellation.Token);
            else await Dispatch(state,cancellation.Token);
        }
        catch (UniversalDataException error) when (Retryable(error))
        { leadership.CatalogUnavailable(error); logger.LogDebug("Dispatch paused after a Catalog change ({Status}).", error.Code); }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested) { }
    }
    private async Task Dispatch(RunState run, CancellationToken cancellation)
    {
        if (run.HomeNodeId is { } home && run.Placement is not { Remote: true } && home != configuration.NodeId)
        {
            queueReasons[run.Id] = "JournalOwnerUnavailable";
            return;
        }
        using var catalog = policy.OpenCatalog();
        CatalogLease? jobLease = null;
        if (run.JobId is { } jobId)
        {
            jobLease = catalog.TryAcquireLease(CatalogKind.Audit, "job/" + jobId, owner, LeaseDuration);
            if (jobLease is null)
                return;
        }
        Task admitted;
        try
        {
            lock (gate)
            {
                if (run.JobId is { } job && jobs.TryGetValue(job, out var schedule) &&
                    QueryRuns(catalog,"Claimed",true,limit:256).Concat(QueryRuns(catalog,"Running",true,limit:256))
                        .Count(other => other.JobId == job) >= schedule.MaxConcurrency)
                {
                    queueReasons[run.Id] = "JobConcurrency";
                    return;
                }
                admitted = DispatchPlacement(run, cancellation);
            }
        }
        finally { if (jobLease is not null) ReleaseJobLease(catalog, jobLease); }
        await admitted;
    }

    private async Task DispatchPlacement(RunState run, CancellationToken cancellation)
    {
        if (run.Elastic is not null)
        {
            await DispatchElastic(run,cancellation);
            return;
        }
        var plan = run.Parallelism;
        bool remote = (run.Debug is null || configuration.Cluster && !WorkerNodeLaunch.SamplesRows(run.Debug)) &&
            (!run.Resume || run.BrokerRestore || run.Placement is { Remote: true });
        using var requirements = remote && remoteNodes.Nodes.Count != 0 ? JsonDocument.Parse(Package(run.PackageId, run.PackageRevision)) : null;
        string rejection = reservations.Rejection(policy.Tenant,remote,requirements?.RootElement,run.Placement,run.Resume);

        if (plan is null || plan.WorkerProcesses == 1) {
            using var reservation = reservations.TryReserve(policy.Tenant, plan, remote, 1,
                !run.BrokerRestore && run.Placement is { Remote: true } ? [run.Placement] : null,
                run.BrokerRestore ? run.Placement : null, requirements?.RootElement);
            if (reservation is not null)
            {
                queueReasons.TryRemove(run.Id, out _);
                await StartWorker(run.Id, reservation.Placements[0], cancellation);
            }
            else
                queueReasons[run.Id] = rejection;
            return;
        }
        await DispatchGroup(run, plan, remote, requirements?.RootElement, cancellation);
    }

    private async Task DispatchGroup(RunState run, WorkerParallelism plan, bool allowRemote, JsonElement? package,
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
        using var reservation = reservations.TryReserve(policy.Tenant, plan, allowRemote, pending.Length, pins, package: package);
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
                if (run.State != "Queued" || run.HomeNodeId is { } home &&
                    run.Placement is not { Remote: true } && home != configuration.NodeId)
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
                    ExecutionLeaseDuration, "workers", policy.MaxConcurrency);
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
        run = run with { State = "Claimed", Owner = owner, HomeNodeId = null, MemberIds = memberIds,
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
                    Shard: shard, Placement: placement, HomeNodeId: placement.NodeId,
                    RequiresJournal: run.RequiresJournal)
                : previous with { State = "Queued", Subject = run.Subject, SubjectStamp = run.SubjectStamp,
                    Attempt = run.Attempt, Resume = previous.ExecutionId.Length != 0, Failure = null,
                    BrokerRestore = run.BrokerRestore && previous.ExecutionId.Length != 0,
                    JournalTicket = run.BrokerRestore && previous.ExecutionId.Length != 0
                        ? Guid.NewGuid().ToString("N") : previous.JournalTicket,
                    ExecutionRevision = previous.ExecutionId.Length == 0 ? 0 : catalog.LatestRevision(CatalogKind.Execution, previous.ExecutionId),
                    Owner = null, ProcessId = null, Shard = shard, Placement = placement,
                    RequiresJournal = run.RequiresJournal, Revision = checked(previous.Revision + 1),
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
        if (catalog.Get(CatalogKind.Audit,"cancel/"+id,run.Attempt) is not null) return "cancel";
        return catalog.Get(CatalogKind.Audit,"stop/"+id,run.Attempt) is not null ||
            run.Assignment is not null && catalog.Get(CatalogKind.Audit,"stop/"+run.Id,run.Attempt) is not null ? "stop" : null;
    }

    private static void PutControl(Catalog catalog, string action, RunState run)
    {
        string id = action + "/" + run.Id;
        catalog.Put(CatalogKind.Audit, id, run.Attempt, JsonSerializer.Serialize(
            new { version = ProductInfo.Protocol, id, attempt = run.Attempt }, HostPolicy.JsonOptions));
    }

    private async Task StartWorker(string id, WorkerPlacement placement, CancellationToken cancellation,
        WorkerStartBarrier? barrier = null, CatalogLease? reserved = null, RunState? continuation = null)
    {
        RunState state;
        CatalogLease lease;
        if (continuation is not null)
        {
            state = continuation;
            lease = reserved!;
            leases[lease.Id] = lease;
        }
        else lock(gate) {
            state = Get(id);
            using var catalog=policy.OpenCatalog();
            if (state.State != "Queued") {
                if (reserved is not null) catalog.ReleaseLease(reserved);
                return;
            }
            var acquired=reserved ?? catalog.TryAcquireLease(CatalogKind.Audit,"run/"+id,owner,ExecutionLeaseDuration,"workers",policy.MaxConcurrency);
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
            state = state with { State = "Claimed",Owner = owner,Placement = placement, StartAuthorized = false,
                AgentBootId=null,DispatchLeaseId=null,ReportSequence=0,TaskGeneration=lease.Generation,
                HomeNodeId = placement.Remote ? placement.NodeId : state.HomeNodeId,
                Revision = checked(state.Revision + 1),TimestampMs = Now }; Save(state);
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
        WorkerSupervisor.Lease? localWorker = null;
        WorkerRequest? localRequest = null;
        RemoteWorkerSession? remote = null;
        bool startSent = continuation is not null, terminalRecorded = false, reattach = false;
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
                    await local.Process.StandardInput.WriteLineAsync(command.AsMemory(), token);
                    await local.Process.StandardInput.FlushAsync(token);
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
                        await SendPipe(action,heartbeatStop.Token);
                        deadline.CancelAfter(TimeSpan.FromSeconds(policy.CancellationGraceSeconds +
                            (placement.Remote ? 5 : 0)));
                    }
                    if (++tick%5==0) {
                        leases[lease.Id]=catalog.RenewLease(lease,ExecutionLeaseDuration);
                        if (remoteState.Task.IsCompletedSuccessfully)
                            await remoteState.Task.Result.RenewAsync(heartbeatStop.Token);
                    }
                }
            } catch(OperationCanceledException) when(heartbeatStop.IsCancellationRequested) { }
              catch(Exception error) when(error is UniversalDataException or IOException or InvalidOperationException or System.Net.WebSockets.WebSocketException) {
                  if (error is UniversalDataException native) leadership.CatalogUnavailable(native);
                  deadline.Cancel();
              }
        }
        var heartbeat=HoldLease();
        try {
            var identity = identities.Find(state.Subject,state.SubjectStamp) ?? throw new UniversalDataException((int)StatusCode.Auth,"Dispatch authority expired");
            identity.Require("package.execute",state.PackageId); if (state.Debug is not null) identity.Require("package.debug",state.PackageId);
            if (state.DeploymentId is not null) identity.Require("deployment.execute",state.DeploymentId);
            using var catalog = policy.OpenCatalog();
            if (continuation is null)
            {
            var scoped = root.Bind(identity); string package = Package(state.PackageId,state.PackageRevision);
            JsonElement? snapshot = null;
            if (state.DeploymentId is not null) {
                using var documents = JsonDocument.Parse(catalog.BindingSnapshot(state.DeploymentId,ulong.Parse(state.DeploymentRevision!),state.PackageId));
                foreach(var document in documents.RootElement.EnumerateArray())
                    if (document.GetProperty("kind").GetInt32() == (int)CatalogKind.Package) identity.Require("package.execute",document.GetProperty("id").GetString()!);
                snapshot = documents.RootElement.Clone();
            }
            bool isolated = scoped.RequiresIsolation(package) || snapshot is { } documentsSnapshot && documentsSnapshot.EnumerateArray().Any(document =>
                document.GetProperty("kind").GetInt32() == (int)CatalogKind.Package && scoped.RequiresIsolation(document.GetProperty("document").GetRawText()));
            var budget = state.Parallelism is not null ? placement.Budget : null;
            if (budget is not null) scoped = scoped with { MaxConcurrency=budget.CpuSlots,MemoryLimit=budget.NativeMemoryBytes };
            ulong startingRevision = state.BrokerRestore ? catalog.LatestRevision(CatalogKind.Execution,state.ExecutionId) : state.ExecutionRevision;
            var request = new WorkerRequest(ProductInfo.Protocol,id,"execute",scoped,identity,package,state.ParametersReference,state.ExecutionId,
                state.Resume && !state.BrokerRestore,startingRevision,
                state.ExecutionId.Length == 0 ? 0 : catalog.LatestRevision(CatalogKind.Event,state.ExecutionId),state.Debug,ControlReference:state.ControlReference,Isolated:isolated,
                BindingSnapshot:snapshot,DeploymentId:state.DeploymentId,DeploymentRevision:state.DeploymentRevision is null ? 0 : ulong.Parse(state.DeploymentRevision),
                Budget: budget, Shard: state.Shard ?? (state.Assignment is null && state.Parallelism is { } plan && plan.ConsumerLanesPerWorker > 1
                    ? new WorkerShard(0,1,plan.ConsumerLanesPerWorker) : null),
                BrokerRestore:state.BrokerRestore, JournalTicket:state.JournalTicket, Assignment:state.Assignment);
            if (placement.Remote) {
                remote = await remoteNodes.StartAsync(placement, request, id, lease.Generation, deadline.Token);
                remoteWorkers[id] = remote;remoteState.TrySetResult(remote);
            } else {
                localRequest = request;
                localWorker = await supervisor.RentAsync(request,placement.Budget,deadline.Token);
                process = localWorker.Process;
                workers[id] = localWorker;
            }
            }
            else
            {
                remote = await remoteNodes.RecoverAsync(state,deadline.Token);
                remoteWorkers[id] = remote;
                remoteState.TrySetResult(remote);
            }
            long lastCursor = Stopwatch.GetTimestamp();
            var reports = new WorkerReportReader(catalog, state, lease, token => remote is null
                ? WorkerProtocol.ReadLine(process!.StandardOutput,67108864,token) : new ValueTask<string?>(remote.ReadAsync(token)), () =>
                {
                    if (remote is null || !startSent || Stopwatch.GetElapsedTime(lastCursor) < TimeSpan.FromSeconds(5)) return;
                    lock (gate)
                    {
                        var run = Get(id);
                        Save(run with { ReportSequence=remote.Sequence,Revision=checked(run.Revision+1) },lease);
                    }
                    lastCursor = Stopwatch.GetTimestamp();
                });
            string nativeId = state.ExecutionId;
            if (continuation is null)
            {
            var ready = await reports.ReadAsync("ready", deadline.Token);
            nativeId = ready.GetProperty("executionId").GetString()!;
            if (!ready.TryGetProperty("controlReady",out var controlReady) || controlReady.ValueKind != JsonValueKind.True)
                throw new InvalidDataException("Worker control endpoint is not ready");
            if (!ready.TryGetProperty("historyComplete",out var history) || history.ValueKind!=JsonValueKind.True ||
                reports.NativeId is { } announced && announced != nativeId) throw new InvalidDataException("Worker Native history is incomplete");
            if (state.Resume && state.ExecutionId != nativeId) throw new InvalidDataException("Recovery changed Native identity");
            var readyRecord = ready.GetProperty("record");
            if (readyRecord.GetProperty("id").GetString() != nativeId || readyRecord.GetProperty("packageId").GetString() != state.PackageId)
                throw new InvalidDataException("Worker ready record differs from dispatch");
            catalog.PutLeased(CatalogKind.Execution,nativeId,ready.GetProperty("revision").GetUInt64(),readyRecord.GetRawText(),lease);
            reports.BindReady(nativeId);
            lock(gate) {
                if (Get(id).State != "Claimed" || Get(id).Owner != owner) return;
                CheckDeployment(state.DeploymentId,state.DeploymentRevision,state.PackageId,state.PackageRevision);
                if (Control(catalog, state) is not null) {
                    Interlocked.Exchange(ref cancelled,1);
                    throw new OperationCanceledException("Dispatch was cancelled before Native start");
                }
                state = Get(id) with { State = barrier is null ? "Running" : "Claimed",Failure=null,ExecutionId = nativeId,
                    StartAuthorized=barrier is null, ProcessId=ready.GetProperty("processId").GetInt32(),
                    AgentBootId=remote?.Identity.AgentBootId,DispatchLeaseId=remote?.Identity.Lease,
                    ReportSequence=remote?.Sequence ?? 0,
                    ProcessStartedAtMs = process is null ? null : ProcessStartTime(ready.GetProperty("processId").GetInt32()),
                    Revision = checked(Get(id).Revision + 1),TimestampMs = Now }; Save(state);
            }
            if (barrier is not null) {
                await barrier.ReadyAsync(deadline.Token);
                lock (gate) {
                    var current = Get(id);
                    state = current with { State="Running",StartAuthorized=true,Revision=checked(current.Revision+1),TimestampMs=Now };
                    Save(state);
                }
            }
            }
            startSent = true;
            if (remote is not null)
            {
                leases[lease.Id] = catalog.RenewLease(lease,ExecutionLeaseDuration);
                await remote.RenewAsync(deadline.Token);
            }
            await SendPipe("start",deadline.Token);
            var terminal = await reports.ReadAsync("terminal", deadline.Token);
            if (terminal.GetProperty("executionId").GetString() != nativeId) throw new InvalidDataException("Worker terminal identity differs");
            var terminalRecord = terminal.GetProperty("record");
            if (terminalRecord.GetProperty("id").GetString() != nativeId || terminalRecord.GetProperty("packageId").GetString() != state.PackageId)
                throw new InvalidDataException("Worker terminal record differs from dispatch");
            catalog.PutLeased(CatalogKind.Execution,nativeId,terminal.GetProperty("revision").GetUInt64(),terminalRecord.GetRawText(),lease);
            if(terminal.TryGetProperty("debug",out var debug)&&debug.ValueKind==JsonValueKind.Object) {
                string debugId="debug/"+id;ulong revision=catalog.LatestRevision(CatalogKind.Audit,debugId)+1;
                catalog.PutLeased(CatalogKind.Audit,debugId,revision,JsonSerializer.Serialize(new {version=ProductInfo.Protocol,id=debugId,snapshot=debug},HostPolicy.JsonOptions),lease);
            }
            int result = terminal.GetProperty("result").GetInt32(); string final = terminal.GetProperty("state").GetString()!;
            OperationError? detail = terminal.TryGetProperty("error", out var terminalError) && terminalError.ValueKind == JsonValueKind.Object
                ? terminalError.Deserialize<OperationError>(HostPolicy.JsonOptions) : null;
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
                state = current with { State = final,Result = result,ReportSequence=remote?.Sequence ?? 0,
                    Failure = result == 0 ? null : new RunFailure(result, "native.execute", "NativeExecution", detail),
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
            else await remote.AcknowledgeAsync(deadline.Token);
        } catch(Exception error) when(error is not OutOfMemoryException) {
            barrier?.Fail();
            if (error is UniversalDataException storage) leadership.CatalogUnavailable(storage);
            bool unknown = startSent || state.StartAuthorized || state.BrokerRestore ||
                state.Resume && state.Result==(int)StatusCode.CommitUnknown;
            reattach = configuration.Cluster && placement.Remote && unknown && !terminalRecorded && remote?.ExitReported != true &&
                (error is IOException or OperationCanceledException or System.Net.WebSockets.WebSocketException ||
                 error is WorkerFailureException { Failure.Result: (int)StatusCode.WorkerLost } ||
                 error is UniversalDataException native && Retryable(native));
            if (reattach) return;
            if (unknown && placement.Remote) {
                await heartbeatStop.CancelAsync();
                await heartbeat;
            }
            var failure=error is WorkerFailureException worker?worker.Failure:new RunFailure(error is UniversalDataException nativeError?nativeError.Status:error is IOException?(int)StatusCode.Io:(int)StatusCode.Invalid,"dispatch",error.GetType().Name);
            bool retryPlacement = !unknown && Volatile.Read(ref cancelled) == 0 && placement.Remote &&
                failure.Result is (int)StatusCode.Busy or (int)StatusCode.Resource or (int)StatusCode.WorkerLost;
            if (retryPlacement) queueReasons[id] = "WorkerCapacity";
            try { lock(gate) { var current = Get(id); if (!terminalRecorded && current.State != "Cancelled") Save(current with {
                State = unknown ? "RecoveryRequired" : Volatile.Read(ref cancelled)!=0 ? "Cancelled" : retryPlacement ? "Queued" : "Failed",
                Result = unknown ? (int)StatusCode.CommitUnknown : retryPlacement ? 0 : failure.Result,Failure=retryPlacement ? null : failure,
                Placement=retryPlacement && !current.Resume ? null : current.Placement,
                NotBeforeMs=retryPlacement ? checked(Now+1000) : current.NotBeforeMs,
                RecoverySafeAfterMs=unknown && placement.Remote
                    ? checked((long)leases.GetValueOrDefault(lease.Id,lease).UntilMilliseconds) : 0,
                Revision = checked(current.Revision + 1),TimestampMs = Now }); } }
            catch(UniversalDataException fenced) when(fenced.Code==StatusCode.Fenced) { /* New owner will reconcile unknown effects. */ }
        } finally {
            barrier?.Fail();
            heartbeatStop.Cancel();await heartbeat;
            try {
                if (remote is not null) await remote.DisposeAsync();
                if (localWorker is not null)
                {
                    try
                    {
                        if (startSent && !terminalRecorded && !localWorker.Process.HasExited && localRequest is not null)
                            await localWorker.DrainAsync(localRequest,Token(state));
                    }
                    finally { await localWorker.DisposeAsync(); }
                }
            } finally {
                workers.TryRemove(id,out _);
                remoteWorkers.TryRemove(id,out _);
                leases.TryRemove(lease.Id,out _);
                if (!reattach)
                {
                    using var catalog=policy.OpenCatalog();
                    try { catalog.ReleaseLease(lease); } catch(UniversalDataException error) when(Retryable(error)) { }
                }
            }
        }
    }
}
