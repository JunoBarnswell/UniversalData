using System.Collections.Concurrent;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text.Json;
using UniversalData.Hosting;

namespace UniversalData.Management;

public sealed record ProductConfiguration(string WorkerPath);
public sealed record RunRequest(string PackageId, string PackageRevision, string Parameters = "{}", JsonElement? Debug = null,
    string? DeploymentId = null, string? DeploymentRevision = null);
public sealed record DeploymentRequest(string Id, string PackageId, string PackageRevision);
public sealed record DeploymentState(int Version, string Id, string PackageId, string PackageRevision,
    string DeploymentRevision, bool Enabled, long TimestampMs, ulong Revision = 1);
public sealed record JobRequest(int Version, string Id, string PackageId, string PackageRevision, string? Parameters,
    bool Enabled, uint EverySeconds, long NextDueMs, uint MaxConcurrency = 1, uint MaxAttempts = 1,
    uint RetryDelaySeconds = 30, string? DeploymentId = null, string? DeploymentRevision = null, string? ParametersReference = null);
public sealed record JobState(int Version, string Id, string PackageId, string PackageRevision, string ParametersReference,
    bool Enabled, uint EverySeconds, long NextDueMs, string Subject, string SubjectStamp, uint MaxConcurrency = 1, uint MaxAttempts = 1,
    uint RetryDelaySeconds = 30, long TimestampMs = 0, ulong Revision = 1, string? DeploymentId = null, string? DeploymentRevision = null, int RecordVersion = 2);
public sealed record RunState(int Version, string Id, string PackageId, string PackageRevision, string ParametersReference,
    string State, string Subject, string SubjectStamp, string ControlReference, string ExecutionId = "", bool Resume = false, ulong ExecutionRevision = 0,
    int Result = 0, int? ProcessId = null, string? Owner = null, long TimestampMs = 0, ulong Revision = 1,
    long? ProcessStartedAtMs = null, string? JobId = null, uint Attempt = 1, long NotBeforeMs = 0,
    JsonElement? Debug = null, string? DeploymentId = null, string? DeploymentRevision = null, int RecordVersion = 2);

/// <summary>Tenant-separated dispatch over one Native execution and Catalog contract.</summary>
public sealed class RunDispatcher(HostPolicy root, ProductConfiguration configuration, IdentityDirectory identities) : BackgroundService
{
    private readonly ConcurrentDictionary<string,TenantDispatcher> tenants = new(StringComparer.Ordinal);
    public TenantDispatcher For(HostIdentity identity) => tenants.GetOrAdd(identity.Tenant,tenant =>
        new(root.Bind(HostIdentity.Operator(tenant)),root,configuration,identities));
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        Directory.CreateDirectory(root.DataDirectory);
        using var ownership = new FileStream(Path.Combine(root.DataDirectory,"management.lock"),FileMode.OpenOrCreate,FileAccess.ReadWrite,FileShare.None);
        foreach (var tenant in root.Tenants is null ? Array.Empty<string>() : root.Tenants.Keys.AsEnumerable()) For(HostIdentity.Operator(tenant));
        await Task.WhenAll(tenants.Values.Select(dispatcher => dispatcher.Run(stoppingToken)));
    }
}
public sealed class TenantDispatcher(HostPolicy policy, HostPolicy root, ProductConfiguration configuration, IdentityDirectory identities)
{
    private readonly object gate = new();
    private readonly string owner = Guid.NewGuid().ToString("N");
    private readonly Dictionary<string,RunState> states = new(StringComparer.Ordinal);
    private readonly Dictionary<string,JobState> jobs = new(StringComparer.Ordinal);
    private readonly Dictionary<string,DeploymentState> deployments = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string,Process> workers = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string,Task> monitors = new(StringComparer.Ordinal);
    private static long Now => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    public RunState[] List(HostIdentity identity) { lock(gate) return states.Values.Where(run => identity.Allows("execution.read",run.Id)).OrderByDescending(run => run.TimestampMs).ToArray(); }
    public RunState? Get(HostIdentity identity,string id) { identity.Require("execution.read",id); lock(gate) return states.GetValueOrDefault(id); }
    public JobState[] Jobs(HostIdentity identity) { lock(gate) return jobs.Values.Where(job => identity.Allows("job.read",job.Id)).OrderBy(job => job.Id).ToArray(); }
    public DeploymentState[] Deployments(HostIdentity identity) { lock(gate) return deployments.Values.Where(item => identity.Allows("deployment.read",item.Id)).OrderBy(item => item.Id).ToArray(); }
    private RunState Get(string id) { lock(gate) return states.GetValueOrDefault(id) ?? throw new ArgumentException("Execution does not exist"); }
    private string Package(string id,string revision)
    {
        using var catalog = policy.OpenCatalog();
        return catalog.Get(CatalogKind.Package,id,ulong.Parse(revision)) ?? throw new ArgumentException("Package version does not exist");
    }
    private void Save(RunState value)
    {
        using var catalog = policy.OpenCatalog();
        catalog.Put(CatalogKind.Audit,"run/" + value.Id,value.Revision,
            JsonSerializer.Serialize(value with { Id = "run/" + value.Id },HostPolicy.JsonOptions)); states[value.Id] = value;
    }
    private void Save(JobState value)
    {
        using var catalog = policy.OpenCatalog();
        catalog.Put(CatalogKind.Audit,"job/" + value.Id,value.Revision,
            JsonSerializer.Serialize(value with { Id = "job/" + value.Id },HostPolicy.JsonOptions)); jobs[value.Id] = value;
    }
    private void Save(DeploymentState value)
    {
        using var catalog = policy.OpenCatalog();
        catalog.Put(CatalogKind.Audit,"deployment/" + value.Id,value.Revision,
            JsonSerializer.Serialize(value with { Id = "deployment/" + value.Id },HostPolicy.JsonOptions)); deployments[value.Id] = value;
    }
    public DeploymentState Deploy(HostIdentity identity,DeploymentRequest request)
    {
        identity.Require("deployment.write",request.Id); identity.Require("package.deploy",request.PackageId);
        _ = Package(request.PackageId,request.PackageRevision);
        lock(gate) {
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
            state = deployments.GetValueOrDefault(id) ?? throw new ArgumentException("Deployment does not exist");
            state = state with { Enabled = enabled,TimestampMs = Now,Revision = checked(state.Revision + 1) }; Save(state);
            running = enabled ? [] : states.Values.Where(run => run.DeploymentId == id && run.State == "Running").Select(run => run.Id).ToArray();
            if (!enabled) foreach(var run in states.Values.Where(run => run.DeploymentId == id && run.State is "Queued" or "Claimed").ToArray())
                Save(run with { State = "Cancelled",Revision = checked(run.Revision + 1),TimestampMs = Now });
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
        _ = Package(request.PackageId,request.PackageRevision);
        lock(gate) {
            string reference;
            if (request.Parameters is not null) reference = new ParameterVault(policy).Store(request.Parameters);
            else if (jobs.TryGetValue(request.Id,out var existing) && request.ParametersReference == existing.ParametersReference) reference = existing.ParametersReference;
            else reference = new ParameterVault(policy).Store("{}");
            CheckDeployment(request.DeploymentId,request.DeploymentRevision,request.PackageId,request.PackageRevision);
            if (request.DeploymentId is not null) identity.Require("deployment.execute",request.DeploymentId);
            var job = new JobState(ProductInfo.Protocol,request.Id,request.PackageId,request.PackageRevision,reference,request.Enabled,request.EverySeconds,
                request.NextDueMs == 0 ? Now : request.NextDueMs,identity.Subject,identity.Stamp,request.MaxConcurrency,request.MaxAttempts,request.RetryDelaySeconds,Now,
                checked((jobs.GetValueOrDefault(request.Id)?.Revision ?? 0) + 1),request.DeploymentId,request.DeploymentRevision);
            Save(job); return job;
        }
    }
    private string ControlReference() => new ParameterVault(policy).Store(JsonSerializer.Serialize(new { token = Convert.ToHexString(RandomNumberGenerator.GetBytes(32)) }));
    public RunState Enqueue(HostIdentity identity,RunRequest request)
    {
        identity.Require("package.execute",request.PackageId); _ = Package(request.PackageId,request.PackageRevision);
        if (request.Debug is not null) identity.Require("package.debug",request.PackageId);
        if (request.DeploymentId is not null) identity.Require("deployment.execute",request.DeploymentId);
        var reference = new ParameterVault(policy).Store(request.Parameters);
        lock(gate) {
            CheckDeployment(request.DeploymentId,request.DeploymentRevision,request.PackageId,request.PackageRevision);
            var state = new RunState(ProductInfo.Protocol,Guid.NewGuid().ToString("N"),request.PackageId,request.PackageRevision,reference,"Queued",identity.Subject,identity.Stamp,ControlReference(),
                TimestampMs:Now,Debug:request.Debug?.Clone(),DeploymentId:request.DeploymentId,DeploymentRevision:request.DeploymentRevision); Save(state); return state;
        }
    }
    public RunState Resume(HostIdentity identity,string id)
    {
        identity.Require("execution.resume",id);
        lock(gate) {
            var state = Get(id); identity.Require("package.execute",state.PackageId);
            if (state.State != "RecoveryRequired" || state.ExecutionId.Length == 0 || workers.ContainsKey(id) || Alive(state)) throw new ArgumentException("Execution is not ready for recovery");
            using var catalog = policy.OpenCatalog();
            state = state with { State = "Queued",Resume = true,ExecutionRevision = catalog.LatestRevision(CatalogKind.Execution,state.ExecutionId),
                Subject = identity.Subject,SubjectStamp = identity.Stamp,Owner = null,ProcessId = null,Revision = checked(state.Revision + 1),TimestampMs = Now };
            Save(state); return state;
        }
    }
    private string Token(RunState state) { using var value = JsonDocument.Parse(new ParameterVault(policy).Resolve(state.ControlReference)); return value.RootElement.GetProperty("token").GetString()!; }
    public async Task Cancel(HostIdentity identity,string id) { identity.Require("execution.cancel",id); await CancelProcess(id); }
    private async Task CancelProcess(string id)
    {
        var state = Get(id);
        if (state.State == "Running" && Alive(state)) {
            try { await WorkerControl.Send(policy.ControlDirectory(id),id,Token(state),"cancel"); } catch (IOException) { }
            if (workers.TryGetValue(id,out var process)) {
                _ = Task.Run(async () => { await Task.Delay(TimeSpan.FromSeconds(policy.CancellationGraceSeconds));
                    try { if (!process.HasExited) await WorkerLauncher.Terminate(process,id,policy.RequiresIsolation(Package(Get(id).PackageId,Get(id).PackageRevision))); } catch (InvalidOperationException) { } });
            }
        } else lock(gate) { state = Get(id); if (state.State is "Queued" or "Claimed") Save(state with { State = "Cancelled",Revision = checked(state.Revision + 1),TimestampMs = Now }); }
    }
    public async Task<JsonElement> Debug(HostIdentity identity,string id,JsonElement? command = null)
    {
        identity.Require("execution.debug",id); var state = Get(id);
        if (state.State != "Running" || state.Debug is null || !Alive(state)) throw new ArgumentException("Execution is not an active debug session");
        return await WorkerControl.Send(policy.ControlDirectory(id),id,Token(state),command is null ? "snapshot" : "debug",command);
    }
    private static bool Alive(RunState state)
    {
        if (state.ProcessId is not { } id) return false;
        try { using var process = Process.GetProcessById(id); return !process.HasExited && state.ProcessStartedAtMs == new DateTimeOffset(process.StartTime.ToUniversalTime()).ToUnixTimeMilliseconds(); }
        catch(ArgumentException) { return false; }
    }
    private void Restore()
    {
        using var catalog = policy.OpenCatalog(); string? afterId = null; ulong afterRevision = 0;
        while(true) {
            var query = new Dictionary<string,object> { ["kind"] = (int)CatalogKind.Audit,["limit"] = 100 };
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
        foreach(var state in states.Values.ToArray()) {
            if (state.State == "Claimed") Save(state with { State = "RecoveryRequired",Result = (int)StatusCode.CommitUnknown,Revision = checked(state.Revision + 1),TimestampMs = Now });
            else if (state.State == "Running" && !Alive(state)) Save(state with { State = "RecoveryRequired",Revision = checked(state.Revision + 1),TimestampMs = Now });
        }
    }
    private void Reconcile()
    {
        foreach(var state in states.Values.Where(run => run.State == "Running" && !workers.ContainsKey(run.Id) && !Alive(run)).ToArray())
            Save(state with { State = "RecoveryRequired",Result = (int)StatusCode.CommitUnknown,Revision = checked(state.Revision + 1),TimestampMs = Now });
    }
    private void Schedule()
    {
        foreach(var job in jobs.Values.Where(job => job.Enabled && job.NextDueMs <= Now).ToArray()) {
            if (states.Values.Count(run => run.JobId == job.Id && run.State is "Queued" or "Claimed" or "Running") >= job.MaxConcurrency) continue;
            if (identities.Find(job.Subject,job.SubjectStamp) is not { } identity || !identity.Allows("package.execute",job.PackageId)) {
                Save(job with { Enabled = false,TimestampMs = Now,Revision = checked(job.Revision + 1) }); continue;
            }
            if (job.DeploymentId is not null && (!deployments.TryGetValue(job.DeploymentId,out var deployment) || !deployment.Enabled || deployment.DeploymentRevision != job.DeploymentRevision)) continue;
            var digest = SHA256.HashData(System.Text.Encoding.UTF8.GetBytes($"{policy.Tenant}/{job.Id}/{job.Revision}/{job.NextDueMs}"));
            var id = Convert.ToHexString(digest.AsSpan(0,16)).ToLowerInvariant();
            if (!states.ContainsKey(id)) Save(new RunState(ProductInfo.Protocol,id,job.PackageId,job.PackageRevision,job.ParametersReference,"Queued",job.Subject,job.SubjectStamp,ControlReference(),
                TimestampMs:job.NextDueMs,JobId:job.Id,DeploymentId:job.DeploymentId,DeploymentRevision:job.DeploymentRevision));
            Save(job with { NextDueMs = checked(Now + job.EverySeconds * 1000L),TimestampMs = Now,Revision = checked(job.Revision + 1) });
        }
    }
    public async Task Run(CancellationToken cancellation)
    {
        lock(gate) Restore();
        try {
            while(!cancellation.IsCancellationRequested) {
                RunState[] queued;
                lock(gate) { Reconcile(); Schedule(); queued = states.Values.Where(run => run.State == "Queued" && run.NotBeforeMs <= Now).OrderBy(run => run.TimestampMs).ToArray(); }
                foreach(var state in queued) {
                    if (monitors.Count >= policy.MaxConcurrency) break;
                    var task = StartWorker(state.Id,cancellation); monitors[state.Id] = task;
                    _ = task.ContinueWith(_ => monitors.TryRemove(state.Id,out var ignored),TaskScheduler.Default);
                }
                await Task.Delay(250,cancellation);
            }
        } finally { foreach(var process in workers.Values) { try { if (!process.HasExited) await process.StandardInput.WriteLineAsync("cancel"); } catch(IOException) { } } }
    }
    private async Task StartWorker(string id,CancellationToken cancellation)
    {
        RunState state;
        lock(gate) { state = Get(id); if (state.State != "Queued") return;
            CheckDeployment(state.DeploymentId,state.DeploymentRevision,state.PackageId,state.PackageRevision);
            state = state with { State = "Claimed",Owner = owner,Revision = checked(state.Revision + 1),TimestampMs = Now }; Save(state); }
        Process? process = null; bool startSent = false, isolated = false, terminalRecorded = false;
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellation); deadline.CancelAfter(TimeSpan.FromSeconds(policy.ExecutionTimeoutSeconds + policy.CancellationGraceSeconds));
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
            var request = new WorkerRequest(ProductInfo.Protocol,id,"execute",scoped,identity,package,state.ParametersReference,state.ExecutionId,state.Resume,state.ExecutionRevision,
                state.ExecutionId.Length == 0 ? 0 : catalog.LatestRevision(CatalogKind.Event,state.ExecutionId),state.Debug,ControlReference:state.ControlReference,Isolated:isolated,
                BindingSnapshot:snapshot,DeploymentId:state.DeploymentId,DeploymentRevision:state.DeploymentRevision is null ? 0 : ulong.Parse(state.DeploymentRevision));
            process = WorkerLauncher.Launch(configuration.WorkerPath,request); workers[id] = process;
            _ = WorkerProtocol.Discard(process.StandardError,deadline.Token);
            await process.StandardInput.WriteLineAsync(JsonSerializer.Serialize(request,HostPolicy.JsonOptions)); await process.StandardInput.FlushAsync(deadline.Token);
            var pendingEvents = new List<ExecutionEvent>();
            async Task<JsonElement> Read(string expected) {
                while(true) {
                    var text = await WorkerProtocol.ReadLine(process.StandardOutput,67108864,deadline.Token) ?? throw new InvalidDataException("Worker outcome is unknown");
                    using var message = JsonDocument.Parse(text); var value = message.RootElement;
                    if (value.GetProperty("version").GetInt32() != ProductInfo.Protocol || value.GetProperty("ticket").GetString() != id) throw new InvalidDataException("Worker identity differs from dispatch");
                    if (value.GetProperty("kind").GetString() == "event") {
                        var observed = value.GetProperty("value").Deserialize<ExecutionEvent>(HostPolicy.JsonOptions)!;
                        if (observed.PackageId != state.PackageId) throw new InvalidDataException("Event package differs from execution");
                        if (state.ExecutionId.Length == 0) pendingEvents.Add(observed);
                        else { if (observed.ExecutionId != state.ExecutionId) throw new InvalidDataException("Event identity differs from execution"); catalog.Record(observed); }
                        continue;
                    }
                    if (value.GetProperty("kind").GetString() != expected) throw new InvalidDataException("Worker phase differs from dispatch");
                    return value.Clone();
                }
            }
            var ready = await Read("ready"); string nativeId = ready.GetProperty("executionId").GetString()!;
            foreach(var observed in pendingEvents) {
                if (observed.ExecutionId != nativeId) throw new InvalidDataException("Initial event identity differs from execution"); catalog.Record(observed);
            }
            if (state.Resume && state.ExecutionId != nativeId) throw new InvalidDataException("Recovery changed Native identity");
            catalog.Put(CatalogKind.Execution,nativeId,ready.GetProperty("revision").GetUInt64(),ready.GetProperty("record").GetRawText());
            lock(gate) {
                if (Get(id).State != "Claimed" || Get(id).Owner != owner) return;
                CheckDeployment(state.DeploymentId,state.DeploymentRevision,state.PackageId,state.PackageRevision);
                state = Get(id) with { State = "Running",ExecutionId = nativeId,ProcessId = process.Id,ProcessStartedAtMs = new DateTimeOffset(process.StartTime.ToUniversalTime()).ToUnixTimeMilliseconds(),
                    Revision = checked(Get(id).Revision + 1),TimestampMs = Now }; Save(state);
            }
            startSent = true; await process.StandardInput.WriteLineAsync("start"); await process.StandardInput.FlushAsync(deadline.Token);
            var terminal = await Read("terminal");
            if (terminal.GetProperty("executionId").GetString() != nativeId) throw new InvalidDataException("Worker terminal identity differs");
            catalog.Put(CatalogKind.Execution,nativeId,terminal.GetProperty("revision").GetUInt64(),terminal.GetProperty("record").GetRawText());
            int result = terminal.GetProperty("result").GetInt32(); string final = terminal.GetProperty("state").GetString()!;
            if (final is not ("Succeeded" or "Failed" or "Cancelled" or "RecoveryRequired")) throw new InvalidDataException("Invalid Native terminal state");
            lock(gate) {
                state = Get(id) with { State = final,Result = result,Revision = checked(Get(id).Revision + 1),TimestampMs = Now };
                if (final == "Failed" && result != (int)StatusCode.CommitUnknown && state.JobId is { } jobId && jobs.TryGetValue(jobId,out var job) && job.Enabled && state.Attempt < job.MaxAttempts)
                    state = state with { State = "Queued",Resume = true,ExecutionRevision = catalog.LatestRevision(CatalogKind.Execution,nativeId),Attempt = checked(state.Attempt + 1),
                        NotBeforeMs = checked(Now + job.RetryDelaySeconds * 1000L),ProcessId = null,Owner = null };
                Save(state);
            }
            terminalRecorded = true;
            await process.WaitForExitAsync(deadline.Token);
        } catch(Exception error) when(error is not OutOfMemoryException) {
            bool unknown = startSent || isolated && process is not null;
            lock(gate) { var current = Get(id); if (!terminalRecorded && current.State != "Cancelled") Save(current with { State = unknown ? "RecoveryRequired" : "Failed",
                Result = unknown ? (int)StatusCode.CommitUnknown : error is UniversalDataException native ? native.Status : (int)StatusCode.Invalid,
                Revision = checked(current.Revision + 1),TimestampMs = Now }); }
        } finally {
            workers.TryRemove(id,out _);
            if (process is not null) { if (!process.HasExited) await WorkerLauncher.Terminate(process,id,isolated); process.Dispose(); }
        }
    }
}
