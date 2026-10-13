using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text.Json;
using UniversalData.Hosting;

namespace UniversalData.Management;

public sealed record ProductConfiguration(string PolicyPath, string WorkerPath);
public sealed record RunRequest(string PackageId, string PackageRevision, string Parameters = "{}", JsonElement? Debug = null,
    string? DeploymentId = null, string? DeploymentRevision = null);
public sealed record DeploymentRequest(string Id, string PackageId, string PackageRevision);
public sealed record DeploymentState(int Version, string Id, string PackageId, string PackageRevision,
    string DeploymentRevision, bool Enabled, long TimestampMs, ulong Revision = 1);
public sealed record JobState(int Version, string Id, string PackageId, string PackageRevision, string Parameters,
    bool Enabled, uint EverySeconds, long NextDueMs, uint MaxConcurrency = 1, uint MaxAttempts = 1,
    uint RetryDelaySeconds = 30, long TimestampMs = 0, ulong Revision = 1);
public sealed record RunState(int Version, string Id, string PackageId, string PackageRevision, string Parameters,
    string State, string ExecutionId = "", bool Resume = false, ulong ExecutionRevision = 0,
    int Result = 0, int? ProcessId = null, string? Owner = null, long TimestampMs = 0, ulong Revision = 1,
    long? ProcessStartedAtMs = null, string? JobId = null, uint Attempt = 1, long NotBeforeMs = 0,
    JsonElement? Debug = null, string? DeploymentId = null, string? DeploymentRevision = null);

/// <summary>Process dispatch only. Package execution and recovery remain Native.</summary>
public sealed class RunDispatcher(HostPolicy policy, ProductConfiguration configuration) : BackgroundService
{
    private readonly object stateLock = new();
    private readonly string owner = Guid.NewGuid().ToString("N");
    private readonly Dictionary<string, RunState> states = new();
    private readonly Dictionary<string, JobState> jobs = new();
    private readonly Dictionary<string, DeploymentState> deployments = new();
    private readonly ConcurrentDictionary<string, Process> workers = new();
    private readonly ConcurrentDictionary<string, Task> monitors = new();
    private const string Prefix = "run/";
    private const string JobPrefix = "job/";
    private const string DeploymentPrefix = "deployment/";
    private static long Now => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    public RunState[] List() { lock (stateLock) return states.Values.OrderByDescending(run => run.TimestampMs).ToArray(); }
    public RunState? Get(string id) { lock (stateLock) return states.GetValueOrDefault(id); }
    public JobState[] Jobs() { lock (stateLock) return jobs.Values.OrderBy(job => job.Id).ToArray(); }
    public DeploymentState[] Deployments() { lock (stateLock) return deployments.Values.OrderBy(item => item.Id).ToArray(); }
    public DeploymentState Deploy(DeploymentRequest request)
    {
        using var catalog = policy.OpenCatalog();
        var packageRevision = ulong.Parse(request.PackageRevision);
        if (catalog.Get(CatalogKind.Package, request.PackageId, packageRevision) is null) throw new ArgumentException("Package version does not exist");
        lock (stateLock) {
            var revision = checked(catalog.LatestRevision(CatalogKind.Deployment, request.Id) + 1);
            var projectId = "deployment-project/" + request.Id;
            catalog.Put(CatalogKind.Project, projectId, revision, JsonSerializer.Serialize(new { version = ProductInfo.Protocol,
                id = projectId, packages = new[] { new { id = request.PackageId, revision = packageRevision } } }, HostPolicy.JsonOptions));
            catalog.Put(CatalogKind.Deployment, request.Id, revision, JsonSerializer.Serialize(new { version = ProductInfo.Protocol,
                id = request.Id, project = new { id = projectId, revision } }, HostPolicy.JsonOptions));
            var bound = catalog.BindPackage(request.Id, revision, request.PackageId);
            using var execution = policy.CreateExecution(bound.Package, bound.Parameters);
            var state = new DeploymentState(ProductInfo.Protocol, request.Id, request.PackageId, request.PackageRevision,
                revision.ToString(System.Globalization.CultureInfo.InvariantCulture), true, Now,
                deployments.GetValueOrDefault(request.Id)?.Revision + 1 ?? 1);
            SaveDeployment(state); return state;
        }
    }
    private void SaveDeployment(DeploymentState state)
    {
        using var catalog = policy.OpenCatalog();
        catalog.Put(CatalogKind.Audit, DeploymentPrefix + state.Id, state.Revision,
            JsonSerializer.Serialize(state with { Id = DeploymentPrefix + state.Id }, HostPolicy.JsonOptions));
        deployments[state.Id] = state;
    }
    public DeploymentState ActivateDeployment(string id, bool enabled)
    {
        lock (stateLock) {
            var state = deployments.GetValueOrDefault(id) ?? throw new ArgumentException("Deployment does not exist");
            state = state with { Enabled = enabled, TimestampMs = Now, Revision = state.Revision + 1 };
            SaveDeployment(state);
            if (!enabled) foreach (var run in states.Values.Where(run => run.DeploymentId == id && run.State is "Queued" or "Claimed").ToArray())
                Save(run with { State = "Cancelled", Revision = run.Revision + 1, TimestampMs = Now });
            return state;
        }
    }
    public JobState SaveJob(JobState job)
    {
        if (job.Version != ProductInfo.Protocol || job.Id.Length == 0 || job.EverySeconds == 0 || job.MaxConcurrency == 0 ||
            job.MaxConcurrency > policy.MaxConcurrency || job.MaxAttempts is < 1 or > 10 || job.RetryDelaySeconds > 86400)
            throw new ArgumentException("Invalid job policy");
        using var catalog = policy.OpenCatalog();
        if (catalog.Get(CatalogKind.Package, job.PackageId, ulong.Parse(job.PackageRevision)) is null)
            throw new ArgumentException("Package version does not exist");
        lock (stateLock) {
            job = job with { Revision = jobs.GetValueOrDefault(job.Id)?.Revision + 1 ?? 1,
                TimestampMs = Now, NextDueMs = job.NextDueMs == 0 ? Now : job.NextDueMs };
            SaveJobRecord(job); return job;
        }
    }
    private void SaveJobRecord(JobState job)
    {
        using var catalog = policy.OpenCatalog();
        catalog.Put(CatalogKind.Audit, JobPrefix + job.Id, job.Revision,
            JsonSerializer.Serialize(job with { Id = JobPrefix + job.Id }, HostPolicy.JsonOptions));
        jobs[job.Id] = job;
    }
    private void Save(RunState state)
    {
        using var catalog = policy.OpenCatalog();
        catalog.Put(CatalogKind.Audit, Prefix + state.Id, state.Revision,
            JsonSerializer.Serialize(state with { Id = Prefix + state.Id }, HostPolicy.JsonOptions));
        states[state.Id] = state;
    }
    public RunState Enqueue(RunRequest request)
    {
        using var catalog = policy.OpenCatalog();
        if (catalog.Get(CatalogKind.Package, request.PackageId, ulong.Parse(request.PackageRevision)) is null)
            throw new ArgumentException("Package version does not exist");
        using var parameters = JsonDocument.Parse(request.Parameters);
        if (parameters.RootElement.ValueKind != JsonValueKind.Object) throw new ArgumentException("Parameters must be an object");
        lock (stateLock) {
            if (request.DeploymentId is { } deploymentId &&
                (!deployments.TryGetValue(deploymentId, out var deployment) || !deployment.Enabled ||
                 deployment.DeploymentRevision != request.DeploymentRevision || deployment.PackageId != request.PackageId ||
                 deployment.PackageRevision != request.PackageRevision)) throw new ArgumentException("Deployment binding is inactive or stale");
            var state = new RunState(ProductInfo.Protocol, Guid.NewGuid().ToString("N"), request.PackageId, request.PackageRevision,
                request.Parameters, "Queued", TimestampMs: Now, Debug: request.Debug?.Clone(),
                DeploymentId: request.DeploymentId, DeploymentRevision: request.DeploymentRevision);
            Save(state); return state;
        }
    }
    public RunState Resume(string id)
    {
        lock (stateLock) {
            var state = states.GetValueOrDefault(id) ?? throw new ArgumentException("Execution does not exist");
            if (state.State != "RecoveryRequired" || state.ExecutionId.Length == 0 || workers.ContainsKey(id) || Alive(state))
                throw new ArgumentException("Execution is not ready for recovery");
            using var catalog = policy.OpenCatalog();
            var revision = catalog.LatestRevision(CatalogKind.Execution, state.ExecutionId);
            state = state with { State = "Queued", Resume = true, ExecutionRevision = revision,
                Owner = null, ProcessId = null, Revision = state.Revision + 1, TimestampMs = Now };
            Save(state); return state;
        }
    }
    public async Task Cancel(string id)
    {
        var current = Get(id) ?? throw new ArgumentException("Execution does not exist");
        if (current.State == "Running" && Alive(current)) { await WorkerControl.Send(id, "cancel"); return; }
        lock (stateLock) {
            var state = states.GetValueOrDefault(id) ?? throw new ArgumentException("Execution does not exist");
            if (state.State is "Queued" or "Claimed") Save(state with { State = "Cancelled", Revision = state.Revision + 1, TimestampMs = Now });
        }
    }
    public async Task<JsonElement> Debug(string id, JsonElement? command = null)
    {
        var state = Get(id) ?? throw new ArgumentException("Execution does not exist");
        if (state.State != "Running" || state.Debug is null || !Alive(state))
            throw new ArgumentException("Execution is not an active debug session");
        return await WorkerControl.Send(id, command is null ? "snapshot" : "debug", command);
    }
    private void Restore()
    {
        using var catalog = policy.OpenCatalog();
        string? afterId = null; ulong afterRevision = 0;
        while (true) {
            var query = new Dictionary<string, object> { ["kind"] = (int)CatalogKind.Audit, ["limit"] = 100 };
            if (afterId is not null) { query["afterId"] = afterId; query["afterRevision"] = afterRevision; }
            using var page = JsonDocument.Parse(catalog.Query(JsonSerializer.Serialize(query, HostPolicy.JsonOptions)));
            if (page.RootElement.GetArrayLength() == 0) break;
            foreach (var entry in page.RootElement.EnumerateArray()) {
                afterId = entry.GetProperty("id").GetString()!; afterRevision = entry.GetProperty("revision").GetUInt64();
                if (afterId.StartsWith(DeploymentPrefix, StringComparison.Ordinal)) {
                    var deployment = entry.GetProperty("document").Deserialize<DeploymentState>(HostPolicy.JsonOptions)!;
                    deployment = deployment with { Id = deployment.Id[DeploymentPrefix.Length..] };
                    if (!deployments.TryGetValue(deployment.Id, out var prior) || prior.Revision < deployment.Revision) deployments[deployment.Id] = deployment;
                    continue;
                }
                if (afterId.StartsWith(JobPrefix, StringComparison.Ordinal)) {
                    var job = entry.GetProperty("document").Deserialize<JobState>(HostPolicy.JsonOptions)!;
                    job = job with { Id = job.Id[JobPrefix.Length..] };
                    if (!jobs.TryGetValue(job.Id, out var prior) || prior.Revision < job.Revision) jobs[job.Id] = job;
                    continue;
                }
                if (!afterId.StartsWith(Prefix, StringComparison.Ordinal)) continue;
                var state = entry.GetProperty("document").Deserialize<RunState>(HostPolicy.JsonOptions)!;
                state = state with { Id = state.Id[Prefix.Length..] };
                if (!states.TryGetValue(state.Id, out var existing) || existing.Revision < state.Revision) states[state.Id] = state;
            }
        }
        foreach (var state in states.Values.ToArray())
            if (state.State == "Claimed")
                Save(state with { State = "Queued", Owner = null, ProcessId = null, Revision = state.Revision + 1, TimestampMs = Now });
            else if (state.State == "Running" && !Alive(state))
                Save(state with { State = "RecoveryRequired", Revision = state.Revision + 1, TimestampMs = Now });
    }
    private static bool Alive(RunState state)
    {
        if (state.ProcessId is not { } id) return false;
        try { using var process = Process.GetProcessById(id); return !process.HasExited &&
            state.ProcessStartedAtMs == new DateTimeOffset(process.StartTime.ToUniversalTime()).ToUnixTimeMilliseconds(); }
        catch (ArgumentException) { return false; }
    }
    private void Reconcile()
    {
        using var catalog = policy.OpenCatalog();
        foreach (var state in List().Where(run => run.State == "Running" && !workers.ContainsKey(run.Id))) {
            var revision = catalog.LatestRevision(CatalogKind.Execution, state.ExecutionId);
            var json = revision == 0 ? null : catalog.Get(CatalogKind.Execution, state.ExecutionId, revision);
            if (json is not null) {
                using var document = JsonDocument.Parse(json);
                int native = document.RootElement.GetProperty("state").GetInt32();
                if (native is 5 or 6 or 7 or 9) {
                    lock (stateLock) Save(state with { State = ((ExecutionState)native).ToString(),
                        Result = document.RootElement.GetProperty("result").GetInt32(), Revision = state.Revision + 1, TimestampMs = Now });
                    continue;
                }
            }
            if (!Alive(state)) lock (stateLock) Save(state with { State = "RecoveryRequired",
                Result = (int)StatusCode.CommitUnknown, Revision = state.Revision + 1, TimestampMs = Now });
        }
    }
    private void Schedule()
    {
        lock (stateLock) {
            foreach (var job in jobs.Values.Where(job => job.Enabled && job.NextDueMs <= Now).ToArray()) {
                if (states.Values.Count(run => run.JobId == job.Id && run.State is "Queued" or "Claimed" or "Running") >= job.MaxConcurrency) continue;
                var identity = System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes($"{job.Id}/{job.Revision}/{job.NextDueMs}"));
                var id = Convert.ToHexString(identity.AsSpan(0, 16)).ToLowerInvariant();
                if (!states.ContainsKey(id)) Save(new RunState(ProductInfo.Protocol, id, job.PackageId, job.PackageRevision,
                    job.Parameters, "Queued", TimestampMs: job.NextDueMs, JobId: job.Id));
                SaveJobRecord(job with { NextDueMs = checked(Now + job.EverySeconds * 1000L), TimestampMs = Now, Revision = job.Revision + 1 });
            }
        }
    }
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // One manager owns this single-machine product directory. Native WAL
        // still allows all worker processes and SDK readers to share Catalog.
        using var ownership = new FileStream(Path.Combine(policy.DataDirectory, "management.lock"),
            FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        lock (stateLock) Restore();
        while (!stoppingToken.IsCancellationRequested) {
            Reconcile();
            Schedule();
            foreach (var state in List().Where(run => run.State == "Queued" && run.NotBeforeMs <= Now)) {
                if (workers.Count >= policy.MaxConcurrency) break;
                var task = StartWorker(state.Id, stoppingToken); monitors[state.Id] = task;
                _ = task.ContinueWith(_ => monitors.TryRemove(state.Id, out var ignored), TaskScheduler.Default);
            }
            await Task.Delay(250, stoppingToken);
        }
    }
    private async Task StartWorker(string id, CancellationToken cancellation)
    {
        RunState state;
        lock (stateLock) {
            state = states[id]; if (state.State != "Queued") return;
            state = state with { State = "Claimed", Owner = owner, Revision = state.Revision + 1, TimestampMs = Now }; Save(state);
        }
        Process? process = null; bool startSent = false;
        try {
            using var catalog = policy.OpenCatalog();
            var package = catalog.Get(CatalogKind.Package, state.PackageId, ulong.Parse(state.PackageRevision))!;
            var parameters = state.Parameters;
            if (state.DeploymentId is { } deploymentId) {
                var bound = catalog.BindPackage(deploymentId, ulong.Parse(state.DeploymentRevision!), state.PackageId,
                    packageParameters: state.Parameters);
                package = bound.Package; parameters = bound.Parameters;
            }
            var start = new ProcessStartInfo("dotnet") { RedirectStandardInput = true, RedirectStandardOutput = true,
                RedirectStandardError = true, UseShellExecute = false, CreateNoWindow = true };
            start.ArgumentList.Add(configuration.WorkerPath); start.ArgumentList.Add(configuration.PolicyPath);
            process = Process.Start(start) ?? throw new InvalidOperationException("Worker failed to start");
            workers[id] = process;
            _ = process.StandardError.ReadToEndAsync(cancellation); // Native/log API owns diagnostics; no secret stderr is exposed.
            await process.StandardInput.WriteLineAsync(JsonSerializer.Serialize(new { version = ProductInfo.Protocol, ticket = id,
                package, parameters, executionId = state.ExecutionId, resume = state.Resume,
                revision = state.ExecutionRevision, debug = state.Debug }, HostPolicy.JsonOptions));
            await process.StandardInput.FlushAsync(cancellation);
            var ready = await process.StandardOutput.ReadLineAsync(cancellation) ?? throw new InvalidDataException("Worker did not become ready");
            using var message = JsonDocument.Parse(ready);
            if (message.RootElement.GetProperty("version").GetInt32() != ProductInfo.Protocol ||
                message.RootElement.GetProperty("kind").GetString() != "ready" || message.RootElement.GetProperty("ticket").GetString() != id)
                throw new InvalidDataException("Worker handshake differs from dispatch");
            var nativeId = message.RootElement.GetProperty("executionId").GetString()!;
            lock (stateLock) {
                if (states[id].State != "Claimed" || states[id].Owner != owner) return;
                state = states[id] with { State = "Running", ExecutionId = nativeId, ProcessId = process.Id,
                    ProcessStartedAtMs = new DateTimeOffset(process.StartTime.ToUniversalTime()).ToUnixTimeMilliseconds(),
                    Revision = states[id].Revision + 1, TimestampMs = Now }; Save(state);
            }
            startSent = true;
            await process.StandardInput.WriteLineAsync("start"); await process.StandardInput.FlushAsync(cancellation);
            var terminal = await process.StandardOutput.ReadLineAsync(cancellation) ?? throw new InvalidDataException("Worker outcome is unknown");
            using var outcome = JsonDocument.Parse(terminal);
            if (outcome.RootElement.GetProperty("version").GetInt32() != ProductInfo.Protocol ||
                outcome.RootElement.GetProperty("kind").GetString() != "terminal" || outcome.RootElement.GetProperty("ticket").GetString() != id ||
                outcome.RootElement.GetProperty("executionId").GetString() != nativeId)
                throw new InvalidDataException("Worker outcome differs from dispatch");
            int result = outcome.RootElement.GetProperty("result").GetInt32();
            string final = outcome.RootElement.GetProperty("state").GetString()!;
            lock (stateLock) {
                state = states[id] with { State = final, Result = result, Revision = states[id].Revision + 1, TimestampMs = Now };
                if (final == "Failed" && result != (int)StatusCode.CommitUnknown && state.JobId is { } jobId &&
                    jobs.TryGetValue(jobId, out var job) && job.Enabled && state.Attempt < job.MaxAttempts) {
                    state = state with { State = "Queued", Resume = true,
                        ExecutionRevision = catalog.LatestRevision(CatalogKind.Execution, state.ExecutionId),
                        Attempt = state.Attempt + 1, NotBeforeMs = checked(Now + job.RetryDelaySeconds * 1000L), ProcessId = null, Owner = null };
                }
                Save(state);
            }
            await process.WaitForExitAsync(cancellation);
        } catch (Exception error) when (error is not OutOfMemoryException) {
            lock (stateLock) Save(states[id] with { State = startSent ? "RecoveryRequired" : "Failed", Result = startSent ? (int)StatusCode.CommitUnknown : (int)StatusCode.Invalid,
                Revision = states[id].Revision + 1, TimestampMs = Now });
        } finally {
            workers.TryRemove(id, out _);
            if (process is not null) {
                if (!process.HasExited) { try { await process.StandardInput.WriteLineAsync("cancel"); } catch { } }
                process.Dispose();
            }
        }
    }
}
