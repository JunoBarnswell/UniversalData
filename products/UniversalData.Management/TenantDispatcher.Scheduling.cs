using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using UniversalData;
using UniversalData.Hosting;

namespace UniversalData.Management;

public sealed partial class TenantDispatcher
{
    private long schedulerStartedMs = -1;
    private static readonly JsonSerializerOptions LegacyJobJson = new(HostPolicy.JsonOptions)
    {
        RespectRequiredConstructorParameters = false,
        RespectNullableAnnotations = false
    };

    public JobState SaveJob(HostIdentity identity, JobRequest request)
    {
        identity.Require("job.write", request.Id);
        identity.Require("package.execute", request.PackageId);
        if (request.Version != ProductInfo.Protocol || !HostIdentity.ValidId(request.Id) ||
            request.Schedules is null || request.Schedules.Length > 64 ||
            request.MaxConcurrency == 0 || request.MaxConcurrency > policy.MaxConcurrency ||
            request.MaxAttempts is < 1 or > 10 || request.RetryDelaySeconds > 86400)
            throw new ArgumentException("Invalid job policy");
        foreach (var schedule in request.Schedules)
            NativeJobScheduler.Validate(schedule);
        if (request.Schedules.Select(schedule => schedule.Id).Distinct(StringComparer.Ordinal).Count() != request.Schedules.Length)
            throw new ArgumentException("Schedule identities must be unique within the job");
        var package = Package(request.PackageId, request.PackageRevision);
        _ = WorkerParallelism.Read(package, policy.MaxConcurrency);
        CheckElastic(request.Elastic,package,null);

        lock (gate)
        {
            RefreshDefinitions();
            var prior = jobs.GetValueOrDefault(request.Id);
            var reference = request.Parameters is not null ? ParametersReference(request.Parameters) :
                request.ParametersReference == prior?.ParametersReference ? prior?.ParametersReference ?? "" : "";
            CheckDeployment(request.DeploymentId, request.DeploymentRevision, request.PackageId, request.PackageRevision);
            if (request.DeploymentId is not null)
                identity.Require("deployment.execute", request.DeploymentId);
            using var clock = policy.OpenCatalog();
            var now = clock.WorkerClock;
            var schedules = request.Schedules.Select(schedule =>
            {
                var previous = prior?.Schedules.SingleOrDefault(cursor => cursor.Schedule.Id == schedule.Id);
                if (previous is not null && prior!.Enabled == request.Enabled &&
                    (previous.Schedule with { Weekdays = null }) == (schedule with { Weekdays = null }) &&
                    (previous.Schedule.Weekdays ?? []).SequenceEqual(schedule.Weekdays ?? []))
                    return previous;
                return new ScheduleCursor(schedule, NativeJobScheduler.Next(schedule, now - 1));
            }).ToArray();
            var job = new JobState(ProductInfo.Protocol, request.Id, request.PackageId, request.PackageRevision,
                reference, request.Enabled, schedules, identity.Subject, identity.Stamp, request.MaxConcurrency,
                request.MaxAttempts, request.RetryDelaySeconds, now, checked((prior?.Revision ?? 0) + 1),
                request.DeploymentId, request.DeploymentRevision, Elastic: request.Elastic);
            Save(job);
            return job;
        }
    }

    public long[] PreviewSchedule(HostIdentity identity, string jobId, JobSchedule schedule)
    {
        identity.Require("job.read", jobId);
        NativeJobScheduler.Validate(schedule);
        var times = new List<long>(5);
        using var clock = policy.OpenCatalog();
        long after = clock.WorkerClock - 1;
        while (times.Count < 5 && NativeJobScheduler.Next(schedule, after) is { } next)
        {
            times.Add(next);
            after = next;
        }
        return [.. times];
    }

    public JobOccurrence[] Occurrences(HostIdentity identity, string jobId, string? afterId = null)
    {
        identity.Require("job.read", jobId);
        if (!HostIdentity.ValidId(jobId) || afterId is not null && !HostIdentity.ValidId(afterId))
            throw new ArgumentException("Invalid occurrence cursor");
        string prefix = $"occurrence/{jobId}/";
        using var catalog = policy.OpenCatalog();
        using var page = JsonDocument.Parse(catalog.Query(JsonSerializer.Serialize(new
        {
            kind = (int)CatalogKind.Audit, limit = 100, latestOnly = true, afterId = prefix + afterId
        }, HostPolicy.JsonOptions)));
        return page.RootElement.EnumerateArray().Where(row =>
                row.GetProperty("id").GetString()!.StartsWith(prefix, StringComparison.Ordinal))
            .Select(row => row.GetProperty("document").Deserialize<JobOccurrence>(HostPolicy.JsonOptions)!)
            .Select(occurrence => occurrence with { Id = occurrence.Id[prefix.Length..] }).ToArray();
    }

    public JobOccurrence RunJob(HostIdentity identity, string jobId, JobRunRequest request)
    {
        identity.Require("job.write", jobId);
        if (!HostIdentity.ValidId(jobId) || !HostIdentity.ValidId(request.RequestId) ||
            request.OccurrenceId is not null && !HostIdentity.ValidId(request.OccurrenceId))
            throw new ArgumentException("Invalid job occurrence identity");
        var occurrenceId = request.OccurrenceId ?? "manual-" + OccurrenceId(jobId, request.RequestId);
        var key = OccurrenceKey(jobId, occurrenceId);
        using var catalog = policy.OpenCatalog();
        if (ReadOccurrence(catalog, key, occurrenceId) is { RunId: not null } issued)
            return issued;
        lock (gate)
        {
            var lease = catalog.TryAcquireLease(CatalogKind.Audit, "job/" + jobId, owner, LeaseDuration);
            if (lease is null)
                return ReadOccurrence(catalog, key, occurrenceId) is { RunId: not null } current ? current :
                    throw new UniversalDataException((int)StatusCode.Busy, "Job is owned by another scheduler");
            try
            {
                RefreshDefinitions();
                var job = jobs.GetValueOrDefault(jobId) ?? throw new ArgumentException("Job does not exist");
                var previous = ReadOccurrence(catalog, key, occurrenceId);
                if (previous is { RunId: not null })
                    return previous;
                if (request.OccurrenceId is not null && previous?.State != "Missed")
                    throw new ArgumentException("The selected occurrence was not missed");
                var occurrence = previous ?? CreateOccurrence(job, occurrenceId, null, catalog.WorkerClock);
                return PublishOccurrence(catalog, lease, job, occurrence, identity, null, "Queued");
            }
            finally { ReleaseCatalogLease(catalog, lease); }
        }
    }

    private static JobOccurrence? ReadOccurrence(Catalog catalog, string key, string id)
    {
        string? document = catalog.GetConfirmed(CatalogKind.Audit, key);
        return document is null ? null : JsonSerializer.Deserialize<JobOccurrence>(document, HostPolicy.JsonOptions)! with { Id = id };
    }

    private void Schedule()
    {
        using var clock = policy.OpenCatalog();
        var now = clock.WorkerClock;
        if (schedulerStartedMs < 0)
            schedulerStartedMs = now;
        foreach (var snapshot in jobs.Values.Where(job => job.RecordVersion == 2 ||
                     job.Enabled && job.Schedules.Any(cursor => cursor.Schedule.Enabled && cursor.NextDueMs <= now))
                     .ToArray())
        {
            using var catalog = policy.OpenCatalog();
            var lease = catalog.TryAcquireLease(CatalogKind.Audit, "job/" + snapshot.Id, owner, LeaseDuration);
            if (lease is null)
                continue;
            try
            {
                RefreshDefinitions();
                var job = jobs[snapshot.Id];
                if (job.RecordVersion == 2)
                {
                    Write("job/" + job.Id, checked(job.Revision + 1), JsonSerializer.Serialize(job with
                    {
                        Id = "job/" + job.Id, Revision = checked(job.Revision + 1),
                        Version = ProductInfo.Protocol, RecordVersion = 3, TimestampMs = now
                    }, HostPolicy.JsonOptions), lease);
                    continue;
                }
                if (!job.Enabled)
                    continue;
                var selected = NativeJobScheduler.SelectDue(job.Schedules.Select(cursor => cursor.NextDueMs).ToArray(),
                    job.Schedules.Select(cursor => cursor.Schedule.Enabled).ToArray(), now);
                if (selected is not { } dueSelection)
                    continue;
                var cursor = job.Schedules[dueSelection.Index];
                long due = dueSelection.Due;
                var identity = identities.Find(job.Subject, job.SubjectStamp);
                if (identity is null || !identity.Allows("package.execute", job.PackageId) ||
                    job.DeploymentId is not null && !identity.Allows("deployment.execute", job.DeploymentId))
                    continue;

                bool missed = due < schedulerStartedMs;
                string id = $"{due:D15}-{OccurrenceId(job.Id, cursor.Schedule.Id)}";
                var occurrence = CreateOccurrence(job, id, cursor.Schedule.Id, due);
                try
                {
                    PublishOccurrence(catalog, lease, job, occurrence, identity, cursor, missed ? "Missed" : "Queued");
                }
                catch (Exception error) when (error is ArgumentException ||
                    error is UniversalDataException { Code: StatusCode.Incompatible })
                {
                    PublishOccurrence(catalog, lease, job, occurrence, identity, cursor, "Blocked");
                }
            }
            catch (UniversalDataException error) when (error.Code is StatusCode.Busy or StatusCode.Fenced)
            {
                // The next scheduler tick will use the current Catalog head.
            }
            finally { ReleaseCatalogLease(catalog, lease); }
        }
    }

    private JobOccurrence PublishOccurrence(Catalog catalog, CatalogLease lease, JobState job,
        JobOccurrence occurrence, HostIdentity identity, ScheduleCursor? cursor, string state)
    {
        long now = catalog.WorkerClock;
        RunState? run = null;
        if (state == "Queued")
        {
            identity.Require("package.execute", occurrence.PackageId);
            CheckDeployment(occurrence.DeploymentId, occurrence.DeploymentRevision,
                occurrence.PackageId, occurrence.PackageRevision);
            if (occurrence.DeploymentId is not null)
                identity.Require("deployment.execute", occurrence.DeploymentId);
            string package = Package(occurrence.PackageId, occurrence.PackageRevision);
            using var native = new NativePackage(package, policy.MemoryLimit);
            string runId = OccurrenceId(job.Id, occurrence.Id);
            run = new RunState(ProductInfo.Protocol, runId, occurrence.PackageId, occurrence.PackageRevision,
                job.ParametersReference, "Queued", identity.Subject, identity.Stamp, ControlReference(),
                TimestampMs: now, JobId: job.Id, DeploymentId: occurrence.DeploymentId,
                DeploymentRevision: occurrence.DeploymentRevision,
                Parallelism: WorkerParallelism.Read(package, policy.MaxConcurrency),
                RequiresJournal: native.RequiresJournal, Elastic: job.Elastic, Continuous: native.IsContinuous);
        }
        var next = job with
        {
            Version = ProductInfo.Protocol, Revision = checked(job.Revision + 1), TimestampMs = now,
            Schedules = cursor is null ? job.Schedules : job.Schedules.Select(current =>
                current.Schedule.Id == cursor.Schedule.Id
                    ? current with { NextDueMs = NativeJobScheduler.Next(current.Schedule, occurrence.TimestampMs) }
                    : current).ToArray()
        };
        var stored = occurrence with
        {
            Version = ProductInfo.Protocol, State = state, RunId = run?.Id,
            Revision = occurrence.State == "Missed" ? checked(occurrence.Revision + 1) : 1
        };
        string key = OccurrenceKey(job.Id, stored.Id);
        catalog.PublishOccurrence(lease, next.Revision,
            JsonSerializer.Serialize(next with { Id = lease.Id }, HostPolicy.JsonOptions),
            key, stored.Revision, JsonSerializer.Serialize(stored with { Id = key }, HostPolicy.JsonOptions),
            run is null ? null : "run/" + run.Id,
            run is null ? null : JsonSerializer.Serialize(run with { Id = "run/" + run.Id }, HostPolicy.JsonOptions));
        jobs[next.Id] = next;
        if (run is not null)
            states[run.Id] = run;
        return stored;
    }

    private static JobOccurrence CreateOccurrence(JobState job, string id, string? schedule, long timestamp) =>
        new(ProductInfo.Protocol, id, job.Id, schedule, timestamp, "Pending", job.PackageId,
            job.PackageRevision, job.DeploymentId, job.DeploymentRevision, null);

    private static string OccurrenceKey(string jobId, string id) => $"occurrence/{jobId}/{id}";

    private static string OccurrenceId(string job, string identity) => Convert.ToHexString(
        SHA256.HashData(Encoding.UTF8.GetBytes($"{job}/{identity}")).AsSpan(0,16)).ToLowerInvariant();

    private static JobState ReadJob(JsonElement document)
    {
        bool legacy = document.GetProperty("version").GetInt32() == 14 &&
            document.TryGetProperty("recordVersion", out var record) && record.GetInt32() == 2;
        var job = document.Deserialize<JobState>(legacy ? LegacyJobJson : HostPolicy.JsonOptions)
            ?? throw new InvalidDataException("Invalid Catalog job");
        if (job.ParametersReference is null || job.Subject is null)
            throw new InvalidDataException("Legacy job requires explicit credential migration");
        if (legacy)
        {
            var cursor = document.GetProperty("nextDueMs").GetInt64();
            var schedule = new JobSchedule("default", JobFrequency.Interval,
                DateTimeOffset.FromUnixTimeMilliseconds(cursor),
                IntervalSeconds: document.GetProperty("everySeconds").GetUInt32());
            NativeJobScheduler.Validate(schedule);
            job = job with { Schedules = [new(schedule, cursor)] };
        }
        else if (job.RecordVersion != 3 || job.Schedules is null)
            throw new InvalidDataException("Invalid job schedule revision");
        return job with { Id = job.Id[4..] };
    }
}
