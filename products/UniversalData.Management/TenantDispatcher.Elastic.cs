using System.Text.Json;
using UniversalData.Hosting;

namespace UniversalData.Management;

public sealed partial class TenantDispatcher
{
    private void CheckElastic(KafkaElasticity? elastic,string package,JsonElement? debug)
    {
        if (elastic is null) return;
        if (debug is not null) throw new ArgumentException("Elastic Kafka cannot use execution debugging");
        elastic.ReadSource(package,policy.MaxConcurrency);
    }

    private async Task<KafkaPartitionPosition[]> SampleKafka(RunState run, KafkaSource source, CancellationToken cancellation)
    {
        var subject = identities.Find(run.Subject,run.SubjectStamp)
            ?? throw new UniversalDataException((int)StatusCode.Auth,"Kafka dispatch authority expired");
        subject.Require("package.execute",run.PackageId);
        var scoped = root.Bind(subject);
        string package = Package(run.PackageId,run.PackageRevision);
        string parameters = run.ParametersReference.Length == 0 ? "{}" : new ParameterVault(scoped).Resolve(run.ParametersReference);
        if (run.DeploymentId is { } deployment)
        {
            subject.Require("deployment.execute",deployment);
            using var catalog = policy.OpenCatalog();
            (package,parameters) = catalog.BindPackage(deployment,ulong.Parse(run.DeploymentRevision!),run.PackageId,
                packageParameters:parameters);
        }
        var sourcePolicy = scoped with { Cluster = null };
        var vault = new ParameterVault(sourcePolicy);
        string reference = parameters == "{}" ? "" : vault.Store(parameters);
        try
        {
            var payload = JsonSerializer.SerializeToElement(new { run.PackageId,source.Connection,source.Topic,source.Group },HostPolicy.JsonOptions);
            var request = new WorkerRequest(ProductInfo.Protocol,Guid.NewGuid().ToString("N"),"kafkaPositions",
                sourcePolicy,subject,package,ParametersReference:reference,Payload:payload,Isolated:scoped.RequiresIsolation(package));
            var sample = await supervisor.InspectAsync(request,cancellation);
            return sample.Deserialize<KafkaPartitionPosition[]>(HostPolicy.JsonOptions)
                ?? throw new InvalidDataException("Kafka position sample is empty");
        }
        finally { vault.RemoveLocal(reference); }
    }

    private async Task DispatchElastic(RunState initial, CancellationToken stopping)
    {
        using var catalog = policy.OpenCatalog();
        var key = "run/" + initial.Id;
        var lease = catalog.TryAcquireLease(CatalogKind.Audit,key,owner,ExecutionLeaseDuration);
        if (lease is null && configuration.Cluster && initial.Owner is { } previous && initial.TaskGeneration != 0 &&
            initial.State == "Running")
            lease = catalog.AdoptLease(new(CatalogKind.Audit,key,previous,initial.TaskGeneration,0),owner,ExecutionLeaseDuration);
        if (lease is null) return;
        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(stopping);
        var heartbeat = HoldGroupLease(lease,lifetime,lifetime.Token);
        try
        {
            var run = Get(initial.Id);
            if (run.State is not ("Queued" or "Running")) return;
            if (run.State == "Queued")
            {
                lock (gate)
                {
                    if (Control(catalog,run) is not null)
                    {
                        Save(run with { State="Cancelled",Result=(int)StatusCode.Cancelled,Revision=checked(run.Revision+1) },lease);
                        return;
                    }
                    run = run with { State="Claimed",Owner=owner,StartAuthorized=false,
                        TaskGeneration=lease.Generation,Revision=checked(run.Revision+1),TimestampMs=catalog.WorkerClock };
                    Save(run,lease);
                }
            }
            var controller = run.Elastic!;
            var plan = run.ElasticPlan;
            var package = Package(run.PackageId,run.PackageRevision);
            using var requirements = JsonDocument.Parse(package);
            if (plan is null)
            {
                try
                {
                    var source = controller.ReadSource(package,policy.MaxConcurrency);
                    var sample = await SampleKafka(run,source,lifetime.Token);
                    plan = controller.Freeze(source,sample,policy.MaxConcurrency,catalog.WorkerClock);
                }
                catch (Exception error) when (error is UniversalDataException or ArgumentException or IOException)
                {
                    var result = error is UniversalDataException native ? native.Code :
                        error is ArgumentException ? StatusCode.Invalid : StatusCode.Io;
                    var decision = RetryDecision(run, (int)result,
                        brokerRestore:run.BrokerRestore, remote:run.Placement?.Remote == true);
                    bool retry = decision.State == NativeRunState.Queued;
                    string reason = retry ? "KafkaPositionsUnavailable" : decision.State == NativeRunState.RecoveryRequired ?
                        "KafkaPartitionRecoveryRequired" : result == StatusCode.Auth ?
                        "DispatchAuthorityExpired" : "KafkaPositionsRejected";
                    if (retry) queueReasons[run.Id] = reason;
                    else queueReasons.TryRemove(run.Id, out _);
                    lock (gate) Save(run with { State=RetryState(decision.State), Owner=null,
                        NotBeforeMs=retry ? decision.NotBeforeMilliseconds : run.NotBeforeMs,
                        Revision=checked(run.Revision+1),
                        Failure=decision.Result == StatusCode.Ok ? null :
                            new RunFailure((int)decision.Result,"kafka.sample",reason),
                        Result=(int)decision.Result },lease);
                    return;
                }
            }
            queueReasons.TryRemove(run.Id,out _);
            if (run.Owner != owner || run.State != "Running" || run.ElasticPlan is null)
            {
                lock (gate)
                {
                    run = run with { State="Running",Owner=owner,TaskGeneration=lease.Generation,
                        StartAuthorized=true,ElasticPlan=plan,MemberIds=plan.Groups.Select(group => group.Id).ToArray(),
                        Revision=checked(run.Revision+1),TimestampMs=catalog.WorkerClock };
                    Save(run,lease);
                }
            }
            while (!lifetime.IsCancellationRequested)
            {
                bool stop = false;
                lock (gate)
                {
                    run = Get(run.Id);
                    plan = run.ElasticPlan!;
                    stop = Control(catalog,run) is not null;
                    if (stop)
                        foreach (var group in plan.Groups)
                            StopKafkaGroup(catalog,group.Id);
                    else
                        run = AdvanceElastic(catalog,lease,run,requirements.RootElement,lifetime.Token);

                    if (stop || run.ElasticPlan?.Reason == "KafkaPartitionRecoveryRequired")
                    {
                        var members = run.MemberIds ?? [];
                        if (members.All(id => ReadRun(catalog,id)?.State is not ("Queued" or "Claimed" or "Running")))
                        {
                            if (members.Any(id => ReadRun(catalog,id) is null))
                                Save(run with { State="Stopped",Revision=checked(run.Revision+1),TimestampMs=catalog.WorkerClock },lease);
                            else
                                SaveGroupOutcome(catalog,lease,run,members);
                            return;
                        }
                    }
                }
                long now = catalog.WorkerClock;
                if (!stop && run.ElasticPlan is { Next:null } current && now - current.LastSampleMs >= 5000 &&
                    current.Reason != "KafkaPartitionRecoveryRequired")
                {
                    KafkaElasticState observed;
                    try
                    {
                        var sample = await SampleKafka(run,current.Source,lifetime.Token);
                        observed = controller.Observe(current,sample,policy.MaxConcurrency,now);
                    }
                    catch (Exception error) when (error is IOException or UniversalDataException or ArgumentException)
                    {
                        var result = error is UniversalDataException native ? native.Code :
                            error is ArgumentException ? StatusCode.Invalid : StatusCode.Io;
                        var decision = RetryDecision(run, (int)result,
                            brokerRestore:run.BrokerRestore, remote:run.Placement?.Remote == true);
                        string reason = decision.State switch
                        {
                            NativeRunState.Queued => "KafkaPositionsUnavailable",
                            NativeRunState.RecoveryRequired => "KafkaPartitionRecoveryRequired",
                            _ => "KafkaPositionsRejected"
                        };
                        observed = current with { Reason=reason,LastSampleMs=now,HighSamples=0,LowSinceMs=0 };
                    }
                    lock (gate) SaveElasticDecision(catalog,lease,run,current,observed,requirements.RootElement);
                }
                await Task.Delay(500,lifetime.Token);
            }
        }
        finally
        {
            await lifetime.CancelAsync();
            await heartbeat;
            ReleaseCatalogLease(catalog, lease);
        }
    }

    private RunState AdvanceElastic(Catalog catalog, CatalogLease lease, RunState run, JsonElement package, CancellationToken cancellation)
    {
        var plan = run.ElasticPlan!;
        if (plan.Next is { } next)
        {
            var retired = plan.Groups.Where(group => !next.Any(candidate => candidate.Id == group.Id)).ToArray();
            foreach (var group in retired) StopKafkaGroup(catalog,group.Id);
            if (retired.All(group => ReadRun(catalog,group.Id)?.State is not ("Queued" or "Claimed" or "Running")))
            {
                if (retired.Any(group => ReadRun(catalog,group.Id) is { State:"RecoveryRequired" } member &&
                    member.Failure?.Phase is not ("worker.report" or "dispatch.reconcile")))
                {
                    plan = plan with { Next=null,Reason="KafkaPartitionRecoveryRequired" };
                    return PersistElastic(catalog,run,lease,plan);
                }
                plan = plan with { Revision=checked(plan.Revision+1),Groups=next,Next=null,
                    Reason=null,LastChangeMs=catalog.WorkerClock,HighSamples=0,
                    LowSinceMs=next.Length < plan.Groups.Length ? plan.LowSinceMs : 0 };
                run = PersistElastic(catalog,run,lease,plan);
            }
        }
        if (plan.Reason == "KafkaPartitionRecoveryRequired")
            return run;

        foreach (var group in plan.Groups)
        {
            if (plan.Next is { } replacement && !replacement.Any(candidate => candidate.Id == group.Id))
                continue;
            var member = ReadRun(catalog,group.Id);
            if (member?.State == "RecoveryRequired")
            {
                if (member.Failure?.Phase is not ("dispatch.reconcile" or "worker.report"))
                    return PersistElastic(catalog,run,lease,plan with { Reason="KafkaPartitionRecoveryRequired" });
                var replacementGroup = KafkaElasticGroup.Create(group.Partitions,true);
                return PersistElastic(catalog,run,lease,plan with
                {
                    Reason="KafkaWorkerLost",
                    Next=plan.Groups.Select(item => item.Id == group.Id ? replacementGroup : item).ToArray()
                });
            }
            if (member?.State is "Failed" or "Cancelled")
                return PersistElastic(catalog,run,lease,plan with { Reason="KafkaPartitionRecoveryRequired" });
            if (member is null)
            {
                member = new RunState(ProductInfo.Protocol,group.Id,run.PackageId,run.PackageRevision,run.ParametersReference,"Queued",
                    run.Subject,run.SubjectStamp,ControlReference(),TimestampMs:catalog.WorkerClock,ParentId:run.Id,Attempt:run.Attempt,
                    Parallelism:group.Parallelism,DeploymentId:run.DeploymentId,DeploymentRevision:run.DeploymentRevision,
                    Assignment:new KafkaAssignment(run.Id,plan.Source.Topic,group.Partitions), Continuous:run.Continuous);
                Save(member);
            }
            if (member.State != "Queued" || monitors.ContainsKey(member.Id))
                continue;
            var reservation = group.Reservation is null ? null :
                reservations.TryReserve(policy.Tenant,group.Parallelism,true,1,[group.Reservation],package:package);
            reservation ??= reservations.TryReserve(policy.Tenant,group.Parallelism,true,1,package:package);
            if (reservation is not null)
                Track(member.Id,RunElasticGroup(member.Id,reservation,cancellation));
        }
        return run;
    }

    private void SaveElasticDecision(Catalog catalog, CatalogLease lease, RunState run, KafkaElasticState current,
        KafkaElasticState observed, JsonElement package)
    {
        if (observed.Next is { } next)
        {
            var additions = next.Where(group => !current.Groups.Any(prior => prior.Id == group.Id)).ToArray();
            var retiring = current.Groups.Where(group => !next.Any(candidate => candidate.Id == group.Id))
                .Select(group => ReadRun(catalog,group.Id))
                .Where(group => group?.State is "Claimed" or "Running")
                .Select(group => group!.Placement).OfType<WorkerPlacement>().ToArray();
            var budget = reservations.PlanHandoff(policy.Tenant,package,additions,retiring);
            if (budget is null)
                observed = observed with { Next=null,Reason="KafkaResizeCapacity" };
            else
            {
                int slot = 0;
                observed = observed with { Next=next.Select(group => additions.Contains(group)
                    ? group with { Reservation=budget[slot++] } : group).ToArray() };
            }
        }
        PersistElastic(catalog,run,lease,observed);
    }

    private RunState PersistElastic(Catalog catalog,RunState run,CatalogLease lease,KafkaElasticState plan)
    {
        var next = run with { Revision=checked(run.Revision+1),TimestampMs=catalog.WorkerClock,
            ElasticPlan=plan,MemberIds=plan.Groups.Select(group => group.Id).ToArray() };
        Save(next,lease);
        return next;
    }

    private void StopKafkaGroup(Catalog catalog,string id)
    {
        if (ReadRun(catalog,id) is not { } group)
            return;
        if (group.State == "Queued" && !catalog.LeaseActive(CatalogKind.Audit,"run/"+id))
        {
            Save(group with { State="Stopped",Revision=checked(group.Revision+1),TimestampMs=catalog.WorkerClock });
            return;
        }
        if (group.State is "Claimed" or "Running")
            PutControl(catalog,"stop",group);
    }

    private async Task RunElasticGroup(string id,WorkerReservation reservation,CancellationToken cancellation)
    {
        using (reservation)
        {
            try { await StartWorker(id,reservation.Placements[0],cancellation); }
            catch (UniversalDataException error) when (CatalogInterruption(error)) { leadership.CatalogUnavailable(error); }
        }
    }
}
