using System.Collections.Concurrent;
using System.Text.Json;
using UniversalData.Hosting;

namespace UniversalData.Management;

/// <summary>A bounded view of the existing Native Catalog, without a second event log.</summary>
public static class ExecutionFeed
{
    private static readonly ConcurrentDictionary<string,SemaphoreSlim> subscriptions = new(StringComparer.Ordinal);
    public static async Task Stream(HttpContext context,string ticket,RunDispatcher dispatcher,HostPolicy root,IdentityDirectory identities)
    {
        var identity=context.Identity(identities);identity.Require("execution.read",ticket);
        var scope=root.Bind(identity);var tenant=dispatcher.For(identity);
        if(tenant.Get(identity,ticket) is null){context.Response.StatusCode=404;return;}
        var quota=subscriptions.GetOrAdd(identity.Tenant,_=>new SemaphoreSlim(16,16));
        if(!await quota.WaitAsync(0,context.RequestAborted)){context.Response.StatusCode=429;return;}
        try {
            ulong runRevision=0,executionRevision=0,eventRevision=0;
            string cursor=context.Request.Headers["Last-Event-ID"].ToString();
            if(cursor.Length>128)throw new ArgumentException("Invalid continuation");
            if(cursor.Length!=0) {
                string[] parts=cursor.Split('.');
                if(parts.Length!=3||!ulong.TryParse(parts[0],out runRevision)||!ulong.TryParse(parts[1],out executionRevision)||!ulong.TryParse(parts[2],out eventRevision))
                    throw new ArgumentException("Invalid continuation");
            }
            context.Response.ContentType="text/event-stream";
            context.Response.Headers.CacheControl="no-store";context.Response.Headers["X-Accel-Buffering"]="no";
            using var lifetime=CancellationTokenSource.CreateLinkedTokenSource(context.RequestAborted);lifetime.CancelAfter(TimeSpan.FromMinutes(30));
            bool first=true;
            while(!lifetime.IsCancellationRequested) {
                // Revalidate the current principal/grants for every page, including permission revocation.
                identity=context.Identity(identities);identity.Require("execution.read",ticket);
                var run=tenant.Get(identity,ticket) ?? throw new ArgumentException("Execution was not found");
                scope=root.Bind(identity);
                using var catalog=scope.OpenCatalog();
                ulong revision=run.ExecutionId.Length==0?0:catalog.LatestRevision(CatalogKind.Execution,run.ExecutionId);
                ulong tail=run.ExecutionId.Length==0?0:catalog.LatestRevision(CatalogKind.Event,run.ExecutionId);
                bool reset=eventRevision>tail;
                if(reset)eventRevision=0;
                using var page=JsonDocument.Parse(run.ExecutionId.Length!=0&&identity.Allows("execution.logs",ticket)
                    ?catalog.Query(JsonSerializer.Serialize(new {kind=(int)CatalogKind.Event,id=run.ExecutionId,afterRevision=eventRevision,limit=100},HostPolicy.JsonOptions)):"[]");
                bool gap=reset||revision<executionRevision||run.Revision<runRevision;
                if(page.RootElement.GetArrayLength()>0) {
                    ulong firstSequence=page.RootElement[0].GetProperty("revision").GetUInt64();
                    gap|=eventRevision!=0&&firstSequence>eventRevision+1;
                    eventRevision=page.RootElement[page.RootElement.GetArrayLength()-1].GetProperty("revision").GetUInt64();
                }
                bool terminal=run.State is "Succeeded" or "Failed" or "Cancelled" or "Stopped" or "RecoveryRequired";
                if(first||gap||terminal||revision!=executionRevision||run.Revision!=runRevision||page.RootElement.GetArrayLength()>0) {
                    var facts=await tenant.Metrics(identity,ticket);
                    string payload=JsonSerializer.Serialize(new {run=RunStateView.From(run),facts,events=page.RootElement,gap,source="native-catalog",terminal},HostPolicy.JsonOptions);
                    if(System.Text.Encoding.UTF8.GetByteCount(payload)>8388608)throw new IOException("Observation snapshot exceeds quota");
                    executionRevision=revision;runRevision=run.Revision;first=false;
                    using var write=CancellationTokenSource.CreateLinkedTokenSource(lifetime.Token);write.CancelAfter(TimeSpan.FromSeconds(5));
                    await context.Response.WriteAsync($"id: {runRevision}.{executionRevision}.{eventRevision}\nevent: snapshot\ndata: {payload}\n\n",write.Token);
                    await context.Response.Body.FlushAsync(write.Token);
                } else {
                    using var write=CancellationTokenSource.CreateLinkedTokenSource(lifetime.Token);write.CancelAfter(TimeSpan.FromSeconds(5));
                    await context.Response.WriteAsync(": heartbeat\n\n",write.Token);await context.Response.Body.FlushAsync(write.Token);
                }
                if(terminal&&page.RootElement.GetArrayLength()<100)break;
                if(page.RootElement.GetArrayLength()<100)await Task.Delay(1000,lifetime.Token);
            }
        } catch(OperationCanceledException) { }
        finally {quota.Release();}
    }
}
