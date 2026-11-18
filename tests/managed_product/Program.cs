using System.Runtime.InteropServices;
using System.Text.Json;
using UniversalData;

NativeLibrary.SetDllImportResolver(typeof(Execution).Assembly, (name, _, _) =>
    name == "ud_data" ? NativeLibrary.Load(Path.GetFullPath(args[0])) : IntPtr.Zero);
var root = Path.Combine(Path.GetTempPath(), "ud-managed-" + Guid.NewGuid().ToString("N"));
Directory.CreateDirectory(root);
static void Require(bool value, string message) { if (!value) throw new Exception(message); }
static void Throws<T>(Action action) where T : Exception
{ try { action(); } catch (T) { return; } throw new Exception("Expected " + typeof(T).Name); }
static bool Allow(string tenant, string resource, ConnectionAccess access) => true;
static string TaskPackage(string kind, object? extensions = null) => JsonSerializer.Serialize(new {
    version=ProductInfo.Protocol, id = "managed-product", extensions,
    controlFlow = new { tasks = new[] { new { id = "task", kind, config = new { } } }, constraints = Array.Empty<object>() },
    dataFlows = Array.Empty<object>() });
// Leave absent extension lists absent, rather than introducing a null protocol value.
static string PlainPackage(string kind) => TaskPackage(kind).Replace(",\"extensions\":null", "");
try {
    using(var temporal=new Batch("{\"version\":" + ProductInfo.Protocol + ",\"fields\":[{\"name\":\"instant\",\"type\":\"datetime\",\"nullable\":false,\"scalar\":{\"type\":\"DT_DBTIMESTAMPOFFSET\",\"fractionalDigits\":7}}]}","[{\"instant\":\"9999-12-31T23:59:59.9999999+00:00\"}]")) {
        Require(temporal.GetDateTime(0,0)==new TemporalValue(DateTime.MaxValue.Ticks,0),"C# preserves the Native 100ns value and offset");
        using var restored=Batch.FromWire(temporal.ToWire());Require(restored.GetDateTime(0,0)==temporal.GetDateTime(0,0),"C# Wire temporal metadata");
    }
    var counts = new Counts();
    using (var registry = new Registry()) {
        registry.RegisterTask(new("managed.task", "Managed task", "{}", () => new CountingTask(counts), ThreadSafe: true));
        using (var execution = new Execution(PlainPackage("managed.task"), "local", Allow, registry: registry,
            observe: value => { Require(value.ExecutionId.Length != 0 && value.Sequence > 0, "Native event identity");
                Require(Math.Abs((long)value.TimestampMilliseconds - DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()) < 10000, "Persistent events use UTC wall time"); counts.Events++; })) {
            Require(Guid.TryParse(execution.Id, out _), "Native generated execution ID");
            Throws<UniversalDataException>(registry.Dispose);
            GC.Collect(); GC.WaitForPendingFinalizers(); execution.Execute();
            Require(execution.Metrics.ExecutedTasks == 1, "Native executed managed task");
        }
        Require(counts.Executed == 1 && counts.Finished == 1 && counts.Disposed == 1 && counts.Events > 2,
            "Managed task callback ownership and lifecycle");
        Throws<ObjectDisposedException>(() => _ = counts.Borrowed!.TaskId);
    }
    string empty = "{\"version\":" + ProductInfo.Protocol + ",\"id\":\"observer\",\"controlFlow\":{\"tasks\":[],\"constraints\":[]},\"dataFlows\":[]}";
    using (var execution = new Execution(empty, "local", Allow, observe: _ => throw new Exception("observer failure"))) {
        Throws<UniversalDataException>(execution.Execute);
        Require(execution.Metrics.Result == StatusCode.Io, "Observer failure reaches Native error lifecycle");
    }
    var source = Path.Combine(root, "source.ndjson"); var destination = Path.Combine(root, "destination.ndjson");
    var csvSource=Path.Combine(root,"preview.csv");File.WriteAllText(csvSource,"id,label\n1,first\n2,second\n");
    var inspectionPackage=JsonSerializer.Serialize(new {version=ProductInfo.Protocol,id="managed-preview",connections=new[]{new{id="input",driver="file",resource=csvSource}},controlFlow=new{tasks=Array.Empty<object>(),constraints=Array.Empty<object>()},dataFlows=Array.Empty<object>()});
    using(var previewExecution=new Execution(inspectionPackage,"local",Allow)) {
        Require(previewExecution.ConnectorCatalog("[]")=="[]","C# connector metadata projection");
        using(var cloned=JsonDocument.Parse(previewExecution.ReidentifyPackage("managed-preview-copy")))Require(cloned.RootElement.GetProperty("id").GetString()=="managed-preview-copy","C# Native package identity projection");
        using var discovered=JsonDocument.Parse(previewExecution.InspectFile("input","{\"format\":\"csv\",\"options\":{\"header\":true}}"));
        string pipeline=JsonSerializer.Serialize(new {id="preview",components=new[]{new{id="read",component="file.csv.source",config=new{connection="input",schema=discovered.RootElement.GetProperty("schema")}}},paths=Array.Empty<object>()});
        using var preview=JsonDocument.Parse(previewExecution.Preview(pipeline,"read",2,1));
        Require(preview.RootElement.GetProperty("result").GetInt32()==0 && preview.RootElement.GetProperty("snapshot").GetProperty("preview")[0].GetProperty("totalRows").GetInt32()==2,"C# calls bounded Native Pipeline preview");
        using var progress=JsonDocument.Parse(previewExecution.Progress());Require(progress.RootElement.GetProperty("pipelines").GetArrayLength()==0,"C# reads Native progress after plan disposal");
    }
    // Port IDs belong to their Package namespace. A test/template snapshot
    // resolves those references in Native before changing the Package ID.
    var cloneNode=System.Text.Json.Nodes.JsonNode.Parse(inspectionPackage)!.AsObject();
    cloneNode["connections"]!.AsArray().Add(JsonSerializer.SerializeToNode(new{id="output",driver="file",resource=destination}));
    cloneNode["controlFlow"]=JsonSerializer.SerializeToNode(new{tasks=new[]{new{id="flow",kind="dataFlow",config=new{pipeline="main"}}},constraints=Array.Empty<object>()});
    using(var inspection=new Execution(inspectionPackage,"local",Allow)) {
        using var discovery=JsonDocument.Parse(inspection.InspectFile("input","{\"format\":\"csv\",\"options\":{\"header\":true}}"));
        cloneNode["dataFlows"]=JsonSerializer.SerializeToNode(new[]{new{id="main",components=new object[]{
            new{id="read",component="file.csv.source",config=new{connection="input",schema=discovery.RootElement.GetProperty("schema")}},
            new{id="write",component="file.csv.destination",config=new{connection="output",fileMode="overwrite"}}},paths=new[]{new{from="read",output=0,to="write",input=0}}}});
    }
    using(var bound=new Execution(cloneNode.ToJsonString(),"local",Allow)) {
        foreach(string id in new[]{"read","write"}) {
            var component=cloneNode["dataFlows"]![0]!["components"]!.AsArray().Single(node=>node!["id"]!.GetValue<string>()==id)!;
            using var ports=JsonDocument.Parse(bound.ComponentPorts("main",id,component["component"]!.GetValue<string>(),component["config"]!.ToJsonString()));
            string identity=ports.RootElement.EnumerateArray().First(port=>port.GetProperty("direction").GetInt32()==(id=="read"?2:1)&&port.GetProperty("index").GetInt32()==0).GetProperty("id").GetString()!;
            cloneNode["dataFlows"]![0]!["paths"]![0]![id=="read"?"output":"input"]=identity;
        }
    }
    using(var bound=new Execution(cloneNode.ToJsonString(),"local",Allow)) {
        string snapshot=bound.ReidentifyPackage("managed-port-copy");using var parsed=JsonDocument.Parse(snapshot);
        Require(parsed.RootElement.GetProperty("dataFlows")[0].GetProperty("paths")[0].GetProperty("output").ValueKind==JsonValueKind.Number,"Native normalized the bound output port");
        using var clone=new Execution(snapshot,"local",Allow);Require(clone.Explain().Contains("main",StringComparison.Ordinal)&&!File.Exists(destination),"Native clone rebinding introduced no destination write");
    }
    File.WriteAllText(source, "{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n{\"id\":4}\n{\"id\":5}\n");
    var document = JsonSerializer.Serialize(new {
        version=ProductInfo.Protocol, id = "managed-flow", connections = new[] { new { id = "source", driver = "file", resource = source }, new { id = "target", driver = "file", resource = destination } },
        controlFlow = new { tasks = new[] { new { id = "flow", kind = "dataFlow", config = new { pipeline = "rows" } } }, constraints = Array.Empty<object>() },
        dataFlows = new[] { new { id = "rows", components = new object[] {
            new { id = "read", component = "file.ndjson.source", config = new { connection = "source", schema = new { version=ProductInfo.Protocol, fields = new[] { new { name = "id", type = "int64", nullable = false } } } } },
            new { id = "managed", component = "managed.pass", config = new { } },
            new { id = "write", component = "file.ndjson.destination", config = new { connection = "target" } } },
            paths = new[] { new { from = "read", output = 0, to = "managed", input = 0 }, new { from = "managed", output = 0, to = "write", input = 0 } } } } });
    using(var functions=new Registry()) {
        functions.RegisterFunction(new("managed.triple","Triple",1,1,
            _ => "{\"version\":" + ProductInfo.Protocol + ",\"fields\":[{\"name\":\"result\",\"type\":\"int64\",\"nullable\":false}]}",
            (context,input) => {context.CheckCancellation();using var metadata=JsonDocument.Parse(context.ResultSchema);string name=metadata.RootElement.GetProperty("fields")[0].GetProperty("name").GetString()!;return context.CreateResult(JsonSerializer.Serialize(new[]{new Dictionary<string,long>{{name,checked(input!.GetInt64(0,0)!.Value*3)}}}));},ThreadSafe:true));
        string custom=document.Replace("managed.pass","derive").Replace("\"config\":{}","\"config\":{\"columns\":[{\"name\":\"triple\",\"expression\":\"managed.triple([id])+1L\"}]}");
        using var execution=new Execution(custom,"local",Allow,registry:functions,maxBatchRows:2,maxConcurrency:4);
        using var compilation=JsonDocument.Parse(execution.CompileExpression("{\"version\":" + ProductInfo.Protocol + ",\"fields\":[{\"name\":\"id\",\"type\":\"int64\",\"nullable\":false}]}","managed.triple([id])+1L"));
        Require(compilation.RootElement.GetProperty("result").GetInt32()==0,"Managed function binds into Native IR");
        GC.Collect();GC.WaitForPendingFinalizers();execution.Execute();
        var values=File.ReadAllLines(destination).Select(line=>JsonDocument.Parse(line)).ToArray();
        Require(values.Select(value=>value.RootElement.GetProperty("triple").GetInt64()).SequenceEqual(new long[]{4,7,10,13,16}),"Managed function evaluates through Native derive");
        foreach(var value in values)value.Dispose();File.Delete(destination);
    }
    var transformed = new Counts();
    using (var registry = new Registry()) {
        registry.RegisterComponent(new("managed.pass", "Managed pass", ComponentCategory.Transform, 1, 1, "{}",
            (_, inputs) => [inputs[0]], () => new Pass(transformed), SynchronousInput: 0, ThreadSafe: true));
        using var execution = new Execution(document, "local", Allow, registry: registry, maxBatchRows: 2, maxConcurrency: 4);
        execution.ConfigureDebug("{\"previewRows\":2,\"definitionBreakpoints\":[[\"flow\"]]}");
        execution.Start();
        static JsonDocument Paused(Execution execution) {
            var deadline = DateTime.UtcNow.AddSeconds(10);
            while (DateTime.UtcNow < deadline) {
                var snapshot = JsonDocument.Parse(execution.DebugSnapshot());
                if (snapshot.RootElement.GetProperty("paused").GetBoolean()) return snapshot;
                snapshot.Dispose(); Thread.Sleep(5);
            }
            throw new Exception("Native safe pause timed out");
        }
        using (var paused = Paused(execution)) Require(paused.RootElement.GetProperty("maxConcurrency").GetInt32() == 1, "Debug Native worker budget");
        execution.DebugControl("{\"action\":\"step\"}");
        using (var paused = Paused(execution)) Require(paused.RootElement.GetProperty("preview").GetArrayLength() > 0, "Bounded Native edge preview after step");
        execution.DebugControl("{\"action\":\"continue\"}");
        while (!execution.Wait(100)) { }
        Require(execution.Metrics.State == ExecutionState.Succeeded && File.ReadAllLines(destination).Length == 5, "Managed transform and resumed debug delivery");
        Throws<ObjectDisposedException>(() => _ = transformed.Batch!.Rows);
    }
    Require(transformed.Rows == 5 && transformed.Disposed == 1, "Managed component row ownership");
    using (var registry = new Registry()) {
        using var entered = new ManualResetEventSlim();
        registry.RegisterTask(new("managed.cancel", "Cancelable", "{}", () => new BlockingTask(entered), ThreadSafe: true));
        using var execution = new Execution(PlainPackage("managed.cancel"), "local", Allow, registry: registry);
        execution.Start(); Require(entered.Wait(5000), "Managed task started"); execution.Cancel();
        Throws<UniversalDataException>(() => { while (!execution.Wait(100)) { } });
        Require(execution.Metrics.State == ExecutionState.Cancelled, "Cancellation remains Native");
    }
    const string script = """
        using UniversalData;
        public sealed class Module : IManagedModule {
            public void Register(Registry registry) => registry.RegisterTask(new("script.task", "Compiled script", "{}", () => new Script(), ThreadSafe: true));
        }
        public sealed class Script : IManagedTask {
            public void Execute(ManagedTaskContext context, string json) => context.CheckCancellation();
            public void Dispose() { }
        }
        """;
    var assembly = CSharpDeployment.Compile(script, Path.Combine(root, "assemblies"), "local", Allow);
    var fixedPackage = TaskPackage("script.task", new[] { new { kind = assembly.Kind, path = assembly.Path, sha256 = assembly.Sha256 } });
    using (var execution = new Execution(fixedPackage, "local", Allow)) {
        execution.Execute(); Require(execution.Metrics.ExecutedTasks == 1, "Fixed script assembly uses Native Registry");
    }
    using (var catalog = new Catalog(Path.Combine(root, "catalog"), "local", Allow)) {
        var lease=catalog.TryAcquireLease(CatalogKind.Audit,"run/sdk","dotnet-node",30000,"sdk-pool",1)!;
        Require(lease.Generation==1 && catalog.LeaseActive(CatalogKind.Audit,"run/sdk"),"Native Catalog lease projection");
        Require(catalog.TryAcquireLease(CatalogKind.Audit,"run/sdk","other-node",30000,"sdk-pool",1) is null,"Exclusive Native claim");
        catalog.CompareExchange(CatalogKind.Audit,"run/sdk",0,"{\"version\":" + ProductInfo.Protocol + ",\"id\":\"run/sdk\",\"state\":\"Queued\"}",lease);
        Throws<UniversalDataException>(()=>catalog.CompareExchange(CatalogKind.Audit,"run/sdk",0,"{\"version\":" + ProductInfo.Protocol + ",\"id\":\"run/sdk\"}",lease));
        catalog.RenewLease(lease);catalog.ReleaseLease(lease);
        Require(!catalog.LeaseActive(CatalogKind.Audit,"run/sdk"),"Native lease release");
        Throws<UniversalDataException>(()=>catalog.RenewLease(lease));
        using var execution = new Execution(empty, "local", Allow, observe: catalog.Record);
        execution.Execute();
        var latest = catalog.LatestRevision(CatalogKind.Event, execution.Id);
        Require(latest > 2, "Persistent observer events");
        var removed = catalog.PruneEvents(ulong.MaxValue >> 1);
        Require(removed > 0 && catalog.LatestRevision(CatalogKind.Event, execution.Id) == latest, "Retention preserves event sequence watermark");
    }
    Catalog? guarded=null;bool reenter=false;
    using(guarded=new Catalog(Path.Combine(root,"guarded-catalog"),"local",(_,_,_)=> { if(reenter) guarded!.Dispose();return true; })) {
        reenter=true;
        try { _=guarded.TryAcquireLease(CatalogKind.Audit,"run/guarded","dotnet-node");throw new Exception("Catalog authorizer disposed its live Native handle"); }
        catch(UniversalDataException error) when(error.Code==StatusCode.Auth) { }
        reenter=false;
        var lease=guarded.TryAcquireLease(CatalogKind.Audit,"run/guarded","dotnet-node")!;
        guarded.ReleaseLease(lease);
    }
    Console.WriteLine("Managed Registry/task/component ownership, expired borrows, GC, observer fault, cancellation, Native debug/step, deployment compilation and Catalog retention passed");
} finally { Directory.Delete(root, recursive: true); }

sealed class Counts { public int Executed, Finished, Disposed, Events, Rows; public ManagedTaskContext? Borrowed; public ManagedBatch? Batch; }
sealed class CountingTask(Counts counts) : IManagedTask {
    public void Execute(ManagedTaskContext context, string json) { context.CheckCancellation(); counts.Borrowed = context; counts.Executed++; GC.Collect(); }
    public void Finish() => counts.Finished++;
    public void Dispose() => counts.Disposed++;
}
sealed class Pass(Counts counts) : IManagedComponent {
    public StatusCode Step(ManagedComponentContext context, ManagedComponentEvent input) {
        if (input.Kind == ComponentEventKind.Poll) return StatusCode.End;
        if (input.Kind != ComponentEventKind.Data) return StatusCode.Ok;
        counts.Batch = input.Batch!; counts.Rows += (int)input.Batch!.Rows; context.Emit(0, input.Batch); GC.Collect(); return StatusCode.Ok;
    }
    public void Dispose() => counts.Disposed++;
}
sealed class BlockingTask(ManualResetEventSlim entered) : IManagedTask {
    public void Execute(ManagedTaskContext context, string json) { entered.Set(); while (true) { context.CheckCancellation(); Thread.Sleep(2); } }
    public void Dispose() { }
}
