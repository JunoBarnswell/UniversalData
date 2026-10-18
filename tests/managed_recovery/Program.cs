using System.Runtime.InteropServices;
using System.Text.Json;
using UniversalData;

NativeLibrary.SetDllImportResolver(typeof(Execution).Assembly,(name,_,_) => name == "ud_data" ? NativeLibrary.Load(Path.GetFullPath(args[0])) : IntPtr.Zero);
string root = Path.GetFullPath(args[1]); bool resume = args[2] == "resume";
Directory.CreateDirectory(root);
using var registry = new Registry();
new RecoveryModule().Register(registry);
var package = JsonSerializer.Serialize(new { version=12,id = "managed-durable",controlFlow = new { tasks = new[] { new { id = "flow",kind = "dataFlow",config = new { pipeline = "rows" } } },constraints = Array.Empty<object>() },
    dataFlows = new[] { new { id = "rows",components = new[] { new { id = "source",component = "managed.source",config = new { root } },new { id = "transform",component = "managed.pass",config = new { root } },new { id = "target",component = "managed.destination",config = new { root } } },
        paths = new[] { new { from = "source",output = 0,to = "transform",input = 0 },new { from = "transform",output = 0,to = "target",input = 0 } } } } });
bool windowMode = args.Length > 3 && args[3] == "window";
if(windowMode) {
    var document=System.Text.Json.Nodes.JsonNode.Parse(package)!;
    var spill=Path.Combine(root,"spill");Directory.CreateDirectory(spill);
    document["connections"]=System.Text.Json.Nodes.JsonNode.Parse(JsonSerializer.Serialize(new[]{new{id="spill",driver="file",resource=spill}}));
    document["policy"]=System.Text.Json.Nodes.JsonNode.Parse("""
      {"buffers":{"connection":"spill","residentBytes":1,"workingBytes":1048576,"spillBytes":100000000,"maxBlocks":256}}
      """);
    var flow=document["dataFlows"]![0]!;
    flow["components"]!.AsArray().Insert(2,System.Text.Json.Nodes.JsonNode.Parse("""
      {"id":"window","component":"eventWindow","config":{"timeField":"id","sizeMicros":1,"stateBytes":8192,"plan":{"version":12,"group":[],"aggregates":[{"function":"count","column":"*","name":"count"}]}}}
      """));
    flow["paths"]=System.Text.Json.Nodes.JsonNode.Parse("""
      [{"from":"source","output":0,"to":"transform","input":0},{"from":"transform","output":0,"to":"window","input":0},{"from":"window","output":0,"to":"target","input":0}]
      """);
    package=document.ToJsonString();
}
string idFile = Path.Combine(root,"identity"); string id = resume ? File.ReadAllText(idFile) : "";
using var execution = new Execution(package,"test",(_,_,_) => true,executionId:id,stateDirectory:Path.Combine(root,"journal"),resume:resume,maxConcurrency:4,maxBatchRows:2,registry:registry,observe:value=>{if(value.Result!=StatusCode.Ok)Console.Error.WriteLine(value.ErrorChain);});
if (!resume) File.WriteAllText(idFile,execution.Id); else if (execution.Id != id) throw new Exception("Native identity changed on resume");
using(var plan = JsonDocument.Parse(execution.Explain())) {
    var transform = plan.RootElement.GetProperty("pipelines").EnumerateArray().SelectMany(pipeline => pipeline.GetProperty("trees").EnumerateArray())
        .SelectMany(tree => tree.GetProperty("nodes").EnumerateArray()).Single(node => node.GetProperty("id").GetString() == "transform");
    if (transform.GetProperty("estimatedRows").GetUInt64() != 12 || transform.GetProperty("rowWidthEstimate").GetUInt64() != 8 ||
        !transform.GetProperty("ports").EnumerateArray().Any(port => port.GetProperty("name").GetString() == "rows")) throw new Exception("Managed physical plan/ports differ from the Native plan");
}
execution.Execute();
if(windowMode && execution.Metrics.SpillBytes==0)throw new Exception("Window recovery did not exercise real disk spill");
if (execution.Metrics.State != ExecutionState.Succeeded) throw new Exception("Recovery did not succeed");
Console.WriteLine(execution.Id); return 0;

sealed class Source(string root) : IManagedComponent
{
    public const string Schema = "{\"version\":12,\"fields\":[{\"name\":\"id\",\"type\":\"int64\",\"nullable\":false}]}";
    private const string State = "{\"version\":12,\"fields\":[{\"name\":\"position\",\"type\":\"int64\",\"nullable\":false},{\"name\":\"epoch\",\"type\":\"int64\",\"nullable\":false}]}";
    private string schema = ""; private long index; private ulong epoch; private bool barrier;
    public void Open(ManagedComponentContext context,string config,string[] inputs,string[] outputs) {
        using var definition = JsonDocument.Parse(config); root = definition.RootElement.GetProperty("root").GetString()!;
        schema = outputs[0]; epoch = context.RecoveryEpoch;
        if (context.RecoveryPosition.Length != 0) index = long.Parse(context.RecoveryPosition);
    }
    public StatusCode Step(ManagedComponentContext context,ManagedComponentEvent input) {
        context.CheckCancellation();
        if (barrier) { barrier = false; epoch++; context.EmitEvent(0,new(ComponentEventKind.Barrier,0,null,epoch,1,0,index.ToString())); return StatusCode.Ok; }
        if (index == 12) return StatusCode.End;
        using var batch = context.CreateBatch(schema,"[{\"id\":" + index + "}]");
        // Exercise Native-generated provenance, including its restored row counter.
        context.Emit(0,batch); index++; barrier = true;
        if(index==3 && File.Exists(Path.Combine(root,"pre-barrier")) && !File.Exists(Path.Combine(root,"continue"))) {
            File.WriteAllText(Path.Combine(root,"kill-ready"),index.ToString());
            while(!File.Exists(Path.Combine(root,"continue"))) Thread.Sleep(10);
        }
        return StatusCode.Ok;
    }
    public ManagedBatch Checkpoint(ManagedComponentContext context) => context.CreateBatch(State,$"[{{\"position\":{index},\"epoch\":{epoch}}}]");
    public void Restore(ManagedComponentContext context,ManagedBatch snapshot) {
        index = snapshot.GetInt64(0,0)!.Value; epoch = checked((ulong)snapshot.GetInt64(0,1)!.Value); barrier = false;
    }
    public void ConfirmEpoch(ulong value,string position) {
        if (value == 3 && !File.Exists(Path.Combine(root,"continue"))) {
            File.WriteAllText(Path.Combine(root,"kill-ready"),position);
            while(!File.Exists(Path.Combine(root,"continue"))) Thread.Sleep(10);
        }
    }
    public void Dispose() { }
}
sealed class Pass : IManagedComponent
{
    public StatusCode Step(ManagedComponentContext context,ManagedComponentEvent input) {
        if (input.Kind == ComponentEventKind.Data) context.Emit(0,input.Batch!);
        return input.Kind == ComponentEventKind.Poll ? StatusCode.End : StatusCode.Ok;
    }
    public void Dispose() { }
}
sealed class Destination(string root) : IManagedComponent
{
    private const string State = "{\"version\":12,\"fields\":[{\"name\":\"count\",\"type\":\"int64\",\"nullable\":false}]}";
    private long count;
    public void Open(ManagedComponentContext context,string config,string[] inputs,string[] outputs) {
        using var definition = JsonDocument.Parse(config); root = definition.RootElement.GetProperty("root").GetString()!;
    }
    public StatusCode Step(ManagedComponentContext context,ManagedComponentEvent input) {
        if (input.Kind == ComponentEventKind.Data) {
            var path = Path.Combine(root,"rows.json");
            var rows = File.Exists(path) ? JsonSerializer.Deserialize<Dictionary<string,long>>(File.ReadAllText(path))! : new();
            for(uint row = 0; row < input.Batch!.Rows; row++) {
                using var identity = JsonDocument.Parse(input.Batch.RowIdentity(row));
                string key = identity.RootElement.GetProperty("id").GetString()!;
                long value = input.Batch.GetInt64(row,0)!.Value;
                if (rows.TryGetValue(key,out long prior) && prior != value) throw new Exception("Recovered Source reused a Row Identity for another row");
                rows[key] = value;
            }
            var temporary = path + ".next";
            using(var file = new FileStream(temporary,FileMode.Create,FileAccess.Write,FileShare.None)) { JsonSerializer.Serialize(file,rows); file.Flush(true); }
            File.Move(temporary,path,true); count = rows.Count;
        }
        return input.Kind == ComponentEventKind.Poll ? StatusCode.End : StatusCode.Ok;
    }
    public ManagedBatch Checkpoint(ManagedComponentContext context) => context.CreateBatch(State,$"[{{\"count\":{count}}}]");
    public void Restore(ManagedComponentContext context,ManagedBatch snapshot) => count = snapshot.GetInt64(0,0)!.Value;
    public void Dispose() { }
}
