using System.Text.Json;
using UniversalData;
using UniversalData.Hosting;

if (args.Length != 0) throw new ArgumentException("Worker uses the private inherited-pipe request protocol");
var input = await WorkerProtocol.ReadLine(Console.In,67108864) ?? throw new InvalidDataException("Missing Worker request");
using(var envelope=JsonDocument.Parse(input))
    if (!envelope.RootElement.TryGetProperty("contractRevision",out var contract) || contract.GetInt32()!=ProductInfo.WorkerRevision)
        throw new InvalidDataException("Incompatible Worker contract revision");
var request = JsonSerializer.Deserialize<WorkerRequest>(input,HostPolicy.JsonOptions) ?? throw new InvalidDataException("Invalid Worker request");
if (request.Version != ProductInfo.Protocol || !Guid.TryParseExact(request.Ticket,"N",out _) || request.Policy is null ||
    request.Identity is null || request.Identity.Grants is null || !HostIdentity.ValidId(request.Identity.Tenant) ||
    request.Mode is not ("execute" or "components" or "validate" or "ports" or "schema" or "compile" or "expression" or "expressionLanguage"))
    throw new InvalidDataException("Incompatible Worker request");
var policy = request.Policy with { Tenant = request.Identity.Tenant, Identity = request.Identity, Isolated = request.Isolated };
var directory = policy.ExecutionDirectory(request.Ticket); Directory.CreateDirectory(directory);
var parameters = request.ParametersReference.Length == 0 ? "{}" : new ParameterVault(policy).Resolve(request.ParametersReference);
var output = new object();
void Reply(object value) { lock (output) { Console.WriteLine(JsonSerializer.Serialize(value,HostPolicy.JsonOptions)); } }
if (request.Mode != "execute") {
    var inspectionFilter = new SecretFilter(parameters);
    int status = 0; object value = new { };
    try {
        if(request.Mode=="expressionLanguage") { using var language=JsonDocument.Parse(ExpressionLanguage.Describe());value=language.RootElement.Clone(); }
        else if (request.Mode == "compile") {
            request.Identity.Require("script.compile","*");
            value = CSharpDeployment.Compile(request.Payload!.Value.GetProperty("source").GetString()!,Path.Combine(policy.DataDirectory,"assemblies"),policy.Tenant,
                (_,resource,access) => access == ConnectionAccess.Write && HostPolicy.Inside(resource,Path.Combine(policy.DataDirectory,"assemblies")));
        } else {
            using var execution = policy.CreateExecution(request.Package,parameters);
            string result = request.Mode switch {
                "components" => execution.ComponentCatalog(),
                "expression" => execution.CompileExpression(request.Payload!.Value.GetProperty("schema").GetRawText(),request.Payload.Value.GetProperty("source").GetString()!),
                "validate" => execution.Explain(),
                "ports" => execution.ComponentPorts(request.Payload!.Value.GetProperty("pipelineId").GetString()!,request.Payload.Value.GetProperty("componentId").GetString()!,
                    request.Payload.Value.GetProperty("kind").GetString()!,request.Payload.Value.GetProperty("configuration").GetRawText()),
                "schema" => execution.DiscoverSchema(request.Payload!.Value.GetProperty("connection").GetString()!,request.Payload.Value.GetProperty("table").GetString() ?? ""),
                _ => throw new ArgumentException("Unknown Worker operation") };
            using var document = JsonDocument.Parse(inspectionFilter.Json(result)); value = document.RootElement.Clone();
        }
    } catch (UniversalDataException error) { status = error.Status; }
      catch (Exception error) when (error is ArgumentException or JsonException or IOException or InvalidOperationException) { status = (int)StatusCode.Invalid; }
    Reply(new { version = ProductInfo.Protocol, ticket = request.Ticket, kind = "inspection", result = status, value }); return status == 0 ? 0 : 1;
}
// The worker only sees its own Native Catalog. Management imports Native records over the authenticated dispatch pipe.
var workerPolicy = policy with { DataDirectory = directory };
using var catalog = workerPolicy.OpenCatalog();
string packageJson = request.Package;
if (request.BindingSnapshot is { } snapshot) {
    foreach(var entry in snapshot.EnumerateArray()) catalog.Put((CatalogKind)entry.GetProperty("kind").GetInt32(),entry.GetProperty("id").GetString()!,
        entry.GetProperty("revision").GetUInt64(),entry.GetProperty("document").GetRawText());
    using var original = JsonDocument.Parse(packageJson);
    var binding = catalog.BindPackage(request.DeploymentId!,request.DeploymentRevision,original.RootElement.GetProperty("id").GetString()!,packageParameters:parameters);
    packageJson = binding.Package; parameters = binding.Parameters;
}
var secretFilter = new SecretFilter(parameters);
void PublishCatalog(CatalogKind kind,object records) => Reply(new { version=ProductInfo.Protocol,ticket=request.Ticket,
    kind="catalog",catalogKind=(int)kind,records });
void ReplayCatalog(CatalogKind kind,string id,ulong after) {
    while(true) {
        using var page=JsonDocument.Parse(catalog.Query(JsonSerializer.Serialize(new { kind=(int)kind,id,afterRevision=after,limit=100 },HostPolicy.JsonOptions)));
        if (page.RootElement.GetArrayLength()==0) break;
        PublishCatalog(kind,page.RootElement);
        after=page.RootElement[page.RootElement.GetArrayLength()-1].GetProperty("revision").GetUInt64();
    }
}
// Persisted Native records, not a host counter, determine both revision and
// event continuation. A former Worker may have recorded facts after losing its
// management pipe. Replay them before admitting another start handshake.
ulong eventSequence=request.EventSequence;
if (request.Resume) {
    ReplayCatalog(CatalogKind.Execution,request.ExecutionId,request.Revision);
    ReplayCatalog(CatalogKind.Event,request.ExecutionId,request.EventSequence);
    eventSequence=Math.Max(eventSequence,catalog.LatestRevision(CatalogKind.Event,request.ExecutionId));
}
void Observe(ExecutionEvent value) {
    // Provider messages may contain data. Persist only Native structured error codes/locations at the product boundary.
    using var errors = JsonDocument.Parse(value.ErrorChain);
    var chain = errors.RootElement.EnumerateArray().Select(error => new {
        result = error.GetProperty("result").GetInt32(),taskId = error.GetProperty("taskId").GetString(),
        pipelineId = error.GetProperty("pipelineId").GetString(),componentId = error.GetProperty("componentId").GetString() });
    catalog.Record(value with { ErrorChain=JsonSerializer.Serialize(chain,HostPolicy.JsonOptions) });
    PublishCatalog(CatalogKind.Event,new[] { new { id=value.ExecutionId,revision=value.Sequence,
        document=JsonSerializer.Deserialize<JsonElement>(catalog.Get(CatalogKind.Event,value.ExecutionId,value.Sequence)!) } });
}
using var executionRun = policy.CreateExecution(packageJson,parameters,request.ExecutionId,directory,request.Resume,observe:Observe,eventSequenceStart:eventSequence);
if (request.Debug is { } debug) executionRun.ConfigureDebug(debug.GetRawText());
using var controls = new CancellationTokenSource();
using var controlDocument = JsonDocument.Parse(new ParameterVault(policy).Resolve(request.ControlReference));
string controlToken = controlDocument.RootElement.GetProperty("token").GetString()!;
var controlServer = WorkerControl.Serve(policy.ControlDirectory(request.Ticket),request.Ticket,controlToken,executionRun,controls.Token,secretFilter);
try {
    ulong readyRevision=checked(Math.Max(request.Revision,catalog.LatestRevision(CatalogKind.Execution,executionRun.Id))+1);
    catalog.Record(executionRun,readyRevision);
    Reply(new { version = ProductInfo.Protocol, kind = "ready", controlReady = true, historyComplete = true,ticket = request.Ticket, executionId = executionRun.Id, revision = readyRevision,
        record = JsonSerializer.Deserialize<JsonElement>(catalog.Get(CatalogKind.Execution,executionRun.Id,readyRevision)!) });
    var start = await WorkerProtocol.ReadLine(Console.In,64); if (start != "start") return 2;
    executionRun.Start();
    _ = Task.Run(async () => {
        while (await WorkerProtocol.ReadLine(Console.In,64) is { } command) if (command == "cancel") executionRun.Cancel();
        executionRun.Cancel();
    });
    int resultCode = 0;
    try { while (!executionRun.Wait(200)) { } } catch (UniversalDataException error) { resultCode = error.Status; }
    var metrics = executionRun.Metrics;
    ulong terminalRevision = checked(readyRevision + 1); catalog.Record(executionRun,terminalRevision);
    Reply(new { version = ProductInfo.Protocol, kind = "terminal", ticket = request.Ticket, executionId = executionRun.Id,
        state = metrics.State.ToString(), result = resultCode, metrics, revision = terminalRevision,
        record = JsonSerializer.Deserialize<JsonElement>(catalog.Get(CatalogKind.Execution,executionRun.Id,terminalRevision)!) });
    return resultCode == 0 ? 0 : 1;
} finally { controls.Cancel(); await controlServer; }
