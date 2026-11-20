using System.Text.Json;
using UniversalData;
using UniversalData.Hosting;

if (args is ["session"]) return await UniversalData.Worker.SdkWorker.Run();
if (args is ["agent", var configPath]) return await UniversalData.Worker.WorkerNodeAgent.RunAsync(configPath);
if (args.Length != 0) throw new ArgumentException("Worker uses the private inherited-pipe request protocol");
var input = await WorkerProtocol.ReadLine(Console.In,67108864) ?? throw new InvalidDataException("Missing Worker request");
using(var envelope=JsonDocument.Parse(input))
    if (!envelope.RootElement.TryGetProperty("contractRevision",out var contract) || contract.GetInt32()!=ProductInfo.WorkerRevision)
        throw new InvalidDataException("Incompatible Worker contract revision");
var request = JsonSerializer.Deserialize<WorkerRequest>(input,HostPolicy.JsonOptions) ?? throw new InvalidDataException("Invalid Worker request");
if (request.Version != ProductInfo.Protocol || !Guid.TryParseExact(request.Ticket,"N",out _) || request.Policy is null ||
    request.Identity is null || request.Identity.Grants is null || !HostIdentity.ValidId(request.Identity.Tenant) ||
    request.Mode is not ("execute" or "components" or "connectors" or "validate" or "reidentify" or "ports" or "schema" or "compile" or "expression" or "expressionLanguage" or "preview" or "file" or "metadata" or "messageConnection" or "schemaCompare"))
    throw new InvalidDataException("Incompatible Worker request");
var policy = request.Policy with { Tenant = request.Identity.Tenant, Identity = request.Identity, Isolated = request.Isolated };
using var nativePackage = request.Mode == "execute"
    ? new NativePackage(request.Package, policy.MemoryLimit)
    : null;
bool requiresJournal = nativePackage?.RequiresJournal ?? true;
if (!requiresJournal && (request.Resume || request.JournalTicket is not null))
    throw new InvalidDataException("A stream execution cannot use recovery or a Journal ticket");
if (request.JournalTicket is { } journal && (request.Mode != "execute" || !Guid.TryParseExact(journal, "N", out _)) ||
    request.BrokerRestore && (request.Resume || request.ExecutionId.Length == 0 || request.JournalTicket is null ||
        request.JournalTicket == request.Ticket))
    throw new InvalidDataException("Incompatible Worker recovery request");
var directory = policy.ExecutionDirectory(request.JournalTicket ?? request.Ticket);
var parameters = request.ParametersReference.Length == 0 ? "{}" : new ParameterVault(policy).Resolve(request.ParametersReference);
var output = new object();
void Reply(object value) { lock (output) { Console.WriteLine(JsonSerializer.Serialize(value,HostPolicy.JsonOptions)); } }
if (request.Mode != "execute") {
    string? validationState=request.Mode is "validate" or "reidentify"?Path.Combine(directory,"validation"):null;
    if(validationState is not null)Directory.CreateDirectory(directory);
    var inspectionFilter = new SecretFilter(parameters);
    int status = 0; object value = new { };
    try {
        if(request.Mode=="expressionLanguage") { using var language=JsonDocument.Parse(ExpressionLanguage.Describe());value=language.RootElement.Clone(); }
        else if (request.Mode == "compile") {
            request.Identity.Require("script.compile","*");
            value = CSharpDeployment.Compile(request.Payload!.Value.GetProperty("source").GetString()!,Path.Combine(policy.DataDirectory,"assemblies"),policy.Tenant,
                (_,resource,access) => access == ConnectionAccess.Write && HostPolicy.Inside(resource,Path.Combine(policy.DataDirectory,"assemblies")));
        } else {
            using var execution = policy.CreateExecution(request.Package,parameters,stateDirectory:validationState);
            string result = request.Mode switch {
                "components" => execution.ComponentCatalog(),
                "connectors" => execution.ConnectorCatalog(request.Payload!.Value.GetRawText()),
                "reidentify" => execution.ReidentifyPackage(request.Payload!.Value.GetProperty("id").GetString()!),
                "preview" => execution.Preview(request.Payload!.Value.GetProperty("pipeline").GetRawText(),request.Payload.Value.GetProperty("componentId").GetString()!,request.Payload.Value.GetProperty("sourceRows").GetUInt32(),request.Payload.Value.GetProperty("sampleRows").GetUInt32()),
                "schemaCompare" => Schema.Compare(request.Payload!.Value.GetProperty("expected").GetRawText(),request.Payload.Value.GetProperty("actual").GetRawText(),request.Payload.Value.GetProperty("policy").GetRawText(),policy.MemoryLimit),
                "file" => execution.InspectFile(request.Payload!.Value.GetProperty("connection").GetString()!,request.Payload.Value.GetProperty("configuration").GetRawText()),
                "metadata" => execution.Metadata(request.Payload!.Value.GetProperty("connection").GetString()!,request.Payload.Value.GetProperty("kind").GetInt32(),request.Payload.Value.GetProperty("parent").GetString()!,request.Payload.Value.GetProperty("offset").GetUInt64()),
                "messageConnection" => ProbeMessageConnection(execution, request.Payload!.Value.GetProperty("connection").GetString()!),
                "expression" => execution.CompileExpression(request.Payload!.Value.GetProperty("schema").GetRawText(),request.Payload.Value.GetProperty("source").GetString()!),
                "validate" => execution.Explain(),
                "ports" => execution.ComponentPorts(request.Payload!.Value.GetProperty("pipelineId").GetString()!,request.Payload.Value.GetProperty("componentId").GetString()!,
                    request.Payload.Value.GetProperty("kind").GetString()!,request.Payload.Value.GetProperty("configuration").GetRawText()),
                "schema" => execution.DiscoverSchema(request.Payload!.Value.GetProperty("connection").GetString()!,request.Payload.Value.GetProperty("table").GetString() ?? ""),
                _ => throw new ArgumentException("Unknown Worker operation") };
            using var document = JsonDocument.Parse(inspectionFilter.Json(result));
            if(request.Mode=="reidentify") {using var plan=JsonDocument.Parse(inspectionFilter.Json(execution.Explain()));value=JsonSerializer.SerializeToElement(new {package=document.RootElement,plan=plan.RootElement},HostPolicy.JsonOptions);}
            else value = document.RootElement.Clone();
        }
    } catch (UniversalDataException error) { status = error.Status; }
      catch (Exception error) when (error is ArgumentException or JsonException or IOException or InvalidOperationException) { status = (int)StatusCode.Invalid; }
    finally {if(validationState is not null&&Directory.Exists(directory))Directory.Delete(directory,true);}
    Reply(new { version = ProductInfo.Protocol, ticket = request.Ticket, kind = "inspection", result = status, value }); return status == 0 ? 0 : 1;
}
static string ProbeMessageConnection(Execution execution, string connection) {
    var (capabilities, elapsedMs) = execution.TestMessageConnection(connection);
    return JsonSerializer.Serialize(new MessageConnectionTestResult(capabilities, elapsedMs), HostPolicy.JsonOptions);
}
string phase="worker.catalog";
try {
// The worker only sees its own Native Catalog. Management imports Native records over the authenticated dispatch pipe.
if (request.BrokerRestore)
{
    HostFiles.NoLinks(directory);
    if (Directory.Exists(directory) && Directory.EnumerateFileSystemEntries(directory).Any())
        throw new UniversalDataException((int)StatusCode.Fenced, "Broker recovery requires a new local journal");
}
Directory.CreateDirectory(directory);
var workerPolicy = policy with { DataDirectory = directory };
using var catalog = workerPolicy.OpenCatalog();
string packageJson = request.Package;
if (request.BindingSnapshot is { } snapshot) {
    phase="package.bind";
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
    phase="catalog.replay";
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
phase="native.create";
string? journalDirectory = requiresJournal ? directory : null;
using var executionRun = policy.CreateExecution(packageJson,parameters,request.ExecutionId,journalDirectory,request.Resume,observe:Observe,eventSequenceStart:eventSequence,shard:request.Shard,brokerRestore:request.BrokerRestore);
phase="native.debug";
if (request.Debug is { } debug) executionRun.ConfigureDebug(debug.GetRawText());
using var controls = new CancellationTokenSource();
using var controlDocument = JsonDocument.Parse(new ParameterVault(policy).Resolve(request.ControlReference));
string controlToken = controlDocument.RootElement.GetProperty("token").GetString()!;
phase="control.bind";
var controlServer = WorkerControl.Serve(policy.ControlDirectory(request.Ticket),request.Ticket,controlToken,executionRun,controls.Token,secretFilter);
try {
    phase="catalog.ready";
    ulong readyRevision=checked(Math.Max(request.Revision,catalog.LatestRevision(CatalogKind.Execution,executionRun.Id))+1);
    catalog.Record(executionRun,readyRevision);
    Reply(new { version = ProductInfo.Protocol, kind = "ready", controlReady = true, historyComplete = true,ticket = request.Ticket, executionId = executionRun.Id, revision = readyRevision,
        record = JsonSerializer.Deserialize<JsonElement>(catalog.Get(CatalogKind.Execution,executionRun.Id,readyRevision)!) });
    var start = await WorkerProtocol.ReadLine(Console.In,64); if (start != "start") return 2;
    phase="native.execute";
    executionRun.Start();
    _ = Task.Run(async () => {
        while (await WorkerProtocol.ReadLine(Console.In,64) is { } command) if (command == "cancel") executionRun.Cancel();
        executionRun.Cancel();
    });
    int resultCode = 0;
    OperationError? resultDetail = null;
    ulong currentRevision = readyRevision;
    long lastProgress = Environment.TickCount64;
    try {
        while (!executionRun.Wait(200)) {
            if (Environment.TickCount64 - lastProgress < 1000) continue;
            try {
                ulong revision=checked(currentRevision+1);
                catalog.RecordProgress(executionRun,revision);currentRevision=revision;
                PublishCatalog(CatalogKind.Execution,new[] { new { id=executionRun.Id,revision=currentRevision,
                    document=JsonSerializer.Deserialize<JsonElement>(catalog.Get(CatalogKind.Execution,executionRun.Id,currentRevision)!) } });
            } catch(UniversalDataException sample) when(sample.Code is StatusCode.Resource or StatusCode.Busy or StatusCode.Transient) {
                // Optional observation pressure must not replace the Native result.
                // The last persisted observation retains its own Native timestamp.
            }
            lastProgress = Environment.TickCount64;
        }
    } catch (UniversalDataException error) { resultCode = error.Status; resultDetail = error.Detail; }
    var metrics = executionRun.Metrics;
    ulong terminalRevision = checked(currentRevision + 1); catalog.Record(executionRun,terminalRevision);
    Reply(new { version = ProductInfo.Protocol, kind = "terminal", ticket = request.Ticket, executionId = executionRun.Id,
        state = metrics.State.ToString(), result = resultCode, error = resultDetail, metrics, revision = terminalRevision,
        record = JsonSerializer.Deserialize<JsonElement>(catalog.Get(CatalogKind.Execution,executionRun.Id,terminalRevision)!),
        debug = request.Debug is null ? (JsonElement?)null : JsonSerializer.Deserialize<JsonElement>(secretFilter.Json(executionRun.DebugSnapshot())) });
    return resultCode == 0 ? 0 : 1;
} finally { controls.Cancel(); await controlServer; }
} catch(Exception error) when(error is not OutOfMemoryException) {
    int result=error is UniversalDataException native?native.Status:error is UnauthorizedAccessException?(int)StatusCode.Auth:error is IOException?(int)StatusCode.Io:error is OperationCanceledException?(int)StatusCode.Cancelled:(int)StatusCode.Invalid;
    // Exception text and stderr may contain provider credentials. The private
    // product protocol exports only a Native status and fixed lifecycle phase.
    Reply(new {version=ProductInfo.Protocol,ticket=request.Ticket,kind="failed",result,phase,errorType=error.GetType().Name});
    return 1;
}
