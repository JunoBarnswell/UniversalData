using Microsoft.AspNetCore.Server.Kestrel.Https;
using System.Text.Json;
using System.Text.Json.Nodes;
using UniversalData;
using UniversalData.Hosting;
using UniversalData.Management;

var builder = WebApplication.CreateBuilder(new WebApplicationOptions { Args=args,ContentRootPath=AppContext.BaseDirectory });
builder.Services.ConfigureHttpJsonOptions(options => {
    options.SerializerOptions.RespectNullableAnnotations = true;
    options.SerializerOptions.RespectRequiredConstructorParameters = true;
});
builder.Logging.AddFilter("Microsoft.AspNetCore.Hosting.Diagnostics",LogLevel.Warning);
var policy = HostPolicy.Load(Path.GetFullPath(builder.Configuration["policy"] ?? "host-policy.json"));
string? connectionFile = builder.Configuration["Cluster:ConnectionFile"];
string? encryptionFile = builder.Configuration["Cluster:EncryptionKeyFile"];
if ((connectionFile is null) != (encryptionFile is null))
    throw new InvalidDataException("Cluster Catalog requires both secret files");
ClusterCatalog? cluster = connectionFile is null ? null : new(Path.GetFullPath(connectionFile), Path.GetFullPath(encryptionFile!));
if (policy.Tenants is null || policy.Tenants.Count == 0 || policy.Tenants.ContainsKey(ClusterCatalog.SystemTenant) ||
    cluster is null && policy.IdentityFile is null)
    throw new InvalidDataException("Management requires identities and explicit tenant policies");
policy = policy with { Cluster = cluster };
var clusterState = cluster is null ? null : new ClusterHostState(cluster);
var identities = clusterState?.Identities() ?? new IdentityDirectory(policy.IdentityFile!);
if (cluster is null) _ = identities.Find("__configuration_probe__");
Directory.CreateDirectory(policy.DataDirectory);
var urls = builder.Configuration["urls"] ?? "http://127.0.0.1:5080";
foreach (string address in urls.Split(';')) {
    if (!Uri.TryCreate(address,UriKind.Absolute,out var uri) || uri.Scheme != "https" && !uri.IsLoopback)
        throw new InvalidDataException("Remote management endpoints require HTTPS");
}
builder.WebHost.UseUrls(urls);
var workerNodeTrust = builder.Configuration.GetSection("WorkerNodes").Get<Dictionary<string, WorkerNodeTrust>>()
    ?? new Dictionary<string, WorkerNodeTrust>(StringComparer.Ordinal);
if (workerNodeTrust.Any(pair => !HostIdentity.ValidId(pair.Key) || !WorkerNodeCertificate.ValidPin(pair.Value.CertificateSha256) ||
    pair.Value.Tenants is not { Length: > 0 } || pair.Value.Tenants.Any(tenant => !HostIdentity.ValidId(tenant) || !policy.Tenants.ContainsKey(tenant))) ||
    workerNodeTrust.Values.Select(entry => entry.CertificateSha256).Distinct(StringComparer.OrdinalIgnoreCase).Count() != workerNodeTrust.Count)
    throw new InvalidDataException("Invalid Worker node trust configuration");
builder.WebHost.ConfigureKestrel(options => {
    options.Limits.MaxRequestBodySize = 67108864;
    if (workerNodeTrust.Count != 0)
        options.ConfigureHttpsDefaults(https => https.ClientCertificateMode = ClientCertificateMode.AllowCertificate);
});
string nodeId=builder.Configuration["node"] ?? Guid.NewGuid().ToString("N");
if (!HostIdentity.ValidId(nodeId)) throw new InvalidDataException("Invalid dispatcher node identity");
var configuration = new ProductConfiguration(Path.GetFullPath(builder.Configuration["worker"] ?? Path.Combine(AppContext.BaseDirectory,"worker","ud_worker.dll")),nodeId,cluster is not null);
string? supervisorPath = builder.Configuration["worker-supervisor-config"];
var supervisor = new WorkerSupervisor(configuration.WorkerPath, policy.MaxConcurrency,
    Math.Min(policy.MaxConcurrency, (uint)Environment.ProcessorCount),
    (ulong)Math.Max(0, GC.GetGCMemoryInfo().TotalAvailableMemoryBytes), policy.MemoryLimit, supervisorPath,
    nodeDirectory: policy.DataDirectory, nodeMaxProcesses: policy.MaxConcurrency,
    nodeMaxConnectionsPerSource: policy.MaxConnectionsPerSource);
builder.Services.AddSingleton<WorkerSupervisor>(_ => supervisor);
builder.Services.AddSingleton(policy); builder.Services.AddSingleton(configuration); builder.Services.AddSingleton(identities);
builder.Services.AddSingleton(services => new ClusterLeadership(cluster, nodeId, services.GetRequiredService<ILogger<ClusterLeadership>>()));
builder.Services.AddHostedService(services => services.GetRequiredService<ClusterLeadership>());
builder.Services.AddSingleton<WorkerNodeDirectory>();
builder.Services.AddSingleton<WorkerReservations>();
builder.Services.AddSingleton<AnalysisWorkspace>();
builder.Services.AddSingleton<RunDispatcher>(); builder.Services.AddHostedService(services => services.GetRequiredService<RunDispatcher>());
ManagementSecurity.Configure(builder,policy,identities,clusterState);
var app = builder.Build();
var leadership = app.Services.GetRequiredService<ClusterLeadership>();
app.Use(async (context, next) =>
{
    var term = leadership.Current;
    if (leadership.Enabled && term is null &&
        (context.Request.Path.StartsWithSegments("/api") || context.Request.Path.StartsWithSegments("/internal/worker-nodes")))
    {
        context.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
        await context.Response.WriteAsJsonAsync(new Result(StatusCode.Transient,
            new(ErrorSource.Catalog, StatusCode.Transient, "Management authority is unavailable")));
        return;
    }
    using var pin = leadership.Pin(term);
    await next();
});
app.Use(async (context,next) => {
    try { await next(); }
    catch(UniversalDataException failure) { leadership.CatalogUnavailable(failure); if (context.Response.HasStarted) throw; context.Response.StatusCode = failure.Code switch
        {
            StatusCode.Auth => 403,
            StatusCode.Transient or StatusCode.Io or StatusCode.Busy or StatusCode.Timeout or StatusCode.CommitUnknown => 503,
            StatusCode.Fenced => 409,
            _ => 422
        };
        await context.Response.WriteAsJsonAsync(new Result(failure.Code, failure.Detail ?? new(ErrorSource.Native, failure.Code, failure.Code.ToString()))); }
    catch(Exception error) when(error is ArgumentException or JsonException or FormatException or OverflowException) {
        if (context.Response.HasStarted) throw; context.Response.StatusCode = 400; await context.Response.WriteAsJsonAsync(new Result(StatusCode.Invalid, new(ErrorSource.Native, StatusCode.Invalid, "Invalid request"))); }
    catch(Exception error) when(error is IOException or InvalidDataException or OperationCanceledException or InvalidOperationException) {
        if (context.Response.HasStarted) throw; context.Response.StatusCode = 503; await context.Response.WriteAsJsonAsync(new Result(StatusCode.Io, new(ErrorSource.Worker, StatusCode.Io, "Host operation is unavailable"))); }
});
ManagementSecurity.Install(app, policy, identities, leadership);
WorkerNodeEndpoint.Map(app, workerNodeTrust);
app.UseDefaultFiles(); app.UseStaticFiles();
var api = app.MapGroup("/api").RequireAuthorization();
app.Services.GetRequiredService<AnalysisWorkspace>().Map(api);
HostIdentity Identity(HttpContext context) => context.Identity(identities);
HostPolicy Scope(HttpContext context) => policy.Bind(Identity(context));
async Task<JsonElement> InspectValue(HttpContext context,string mode,JsonElement package,JsonElement? payload = null,
    string permission = "package.read", string parametersReference = "") {
    var identity = Identity(context); string id = package.GetProperty("id").GetString()!; identity.Require(permission,id);
    var scoped = Scope(context); string text = package.GetRawText();
    var request = new WorkerRequest(ProductInfo.Protocol,Guid.NewGuid().ToString("N"),mode,scoped,identity,text,
        ParametersReference: parametersReference, Payload:payload, Isolated:scoped.RequiresIsolation(text));
    return await supervisor.InspectAsync(request,context.RequestAborted);
}
async Task<IResult> Inspect(HttpContext context,string mode,JsonElement package,JsonElement? payload = null,string permission = "package.read") => Results.Json(await InspectValue(context,mode,package,payload,permission));
string empty = "{\"version\":"+ProductInfo.Protocol+",\"id\":\"component-catalog\",\"controlFlow\":{\"tasks\":[],\"constraints\":[]},\"dataFlows\":[]}";
api.MapGet("/components",async (HttpContext context) => {
    Identity(context).Require("component.read","*"); using var document = JsonDocument.Parse(empty); var scoped = Scope(context);
    return Results.Json(await supervisor.InspectAsync(new(ProductInfo.Protocol,Guid.NewGuid().ToString("N"),"components",scoped,Identity(context),empty),context.RequestAborted));
});
api.MapGet("/connectors",async(HttpContext context)=>{
    var scoped=Scope(context);var identity=Identity(context);identity.Require("component.read","*");identity.Require("resource.list","*");
    var paths=scoped.PluginRoots.Where(Directory.Exists).SelectMany(root=>Directory.EnumerateFiles(root,"*",SearchOption.TopDirectoryOnly))
        .Where(path=>Path.GetFileName(path).StartsWith("ud_connector_",StringComparison.Ordinal)&&Path.GetExtension(path) is ".so" or ".dll" or ".dylib")
        .Where(path=>scoped.Authorize(scoped.Tenant,path,ConnectionAccess.Open)).Distinct(StringComparer.Ordinal).Take(32).ToArray();
    foreach(string path in paths)HostFiles.NoLinks(path);
    return Results.Json(await supervisor.InspectAsync(new(ProductInfo.Protocol,Guid.NewGuid().ToString("N"),"connectors",scoped,identity,empty,
        Payload:JsonSerializer.SerializeToElement(paths,HostPolicy.JsonOptions)),context.RequestAborted));
});
api.MapGet("/expression-language",async (HttpContext context) => {
    Identity(context).Require("component.read","*");using var document=JsonDocument.Parse(empty);var scoped=Scope(context);
    return Results.Json(await supervisor.InspectAsync(new(ProductInfo.Protocol,Guid.NewGuid().ToString("N"),"expressionLanguage",scoped,Identity(context),empty),context.RequestAborted));
});
api.MapPost("/expression",async (ExpressionRequest request,HttpContext context) => {
    using var package=JsonDocument.Parse(DesignerWorkspace.InspectionPackage(request.Package).ToJsonString());
    return await Inspect(context,"expression",package.RootElement,JsonSerializer.SerializeToElement(new {request.Schema,request.Source},HostPolicy.JsonOptions));
});
api.MapPost("/ports",async (PortRequest request,HttpContext context) => {
    using var package=JsonDocument.Parse(DesignerWorkspace.InspectionPackage(request.Package).ToJsonString());
    return await Inspect(context,"ports",package.RootElement,JsonSerializer.SerializeToElement(new { request.PipelineId,request.ComponentId,request.Kind,request.Configuration },HostPolicy.JsonOptions));
});
api.MapPost("/validate",async (JsonElement document,HttpContext context) => await Inspect(context,"validate",document));
api.MapPost("/schema",async (DiscoveryRequest request,HttpContext context) => {
    using var package=JsonDocument.Parse(DesignerWorkspace.InspectionPackage(request.Package).ToJsonString());
    return await Inspect(context,"schema",package.RootElement,
        JsonSerializer.SerializeToElement(new { request.Connection,table = request.Table ?? "" },HostPolicy.JsonOptions),"package.discover");
});
api.MapPost("/preview",async (PreviewRequest request,HttpContext context) => {
    var document=DesignerWorkspace.InspectionPackage(request.Package,true);
    var pipeline=request.Package.GetProperty("dataFlows").EnumerateArray().Single(flow=>flow.GetProperty("id").GetString()==request.PipelineId).Clone();
    using var package=JsonDocument.Parse(document.ToJsonString());
    return await Inspect(context,"preview",package.RootElement,JsonSerializer.SerializeToElement(new {pipeline,request.ComponentId,request.SourceRows,request.SampleRows},HostPolicy.JsonOptions),"package.preview");
});
api.MapPost("/files/inspect",async (FileInspectionRequest request,HttpContext context) => {
    using var package=JsonDocument.Parse(DesignerWorkspace.InspectionPackage(request.Package).ToJsonString());
    return await Inspect(context,"file",package.RootElement,JsonSerializer.SerializeToElement(new {request.Connection,configuration=new {format=request.Format,options=request.Options}},HostPolicy.JsonOptions),"package.discover");
});
api.MapPost("/metadata",async (MetadataRequest request,HttpContext context) => {
    using var package=JsonDocument.Parse(DesignerWorkspace.InspectionPackage(request.Package).ToJsonString());
    return await Inspect(context,"metadata",package.RootElement,JsonSerializer.SerializeToElement(new {request.Connection,request.Kind,request.Parent,request.Offset},HostPolicy.JsonOptions),"package.discover");
});
api.MapPost("/connections/test", async (MessageConnectionTestRequest request, HttpContext context) => {
    using var package = JsonDocument.Parse(DesignerWorkspace.InspectionPackage(request.Package).ToJsonString());
    return await Inspect(context, "messageConnection", package.RootElement,
        JsonSerializer.SerializeToElement(new { request.Connection }, HostPolicy.JsonOptions), "package.discover");
});
api.MapPost("/schema/compare",async (SchemaComparisonRequest request,HttpContext context) => {
    using var package=JsonDocument.Parse(DesignerWorkspace.InspectionPackage(request.Package).ToJsonString());
    return await Inspect(context,"schemaCompare",package.RootElement,JsonSerializer.SerializeToElement(new {request.Expected,request.Actual,request.Policy},HostPolicy.JsonOptions),"package.discover");
});
api.MapPost("/files/upload",async Task<IResult> (HttpContext context) => Results.Ok(await DesignerWorkspace.Upload(Scope(context),context.Request,context.RequestAborted)));
api.MapGet("/drafts/{id}",(string id,HttpContext context) => DesignerWorkspace.ReadDraft(Scope(context),Identity(context),id) is {} draft ? Results.Ok(draft) : Results.NotFound());
api.MapGet("/drafts",(HttpContext context)=>Results.Ok(DesignerWorkspace.ListDrafts(Scope(context),Identity(context))));
api.MapPut("/drafts/{id}",(string id,DraftWrite request,HttpContext context) => Results.Ok(DesignerWorkspace.WriteDraft(Scope(context),Identity(context),id,request)));
api.MapPost("/versions",(PublicationRequest request,HttpContext context) => Results.Ok(DesignerWorkspace.SaveVersion(Scope(context),Identity(context),request.Document,request.ExpectedPackageRevision)));
api.MapPut("/environments/{id}/versions", (string id, EnvironmentVersionRequest request, HttpContext context,
    RunDispatcher dispatcher) => Results.Ok(dispatcher.For(Identity(context)).SaveEnvironment(Identity(context), id, request)));
api.MapPost("/publications",async (JobPublicationRequest request,HttpContext context,RunDispatcher dispatcher) => {
    var identity = Identity(context);
    string id = request.Document.GetProperty("id").GetString()!;
    identity.Require("package.deploy", id); identity.Require("deployment.write", id);
    var tenant = dispatcher.For(identity);
    if (tenant.FindPublication(identity, request) is { } committed)
        return Results.Ok(committed);
    var prepared = tenant.PreparePublication(identity, request);
    TenantDispatcher.RequireScheduledParameters(prepared.Package, request.Schedules ?? [], prepared.Parameters);
    var vault = new ParameterVault(Scope(context));
    string? parametersReference = request.Parameters is null ? null : vault.Store(request.Parameters);
    string inspectedParameters = prepared.Parameters is null ? "" :
        request.Environment is null ? parametersReference! : vault.Store(prepared.Parameters);
    await InspectValue(context, "validate", prepared.Package, parametersReference: inspectedParameters);
    return Results.Ok(tenant.PublishJob(identity, request, parametersReference));
});
api.MapPost("/publications/lookup", (JobPublicationRequest request, HttpContext context, RunDispatcher dispatcher) =>
{
    var identity = Identity(context);
    return dispatcher.For(identity).FindPublication(identity, request) is { } receipt
        ? Results.Ok(receipt) : Results.NotFound();
});
api.MapPost("/tests",async (TestRequest request,HttpContext context,RunDispatcher dispatcher) => {
    var identity=Identity(context);var package=DesignerWorkspace.IsolatedTest(Scope(context),identity,request);
    using var original=JsonDocument.Parse(package.ToJsonString());
    var bound=await InspectValue(context,"reidentify",original.RootElement,JsonSerializer.SerializeToElement(new{id="test-"+Guid.NewGuid().ToString("N")},HostPolicy.JsonOptions));
    using var doc=JsonDocument.Parse(bound.GetProperty("package").GetRawText());var plan=bound.GetProperty("plan");
    foreach(var pipeline in plan.GetProperty("pipelines").EnumerateArray())foreach(var tree in pipeline.GetProperty("trees").EnumerateArray())foreach(var node in tree.GetProperty("nodes").EnumerateArray())
        if((node.GetProperty("effects").GetUInt32()&2)!=0&&node.GetProperty("component").GetString()!="sql.destination")return Results.BadRequest(new {error="Test contains an external side effect outside approved SQL destinations"});
    var saved=DesignerWorkspace.SaveVersion(Scope(context),identity,doc.RootElement,"0");
    string id=doc.RootElement.GetProperty("id").GetString()!;return Results.Accepted(value:RunStateView.From(dispatcher.For(identity).Enqueue(identity,new(id,"1",RequestId:Guid.NewGuid().ToString("N")))));
});
api.MapPost("/scripts/compile",async (ScriptRequest request,HttpContext context) => {
    var identity = Identity(context); identity.Require("script.compile","*"); var scoped = Scope(context);
    return Results.Json(await supervisor.InspectAsync(new(ProductInfo.Protocol,Guid.NewGuid().ToString("N"),"compile",scoped,identity,
        Payload:JsonSerializer.SerializeToElement(new { request.Source },HostPolicy.JsonOptions)),context.RequestAborted));
});
api.MapGet("/packages",(string? afterId,string? afterRevision,HttpContext context) => {
    var identity = Identity(context); using var catalog = Scope(context).OpenCatalog();
    using var entries = JsonDocument.Parse(catalog.Query(Query(CatalogKind.Package,afterId:afterId,afterRevision:afterRevision)));
    return Results.Json(entries.RootElement.EnumerateArray().Where(entry => identity.Allows("package.read",entry.GetProperty("id").GetString()!)).Select(entry => entry.Clone()).ToArray());
});
api.MapGet("/packages/{id}/versions/{revision}",(string id,string revision,HttpContext context) => {
    Identity(context).Require("package.read",id); using var catalog = Scope(context).OpenCatalog(); var document = catalog.Get(CatalogKind.Package,id,ulong.Parse(revision));
    return document is null ? Results.NotFound() : Results.Content(document,"application/json");
});
api.MapGet("/packages/{id}/latest",(string id,HttpContext context) => {
    Identity(context).Require("package.read",id); using var catalog = Scope(context).OpenCatalog();
    return Results.Ok(new { id,revision = catalog.LatestRevision(CatalogKind.Package,id).ToString(System.Globalization.CultureInfo.InvariantCulture) });
});
api.MapGet("/executions",(HttpContext context,RunDispatcher dispatcher) => dispatcher.For(Identity(context)).List(Identity(context)).Select(run => dispatcher.For(Identity(context)).View(run)));
api.MapGet("/deployments",(HttpContext context,RunDispatcher dispatcher) => dispatcher.For(Identity(context)).Deployments(Identity(context)));
api.MapPost("/deployments",(DeploymentRequest request,HttpContext context,RunDispatcher dispatcher) => Results.Ok(dispatcher.For(Identity(context)).Deploy(Identity(context),request)));
api.MapPost("/deployments/{id}/start",async (string id,HttpContext context,RunDispatcher dispatcher) => Results.Ok(await dispatcher.For(Identity(context)).ActivateDeployment(Identity(context),id,true)));
api.MapPost("/deployments/{id}/stop",async (string id,HttpContext context,RunDispatcher dispatcher) => Results.Ok(await dispatcher.For(Identity(context)).ActivateDeployment(Identity(context),id,false)));
api.MapPost("/executions",(RunRequest request,HttpContext context,RunDispatcher dispatcher) => Results.Accepted(value:RunStateView.From(dispatcher.For(Identity(context)).Enqueue(Identity(context),request))));
api.MapGet("/executions/requests/{requestId}", (string requestId, HttpContext context, RunDispatcher dispatcher) =>
{
    var identity = Identity(context);
    return dispatcher.For(identity).FindExecutionRequest(identity, requestId) is { } accepted
        ? Results.Ok(RunStateView.From(accepted)) : Results.NotFound();
});
api.MapGet("/executions/{ticket}",(string ticket,HttpContext context,RunDispatcher dispatcher) => dispatcher.For(Identity(context)).Get(Identity(context),ticket) is { } run ? Results.Ok(dispatcher.For(Identity(context)).View(run)) : Results.NotFound());
api.MapGet("/executions/{ticket}/members", (string ticket, HttpContext context, RunDispatcher dispatcher) =>
    dispatcher.For(Identity(context)).Members(Identity(context), ticket).Select(run => dispatcher.For(Identity(context)).View(run)));
api.MapPost("/executions/{ticket}/stop",async (string ticket,HttpContext context,RunDispatcher dispatcher) => { await dispatcher.For(Identity(context)).Stop(Identity(context),ticket); return Results.Accepted(); });
api.MapGet("/executions/{ticket}/events",async (string ticket,HttpContext context,RunDispatcher dispatcher) => await ExecutionFeed.Stream(context,ticket,dispatcher,policy,identities));
api.MapPost("/executions/{ticket}/cancel",async (string ticket,HttpContext context,RunDispatcher dispatcher) => { await dispatcher.For(Identity(context)).Cancel(Identity(context),ticket); return Results.Accepted(); });
api.MapPost("/executions/{ticket}/resume",(string ticket,bool? brokerRestore,HttpContext context,RunDispatcher dispatcher) => Results.Accepted(value:RunStateView.From(dispatcher.For(Identity(context)).Resume(Identity(context),ticket,brokerRestore ?? false))));
api.MapGet("/executions/{ticket}/metrics",async (string ticket,HttpContext context,RunDispatcher dispatcher) => Results.Ok(await dispatcher.For(Identity(context)).Metrics(Identity(context),ticket)));
api.MapGet("/executions/{ticket}/debug",async (string ticket,HttpContext context,RunDispatcher dispatcher) => Results.Ok(await dispatcher.For(Identity(context)).Debug(Identity(context),ticket)));
api.MapPost("/executions/{ticket}/debug",async (string ticket,JsonElement command,HttpContext context,RunDispatcher dispatcher) => Results.Ok(await dispatcher.For(Identity(context)).Debug(Identity(context),ticket,command)));
api.MapGet("/jobs",(HttpContext context,RunDispatcher dispatcher) => dispatcher.For(Identity(context)).Jobs(Identity(context)).Select(JobView));
api.MapPut("/jobs/{id}",(string id,JobRequest job,HttpContext context,RunDispatcher dispatcher) => id != job.Id ? Results.BadRequest() : Results.Ok(JobView(dispatcher.For(Identity(context)).SaveJob(Identity(context),job))));
api.MapPost("/jobs/{id}/schedule-preview", (string id, JobSchedule schedule, HttpContext context,
    RunDispatcher dispatcher) => Results.Ok(dispatcher.For(Identity(context)).PreviewSchedule(Identity(context), id, schedule)));
api.MapGet("/jobs/{id}/occurrences", (string id, string? afterId, HttpContext context,
    RunDispatcher dispatcher) => Results.Ok(dispatcher.For(Identity(context)).Occurrences(Identity(context), id, afterId)));
api.MapPost("/jobs/{id}/runs", (string id, JobRunRequest request, HttpContext context,
    RunDispatcher dispatcher) => Results.Accepted(value: dispatcher.For(Identity(context)).RunJob(Identity(context), id, request)));
api.MapGet("/logs",(string executionId,string? afterRevision,string? taskId,string? pipelineId,string? componentId,
    ulong? sinceMs,ulong? untilMs,int? result,int? eventKind,HttpContext context,RunDispatcher dispatcher) => {
    var identity = Identity(context);
    var run = dispatcher.For(identity).List(identity).SingleOrDefault(value => value.ExecutionId == executionId) ?? throw new UniversalDataException((int)StatusCode.Auth,"Execution is outside the authenticated scope");
    identity.Require("execution.logs",run.Id); using var catalog = Scope(context).OpenCatalog();
    var query = new Dictionary<string,object> { ["kind"] = (int)CatalogKind.Event,["id"] = executionId,["limit"] = 100 };
    if (afterRevision is not null) query["afterRevision"] = ulong.Parse(afterRevision);
    if (taskId is not null) query["taskId"] = taskId; if (pipelineId is not null) query["pipelineId"] = pipelineId; if (componentId is not null) query["componentId"] = componentId;
    if (sinceMs is not null) query["sinceMs"] = sinceMs; if (untilMs is not null) query["untilMs"] = untilMs; if (result is not null) query["result"] = result; if (eventKind is not null) query["eventKind"] = eventKind;
    return Results.Content(catalog.Query(JsonSerializer.Serialize(query)),"application/json");
});
api.MapPost("/logs/retention",(RetentionRequest request,HttpContext context) => {
    Identity(context).Require("logs.retention","*"); using var catalog = Scope(context).OpenCatalog(); return Results.Ok(new { removed = catalog.PruneEvents(request.BeforeMs,request.Limit) });
});
api.MapGet("/resources",(HttpContext context) => { var scope = Scope(context); scope.Identity.Require("resource.list","*");
    return new { fileRoots = scope.FileRoots,readOnlyRoots = scope.ReadOnlyRoots,networkOrigins = scope.NetworkOrigins,credentialReferences = scope.CredentialFiles.Keys,
        testResources = scope.TestResources ?? [], resources = scope.Resources?.Keys.Where(resource => scope.Identity.Allows("resource.open",resource)) }; });
app.MapGet("/health/live", () => Results.Ok(new { status = "live" })).AllowAnonymous();
app.MapGet("/health/ready", () => leadership.Ready
    ? Results.Ok(new { status = "ready" }) : Results.StatusCode(503)).AllowAnonymous();
api.MapGet("/execution-readiness", (string? packageId, string? packageRevision, HttpContext context, RunDispatcher dispatcher) =>
{
    var identity = Identity(context);
    identity.Require("worker.read", "*");
    return dispatcher.For(identity).ExecutionReadiness(identity, packageId, packageRevision);
});
api.MapGet("/workers", (HttpContext context, WorkerReservations workers) =>
{
    var identity = Identity(context);
    identity.Require("worker.read", "*");
    return workers.Status(identity.Tenant);
});
api.MapGet("/health",() => new { sdk = ProductInfo.Sdk,protocol = ProductInfo.Protocol });
app.MapFallbackToFile("index.html"); app.Run();

static object JobView(JobState value) => new { value.Version,value.Id,value.PackageId,value.PackageRevision,value.ParametersReference,value.Enabled,
    value.Schedules,value.MaxConcurrency,value.MaxAttempts,value.RetryDelaySeconds,value.Revision,value.DeploymentId,value.DeploymentRevision,value.Subject };
static string Query(CatalogKind kind,string? afterId = null,string? afterRevision = null) {
    var query = new Dictionary<string,object> { ["kind"] = (int)kind,["limit"] = 100 };
    if (afterId is not null) query["afterId"] = afterId; if (afterRevision is not null) query["afterRevision"] = ulong.Parse(afterRevision);
    return JsonSerializer.Serialize(query,HostPolicy.JsonOptions);
}
sealed record DiscoveryRequest(JsonElement Package,string Connection,string? Table);
sealed record ScriptRequest(string Source);
sealed record RetentionRequest(ulong BeforeMs,uint Limit = 1000);
sealed record PortRequest(JsonElement Package,string PipelineId,string ComponentId,string Kind,JsonElement Configuration);

record ExpressionRequest(JsonElement Package,JsonElement Schema,string Source);

record SchemaComparisonRequest(JsonElement Package,JsonElement Expected,JsonElement Actual,JsonElement Policy);
