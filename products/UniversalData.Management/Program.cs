using System.Text.Json;
using System.Text.Json.Nodes;
using UniversalData;
using UniversalData.Hosting;
using UniversalData.Management;

var builder = WebApplication.CreateBuilder(args);
builder.Services.ConfigureHttpJsonOptions(options => {
    options.SerializerOptions.RespectNullableAnnotations = true;
    options.SerializerOptions.RespectRequiredConstructorParameters = true;
});
builder.Logging.AddFilter("Microsoft.AspNetCore.Hosting.Diagnostics",LogLevel.Warning);
var policy = HostPolicy.Load(Path.GetFullPath(builder.Configuration["policy"] ?? "host-policy.json"));
if (policy.IdentityFile is null || policy.Tenants is null || policy.Tenants.Count == 0) throw new InvalidDataException("Management requires an identity directory and explicit tenant policies");
var identities = new IdentityDirectory(policy.IdentityFile); _ = identities.Find("__configuration_probe__");
Directory.CreateDirectory(policy.DataDirectory);
var urls = builder.Configuration["urls"] ?? "http://127.0.0.1:5080";
foreach (string address in urls.Split(';')) {
    if (!Uri.TryCreate(address,UriKind.Absolute,out var uri) || uri.Scheme != "https" && !uri.IsLoopback)
        throw new InvalidDataException("Remote management endpoints require HTTPS");
}
builder.WebHost.UseUrls(urls);
builder.WebHost.ConfigureKestrel(options => options.Limits.MaxRequestBodySize = 16777216);
var configuration = new ProductConfiguration(Path.GetFullPath(builder.Configuration["worker"] ?? Path.Combine(AppContext.BaseDirectory,"worker","ud_worker.dll")));
builder.Services.AddSingleton(policy); builder.Services.AddSingleton(configuration); builder.Services.AddSingleton(identities);
builder.Services.AddSingleton<RunDispatcher>(); builder.Services.AddHostedService(services => services.GetRequiredService<RunDispatcher>());
ManagementSecurity.Configure(builder,policy,identities);
var app = builder.Build();
app.Use(async (context,next) => {
    try { await next(); }
    catch(UniversalDataException failure) { if (context.Response.HasStarted) throw; context.Response.StatusCode = failure.Code == StatusCode.Auth ? 403 : 422;
        await context.Response.WriteAsJsonAsync(new { result = failure.Status,error = failure.Code.ToString() }); }
    catch(Exception error) when(error is ArgumentException or JsonException or FormatException or OverflowException) {
        if (context.Response.HasStarted) throw; context.Response.StatusCode = 400; await context.Response.WriteAsJsonAsync(new { error = "Invalid request" }); }
    catch(Exception error) when(error is IOException or InvalidDataException or OperationCanceledException or InvalidOperationException) {
        if (context.Response.HasStarted) throw; context.Response.StatusCode = 503; await context.Response.WriteAsJsonAsync(new { error = "Host operation is unavailable" }); }
});
ManagementSecurity.Install(app,policy,identities);
app.UseDefaultFiles(); app.UseStaticFiles();
var api = app.MapGroup("/api").RequireAuthorization();
HostIdentity Identity(HttpContext context) => context.Identity(identities);
HostPolicy Scope(HttpContext context) => policy.Bind(Identity(context));
async Task<IResult> Inspect(HttpContext context,string mode,JsonElement package,JsonElement? payload = null,string permission = "package.read") {
    var identity = Identity(context); string id = package.GetProperty("id").GetString()!; identity.Require(permission,id);
    var scoped = Scope(context); string text = package.GetRawText();
    var request = new WorkerRequest(ProductInfo.Protocol,Guid.NewGuid().ToString("N"),mode,scoped,identity,text,Payload:payload,Isolated:scoped.RequiresIsolation(text));
    return Results.Json(await WorkerLauncher.Inspect(configuration.WorkerPath,request,context.RequestAborted));
}
const string empty = "{\"version\":10,\"id\":\"component-catalog\",\"controlFlow\":{\"tasks\":[],\"constraints\":[]},\"dataFlows\":[]}";
api.MapGet("/components",async (HttpContext context) => {
    Identity(context).Require("component.read","*"); using var document = JsonDocument.Parse(empty); var scoped = Scope(context);
    return Results.Json(await WorkerLauncher.Inspect(configuration.WorkerPath,new(ProductInfo.Protocol,Guid.NewGuid().ToString("N"),"components",scoped,Identity(context),empty),context.RequestAborted));
});
api.MapPost("/ports",async (PortRequest request,HttpContext context) => {
    var document = JsonNode.Parse(request.Package.GetRawText())!.AsObject();
    document["controlFlow"] = JsonNode.Parse("{\"tasks\":[],\"constraints\":[]}"); document["dataFlows"] = new JsonArray(); document.Remove("layout");
    using var package = JsonDocument.Parse(document.ToJsonString());
    return await Inspect(context,"ports",package.RootElement,JsonSerializer.SerializeToElement(new { request.PipelineId,request.ComponentId,request.Kind,request.Configuration },HostPolicy.JsonOptions));
});
api.MapPost("/validate",async (JsonElement document,HttpContext context) => await Inspect(context,"validate",document));
api.MapPost("/schema",async (DiscoveryRequest request,HttpContext context) => await Inspect(context,"schema",request.Package,
    JsonSerializer.SerializeToElement(new { request.Connection,table = request.Table ?? "" },HostPolicy.JsonOptions),"package.discover"));
api.MapPost("/scripts/compile",async (ScriptRequest request,HttpContext context) => {
    var identity = Identity(context); identity.Require("script.compile","*"); var scoped = Scope(context);
    return Results.Json(await WorkerLauncher.Inspect(configuration.WorkerPath,new(ProductInfo.Protocol,Guid.NewGuid().ToString("N"),"compile",scoped,identity,
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
api.MapPut("/packages/{id}/versions/{revision}",(string id,string revision,JsonElement document,HttpContext context) => {
    var identity = Identity(context); identity.Require("package.write",id);
    if (id != document.GetProperty("id").GetString()) throw new ArgumentException("Package identity differs");
    // Native parsing owns shape/default/type validation; complete binding happens in the same Worker entry.
    using var package = new Package(document.GetRawText());
    var scoped = Scope(context); string text = package.ToJson();
    using var catalog = scoped.OpenCatalog(); catalog.Put(CatalogKind.Package,id,ulong.Parse(revision),text); return Results.Ok(new { id,revision });
});
api.MapGet("/executions",(HttpContext context,RunDispatcher dispatcher) => dispatcher.For(Identity(context)).List(Identity(context)).Select(RunView));
api.MapGet("/deployments",(HttpContext context,RunDispatcher dispatcher) => dispatcher.For(Identity(context)).Deployments(Identity(context)));
api.MapPost("/deployments",(DeploymentRequest request,HttpContext context,RunDispatcher dispatcher) => Results.Ok(dispatcher.For(Identity(context)).Deploy(Identity(context),request)));
api.MapPost("/deployments/{id}/start",async (string id,HttpContext context,RunDispatcher dispatcher) => Results.Ok(await dispatcher.For(Identity(context)).ActivateDeployment(Identity(context),id,true)));
api.MapPost("/deployments/{id}/stop",async (string id,HttpContext context,RunDispatcher dispatcher) => Results.Ok(await dispatcher.For(Identity(context)).ActivateDeployment(Identity(context),id,false)));
api.MapPost("/executions",(RunRequest request,HttpContext context,RunDispatcher dispatcher) => Results.Accepted(value:RunView(dispatcher.For(Identity(context)).Enqueue(Identity(context),request))));
api.MapGet("/executions/{ticket}",(string ticket,HttpContext context,RunDispatcher dispatcher) => dispatcher.For(Identity(context)).Get(Identity(context),ticket) is { } run ? Results.Ok(RunView(run)) : Results.NotFound());
api.MapPost("/executions/{ticket}/cancel",async (string ticket,HttpContext context,RunDispatcher dispatcher) => { await dispatcher.For(Identity(context)).Cancel(Identity(context),ticket); return Results.Accepted(); });
api.MapPost("/executions/{ticket}/resume",(string ticket,HttpContext context,RunDispatcher dispatcher) => Results.Accepted(value:RunView(dispatcher.For(Identity(context)).Resume(Identity(context),ticket))));
api.MapGet("/executions/{ticket}/debug",async (string ticket,HttpContext context,RunDispatcher dispatcher) => Results.Ok(await dispatcher.For(Identity(context)).Debug(Identity(context),ticket)));
api.MapPost("/executions/{ticket}/debug",async (string ticket,JsonElement command,HttpContext context,RunDispatcher dispatcher) => Results.Ok(await dispatcher.For(Identity(context)).Debug(Identity(context),ticket,command)));
api.MapGet("/jobs",(HttpContext context,RunDispatcher dispatcher) => dispatcher.For(Identity(context)).Jobs(Identity(context)).Select(JobView));
api.MapPut("/jobs/{id}",(string id,JobRequest job,HttpContext context,RunDispatcher dispatcher) => id != job.Id ? Results.BadRequest() : Results.Ok(JobView(dispatcher.For(Identity(context)).SaveJob(Identity(context),job))));
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
        resources = scope.Resources?.Keys.Where(resource => scope.Identity.Allows("resource.open",resource)) }; });
api.MapGet("/health",() => new { sdk = ProductInfo.Sdk,protocol = ProductInfo.Protocol });
app.MapFallbackToFile("index.html"); app.Run();

static object RunView(RunState value) => new { value.Version,value.Id,value.PackageId,value.PackageRevision,value.State,value.ExecutionId,value.Resume,value.Result,
    value.TimestampMs,value.Revision,value.JobId,value.Attempt,value.NotBeforeMs,value.Debug,value.DeploymentId,value.DeploymentRevision,value.Subject,value.ProcessId };
static object JobView(JobState value) => new { value.Version,value.Id,value.PackageId,value.PackageRevision,value.ParametersReference,value.Enabled,value.EverySeconds,
    value.NextDueMs,value.MaxConcurrency,value.MaxAttempts,value.RetryDelaySeconds,value.Revision,value.DeploymentId,value.DeploymentRevision,value.Subject };
static string Query(CatalogKind kind,string? afterId = null,string? afterRevision = null) {
    var query = new Dictionary<string,object> { ["kind"] = (int)kind,["limit"] = 100 };
    if (afterId is not null) query["afterId"] = afterId; if (afterRevision is not null) query["afterRevision"] = ulong.Parse(afterRevision);
    return JsonSerializer.Serialize(query,HostPolicy.JsonOptions);
}
sealed record DiscoveryRequest(JsonElement Package,string Connection,string? Table);
sealed record ScriptRequest(string Source);
sealed record RetentionRequest(ulong BeforeMs,uint Limit = 1000);
sealed record PortRequest(JsonElement Package,string PipelineId,string ComponentId,string Kind,JsonElement Configuration);
