using System.Text.Json;
using UniversalData;
using UniversalData.Hosting;
using UniversalData.Management;

var builder = WebApplication.CreateBuilder(args);
var policyPath = Path.GetFullPath(builder.Configuration["policy"] ?? "host-policy.json");
var policy = HostPolicy.Load(policyPath);
Directory.CreateDirectory(policy.DataDirectory);
builder.WebHost.UseUrls(builder.Configuration["urls"] ?? "http://127.0.0.1:5080");
builder.Services.AddSingleton(policy);
builder.Services.AddSingleton(new ProductConfiguration(policyPath,
    Path.GetFullPath(builder.Configuration["worker"] ?? Path.Combine(AppContext.BaseDirectory, "worker", "ud_worker.dll"))));
builder.Services.AddSingleton<RunDispatcher>();
builder.Services.AddHostedService(services => services.GetRequiredService<RunDispatcher>());
var app = builder.Build();
app.Use(async (context, next) =>
{
    try { await next(); }
    catch (UniversalDataException failure) {
        context.Response.StatusCode = failure.Code == StatusCode.Auth ? 403 : 422;
        await context.Response.WriteAsJsonAsync(new { result = failure.Status, error = failure.Code.ToString() });
    }
    catch (ArgumentException) { context.Response.StatusCode = 400; await context.Response.WriteAsJsonAsync(new { error = "Invalid request" }); }
    catch (JsonException) { context.Response.StatusCode = 400; await context.Response.WriteAsJsonAsync(new { error = "Invalid JSON" }); }
});
app.UseDefaultFiles(); app.UseStaticFiles();
app.MapGet("/api/components", (HostPolicy host) => {
    using var execution = host.CreateExecution("{\"version\":10,\"id\":\"component-catalog\",\"controlFlow\":{\"tasks\":[],\"constraints\":[]},\"dataFlows\":[]}");
    return Results.Content(execution.ComponentCatalog(), "application/json");
});
app.MapPost("/api/validate", (JsonElement document, HostPolicy host) => {
    using var execution = host.CreateExecution(document.GetRawText());
    return Results.Content(execution.Explain(), "application/json");
});
app.MapPost("/api/schema", (DiscoveryRequest request, HostPolicy host) => {
    using var execution = host.CreateExecution(request.Package.GetRawText());
    return Results.Content(execution.DiscoverSchema(request.Connection, request.Table ?? ""), "application/json");
});
app.MapPost("/api/scripts/compile", (ScriptRequest request, HostPolicy host) =>
    Results.Ok(CSharpDeployment.Compile(request.Source, Path.Combine(host.DataDirectory, "assemblies"), "local", host.Authorize)));
app.MapGet("/api/packages", (string? afterId, string? afterRevision, HostPolicy host) => {
    using var catalog = host.OpenCatalog();
    return Results.Content(catalog.Query(Query(CatalogKind.Package, afterId: afterId, afterRevision: afterRevision)), "application/json");
});
app.MapGet("/api/packages/{id}/versions/{revision}", (string id, string revision, HostPolicy host) => {
    using var catalog = host.OpenCatalog(); var document = catalog.Get(CatalogKind.Package, id, ulong.Parse(revision));
    return document is null ? Results.NotFound() : Results.Content(document, "application/json");
});
app.MapGet("/api/packages/{id}/latest", (string id, HostPolicy host) => {
    using var catalog = host.OpenCatalog();
    return Results.Ok(new { id, revision = catalog.LatestRevision(CatalogKind.Package, id).ToString(System.Globalization.CultureInfo.InvariantCulture) });
});
app.MapPut("/api/packages/{id}/versions/{revision}", (string id, string revision, JsonElement document, HostPolicy host) => {
    using var execution = host.CreateExecution(document.GetRawText());
    using var catalog = host.OpenCatalog(); catalog.Put(CatalogKind.Package, id, ulong.Parse(revision), document.GetRawText());
    return Results.Ok(new { id, revision, validation = execution.Metrics.State.ToString() });
});
app.MapGet("/api/executions", (RunDispatcher dispatcher) => dispatcher.List());
app.MapGet("/api/deployments", (RunDispatcher dispatcher) => dispatcher.Deployments());
app.MapPost("/api/deployments", (DeploymentRequest request, RunDispatcher dispatcher) => Results.Ok(dispatcher.Deploy(request)));
app.MapPost("/api/deployments/{id}/start", (string id, RunDispatcher dispatcher) => Results.Ok(dispatcher.ActivateDeployment(id, true)));
app.MapPost("/api/deployments/{id}/stop", (string id, RunDispatcher dispatcher) => Results.Ok(dispatcher.ActivateDeployment(id, false)));
app.MapPost("/api/executions", (RunRequest request, RunDispatcher dispatcher) => Results.Accepted(value: dispatcher.Enqueue(request)));
app.MapGet("/api/executions/{ticket}", (string ticket, RunDispatcher dispatcher) => dispatcher.Get(ticket) is { } run ? Results.Ok(run) : Results.NotFound());
app.MapPost("/api/executions/{ticket}/cancel", async (string ticket, RunDispatcher dispatcher) => { await dispatcher.Cancel(ticket); return Results.Accepted(); });
app.MapPost("/api/executions/{ticket}/resume", (string ticket, RunDispatcher dispatcher) => Results.Accepted(value: dispatcher.Resume(ticket)));
app.MapGet("/api/executions/{ticket}/debug", async (string ticket, RunDispatcher dispatcher) => Results.Ok(await dispatcher.Debug(ticket)));
app.MapPost("/api/executions/{ticket}/debug", async (string ticket, JsonElement command, RunDispatcher dispatcher) => Results.Ok(await dispatcher.Debug(ticket, command)));
app.MapGet("/api/jobs", (RunDispatcher dispatcher) => dispatcher.Jobs());
app.MapPut("/api/jobs/{id}", (string id, JobState job, RunDispatcher dispatcher) =>
    id != job.Id ? Results.BadRequest() : Results.Ok(dispatcher.SaveJob(job)));
app.MapGet("/api/logs", (string executionId, string? afterRevision, HostPolicy host) => {
    using var catalog = host.OpenCatalog();
    return Results.Content(catalog.Query(Query(CatalogKind.Event, executionId, afterRevision: afterRevision)), "application/json");
});
app.MapGet("/api/resources", (HostPolicy host) => new {
    fileRoots = host.FileRoots, networkOrigins = host.NetworkOrigins,
    credentialReferences = host.CredentialFiles.Keys, resources = host.Resources?.Keys
});
app.MapGet("/api/health", () => new { sdk = ProductInfo.Sdk, protocol = ProductInfo.Protocol });
app.MapFallbackToFile("index.html");
app.Run();

static string Query(CatalogKind kind, string? id = null, string? afterId = null, string? afterRevision = null)
{
    var query = new Dictionary<string, object> { ["kind"] = (int)kind, ["limit"] = 100 };
    if (id is not null) query["id"] = id;
    if (afterId is not null) query["afterId"] = afterId;
    if (afterRevision is not null) query["afterRevision"] = ulong.Parse(afterRevision);
    return JsonSerializer.Serialize(query, HostPolicy.JsonOptions);
}
sealed record DiscoveryRequest(JsonElement Package, string Connection, string? Table);

sealed record ScriptRequest(string Source);
