using System.Text.Json;
using UniversalData;
using UniversalData.Hosting;

if (args.Length < 3 || args[0] is not ("execute" or "inspect" or "catalog" or "discover" or "compile")) {
    Console.Error.WriteLine("usage: ud_cli execute|inspect|catalog|discover|compile POLICY_FILE INPUT_FILE [PARAMETERS_JSON] [EXECUTION_ID] [resume] [CONNECTION] [TABLE]"); return 2;
}
try {
    var policy = HostPolicy.Load(Path.GetFullPath(args[1]));
    if (args[0] == "compile") {
        var assembly = CSharpDeployment.Compile(File.ReadAllText(args[2]), Path.Combine(policy.DataDirectory, "assemblies"), "local", policy.Authorize);
        Console.WriteLine(JsonSerializer.Serialize(assembly, HostPolicy.JsonOptions)); return 0;
    }
    var document = File.ReadAllText(args[2]); var parameters = args.Length > 3 ? args[3] : "{}";
    var id = args.Length > 4 ? args[4] : ""; bool resume = args.Length > 5 && args[5] == "resume";
    if (resume && id.Length == 0) throw new ArgumentException("Recovery requires the original Native ID");
    using var catalog = policy.OpenCatalog();
    string directory;
    if (resume) {
        using var record = JsonDocument.Parse(catalog.Get(CatalogKind.Audit, "cli/" + id, 1) ?? throw new ArgumentException("Original CLI state not found"));
        directory = record.RootElement.GetProperty("stateDirectory").GetString()!;
    } else directory = Path.Combine(policy.DataDirectory, "cli-executions", Guid.NewGuid().ToString("N"));
    Directory.CreateDirectory(directory);
    using var execution = policy.CreateExecution(document, parameters, id, directory, resume,
        observe: catalog.Record, eventSequenceStart: id.Length == 0 ? 0 : catalog.LatestRevision(CatalogKind.Event, id));
    if (args[0] == "inspect") { Console.WriteLine(execution.Explain()); return 0; }
    if (args[0] == "catalog") { Console.WriteLine(execution.ComponentCatalog()); return 0; }
    if (args[0] == "discover") {
        if (args.Length < 7) throw new ArgumentException("Discovery requires connection and table/format");
        Console.WriteLine(execution.DiscoverSchema(args[6], args.Length > 7 ? args[7] : "")); return 0;
    }
    if (!resume) catalog.Put(CatalogKind.Audit, "cli/" + execution.Id, 1,
        JsonSerializer.Serialize(new { version = ProductInfo.Protocol, id = "cli/" + execution.Id, stateDirectory = directory }, HostPolicy.JsonOptions));
    ulong revision = catalog.LatestRevision(CatalogKind.Execution, execution.Id);
    catalog.Record(execution, checked(revision + 1));
    Console.CancelKeyPress += (_, eventArgs) => { eventArgs.Cancel = true; execution.Cancel(); };
    int result = 0;
    try { execution.Execute(); } catch (UniversalDataException error) { result = error.Status; }
    catalog.Record(execution, checked(revision + 2));
    using var diagnostics = JsonDocument.Parse(execution.Diagnostics());
    var output = System.Text.Json.Nodes.JsonNode.Parse(JsonSerializer.Serialize(execution.Metrics, HostPolicy.JsonOptions))!.AsObject();
    output["version"] = ProductInfo.Protocol; output["executionId"] = execution.Id;
    output["result"] = result; output["diagnostics"] = System.Text.Json.Nodes.JsonNode.Parse(diagnostics.RootElement.GetRawText());
    Console.WriteLine(output.ToJsonString(HostPolicy.JsonOptions));
    return result == 0 ? 0 : 1;
} catch (UniversalDataException error) { Console.Error.WriteLine(JsonSerializer.Serialize(new { result = error.Status, error = error.Code.ToString() }, HostPolicy.JsonOptions)); return 1; }
  catch (Exception error) when (error is ArgumentException or JsonException or IOException) {
    Console.Error.WriteLine(JsonSerializer.Serialize(new { result = (int)StatusCode.Invalid, error = error.GetType().Name }, HostPolicy.JsonOptions)); return 2;
}
