using System.Text.Json;
using UniversalData;
using UniversalData.Hosting;

if (args.Length >= 1 && args[0] == "identity-init") {
    if (args.Length != 4 || !HostIdentity.ValidId(args[2]) || !HostIdentity.ValidId(args[3])) throw new ArgumentException("usage: identity-init IDENTITY_FILE SUBJECT TENANT; password is read from standard input");
    string password;
    if (Console.IsInputRedirected) password = Console.ReadLine() ?? throw new ArgumentException("Missing password on standard input");
    else {
        Console.Error.Write("Password: "); var input = new System.Text.StringBuilder();
        while (true) {
            var key = Console.ReadKey(intercept:true);
            if (key.Key == ConsoleKey.Enter) break;
            if (key.Key == ConsoleKey.Backspace) { if (input.Length != 0) input.Length--; }
            else if (!char.IsControl(key.KeyChar) && input.Length < 1024) input.Append(key.KeyChar);
        }
        Console.Error.WriteLine(); password = input.ToString(); input.Clear();
    }
    var document = new IdentityDocument(1,[new(args[2],args[3],IdentityDirectory.HashPassword(password),Convert.ToHexString(System.Security.Cryptography.RandomNumberGenerator.GetBytes(32)),["administrator"])],
        new() { ["administrator"] = [new("*","*")] });
    HostFiles.WritePrivate(Path.GetFullPath(args[1]),JsonSerializer.SerializeToUtf8Bytes(document,HostPolicy.JsonOptions)); return 0;
}
if (args.Length >= 1 && args[0] == "protect-history") {
    if (args.Length != 4) throw new ArgumentException("usage: protect-history POLICY_FILE CATALOG_DIRECTORY TENANT");
    var migrationPolicy = HostPolicy.Load(Path.GetFullPath(args[1]));
    Console.WriteLine(JsonSerializer.Serialize(new { protectedRecords = HistoryProtection.Protect(migrationPolicy,args[2],args[3]) },HostPolicy.JsonOptions)); return 0;
}
if (args.Length is 2 or 3 && args[0] == "doctor")
{
    try
    {
        string path = Path.GetFullPath(args[1]);
        var host = HostPolicy.Load(path);
        using var catalog = host.OpenCatalog();
        string worker = Worker();
        if (!File.Exists(worker))
            return Report(new(StatusCode.DriverUnavailable, new(ErrorSource.Worker,
                StatusCode.DriverUnavailable, "Worker executable is unavailable")));
        if (args.Length == 3)
        {
            var package = Package.FromJson(File.ReadAllText(args[2]));
            if (!package.IsSuccess)
                return Report(new(package.Code, package.Error));
            var response = await new EtlClient(new(path, WorkerPath: worker))
                .InspectAsync("inspect", package.Value!, "{}");
            if (!response.IsSuccess)
                return Report(new(response.Code, response.Error));
        }
        Console.WriteLine(JsonSerializer.Serialize(new { code = 0, sdk = ProductInfo.Sdk,
            protocol = ProductInfo.Protocol, worker = ProductInfo.WorkerRevision }, HostPolicy.JsonOptions));
        return 0;
    }
    catch (Exception error) when (error is IOException or ArgumentException or UniversalDataException)
    {
        return Report(new(StatusCode.Invalid, new(ErrorSource.Worker, StatusCode.Invalid,
            "Worker configuration could not be opened")));
    }
}
if (args.Length < 3 || args[0] is not ("execute" or "inspect" or "catalog" or "discover" or "compile"))
{
    Console.Error.WriteLine("usage: ud_cli doctor POLICY_FILE [PACKAGE] or ud_cli execute|inspect|catalog|discover|compile POLICY_FILE INPUT_FILE [PARAMETERS_JSON] [EXECUTION_ID] [resume] [CONNECTION] [TABLE]");
    return 2;
}
try
{
    string policyPath = Path.GetFullPath(args[1]);
    var host = HostPolicy.Load(policyPath);
    if (args[0] == "compile")
    {
        var assembly = CSharpDeployment.Compile(File.ReadAllText(args[2]),
            Path.Combine(host.DataDirectory, "assemblies"), "local", host.Authorize);
        Console.WriteLine(JsonSerializer.Serialize(assembly, HostPolicy.JsonOptions));
        return 0;
    }
    var parsed = Package.FromJson(File.ReadAllText(args[2]));
    if (!parsed.IsSuccess)
        return Report(new(parsed.Code, parsed.Error));
    var package = parsed.Value!;
    var client = new EtlClient(new(policyPath, WorkerPath: Worker()));
    string parameters = args.Length > 3 ? args[3] : "{}";
    string id = args.Length > 4 ? args[4] : "";
    bool resume = args.Length > 5 && args[5] == "resume";
    if (args[0] != "execute")
    {
        if (args[0] == "discover" && args.Length < 7)
            throw new ArgumentException("Discovery requires a logical connection");
        var inspected = await client.InspectAsync(args[0], package, parameters,
            args.Length > 6 ? args[6] : null, args.Length > 7 ? args[7] : null);
        if (!inspected.IsSuccess)
            return Report(new(inspected.Code, inspected.Error));
        Console.WriteLine(inspected.Value.GetRawText());
        return 0;
    }
    Result<EtlRun> started;
    if (resume)
    {
        using var catalog = host.OpenCatalog();
        using var info = JsonDocument.Parse(catalog.Get(CatalogKind.Audit, "cli/" + id, 1)
            ?? throw new ArgumentException("Original CLI Journal was not found"));
        started = await client.ResumeAsync(package, id, info.RootElement.GetProperty("ticket").GetString()!, parameters);
    }
    else
        started = await client.StartNamedAsync(package, parameters, id);
    if (!started.IsSuccess)
        return Report(new(started.Code, started.Error));
    await using var run = started.Value!;
    Console.CancelKeyPress += (_, eventArgs) => { eventArgs.Cancel = true; _ = run.CancelAsync(); };
    var result = await run.WaitAsync();
    if (result.Value is null)
        return Report(new(result.Code, result.Error));
    var diagnostics = await run.DiagnosticsAsync();
    var output = System.Text.Json.Nodes.JsonNode.Parse(JsonSerializer.Serialize(result.Value, HostPolicy.JsonOptions))!.AsObject();
    output["version"] = ProductInfo.Protocol;
    output["executionId"] = run.Id;
    output["result"] = (int)result.Code;
    output["code"] = (int)result.Code;
    if (diagnostics.IsSuccess)
        output["diagnostics"] = System.Text.Json.Nodes.JsonNode.Parse(diagnostics.Value.GetRawText());
    if (result.Error is not null)
        output["error"] = JsonSerializer.SerializeToNode(result.Error, HostPolicy.JsonOptions);
    Console.WriteLine(output.ToJsonString(HostPolicy.JsonOptions));
    return result.IsSuccess ? 0 : 1;
}
catch (Exception error) when (error is ArgumentException or JsonException or IOException or UniversalDataException)
{
    var status = error is UniversalDataException native ? native.Code : StatusCode.Invalid;
    return Report(new(status, (error as UniversalDataException)?.Detail ??
        new(ErrorSource.Native, status, status.ToString())));
}

static string Worker() => Environment.GetEnvironmentVariable("UD_WORKER_PATH") ??
    Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "worker",
        OperatingSystem.IsWindows() ? "ud_worker.exe" : "ud_worker"));

static int Report(Result result)
{
    Console.Error.WriteLine(JsonSerializer.Serialize(result, HostPolicy.JsonOptions));
    return (int)result.Code;
}
