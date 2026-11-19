using System.Text.Json;
using UniversalData;
using UniversalData.Hosting;

if (args.Length == 2) { await WorkerManagedRecovery.Run(args[0],args[1]); return; }
if (args.Length != 0) { await WorkerHandshake(args[0]); return; }
static void Require(bool value,string message) { if (!value) throw new Exception(message); }
static void Denied(Action action) { try { action(); } catch(UniversalDataException error) when(error.Code == StatusCode.Auth) { return; } throw new Exception("Unauthorized operation succeeded"); }
string directory = Path.Combine(Path.GetTempPath(),"ud-host-contracts-" + Guid.NewGuid().ToString("N")); Directory.CreateDirectory(directory);
try {
    string resources = Path.Combine(directory,"resources"),data = Path.Combine(directory,"product"),modules = Path.Combine(directory,"modules"),identityFile = Path.Combine(directory,"identities.json");
    Directory.CreateDirectory(Path.Combine(resources,"a")); Directory.CreateDirectory(Path.Combine(resources,"b")); Directory.CreateDirectory(modules);
    string secretFile = Path.Combine(directory,"credential"); HostFiles.WritePrivate(secretFile,"database-credential"u8);
    var root = new HostPolicy(data,[resources],[],[modules],new() { ["database"] = secretFile },IdentityFile:identityFile,Tenants:new() {
        ["a"] = new([Path.Combine(resources,"a")],[],[modules],["database"]),["b"] = new([Path.Combine(resources,"b")],[],[modules],[]) });
    var admin = new HostIdentity("administrator","a",new string('a',64),[new("*","*")]);
    var scope = root.Bind(admin); var other = root.Bind(HostIdentity.Operator("b"));
    Require(scope.Authorize("a",Path.Combine(resources,"a","file.csv"),ConnectionAccess.Write),"Authorized tenant resource denied");
    Require(!scope.Authorize("b",Path.Combine(resources,"a","file.csv"),ConnectionAccess.Read),"Tenant identity bypass");
    Require(!scope.Authorize("a",Path.Combine(resources,"b","file.csv"),ConnectionAccess.Read),"Cross tenant file access");
    Require(!scope.Authorize("a",scope.CatalogDirectory,ConnectionAccess.Read),"Catalog leaked into package file scope");
    Require(scope.Authorize("a",Path.Combine(modules,"ud_connector_sqlite.so"),ConnectionAccess.Open),"Native plugin open denied inside authorized module root");
    Require(!scope.Authorize("a",Path.Combine(modules,"ud_connector_sqlite.so"),ConnectionAccess.Write),"Module load root allowed package writes");
    Require(!scope.Authorize("a",Path.Combine(resources,"a",".ud-imports","a","another-subject","file.csv"),ConnectionAccess.Read),"Another subject's upload was readable");
    var executing = scope with { StateDirectory = scope.ExecutionDirectory(Guid.NewGuid().ToString("N")) };
    Require(executing.Authorize("a",executing.StateDirectory!,ConnectionAccess.CheckpointWrite),"Internal Journal grant denied");
    Require(!executing.Authorize("a",executing.StateDirectory!,ConnectionAccess.Read) &&
        !executing.Authorize("a",Path.Combine(executing.StateDirectory!,"journal.db"),ConnectionAccess.Write),"Package file lease inherited a Journal grant");
    Require(scope.Resolve("a",Path.Combine(resources,"a","file.csv"),"database") == "database-credential","Authorized reference resolution");
    Denied(() => other.Resolve("b",Path.Combine(resources,"b","file.csv"),"database"));
    string parameters = "{\"password\":\"parameter-secret-594672\",\"token\":\"token-519306\",\"ordinary\":12}";
    var vault = new ParameterVault(scope); string reference = vault.Store(parameters);
    Require(vault.Resolve(reference) == parameters,"Protected parameters changed type or content");
    Require(File.ReadAllBytes(Path.Combine(vault.DirectoryFor(reference),"payload")).AsSpan().IndexOf("parameter-secret-594672"u8) < 0,"Ciphertext exposes plaintext");
    Require(!File.ReadAllText(Path.Combine(vault.DirectoryFor(reference),"payload")).Contains("parameter-secret-594672",StringComparison.Ordinal),"Plaintext parameter persisted");
    var payload = Path.Combine(vault.DirectoryFor(reference),"payload"); var bytes = File.ReadAllBytes(payload); bytes[^1] ^= 1; File.WriteAllBytes(payload,bytes);
    try { _ = vault.Resolve(reference); throw new Exception("Tampered parameter set accepted"); } catch(System.Security.Cryptography.CryptographicException) { }
    var identityDocument = new IdentityDocument(1,[new("alice","a",IdentityDirectory.HashPassword("long-password-alice"),new string('1',64),["reader"])],new() { ["reader"] = [new("package.read","approved/*")] });
    HostFiles.WritePrivate(identityFile,JsonSerializer.SerializeToUtf8Bytes(identityDocument,HostPolicy.JsonOptions));
    var identities = new IdentityDirectory(identityFile); var user = identities.Authenticate("alice","long-password-alice")!;
    Require(user is not null && user.Allows("package.read","approved/package") && !user.Allows("package.execute","approved/package"),"Object/role permissions differ");
    Require(identities.Authenticate("alice","wrong") is null && identities.Authenticate("missing","wrong") is null,"Invalid login accepted");
    identities.ChangePassword(user!,"long-password-alice","rotated-password-alice");
    Require(identities.Find("alice",user!.Stamp) is null && identities.Authenticate("alice","long-password-alice") is null && identities.Authenticate("alice","rotated-password-alice") is not null,"Credential rotation did not revoke prior sessions");
    if (!OperatingSystem.IsWindows()) {
        var link = Path.Combine(resources,"a","escape"); Directory.CreateSymbolicLink(link,Path.Combine(resources,"b"));
        Require(!scope.Authorize("a",Path.Combine(link,"file.csv"),ConnectionAccess.Read),"Resource symbolic link escaped tenant scope");
    }
    var filter = new SecretFilter(parameters);
    Require(!filter.Json("{\"detail\":\"error parameter-secret-594672\",\"rows\":12}").Contains("parameter-secret-594672",StringComparison.Ordinal),"Diagnostic parameter redaction");
    Console.WriteLine("Identity, rotation, object permissions, tenant resources, protected references and redaction passed");
} finally { Directory.Delete(directory,true); }

static async Task WorkerHandshake(string worker)
{
    string directory = Path.Combine(Path.GetTempPath(),"ud-worker-handshake-" + Guid.NewGuid().ToString("N"));
    Directory.CreateDirectory(directory);
    try {
        var policy = new HostPolicy(Path.Combine(directory,"state"),[],[],[],new());
        var identity = HostIdentity.Operator("local");
        var request = new WorkerRequest(ProductInfo.Protocol,Guid.NewGuid().ToString("N"),"execute",policy,identity,
            "{\"version\":" + ProductInfo.Protocol + ",\"id\":\"worker\",\"controlFlow\":{\"tasks\":[],\"constraints\":[]},\"dataFlows\":[]}",
            Debug:JsonSerializer.SerializeToElement(new { previewRows = 1,breakpoints = Array.Empty<string>() }),
            ControlReference:new ParameterVault(policy).Store("{\"token\":\"test-control-capability-294871439\"}"));
        using var timeout = new CancellationTokenSource();
        System.Diagnostics.Process Launch() {
            timeout.CancelAfter(TimeSpan.FromSeconds(30));
            return WorkerLauncher.Launch(worker,request);
        }
        async Task<JsonElement> Read(System.Diagnostics.Process process,string kind) {
            while(true) {
                string line = await WorkerProtocol.ReadLine(process.StandardOutput,67108864,timeout.Token) ?? throw new Exception("Worker closed its pipe");
                using var document = JsonDocument.Parse(line); var value = document.RootElement;
                Require(value.GetProperty("ticket").GetString() == request.Ticket,"Worker ticket changed");
                if (value.GetProperty("kind").GetString() == "catalog") continue;
                Require(value.GetProperty("kind").GetString() == kind,"Worker phase differs"); return value.Clone();
            }
        }
        async Task Send(System.Diagnostics.Process process,WorkerRequest value) {
            await process.StandardInput.WriteLineAsync(JsonSerializer.Serialize(value,HostPolicy.JsonOptions));
            await process.StandardInput.FlushAsync(timeout.Token);
        }
        foreach (int? staleRevision in new int?[] { null, 2 }) {
            using var legacy=Launch();
            var output=legacy.StandardOutput.ReadToEndAsync(timeout.Token);
            var errors=legacy.StandardError.ReadToEndAsync(timeout.Token);
            var envelope=System.Text.Json.Nodes.JsonNode.Parse(JsonSerializer.Serialize(request,HostPolicy.JsonOptions))!.AsObject();
            if (staleRevision is int revision) envelope["contractRevision"] = revision;
            else envelope.Remove("contractRevision");
            await legacy.StandardInput.WriteLineAsync(envelope.ToJsonString());legacy.StandardInput.Close();
            await legacy.WaitForExitAsync(timeout.Token);
            Require(legacy.ExitCode!=0 && (await output).Length==0,"Legacy Worker request was admitted");
            _=await errors;
        }
        using(var process = Launch()) {
            var errors = process.StandardError.ReadToEndAsync(timeout.Token);
            try {
                await Send(process,request); var ready = await Read(process,"ready");
                Require(Guid.TryParse(ready.GetProperty("executionId").GetString(),out var id) && id.Version == 4,"Native did not generate an execution ID");
                Require(ready.GetProperty("controlReady").GetBoolean(),"Worker omitted the control readiness contract");
                // Do not send start yet. Ready must already include a usable,
                // authenticated control endpoint, independent of OS scheduling.
                var beforeStart = await WorkerControl.Send(policy.ControlDirectory(request.Ticket),request.Ticket,
                    "test-control-capability-294871439","snapshot",cancellation:timeout.Token);
                Require(!beforeStart.GetProperty("paused").GetBoolean(),"Unstarted execution reported a breakpoint");
                try {
                    await WorkerControl.Send(policy.ControlDirectory(request.Ticket),request.Ticket,"incorrect-capability","snapshot",cancellation:timeout.Token);
                    throw new Exception("Unowned Worker control capability accepted");
                } catch(UniversalDataException error) when(error.Code == StatusCode.Auth) { }

                await process.StandardInput.WriteLineAsync("start"); await process.StandardInput.FlushAsync(timeout.Token);
                var terminal = await Read(process,"terminal");
                Require(terminal.GetProperty("executionId").GetString() == ready.GetProperty("executionId").GetString() &&
                    terminal.GetProperty("state").GetString() == "Succeeded" && terminal.GetProperty("result").GetInt32() == 0,"Native terminal result differs");
                process.StandardInput.Close(); await process.WaitForExitAsync(timeout.Token);
                Require(process.ExitCode == 0,"Worker execution failed: " + await errors);
            } finally { if (!process.HasExited) await WorkerLauncher.Terminate(process,request.Ticket,false); }
        }
        // A real Package cannot read its own Journal directory through a normal
        // file lease. Internal checkpoint grants still allow Native recovery.
        var validDebug=request.Debug;
        request=request with {Ticket=Guid.NewGuid().ToString("N"),Debug=JsonSerializer.SerializeToElement(new {previewRows=999999,breakpoints=new[]{"startup-secret-849821"}})};
        using(var process=Launch()) {
            var errors=process.StandardError.ReadToEndAsync(timeout.Token);
            await Send(process,request);var failed=await Read(process,"failed");
            Require(failed.GetProperty("result").GetInt32()==(int)StatusCode.Schema && failed.GetProperty("phase").GetString()=="native.debug","Worker startup lost its Native status or lifecycle phase");
            Require(!failed.GetRawText().Contains("startup-secret-849821",StringComparison.Ordinal),"Startup failure exposed provider content");
            process.StandardInput.Close();await process.WaitForExitAsync(timeout.Token);Require(process.ExitCode!=0,"Worker startup failure was reported as success");await errors;
        }
        request = request with { Ticket = Guid.NewGuid().ToString("N"),Debug=validDebug };
        string privateFile = Path.Combine(policy.ExecutionDirectory(request.Ticket),"private.ndjson");
        Directory.CreateDirectory(Path.GetDirectoryName(privateFile)!); File.WriteAllText(privateFile,"{\"id\":1}\n");
        string probe = JsonSerializer.Serialize(new { version=ProductInfo.Protocol,id = "private-state-probe",
            variables = new { rows = new { type = "int64",@default = 0 } },
            connections = new[] { new { id = "private",driver = "file",resource = privateFile } },
            controlFlow = new { tasks = new[] { new { id = "read",kind = "dataFlow",config = new { pipeline = "probe" } } },constraints = Array.Empty<object>() },
            dataFlows = new[] { new { id = "probe",components = new object[] {
                new { id = "source",component = "file.ndjson.source",config = new { connection = "private",schema = new { version=ProductInfo.Protocol,fields = new[] { new { name = "id",type = "int64",nullable = false } } } } },
                new { id = "count",component = "profile.statistics",config = new { statistics = new[] { new { kind = "rows",variable = "rows" } } } } },
                paths = new[] { new { from = "source",output = 0,to = "count",input = 0 } } } } });
        request = request with { Package = probe };
        using(var process = Launch()) {
            try {
                var errors = process.StandardError.ReadToEndAsync(timeout.Token);
                await Send(process,request); _ = await Read(process,"ready");
                await process.StandardInput.WriteLineAsync("start"); await process.StandardInput.FlushAsync(timeout.Token);
                var terminal = await Read(process,"terminal");
                Require(terminal.GetProperty("result").GetInt32() == (int)StatusCode.Auth,"Package file lease read private recovery data");
                process.StandardInput.Close(); await process.WaitForExitAsync(timeout.Token); await errors;
            } finally { if (!process.HasExited) await WorkerLauncher.Terminate(process,request.Ticket,false); }
        }
        request = request with { Ticket = Guid.NewGuid().ToString("N"),Package = "{\"version\":" + ProductInfo.Protocol + ",\"id\":\"worker\",\"controlFlow\":{\"tasks\":[],\"constraints\":[]},\"dataFlows\":[]}" };
        using(var process = Launch()) {
            try {
                await Send(process,request); _ = await Read(process,"ready"); process.StandardInput.Close();
                await process.WaitForExitAsync(timeout.Token); Require(process.ExitCode == 2,"Pipe loss authorized an execution");
            } finally { if (!process.HasExited) await WorkerLauncher.Terminate(process,request.Ticket,false); }
        }
        request = request with { Version = 12,Ticket = Guid.NewGuid().ToString("N") };
        using(var process = Launch()) {
            try {
                var errors = process.StandardError.ReadToEndAsync(timeout.Token);
                await Send(process,request); process.StandardInput.Close();
                string result = await process.StandardOutput.ReadToEndAsync(timeout.Token);
                await process.WaitForExitAsync(timeout.Token);
                Require(process.ExitCode != 0 && result.Length == 0,"Stale Worker protocol performed an operation"); await errors;
            } finally { if (!process.HasExited) await WorkerLauncher.Terminate(process,request.Ticket,false); }
        }
        Console.WriteLine("Native ID/control readiness handshake, capability denial, private Journal isolation, terminal identity, pipe loss and stale protocol rejection passed");
    } finally { Directory.Delete(directory,true); }
}
