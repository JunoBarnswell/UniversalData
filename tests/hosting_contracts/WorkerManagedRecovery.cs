using System.Diagnostics;
using System.Security.Cryptography;
using System.Text.Json;
using UniversalData;
using UniversalData.Hosting;

static class WorkerManagedRecovery
{
    public static async Task Run(string worker,string module)
    {
        string directory = Path.Combine(Path.GetTempPath(),"ud-worker-managed-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory); string root = Path.Combine(directory,"effects"); Directory.CreateDirectory(root);
        string digest = Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(module))).ToLowerInvariant();
        var policy = new HostPolicy(Path.Combine(directory,"state"),[root],[],[Path.GetDirectoryName(module)!],new(),TrustedAssemblyHashes:[digest]);
        string package = JsonSerializer.Serialize(new { version=ProductInfo.Protocol,id = "managed-worker-recovery",
            extensions = new[] { new { kind = "managedAssembly",path = module,sha256 = digest } },
            controlFlow = new { tasks = new[] { new { id = "flow",kind = "dataFlow",config = new { pipeline = "rows" } } },constraints = Array.Empty<object>() },
            dataFlows = new[] { new { id = "rows",components = new[] {
                new { id = "source",component = "managed.source",config = new { root } },
                new { id = "transform",component = "managed.pass",config = new { root } },
                new { id = "target",component = "managed.destination",config = new { root } } },
                paths = new[] { new { from = "source",output = 0,to = "transform",input = 0 },new { from = "transform",output = 0,to = "target",input = 0 } } } } });
        var request = new WorkerRequest(UniversalData.ProductInfo.Protocol,Guid.NewGuid().ToString("N"),"execute",policy,HostIdentity.Operator("local"),package,
            ControlReference:new ParameterVault(policy).Store("{\"token\":\"managed-recovery-control-capability\"}"));
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(40));
        try {
            Task<string>? workerErrors=null;
            async Task<JsonElement> Frame(Process process,string kind,bool allowExit = false) {
                while(true) {
                    string? text = await WorkerProtocol.ReadLine(process.StandardOutput,67108864,timeout.Token);
                    if (text is null) {
                        if (allowExit) return default;
                        throw new Exception("Worker terminated without a Native frame: " + (workerErrors is null ? "" : await workerErrors));
                    }
                    using var document = JsonDocument.Parse(text); var value = document.RootElement;
                    if (value.GetProperty("ticket").GetString() != request.Ticket) throw new Exception("Worker ticket changed");
                    if (value.GetProperty("kind").GetString() == "catalog") continue;
                    if (value.GetProperty("kind").GetString() != kind) throw new Exception("Unexpected Worker phase"); return value.Clone();
                }
            }
            async Task<JsonElement> Start(Process process) {
                await process.StandardInput.WriteLineAsync(JsonSerializer.Serialize(request,HostPolicy.JsonOptions)); await process.StandardInput.FlushAsync(timeout.Token);
                var ready = await Frame(process,"ready");
                await process.StandardInput.WriteLineAsync("start"); await process.StandardInput.FlushAsync(timeout.Token); return ready;
            }
            async Task Stop(Process process) {
                if (!process.HasExited) await WorkerLauncher.Terminate(process,request.Ticket,false);
                await process.WaitForExitAsync();
            }
            string nativeId;
            Dictionary<string,long> before;
            using(var process = WorkerLauncher.Launch(worker,request)) {
                var errors = workerErrors = process.StandardError.ReadToEndAsync(timeout.Token);
                try {
                    var ready = await Start(process); nativeId = ready.GetProperty("executionId").GetString()!;
                    // The Worker publishes Native catalog frames before reaching the kill marker.
                    Task<JsonElement> output = Frame(process,"terminal",allowExit:true);
                    while(!File.Exists(Path.Combine(root,"kill-ready"))) {
                        timeout.Token.ThrowIfCancellationRequested();
                        if (output.IsCompleted) {
                            await output;
                            throw new Exception("Managed Worker returned a terminal frame before the kill boundary");
                        }
                        if (process.HasExited) throw new Exception("Managed Worker exited before the kill boundary: " + await errors);
                        await Task.Delay(10,timeout.Token);
                    }
                    before = JsonSerializer.Deserialize<Dictionary<string,long>>(File.ReadAllText(Path.Combine(root,"rows.json")))!;
                    if (before.Count != 3) throw new Exception("Native epoch was not confirmed before the kill");
                    process.Kill(entireProcessTree:true); await process.WaitForExitAsync(timeout.Token); await errors;
                    if ((await output).ValueKind != JsonValueKind.Undefined)
                        throw new Exception("Managed Worker returned a terminal frame before being killed");
                } finally { await Stop(process); }
            }
            File.WriteAllText(Path.Combine(root,"continue"),"resume");
            request = request with { ExecutionId = nativeId,Resume = true,Revision = 1 };
            using(var process = WorkerLauncher.Launch(worker,request)) {
                var errors = workerErrors = process.StandardError.ReadToEndAsync(timeout.Token);
                try {
                    var ready = await Start(process); if (ready.GetProperty("executionId").GetString() != nativeId) throw new Exception("Recovery changed Native identity");
                    var terminal = await Frame(process,"terminal");
                    process.StandardInput.Close(); await process.WaitForExitAsync(timeout.Token);
                    if (terminal.GetProperty("state").GetString() != "Succeeded" || terminal.GetProperty("result").GetInt32() != 0 || process.ExitCode != 0)
                        throw new Exception("Managed Worker recovery failed: " + await errors);
                } finally { await Stop(process); }
            }
            var after = JsonSerializer.Deserialize<Dictionary<string,long>>(File.ReadAllText(Path.Combine(root,"rows.json")))!;
            if (after.Count != 12 || !after.Values.Order().SequenceEqual(Enumerable.Range(0,12).Select(value => (long)value)) ||
                before.Any(row => !after.TryGetValue(row.Key,out long value) || value != row.Value)) throw new Exception("Managed Worker lost, duplicated or reassigned a source row");
            Console.WriteLine("Product Worker: fixed module, Source/Transform/Destination, real process kill, same Native ID/Row IDs and committed epoch recovery passed");
        } finally { Directory.Delete(directory,true); }
    }
}
