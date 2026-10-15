using System.Diagnostics;
using System.Collections.Concurrent;
using System.Text.Json;

namespace UniversalData.Hosting;

/// <summary>All product Native inspection, script compilation and execution use the same Worker process entry.</summary>
public static class WorkerLauncher
{
    private static readonly ConcurrentDictionary<string,SemaphoreSlim> inspections=new(StringComparer.Ordinal);
    public static Process Launch(string workerPath, WorkerRequest request)
    {
        workerPath = Path.GetFullPath(workerPath); HostFiles.NoLinks(workerPath);
        string dotnet = Environment.ProcessPath is { } current && Path.GetFileNameWithoutExtension(current).Equals("dotnet",StringComparison.OrdinalIgnoreCase)
            ? current : Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet";
        bool isolated = request.Isolated;
        if (isolated && !OperatingSystem.IsLinux()) throw new PlatformNotSupportedException("Untrusted modules require an available OS sandbox; this host has none configured");
        var start = new ProcessStartInfo(isolated ? "/usr/bin/systemd-run" : dotnet) { RedirectStandardInput = true, RedirectStandardOutput = true,
            RedirectStandardError = true, UseShellExecute = false, CreateNoWindow = true };
        if (isolated) {
            if (!Path.IsPathFullyQualified(dotnet)) throw new InvalidOperationException("An absolute dotnet host is required for isolation");
            if (!File.Exists("/usr/bin/systemd-run") || !File.Exists("/usr/bin/bwrap")) throw new PlatformNotSupportedException("Untrusted modules require systemd user resource control and bubblewrap");
            // A transient service owns the cgroup before any script runs. No unenforced memory/CPU fallback.
            foreach(string option in new[] { "--user", "--quiet", "--wait", "--pipe", "--collect", "--unit=ud-worker-" + request.Ticket, "--property=MemoryMax=" + checked(request.Policy.MemoryLimit + 268435456UL),
                "--property=TasksMax=64", "--property=CPUQuota=100%", "--property=RuntimeMaxSec=" + request.Policy.ExecutionTimeoutSeconds,
                "--property=KillMode=control-group", "--", "/usr/bin/bwrap" }) start.ArgumentList.Add(option);
            foreach (string option in new[] { "--die-with-parent", "--new-session", "--unshare-all", "--clearenv", "--setenv", "PATH", "/usr/bin:/bin", "--setenv", "LANG", "C.UTF-8", "--cap-drop", "ALL", "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp" }) start.ArgumentList.Add(option);
            foreach(string variable in new[] { "DOTNET_ROOT", "DOTNET_ROOT_X64", "LD_LIBRARY_PATH" })
                if (Environment.GetEnvironmentVariable(variable) is { } value) { start.ArgumentList.Add("--setenv"); start.ArgumentList.Add(variable); start.ArgumentList.Add(value); }
            var mounted = new HashSet<string>(StringComparer.Ordinal);
            void Mount(string path, bool write) {
                path = Path.GetFullPath(path); HostFiles.NoLinks(path);
                if (!File.Exists(path) && !Directory.Exists(path)) throw new IOException("An isolation mount is unavailable");
                if (!mounted.Add(path)) return;
                start.ArgumentList.Add(write ? "--bind" : "--ro-bind"); start.ArgumentList.Add(path); start.ArgumentList.Add(path);
            }
            foreach (string path in new[] { "/usr", "/lib", "/lib64", "/etc/ssl", "/etc/ld.so.cache", "/etc/localtime" })
                if (File.Exists(path) || Directory.Exists(path)) { start.ArgumentList.Add("--ro-bind"); start.ArgumentList.Add(path); start.ArgumentList.Add(path); mounted.Add(path); }
            Mount(Path.GetDirectoryName(dotnet)!,false); Mount(Path.GetDirectoryName(workerPath)!,false);
            foreach (string path in request.Policy.PluginRoots.Concat(request.Policy.ReadOnlyRoots)) Mount(path,false);
            foreach (string path in request.Policy.FileRoots) Mount(path,true);
            foreach(string root in request.Policy.FileRoots.Concat(request.Policy.ReadOnlyRoots).Distinct(StringComparer.Ordinal)) {
                string imports=Path.Combine(root,".ud-imports");
                if(!Directory.Exists(imports))continue;
                HostFiles.NoLinks(imports);start.ArgumentList.Add("--tmpfs");start.ArgumentList.Add(imports);
                string own=Path.Combine(imports,request.Identity.Tenant,request.Identity.Subject);
                if(Directory.Exists(own))Mount(own,request.Policy.FileRoots.Contains(root,StringComparer.Ordinal));
            }
            foreach (string path in (Environment.GetEnvironmentVariable("LD_LIBRARY_PATH") ?? "").Split(Path.PathSeparator,StringSplitOptions.RemoveEmptyEntries)) Mount(path,false);
            foreach (string path in request.Policy.CredentialFiles.Values) Mount(path,false);
            var vault = new ParameterVault(request.Policy);
            foreach (string reference in new[] { request.ParametersReference,request.ControlReference }.Where(value => value.Length != 0)) Mount(vault.DirectoryFor(reference),false);
            var directory = request.Policy.ExecutionDirectory(request.Ticket); Directory.CreateDirectory(directory); Mount(directory,true);
            var controls = request.Policy.ControlDirectory(request.Ticket); Directory.CreateDirectory(controls);
            File.SetUnixFileMode(controls,UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute); Mount(controls,true);
            var assemblies = Path.Combine(request.Policy.DataDirectory,"assemblies");
            Directory.CreateDirectory(assemblies); Mount(assemblies,request.Mode == "compile");
            start.ArgumentList.Add("--chdir"); start.ArgumentList.Add(directory); start.ArgumentList.Add("--"); start.ArgumentList.Add(dotnet);
        }
        start.ArgumentList.Add(workerPath);
        // Do not inherit unrelated application/cloud credentials into an extension process.
        var allowed = new[] { "PATH", "DOTNET_ROOT", "DOTNET_ROOT_X64", "LD_LIBRARY_PATH", "SystemRoot", "WINDIR", "TEMP", "TMP", "TMPDIR", "LANG", "LC_ALL", "XDG_RUNTIME_DIR", "DBUS_SESSION_BUS_ADDRESS" };
        foreach (string key in start.Environment.Keys.ToArray()) if (!allowed.Contains(key,StringComparer.OrdinalIgnoreCase)) start.Environment.Remove(key);
        return Process.Start(start) ?? throw new InvalidOperationException("Worker failed to start");
    }
    public static async Task Terminate(Process process,string ticket,bool isolated) {
        if (isolated && OperatingSystem.IsLinux()) {
            if (!Guid.TryParseExact(ticket,"N",out _)) throw new ArgumentException("Invalid Worker ticket");
            var command = new ProcessStartInfo("/usr/bin/systemctl") { UseShellExecute = false,RedirectStandardOutput = true,RedirectStandardError = true };
            foreach(string argument in new[] { "--user", "kill", "--signal=KILL", "--kill-whom=all", "ud-worker-" + ticket + ".service" }) command.ArgumentList.Add(argument);
            using var control = Process.Start(command);
            if (control is not null) { using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                try { await control.WaitForExitAsync(timeout.Token); } catch(OperationCanceledException) { control.Kill(entireProcessTree:true); } }
        }
        try { if (!process.HasExited) process.Kill(entireProcessTree:true); } catch(InvalidOperationException) { }
    }
    public static async Task<JsonElement> Inspect(string workerPath, WorkerRequest request, CancellationToken cancellation = default)
    {
        var gate=inspections.GetOrAdd(request.Policy.DataDirectory,_=>new SemaphoreSlim((int)Math.Min(2,request.Policy.MaxConcurrency)));
        await gate.WaitAsync(cancellation);
        try{return await InspectCore(workerPath,request,cancellation);}finally{gate.Release();}
    }
    private static async Task<JsonElement> InspectCore(string workerPath,WorkerRequest request,CancellationToken cancellation)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellation); deadline.CancelAfter(TimeSpan.FromSeconds(60));
        using var process = Launch(workerPath,request);
        var discard = WorkerProtocol.Discard(process.StandardError,deadline.Token);
        try {
            await process.StandardInput.WriteLineAsync(JsonSerializer.Serialize(request,HostPolicy.JsonOptions)); await process.StandardInput.FlushAsync(deadline.Token);
            string response = await WorkerProtocol.ReadLine(process.StandardOutput,67108864,deadline.Token) ?? throw new IOException("Worker returned no result");
            using var document = JsonDocument.Parse(response); var root = document.RootElement;
            if (root.GetProperty("version").GetInt32() != ProductInfo.Protocol || root.GetProperty("ticket").GetString() != request.Ticket || root.GetProperty("kind").GetString() != "inspection")
                throw new InvalidDataException("Invalid Worker inspection response");
            int result = root.GetProperty("result").GetInt32(); if (result != 0) throw new UniversalDataException(result,"Native Worker operation failed");
            await process.WaitForExitAsync(deadline.Token); if (process.ExitCode != 0) throw new IOException("Worker inspection failed");
            return root.GetProperty("value").Clone();
        } finally { if (!process.HasExited) await Terminate(process,request.Ticket,request.Isolated); }
    }
}
