using System.Diagnostics;
using System.Text.Json;

namespace UniversalData.Hosting;

/// <summary>All product Native inspection, script compilation and execution use the same Worker process entry.</summary>
public static class WorkerLauncher
{
    public static Process Launch(string workerPath, WorkerRequest request) =>
        Launch(workerPath, request.Ticket, request.Budget, request);

    internal static Process Launch(string workerPath, string processKey, WorkerBudget? budget, WorkerRequest? request)
    {
        workerPath = Path.GetFullPath(workerPath); HostFiles.NoLinks(workerPath);
        string dotnet = Environment.ProcessPath is { } current && Path.GetFileNameWithoutExtension(current).Equals("dotnet",StringComparison.OrdinalIgnoreCase)
            ? current : Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet";
        bool managed = Path.GetExtension(workerPath).Equals(".dll", StringComparison.OrdinalIgnoreCase);
        string executable = managed ? dotnet : workerPath;
        bool isolated = request?.Isolated == true;
        if (isolated && !OperatingSystem.IsLinux()) throw new PlatformNotSupportedException("Untrusted modules require an available OS sandbox; this host has none configured");
        if (!Guid.TryParseExact(processKey, "N", out _)) throw new ArgumentException("Invalid Worker process key");
        if (budget is not null && request is not null) WorkerProcessLimits.Validate(request, budget);
        bool systemd = isolated || (budget is not null && OperatingSystem.IsLinux());
        var start = new ProcessStartInfo(systemd ? "/usr/bin/systemd-run" : executable) { RedirectStandardInput = true, RedirectStandardOutput = true,
            RedirectStandardError = true, UseShellExecute = false, CreateNoWindow = true };
        if (systemd) {
            if (isolated && !Path.IsPathFullyQualified(executable)) throw new InvalidOperationException("An absolute Worker host is required for isolation");
            if (!File.Exists("/usr/bin/systemd-run") || (isolated && !File.Exists("/usr/bin/bwrap")))
                throw new PlatformNotSupportedException("Worker resource control requires systemd and untrusted modules also require bubblewrap");
            foreach (string option in new[] { "--user", "--quiet", "--wait", "--pipe", "--collect", "--unit=ud-worker-" + processKey,
                "--property=KillMode=control-group" })
                start.ArgumentList.Add(option);
            if (budget is null) {
                start.ArgumentList.Add("--property=MemoryMax=" + checked(request!.Policy.MemoryLimit + WorkerSupervisor.ProcessOverhead));
                start.ArgumentList.Add("--property=TasksMax=64");
                start.ArgumentList.Add("--property=CPUQuota=100%");
            } else WorkerProcessLimits.AddLinuxProperties(start, budget);
            if (!isolated) start.ArgumentList.Add("--working-directory=" + Environment.CurrentDirectory);
            start.ArgumentList.Add("--");
        }
        if (isolated) {
            ArgumentNullException.ThrowIfNull(request);
            start.ArgumentList.Add("/usr/bin/bwrap");
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
            Mount(Path.GetDirectoryName(executable)!,false); Mount(Path.GetDirectoryName(workerPath)!,false);
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
            var directory = request.Policy.ExecutionDirectory(request.JournalTicket ?? request.Ticket); Directory.CreateDirectory(directory); Mount(directory,true);
            var controls = request.Policy.ControlDirectory(request.Ticket); Directory.CreateDirectory(controls);
            File.SetUnixFileMode(controls,UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute); Mount(controls,true);
            var assemblies = Path.Combine(request.Policy.DataDirectory,"assemblies");
            Directory.CreateDirectory(assemblies); Mount(assemblies,request.Mode == "compile");
            start.ArgumentList.Add("--chdir"); start.ArgumentList.Add(directory); start.ArgumentList.Add("--"); start.ArgumentList.Add(executable);
        }
        // Do not inherit unrelated application/cloud credentials into an extension process.
        var allowed = new[] { "PATH", "DOTNET_ROOT", "DOTNET_ROOT_X64", "LD_LIBRARY_PATH", "SystemRoot", "WINDIR", "TEMP", "TMP", "TMPDIR", "LANG", "LC_ALL", "XDG_RUNTIME_DIR", "DBUS_SESSION_BUS_ADDRESS",
            "ODBCSYSINI", "TNS_ADMIN", "SSL_CERT_FILE", "SSL_CERT_DIR" };
        foreach (string key in start.Environment.Keys.ToArray()) if (!allowed.Contains(key,StringComparer.OrdinalIgnoreCase)) start.Environment.Remove(key);
        if (systemd && !isolated) {
            start.ArgumentList.Add("/usr/bin/env");
            start.ArgumentList.Add("-i");
            foreach (var variable in start.Environment)
                if (variable.Value is { } value)
                    start.ArgumentList.Add(variable.Key + "=" + value.Replace("$", "$$", StringComparison.Ordinal));
            start.ArgumentList.Add(executable.Replace("$", "$$", StringComparison.Ordinal));
        }
        if (managed)
            start.ArgumentList.Add(systemd ? workerPath.Replace("$", "$$", StringComparison.Ordinal) : workerPath);
        if (OperatingSystem.IsWindows())
            return WorkerProcessLimits.StartWindows(start, budget);
        return Process.Start(start) ?? throw new InvalidOperationException("Worker failed to start");
    }
    public static async Task Terminate(Process process,string ticket,bool isolated) {
        if (OperatingSystem.IsLinux() && (isolated || process.StartInfo.FileName == "/usr/bin/systemd-run")) {
            if (!Guid.TryParseExact(ticket,"N",out _)) throw new ArgumentException("Invalid Worker ticket");
            var command = new ProcessStartInfo("/usr/bin/systemctl") { UseShellExecute = false,RedirectStandardOutput = true,RedirectStandardError = true };
            foreach(string argument in new[] { "--user", "kill", "--signal=KILL", "--kill-whom=all", "ud-worker-" + ticket + ".service" }) command.ArgumentList.Add(argument);
            using var control = Process.Start(command);
            if (control is not null) { using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                try { await control.WaitForExitAsync(timeout.Token); } catch(OperationCanceledException) { control.Kill(entireProcessTree:true); } }
        }
        try { if (!process.HasExited) process.Kill(entireProcessTree:true); } catch(InvalidOperationException) { }
    }
    public static Task<int> WaitReady(Process process, CancellationToken cancellation = default) =>
        WorkerProtocol.ReadProcessIdAsync(process.StandardOutput, cancellation);

    internal static async Task<JsonElement> Inspect(WorkerSupervisor supervisor, WorkerRequest request, CancellationToken cancellation)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellation); deadline.CancelAfter(TimeSpan.FromSeconds(60));
        await using var worker = await supervisor.RentAsync(request, supervisor.OrdinaryBudget, deadline.Token);
        var process = worker.Process;
            string response = await WorkerProtocol.ReadLine(process.StandardOutput,67108864,deadline.Token) ?? throw new IOException("Worker returned no result");
            using var document = JsonDocument.Parse(response); var root = document.RootElement;
            if (root.GetProperty("version").GetInt32() != ProductInfo.Protocol || root.GetProperty("ticket").GetString() != request.Ticket || root.GetProperty("kind").GetString() != "inspection")
                throw new InvalidDataException("Invalid Worker inspection response");
            int result = root.GetProperty("result").GetInt32(); if (result != 0) throw new UniversalDataException(result,"Native Worker operation failed");
            await process.WaitForExitAsync(deadline.Token); if (process.ExitCode != 0) throw new IOException("Worker inspection failed");
            return root.GetProperty("value").Clone();
    }
}
