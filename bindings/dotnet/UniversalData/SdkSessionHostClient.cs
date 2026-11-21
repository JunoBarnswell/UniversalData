using System.Diagnostics;
using System.Reflection;

namespace UniversalData;

internal sealed class SdkSessionHostClient(Process process, Task stderr, SdkHostConfiguration configuration)
    : IAsyncDisposable
{
    private readonly CancellationTokenSource lifetime = new();
    private int disposed;

    internal int ProcessId => process.Id;

    internal static async Task<SdkSessionHostClient> StartAsync(string policyFile, string tenant,
        SdkHostOptions options, CancellationToken cancellation, string? workerPath = null)
    {
        string path = FindWorker(workerPath);
        bool isDll = Path.GetExtension(path).Equals(".dll", StringComparison.OrdinalIgnoreCase);
        CheckDeploymentVersion(isDll ? path : Path.ChangeExtension(path, ".dll"));
        var configuration = new SdkHostConfiguration(ProductInfo.WorkerRevision, Path.GetFullPath(policyFile),
            tenant, options, SdkSessionProtocol.CreateEndpoint(), SdkSessionProtocol.CreateCapability());
        string executable = isDll ? Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet" : path;
        var start = new ProcessStartInfo(executable)
        {
            UseShellExecute = false, RedirectStandardInput = true, RedirectStandardOutput = true,
            RedirectStandardError = true, CreateNoWindow = true
        };
        if (isDll) start.ArgumentList.Add(path);
        start.Environment[SdkSessionProtocol.DeploymentVariable] = path;

        Process process;
        try { process = Process.Start(start) ?? throw Failure(StatusCode.WorkerLost, "SDK Worker host did not start"); }
        catch (System.ComponentModel.Win32Exception)
        { throw Failure(StatusCode.WorkerLost, "Could not start the SDK Worker host executable"); }
        var stderr = WorkerProtocol.Discard(process.StandardError, CancellationToken.None);
        var host = new SdkSessionHostClient(process, stderr, configuration);
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
            timeout.CancelAfter(TimeSpan.FromSeconds(20));
            try { await WorkerProtocol.ReadProcessIdAsync(process.StandardOutput, timeout.Token).ConfigureAwait(false); }
            catch (InvalidDataException)
            { throw Failure(StatusCode.Incompatible, "The SDK and Worker deployment use different protocol revisions"); }
            await WorkerProtocol.WriteAsync(process.StandardInput.BaseStream, configuration, null, timeout.Token).ConfigureAwait(false);
            var response = await WorkerProtocol.ReadAsync<SdkHostStatus>(process.StandardOutput.BaseStream, timeout.Token)
                .ConfigureAwait(false);
            using (response.Batch)
                Check(response.Message, response.Batch is null);
            return host;
        }
        catch (Exception error)
        {
            await host.DisposeAsync().ConfigureAwait(false);
            if (error is EndOfStreamException or IOException)
                throw Failure(StatusCode.WorkerLost, "SDK Worker host stopped during startup");
            if (error is OperationCanceledException && !cancellation.IsCancellationRequested)
                throw Failure(StatusCode.Timeout, "SDK Worker host did not acknowledge startup");
            throw;
        }
    }

    internal async Task<WorkerSession> OpenAsync(CancellationToken cancellation, bool dedicated, uint cpuSlots)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) != 0, this);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellation, lifetime.Token);
        timeout.CancelAfter(configuration.Options.QueueTimeout + TimeSpan.FromSeconds(15));
        try
        {
            await using var endpoint = SdkSessionProtocol.Connect(configuration.Endpoint);
            await endpoint.ConnectAsync(timeout.Token).ConfigureAwait(false);
            await WorkerProtocol.WriteAsync(endpoint,
                new SdkPipeAdmission(ProductInfo.WorkerRevision, configuration.Capability, Dedicated: dedicated,
                    CpuSlots: cpuSlots), null, timeout.Token)
                .ConfigureAwait(false);
            var response = await WorkerProtocol.ReadAsync<SdkHostStatus>(endpoint, timeout.Token).ConfigureAwait(false);
            using (response.Batch)
                Check(response.Message, response.Batch is null);
            if (response.Message.Grant is not { ProcessId: > 0 } grant)
                throw Failure(StatusCode.Protocol, "SDK Worker host did not grant a process");
            return await WorkerSession.AttachAsync(grant, configuration, timeout.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellation.IsCancellationRequested && !lifetime.IsCancellationRequested)
        {
            throw Failure(StatusCode.Timeout, "SDK Worker session admission timed out");
        }
        catch (IOException)
        {
            throw Failure(StatusCode.WorkerLost, "SDK Worker host closed the admission channel");
        }
    }

    internal async Task UpdateSourceAsync(string name, ulong revision, bool enabled, CancellationToken cancellation)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) != 0, this);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellation, lifetime.Token);
        timeout.CancelAfter(configuration.Options.QueueTimeout + TimeSpan.FromSeconds(5));
        await using var endpoint = SdkSessionProtocol.Connect(configuration.Endpoint);
        await endpoint.ConnectAsync(timeout.Token).ConfigureAwait(false);
        await WorkerProtocol.WriteAsync(endpoint, new SdkPipeAdmission(ProductInfo.WorkerRevision,
            configuration.Capability, "source.update", name, new(revision, enabled)), null, timeout.Token)
            .ConfigureAwait(false);
        var response = await WorkerProtocol.ReadAsync<SdkHostStatus>(endpoint, timeout.Token).ConfigureAwait(false);
        using (response.Batch)
            Check(response.Message, response.Batch is null);
    }

    internal static void Check(SdkHostStatus response, bool noBatch)
    {
        if (response.Revision != ProductInfo.WorkerRevision || !noBatch)
            throw Failure(StatusCode.Protocol, "Incompatible SDK Worker host protocol");
        if (response.Code != StatusCode.Ok)
            throw new UniversalDataException((int)response.Code,
                response.Error?.Message ?? "SDK Worker host rejected admission", response.Error);
    }

    internal static UniversalDataException Failure(StatusCode code, string message) =>
        new((int)code, message, new OperationError(ErrorSource.Worker, code, message));

    private static void CheckDeploymentVersion(string assemblyPath)
    {
        if (!File.Exists(assemblyPath)) return;
        AssemblyName entry;
        try { entry = AssemblyName.GetAssemblyName(assemblyPath); }
        catch (Exception error) when (error is IOException or BadImageFormatException or UnauthorizedAccessException)
        { return; }
        if (entry.Name == "ud_worker" && entry.Version is { } workerVersion &&
            typeof(SdkSessionHostClient).Assembly.GetName().Version is { } sdkVersion && workerVersion.Major != sdkVersion.Major)
            throw Failure(StatusCode.Incompatible, $"UniversalData SDK {sdkVersion.Major} requires a matching Worker, found Worker {workerVersion.Major}");
    }

    internal static string FindWorker(string? workerPath)
    {
        if (!OperatingSystem.IsLinux() && !OperatingSystem.IsWindows())
            throw Failure(StatusCode.Unsupported, "SDK Worker sessions require Windows or Linux");
        string? configured = workerPath ?? Environment.GetEnvironmentVariable("UD_WORKER_PATH");
        string name = OperatingSystem.IsWindows() ? "ud_worker.exe" : "ud_worker";
        string runtime = Path.Combine(AppContext.BaseDirectory, "runtimes",
            OperatingSystem.IsWindows() ? "win-x64" : "linux-x64", "worker",
            name);
        string executable = configured ?? (File.Exists(runtime) ? runtime :
            Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "worker", name)));
        if (!Path.IsPathFullyQualified(executable) || !File.Exists(executable))
            throw Failure(StatusCode.DriverUnavailable, "The UniversalData Worker executable is unavailable");
        return Path.GetFullPath(executable);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0) return;
        lifetime.Cancel();
        process.StandardInput.Close();
        using var timeout = new CancellationTokenSource(configuration.Options.CancellationGrace + TimeSpan.FromSeconds(10));
        try { await process.WaitForExitAsync(timeout.Token).ConfigureAwait(false); }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested)
        {
            try { if (!process.HasExited) process.Kill(entireProcessTree: true); }
            catch (InvalidOperationException) { }
            await process.WaitForExitAsync().ConfigureAwait(false);
        }
        finally
        {
            try { await stderr.ConfigureAwait(false); }
            catch (IOException) { }
            process.Dispose();
            lifetime.Dispose();
        }
    }
}
