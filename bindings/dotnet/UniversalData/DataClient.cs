using System.Reflection;

namespace UniversalData;

public sealed class DataClient : IAsyncDisposable
{
    private readonly string policyFile, tenant;
    private readonly SdkHostOptions options;
    private readonly string? workerPath;
    private readonly object gate = new();
    private readonly CancellationTokenSource lifetime = new();
    private Task<SdkSessionHostClient>? host;
    private bool disposed;

    internal DataClient(string policyFile, string tenant, SdkHostOptions options, DataSourceDefinition[] sources,
        SdkModule[] modules, string? defaultSource, string? workerPath)
    {
        this.policyFile = policyFile;
        this.tenant = tenant;
        this.options = options;
        this.workerPath = workerPath;
        DefaultSource = defaultSource;
        Modules = modules;
        Sources = new(sources, ProbeAsync, PublishSource);
    }

    public DataSourceRegistry Sources { get; }
    internal SdkModule[] Modules { get; }
    internal string? DefaultSource { get; }
    internal string? WorkerPath => workerPath;

    public static DataClientBuilder CreateBuilder(string policyFile, string tenant) => new(policyFile, tenant);

    public static DataClient Create(string policyFile, string tenant, Action<DataClientBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var builder = CreateBuilder(policyFile, tenant);
        configure(builder);
        return builder.Build();
    }

    public async Task<DataSession> OpenSessionAsync(CancellationToken cancellation = default) =>
        new(this, await OpenWorkerAsync(cancellation).ConfigureAwait(false));

    public async Task RunAsync(Func<DataContext, CancellationToken, Task> method, CancellationToken cancellation = default)
    {
        await using var session = await OpenSessionAsync(cancellation).ConfigureAwait(false);
        await session.RunAsync(method, cancellation).ConfigureAwait(false);
    }

    public async Task RunAsync<TArgument>(Func<DataContext, TArgument, CancellationToken, Task> method,
        TArgument argument, CancellationToken cancellation = default)
    {
        await using var session = await OpenSessionAsync(cancellation).ConfigureAwait(false);
        await session.RunAsync(method, argument, cancellation).ConfigureAwait(false);
    }

    public async Task<TResult> RunAsync<TResult>(Func<DataContext, CancellationToken, Task<TResult>> method,
        CancellationToken cancellation = default)
    {
        await using var session = await OpenSessionAsync(cancellation).ConfigureAwait(false);
        return await session.RunAsync(method, cancellation).ConfigureAwait(false);
    }

    public async Task<TResult> RunAsync<TArgument, TResult>(Func<DataContext, TArgument, CancellationToken, Task<TResult>> method,
        TArgument argument, CancellationToken cancellation = default)
    {
        await using var session = await OpenSessionAsync(cancellation).ConfigureAwait(false);
        return await session.RunAsync(method, argument, cancellation).ConfigureAwait(false);
    }

    public EtlFlow Etl(string id) => new(this, id);

    public async Task<DataRecoveryResult> RecoverAsync(DataRunReference runReference, CancellationToken cancellation = default)
    {
        ArgumentNullException.ThrowIfNull(runReference);
        await using var session = await OpenSessionAsync(cancellation).ConfigureAwait(false);
        return await session.RecoverAsync(runReference, cancellation).ConfigureAwait(false);
    }

    internal SdkSourceBinding GetBinding(string? name) => DataContext.ResolveBinding(Sources.Snapshot(), name, DefaultSource, false);

    internal async Task<WorkerSession> OpenWorkerAsync(CancellationToken cancellation)
    {
        Task<SdkSessionHostClient> pending;
        lock (gate)
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            if (host is { IsFaulted: true } or { IsCanceled: true }) host = null;
            pending = host ??= Task.Run(StartHostAsync);
        }
        var ready = await pending.WaitAsync(cancellation).ConfigureAwait(false);
        return await ready.OpenAsync(cancellation).ConfigureAwait(false);
    }

    private async Task<SdkSessionHostClient> StartHostAsync()
    {
        var launched = await SdkSessionHostClient.StartAsync(policyFile, tenant, options, lifetime.Token, workerPath)
            .ConfigureAwait(false);
        try
        {
            foreach (var source in Sources.Snapshot())
                await launched.UpdateSourceAsync(source.Definition.Name, source.Revision, source.Enabled, lifetime.Token)
                    .ConfigureAwait(false);
            return launched;
        }
        catch
        {
            await launched.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private void PublishSource(SdkSourceBinding source)
    {
        Task<SdkSessionHostClient>? pending;
        lock (gate) { ObjectDisposedException.ThrowIf(disposed, this); pending = host; }
        if (pending is null) return;
        var active = pending.GetAwaiter().GetResult();
        active.UpdateSourceAsync(source.Definition.Name, source.Revision, source.Enabled, lifetime.Token)
            .GetAwaiter().GetResult();
    }

    internal SdkMethod DescribeMethod(Delegate method, bool hasArgument = true, bool business = false)
    {
        ArgumentNullException.ThrowIfNull(method);
        MethodInfo member = method.Method;
        var type = member.DeclaringType;
        if (method.GetInvocationList().Length != 1 || method.Target is not null || !member.IsStatic ||
            !member.IsPublic || member.IsGenericMethod || type?.IsVisible != true || type?.IsGenericType != false)
            throw new ArgumentException("The Worker requires a public static method on a public, non-generic type", nameof(method));
        var parameters = member.GetParameters();
        if (business && (parameters.Length != (hasArgument ? 3 : 2) || parameters[0].ParameterType != typeof(DataContext) ||
            parameters[^1].ParameterType != typeof(CancellationToken) || !typeof(Task).IsAssignableFrom(member.ReturnType)))
            throw new ArgumentException("The Worker method signature differs from its SDK delegate", nameof(method));
        var module = Modules.SingleOrDefault(m => string.Equals(m.Path, type!.Assembly.Location,
            OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal));
        if (module is null) throw new ArgumentException("The method belongs to an unregistered module", nameof(method));
        return new(module.Sha256, type!.FullName!, member.Name,
            parameters.Select(parameter => parameter.ParameterType.FullName!).ToArray(), hasArgument);
    }

    private async Task<DataSourceProbe> ProbeAsync(string name, CancellationToken cancellation)
    {
        _ = Sources.Get(name);
        await using var session = await OpenSessionAsync(cancellation).ConfigureAwait(false);
        return await session.ProbeAsync(name, cancellation).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        Task<SdkSessionHostClient>? pending;
        lock (gate)
        {
            if (disposed) return;
            disposed = true;
            pending = host;
        }
        lifetime.Cancel();
        try
        {
            if (pending is not null)
            {
                SdkSessionHostClient? ready = null;
                try { ready = await pending.ConfigureAwait(false); }
                catch when (pending.IsFaulted || pending.IsCanceled) { }
                if (ready is not null) await ready.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally { lifetime.Dispose(); }
    }
}
