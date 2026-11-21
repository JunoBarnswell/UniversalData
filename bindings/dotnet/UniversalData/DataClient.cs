namespace UniversalData;

public sealed partial class DataClient : IDataClient
{
    private readonly string policyFile, tenant;
    private readonly SdkHostOptions options;
    private readonly string? workerPath;
    private readonly object gate = new();
    private readonly CancellationTokenSource lifetime = new();
    private readonly WorkerSessionPool jobSessions;
    private Task<SdkSessionHostClient>? host;
    private bool disposed;

    internal DataClient(string policyFile, string tenant, SdkHostOptions options, DataSourceDefinition[] sources,
        string? workerPath)
    {
        this.policyFile = policyFile;
        this.tenant = tenant;
        this.options = options;
        this.workerPath = workerPath;
        jobSessions = new(options.MaxConcurrentSessions);
        Sources = new(sources, ProbeAsync, PublishSource);
    }

    public DataSourceRegistry Sources { get; }

    public static DataClientBuilder CreateBuilder(string policyFile, string tenant) => new(policyFile, tenant);

    public static DataClient Create(string policyFile, string tenant, Action<DataClientBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var builder = CreateBuilder(policyFile, tenant);
        configure(builder);
        return builder.Build();
    }

    internal Task<WorkerSession> RentJobWorkerAsync(uint cpuSlots, bool returnsRows, CancellationToken cancellation) =>
        cpuSlots == 1 ? jobSessions.RentAsync(token => OpenWorkerAsync(token), returnsRows, cancellation) :
            OpenWorkerAsync(cancellation, dedicated: true, cpuSlots: cpuSlots);

    internal ValueTask ReturnJobWorkerAsync(WorkerSession worker, bool returnsRows, bool complete) =>
        jobSessions.ReturnAsync(worker, returnsRows, complete);

    internal async Task<WorkerSession> OpenWorkerAsync(CancellationToken cancellation, bool dedicated = false,
        uint cpuSlots = 1)
    {
        Task<SdkSessionHostClient> pending;
        lock (gate)
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            if (host is { IsFaulted: true } or { IsCanceled: true }) host = null;
            pending = host ??= Task.Run(StartHostAsync);
        }
        var ready = await pending.WaitAsync(cancellation).ConfigureAwait(false);
        return await ready.OpenAsync(cancellation, dedicated, cpuSlots).ConfigureAwait(false);
    }

    private async Task<SdkSessionHostClient> StartHostAsync()
    {
        var launched = await SdkSessionHostClient.StartAsync(policyFile, tenant, options, lifetime.Token, workerPath)
            .ConfigureAwait(false);
        try
        {
            foreach (var source in Sources.Snapshot().Values)
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
            await jobSessions.DisposeAsync().ConfigureAwait(false);
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
