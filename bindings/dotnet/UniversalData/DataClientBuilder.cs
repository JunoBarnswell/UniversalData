namespace UniversalData;

public sealed class DataWorkerOptions
{
    private int sessions = 4, branches = 2, queue = 128, connections = 2, sessionsPerWorker = 1;
    private ulong memory = 268435456;
    private TimeSpan queueTimeout = TimeSpan.FromSeconds(30), executionTimeout = TimeSpan.FromMinutes(5),
        cancellationGrace = TimeSpan.FromSeconds(10);
    private bool isolation;
    private string? executable;
    internal string? Executable => executable;

    public DataWorkerOptions MaxConcurrentSessions(int value) { sessions = value; return this; }
    public DataWorkerOptions MaxSessionsPerWorker(int value) { sessionsPerWorker = value; return this; }
    public DataWorkerOptions MaxBranchesPerSession(int value) { branches = value; return this; }
    public DataWorkerOptions MaxConnectionsPerSource(int value) { connections = value; return this; }
    public DataWorkerOptions QueueCapacity(int value) { queue = value; return this; }
    public DataWorkerOptions QueueTimeout(TimeSpan value) { queueTimeout = value; return this; }
    public DataWorkerOptions ExecutionTimeout(TimeSpan value) { executionTimeout = value; return this; }
    public DataWorkerOptions CancellationGrace(TimeSpan value) { cancellationGrace = value; return this; }
    public DataWorkerOptions MemoryPerSession(ulong bytes) { memory = bytes; return this; }
    public DataWorkerOptions RequireResourceIsolation(bool value = true) { isolation = value; return this; }
    public DataWorkerOptions WorkerPath(string path) { executable = Path.GetFullPath(path); return this; }

    internal SdkHostOptions Freeze()
    {
        if (sessions is < 1 or > 64 || sessionsPerWorker < 1 || sessionsPerWorker > sessions ||
            branches is < 1 or > 64 || connections is < 1 or > 64 ||
            queue is < 1 or > 65536 || memory == 0 || !Valid(queueTimeout) || !Valid(executionTimeout) ||
            !Valid(cancellationGrace) || (isolation && sessions < 2))
            throw new ArgumentException("Invalid SDK Worker resource budget");
        return new(sessions, branches, queue, queueTimeout, executionTimeout, cancellationGrace, memory, connections, isolation,
            sessionsPerWorker);
    }

    private static bool Valid(TimeSpan value) => value > TimeSpan.Zero && value <= TimeSpan.FromDays(7);
}

public sealed class DataClientBuilder
{
    private readonly string policyFile, tenant;
    private readonly Dictionary<string, DataSourceDefinition> sources = new(StringComparer.Ordinal);
    private readonly DataWorkerOptions workers = new();

    internal DataClientBuilder(string policyFile, string tenant)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(policyFile);
        ArgumentException.ThrowIfNullOrWhiteSpace(tenant);
        this.policyFile = Path.GetFullPath(policyFile);
        this.tenant = tenant;
    }

    public DataClientBuilder AddSource(DataSourceDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(definition);
        if (!sources.TryAdd(definition.Name, definition))
            throw new ArgumentException("Duplicate data source name", nameof(definition));
        return this;
    }

    public DataClientBuilder AddPostgreSql(string name, string connectionString) => AddPostgreSql(name, connectionString, null);
    public DataClientBuilder AddPostgreSql(string name, string connectionString, Action<DataSourceOptions>? configure) =>
        AddDatabase(name, DataSourceKind.PostgreSql, connectionString, configure);
    public DataClientBuilder AddMySql(string name, string connectionString) => AddMySql(name, connectionString, null);
    public DataClientBuilder AddMySql(string name, string connectionString, Action<DataSourceOptions>? configure) =>
        AddDatabase(name, DataSourceKind.MySql, connectionString, configure);
    public DataClientBuilder AddMariaDb(string name, string connectionString) => AddMariaDb(name, connectionString, null);
    public DataClientBuilder AddMariaDb(string name, string connectionString, Action<DataSourceOptions>? configure) =>
        AddDatabase(name, DataSourceKind.MariaDb, connectionString, configure);
    public DataClientBuilder AddSqlServer(string name, string connectionString) => AddSqlServer(name, connectionString, null);
    public DataClientBuilder AddSqlServer(string name, string connectionString, Action<DataSourceOptions>? configure) =>
        AddDatabase(name, DataSourceKind.SqlServer, connectionString, configure);
    public DataClientBuilder AddOracle(string name, string connectionString) => AddOracle(name, connectionString, null);
    public DataClientBuilder AddOracle(string name, string connectionString, Action<DataSourceOptions>? configure) =>
        AddDatabase(name, DataSourceKind.Oracle, connectionString, configure);
    public DataClientBuilder AddSqlite(string name, string connectionString) => AddSqlite(name, connectionString, null);
    public DataClientBuilder AddSqlite(string name, string connectionString, Action<DataSourceOptions>? configure) =>
        AddDatabase(name, DataSourceKind.Sqlite, connectionString, configure);
    public DataClientBuilder AddHttp(string name, Uri baseUri) => AddSource(new(name, DataSourceKind.Http, "", baseUri: baseUri));

    public DataClientBuilder AddIceberg(string name, Uri catalog, string storageRoot,
        string? warehouse = null, string? credentialReference = null) =>
        AddSource(new(name, DataSourceKind.Iceberg, "", credentialReference: credentialReference,
            baseUri: catalog, storageRoot: storageRoot, catalogWarehouse: warehouse));

    public DataClientBuilder ConfigureWorkers(Action<DataWorkerOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        configure(workers);
        return this;
    }

    public DataClient Build() => new(policyFile, tenant, workers.Freeze(), sources.Values.ToArray(), workers.Executable);

    private DataClientBuilder AddDatabase(string name, DataSourceKind kind, string connectionString,
        Action<DataSourceOptions>? configure)
    {
        var options = new DataSourceOptions();
        configure?.Invoke(options);
        return AddSource(new(name, kind, connectionString, options.Resource, options.CredentialReference,
            nativeConnectionString: options.NativeConnectionString, initialRevision: options.InitialRevision,
            provisionTransactionReceipts: options.ProvisionTransactionReceipts));
    }
}
