using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using SqlSugar;
using UniversalData;

string root = Directory.CreateTempSubdirectory("ud-nuget-").FullName;
try
{
    string resources = Directory.CreateDirectory(Path.Combine(root, "resources")).FullName;
    string target = Path.Combine(resources, "finance.db");
    using var listener = new TcpListener(IPAddress.Loopback, 0);
    listener.Start();
    using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(100));
    var token = deadline.Token;
    var origin = $"http://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}";
    var rid = OperatingSystem.IsWindows() ? "win-x64" : "linux-x64";
    var runtime = Path.Combine(AppContext.BaseDirectory, "runtimes", rid, "worker");
    var plugins = Path.Combine(AppContext.BaseDirectory, "runtimes", rid, "native");
    var credentials = new Dictionary<string, string> { ["finance"] = Path.Combine(root, "finance.conf") };
    var grants = new Dictionary<string, string[]>();
    var endpoints = new HashSet<string>(StringComparer.Ordinal) { origin };
    var sqlProbes = new Dictionary<string, string>();
    var brokers = new Dictionary<string, string>();
    var brokerTargets = new Dictionary<string, string>();
    var brokerGroups = new Dictionary<string, string>();
    var brokerKinds = new Dictionary<string, DataSourceKind>(StringComparer.Ordinal)
    {
        ["kafka"] = DataSourceKind.Kafka,
        ["rabbitmq"] = DataSourceKind.RabbitMq,
        ["rocketmq"] = DataSourceKind.RocketMq,
        ["activemq-classic"] = DataSourceKind.ActiveMq,
        ["activemq-artemis"] = DataSourceKind.ActiveMq
    };
    await File.WriteAllTextAsync(credentials["finance"], target, token);
    if (args is ["--complete"])
        await LoadCompleteFixtures(sqlProbes, brokers, brokerTargets, brokerGroups, credentials,
            grants, endpoints, root, token);
    var activeBrokerKinds = brokerKinds
        .Where(pair => brokers.ContainsKey(pair.Key))
        .ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal);

    var policy = Path.Combine(root, "policy.json");
    await File.WriteAllTextAsync(policy, JsonSerializer.Serialize(new
    {
        dataDirectory = Path.Combine(root, "state"), fileRoots = new[] { resources },
        networkOrigins = endpoints, pluginRoots = new[] { runtime, plugins, AppContext.BaseDirectory },
        credentialFiles = credentials, resources = grants
    }), token);
    if (typeof(ISqlSugarClient).Assembly.GetName().Name != "SqlSugar")
        throw new InvalidOperationException("NuGet must restore the official SqlSugar assembly");

    var server = Serve(listener, token);
    int created = 0;
    var services = new ServiceCollection().AddUniversalData(policy, "local", builder =>
    {
        Interlocked.Increment(ref created);
        builder.AddHttp("sales", new Uri(origin))
            .AddSqlite("finance", "Data Source=" + target)
            .ConfigureWorkers(options => options.MaxConcurrentSessions(2));
        foreach (var (broker, kind) in activeBrokerKinds)
            builder.AddSource(new(broker, kind, "", resource: "mq-fixture-resource", credentialReference: broker));
    });
    await using var provider = services.BuildServiceProvider();
    if (created != 0) throw new InvalidOperationException("Client construction must wait for resolution");
    var clients = await Task.WhenAll(Enumerable.Range(0, 8)
        .Select(_ => Task.Run(provider.GetRequiredService<IDataClient>, token)));
    var data = clients[0];
    if (created != 1 || clients.Any(client => !ReferenceEquals(client, data)))
        throw new InvalidOperationException("Concurrent resolutions did not share one client");

    await data.RunAsync(DataJob.Define("create-finance").ExecuteSql("finance",
        "CREATE TABLE paid_orders (id INTEGER NOT NULL PRIMARY KEY, label TEXT, amount TEXT NOT NULL)"), token);
    await data.RunAsync(DataJob.Define("create-mq-outbox").ExecuteSql("finance",
        "CREATE TABLE nuget_mq_outbox (id INTEGER NOT NULL PRIMARY KEY, label TEXT)"), token);
    await data.RunAsync(DataJob.Define("seed-mq-outbox").ExecuteSql("finance",
        "INSERT INTO nuget_mq_outbox (id, label) VALUES (1, 'broker-record'), (2, NULL)"), token);
    await data.RunAsync(DataJob.Define("create-mq-inbox").ExecuteSql("finance",
        "CREATE TABLE nuget_mq_inbox (id INTEGER NOT NULL PRIMARY KEY, label TEXT)"), token);

    try
    {
        _ = DataJob.Define("invalid-rule").FromHttp<Order>("sales", "/orders")
            .Where(row => string.Equals(row.Label, "blocked", StringComparison.OrdinalIgnoreCase));
        throw new InvalidOperationException("The authored CLR method was accepted");
    }
    catch (ArgumentException error) when (error.Message.Contains("invalid-rule", StringComparison.Ordinal) &&
                                         error.Message.Contains("System.String.Equals", StringComparison.Ordinal)) { }

    var sync = DataJob.Define<SyncOrders>("orders.sync")
        .WithOptions(new(BatchRows: 4, EdgeCapacity: 2))
        .FromHttp<Order>("sales", "/orders")
        .Where((row, input) => row.IsPaid && row.Id >= input.FromId)
        .Select(row => new StoredPaidOrder
        {
            Id = row.Id, Label = row.Label,
            Amount = row.Amount.ToString(System.Globalization.CultureInfo.InvariantCulture)
        })
        .ToTable("finance", "paid_orders")
        .UpsertBy(row => row.Id);
    string exported = data.Compile(sync).ToJson();
    if (exported.Contains(typeof(Order).Assembly.Location, StringComparison.Ordinal) ||
        exported.Contains("managedAssembly", StringComparison.Ordinal))
        throw new InvalidOperationException("The exported Native rules depend on the authoring DLL");
    var result = await data.RunAsync(sync, new SyncOrders(1), token);
    await server;
    if (result.CommittedRows != 35 || result.RejectedRows != 0 || result.Metrics.Batches < 2)
        throw new InvalidOperationException("The installed Worker did not commit the filtered HTTP rows in batches");

    var read = DataJob.Define("read-paid-orders")
        .FromSql<PaidOrder>("finance", "SELECT id,label,amount FROM paid_orders ORDER BY id", stream: true)
        .Return();
    int verifiedRows = 0;
    await foreach (var row in data.RunAsync(read, token))
    {
        int id = ++verifiedRows;
        if (row.Id != id || row.Label != (id == 2 ? null : $"order-{id}") || row.Amount != id * 1.125m)
            throw new InvalidOperationException($"The typed Native output differs at row {id}");
    }
    using (var verifier = new SqlSugarClient(new ConnectionConfig { DbType = SqlSugar.DbType.Sqlite,
        ConnectionString = "Data Source=" + target + ";Pooling=False", IsAutoCloseConnection = true }))
    {
        var count = Convert.ToInt64(await verifier.Ado.GetScalarAsync("SELECT COUNT(*) FROM paid_orders"));
        if (count != 35 || verifiedRows != count)
            throw new InvalidOperationException("An independent database connection observed a different result");
    }

    var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    var hold = Hold(listener, cancelled, release.Task, token);
    var request = DataJob.Define("cancel-request").FromHttp<Order>("sales", "/hold").Return();
    using var requestCancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
    var pending = data.RunAsync(request, requestCancellation.Token).ToListAsync(requestCancellation.Token).AsTask();
    var query = DataJob.Define("read-page").QueryTable<PaidOrder>("finance", "paid_orders")
        .OrderByDescending(row => row.Id).Skip(1).Take(2)
        .Select(row => new ProbeRow { Value = row.Id }).Return();
    List<ProbeRow> page;
    try
    {
        await cancelled.Task.WaitAsync(token);
        var activePage = data.RunAsync(query, token).ToListAsync(token).AsTask();
        requestCancellation.Cancel();
        try
        {
            await pending;
            throw new InvalidOperationException("The cancelled request returned rows");
        }
        catch (OperationCanceledException) when (requestCancellation.IsCancellationRequested) { }
        page = await activePage;
    }
    finally
    {
        release.TrySetResult();
        await hold;
    }
    if (!page.Select(row => row.Value).SequenceEqual(new long[] { 34, 33 }))
        throw new InvalidOperationException("A separate request changed after cancellation");

    var verified = new HashSet<string>(StringComparer.Ordinal) { "sqlite" };
    if (sqlProbes.Count != 0)
    {
        await data.RunAsync(DataJob.Define("create-probe").ExecuteSql("finance",
            "CREATE TABLE fixture_probe (fixture_value INTEGER NOT NULL)"), token);
        await VerifySql(data, sqlProbes, target, verified, token);
    }
    var mq = await MessageBrokerAcceptance.RunAsync(data, activeBrokerKinds, brokerTargets, brokerGroups,
        target, token);
    foreach (var broker in brokers.Keys)
        verified.Add(activeBrokerKinds[broker] == DataSourceKind.ActiveMq ? "activemq" :
            activeBrokerKinds[broker] switch
            {
                DataSourceKind.Kafka => "kafka",
                DataSourceKind.RabbitMq => "rabbitmq",
                DataSourceKind.RocketMq => "rocketmq",
                _ => throw new InvalidOperationException($"Unsupported MQ source {broker}")
            });
    var brokerFacts = mq.ToDictionary(pair => pair.Key, pair => new
    {
        publishedCommitted = pair.Value.PublishedCommitted,
        consumedCommitted = pair.Value.ConsumedCommitted,
        consumedAcknowledged = pair.Value.ConsumedAcknowledged
    }, StringComparer.Ordinal);
    using var worker = await CompletedWorker(data, query, token);
    await provider.DisposeAsync();
    await worker.WaitForExitAsync(token);
    Console.WriteLine(JsonSerializer.Serialize(new { version = ProductInfo.Sdk, framework = Environment.Version.ToString(),
        rid, http = 200, sqliteRows = verifiedRows, pageRows = page.Count, singleton = true,
        isolatedCancellation = true, workerExited = worker.HasExited, drivers = verified.Order().ToArray(),
        brokers = brokers.Keys.Order().ToArray(), mq = brokerFacts }));
}
finally { Directory.Delete(root, true); }

static T Require<T>(Result<T> result) => result.IsSuccess ? result.Value! :
    throw new InvalidOperationException($"{result.Code} ({result.Error?.Cause})");

static async Task VerifySql(IDataClient data, Dictionary<string, string> probes, string target,
    HashSet<string> verified, CancellationToken token)
{
    var schema = JsonSerializer.SerializeToElement(new
    {
        version = ProductInfo.Protocol,
        fields = new[] { new { name = "fixture_value", type = "int64", nullable = false } }
    });
    var read = DataJob.Define("probe-result")
        .FromSql<ProbeRow>("finance", "SELECT fixture_value FROM fixture_probe", stream: true).Return();
    foreach (var (driver, sql) in probes)
    {
        var probe = Require(Package.FromJson(JsonSerializer.Serialize(new
        {
            version = ProductInfo.Protocol,
            id = "probe-" + driver,
            connections = new[]
            {
                new { id = "source", driver = "ud:" + driver, resource = driver, credentialReference = driver },
                new { id = "finance", driver = "ud:sqlite", resource = target, credentialReference = "finance" }
            },
            controlFlow = new { tasks = new[] { new { id = "run", kind = "dataFlow", config = new { pipeline = "main" } } }, constraints = Array.Empty<object>() },
            dataFlows = new[] { new
            {
                id = "main",
                components = new object[]
                {
                    new { id = "read", component = "sql.source", config = new { connection = "source", sql, readMode = "stream", schema, batchSize = 4096 } },
                    new { id = "write", component = "sql.destination", config = new { connection = "finance", table = "fixture_probe", mode = "append" } }
                },
                paths = new[] { new { from = "read", output = 0, to = "write", input = 0 } }
            } }
        })));
        if ((await data.RunAsync(probe, cancellationToken: token)).CommittedRows != 1)
            throw new InvalidOperationException($"The installed {driver} connector returned no row");
        int rows = 0;
        await foreach (var row in data.RunAsync(read, token))
        {
            if (++rows > 1 || row.Value != 1)
                throw new InvalidOperationException($"Unexpected scalar from the installed {driver} connector");
        }
        if (rows != 1)
            throw new InvalidOperationException($"No scalar from the installed {driver} connector");
        await data.RunAsync(DataJob.Define("clear-probe").ExecuteSql("finance", "DELETE FROM fixture_probe"), token);
        verified.Add(driver);
    }
}

static async Task LoadCompleteFixtures(Dictionary<string, string> probes, Dictionary<string, string> brokers,
    Dictionary<string, string> targets, Dictionary<string, string> groups,
    Dictionary<string, string> credentials, Dictionary<string, string[]> grants, HashSet<string> endpoints,
    string root, CancellationToken token)
{
    probes.Add("postgresql", "SELECT CAST(1 AS BIGINT) AS fixture_value");
    probes.Add("mysql", "SELECT CAST(1 AS SIGNED) AS fixture_value");
    probes.Add("mariadb", "SELECT CAST(1 AS SIGNED) AS fixture_value");
    probes.Add("odbc", "SELECT CAST(1 AS BIGINT) AS fixture_value");
    probes.Add("oracle_oci", "SELECT CAST(1 AS NUMBER(18)) AS \"fixture_value\" FROM dual");
    brokers.Add("kafka", "kafka");
    brokers.Add("rabbitmq", "rabbitmq");
    brokers.Add("rocketmq", "rocketmq");
    brokers.Add("activemq-classic", "activemq");
    brokers.Add("activemq-artemis", "activemq");
    grants["mq-fixture-resource"] = ["Open", "Read", "Discover", "Write", "Network"];
    foreach (var driver in probes.Keys.Concat(brokers.Keys))
    {
        string variable = $"UD_TEST_{driver.ToUpperInvariant().Replace('-', '_')}_CONFIG";
        string file = Environment.GetEnvironmentVariable(variable)
            ?? throw new InvalidOperationException($"The isolated {driver} fixture is unavailable");
        string value = (await File.ReadAllTextAsync(file, token)).Trim();
        if (brokers.ContainsKey(driver))
        {
            using var document = JsonDocument.Parse(value);
            string configuredBroker = document.RootElement.GetProperty("broker").GetString()!;
            if (!string.Equals(configuredBroker, driver, StringComparison.Ordinal))
                throw new InvalidOperationException($"The {driver} fixture identifies itself as {configuredBroker}");
            foreach (var endpoint in document.RootElement.GetProperty("endpoints").EnumerateArray())
            {
                string address = endpoint.GetString()!;
                endpoints.Add(address);
                grants[address] = ["Network"];
            }
            targets[driver] = document.RootElement.GetProperty("jsonTarget").GetString()!;
            bool queue = driver is "rabbitmq" or "activemq-classic" or "activemq-artemis";
            string addressKind = queue ? "queue" : "topic";
            grants[$"mq-fixture-resource/{addressKind}/{targets[driver]}"] = ["Read", "Write"];
            if (document.RootElement.TryGetProperty("group", out var group))
            {
                if (queue)
                    throw new InvalidOperationException($"The queue fixture {driver} must omit group");
                groups[driver] = group.GetString()!;
                grants[$"mq-fixture-resource/group/{groups[driver]}"] = ["Write"];
            }
            else if (driver is "kafka" or "rocketmq")
                throw new InvalidOperationException($"The {driver} fixture did not provide a consumer group");
            value = document.RootElement.GetProperty("credentials").GetRawText();
        }
        credentials[driver] = Path.Combine(root, driver + ".conf");
        await File.WriteAllTextAsync(credentials[driver], value, token);
        grants[driver] = ["Open", "Read", "Discover", "Network"];
    }
}

static async Task Serve(TcpListener listener, CancellationToken cancellation)
{
    using var client = await listener.AcceptTcpClientAsync(cancellation);
    using var stream = client.GetStream();
    using var reader = new StreamReader(stream, leaveOpen: true);
    if (await reader.ReadLineAsync(cancellation) != "GET /orders HTTP/1.1")
        throw new InvalidOperationException("Unexpected request from the installed Native Worker");
    while (await reader.ReadLineAsync(cancellation) is { Length: > 0 }) { }
    var body = JsonSerializer.SerializeToUtf8Bytes(Enumerable.Range(1, 36).Select(id => new
    {
        id, label = id == 2 ? null : $"order-{id}", amount = (id * 1.125m).ToString(System.Globalization.CultureInfo.InvariantCulture),
        is_paid = id < 36
    }));
    await stream.WriteAsync(Encoding.ASCII.GetBytes(
        $"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {body.Length}\r\nConnection: close\r\n\r\n"), cancellation);
    await stream.WriteAsync(body, cancellation);
}

static async Task Hold(TcpListener listener, TaskCompletionSource started, Task release,
    CancellationToken cancellation)
{
    using var client = await listener.AcceptTcpClientAsync(cancellation);
    using var reader = new StreamReader(client.GetStream());
    if (await reader.ReadLineAsync(cancellation) != "GET /hold HTTP/1.1")
        throw new InvalidOperationException("The held request did not use the installed HTTP driver");
    while (await reader.ReadLineAsync(cancellation) is { Length: > 0 }) { }
    started.SetResult();
    await release.WaitAsync(cancellation);
}

static async Task<Process> CompletedWorker(IDataClient data, DataJobQuery<object, ProbeRow> query,
    CancellationToken cancellation)
{
    await using var run = await data.StartAsync(query, cancellation);
    await foreach (var row in run.ReadRowsAsync<ProbeRow>(cancellation)) { }
    return Process.GetProcessById(run.WorkerProcessId);
}
