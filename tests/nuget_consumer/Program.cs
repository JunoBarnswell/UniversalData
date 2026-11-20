using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using SqlSugar;
using UniversalData;

string root = Directory.CreateTempSubdirectory("ud-nuget-").FullName;
try
{
    string resources = Directory.CreateDirectory(Path.Combine(root, "resources")).FullName;
    string source = Path.Combine(resources, "source.db"), target = Path.Combine(resources, "target.db");
    using var listener = new TcpListener(IPAddress.Loopback, 0);
    listener.Start();
    using var httpDeadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
    var request = Serve(listener, httpDeadline.Token);
    var origin = $"http://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}";
    var rid = OperatingSystem.IsWindows() ? "win-x64" : "linux-x64";
    var runtime = Path.Combine(AppContext.BaseDirectory, "runtimes", rid, "worker");
    var plugin = Plugin("sqlite");
    var credentials = new Dictionary<string, string> { ["source"] = Path.Combine(root, "source.conf"), ["target"] = Path.Combine(root, "target.conf") };
    var probes = args is ["--complete"] ? new Dictionary<string, string>
    {
        ["postgresql"] = "SELECT CAST(1 AS BIGINT) AS fixture_value",
        ["mysql"] = "SELECT CAST(1 AS SIGNED) AS fixture_value",
        ["mariadb"] = "SELECT CAST(1 AS SIGNED) AS fixture_value",
        ["odbc"] = "SELECT CAST(1 AS BIGINT) AS fixture_value",
        ["oracle_oci"] = "SELECT CAST(1 AS NUMBER(18)) AS \"fixture_value\" FROM dual"
    } : [];
    var grants = new Dictionary<string, string[]>();
    foreach (var driver in probes.Keys)
    {
        var environmentKey = $"UD_TEST_{driver.ToUpperInvariant()}_CONFIG";
        var fixture = Environment.GetEnvironmentVariable(environmentKey)
            ?? throw new InvalidOperationException($"The isolated {driver} fixture is unavailable");
        credentials[driver] = Path.Combine(root, driver + ".conf");
        await File.WriteAllTextAsync(credentials[driver], (await File.ReadAllTextAsync(fixture)).Trim());
        grants[driver] = ["Open", "Read", "Discover"];
    }
    await File.WriteAllTextAsync(credentials["source"], source);
    await File.WriteAllTextAsync(credentials["target"], target);
    var policy = Path.Combine(root, "policy.json");
    await File.WriteAllTextAsync(policy, JsonSerializer.Serialize(new
    {
        dataDirectory = Path.Combine(root, "state"), fileRoots = new[] { resources },
        networkOrigins = new[] { origin }, pluginRoots = new[] { runtime, AppContext.BaseDirectory }, credentialFiles = credentials,
        resources = grants
    }));
    if (typeof(ISqlSugarClient).Assembly.GetName().Name != "SqlSugar")
        throw new InvalidOperationException("NuGet must restore the official SqlSugar assembly");
    await using var data = DataClient.CreateBuilder(policy, "local")
        .AddSqlite("source", "Data Source=" + source)
        .AddSqlite("target", "Data Source=" + target)
        .AddModule(typeof(InstalledFixture).Assembly)
        .Build();
    await data.RunAsync(InstalledFixture.Prepare, httpDeadline.Token);
    string Plugin(string name) => Path.Combine(runtime,
        "ud_connector_" + name + (OperatingSystem.IsWindows() ? ".dll" : ".so"));
    var schema = JsonSerializer.SerializeToElement(new
    {
        version = ProductInfo.Protocol,
        fields = new[] { new { name = "id", type = "int64", nullable = false }, new { name = "label", type = "utf8", nullable = false } }
    });
    var package = Require(new EtlPackageBuilder("installed-sdk")
        .Connection("web", "http", origin + "/acceptance")
        .Connection("source", plugin, source, "source")
        .Connection("target", plugin, target, "target")
        .HttpTask("http", new("web", ExpectedStatus: 200))
        .SqlSource("read", "source", "SELECT id,label FROM fixture ORDER BY id", schema, stream: true, batchSize: 4)
        .SqlDestination("write", "target", "fixture")
        .Connect("read", "write").DataFlow().Before("http", "run").Build());
    var client = new EtlClient(data);
    await using (var run = await client.StartAsync(package))
    {
        if (run.WorkerProcessId == Environment.ProcessId)
            throw new InvalidOperationException("The SDK execution did not start a separate process");
        if ((await run.WaitAsync(httpDeadline.Token)).CommittedRows != 35)
            throw new InvalidOperationException("The installed ETL did not commit all 35 rows");
    }
    await request;
    await data.RunAsync(InstalledFixture.Verify, httpDeadline.Token);
    var verified = new List<string> { "sqlite" };
    var probeSchema = JsonSerializer.SerializeToElement(new
    {
        version = ProductInfo.Protocol,
        fields = new[] { new { name = "fixture_value", type = "int64", nullable = false } }
    });
    foreach (var (driver, sql) in probes)
    {
        var probe = Require(new EtlPackageBuilder("installed-" + driver)
            .Connection("probe", Plugin(driver), driver, driver)
            .Connection("target", plugin, target, "target")
            .SqlSource("read", "probe", sql, probeSchema, stream: true)
            .SqlDestination("write", "target", "fixture_probe")
            .Connect("read", "write").DataFlow().Build());
        await using (var run = await client.StartAsync(probe))
        {
            if ((await run.WaitAsync()).CommittedRows != 1)
                throw new InvalidOperationException($"The installed {driver} connector did not return a row");
        }
        if (await data.RunAsync(InstalledFixture.TakeProbe) != 1L)
            throw new InvalidOperationException($"Unexpected scalar from the installed {driver} driver");
        verified.Add(driver);
    }
    Console.WriteLine(JsonSerializer.Serialize(new { version = ProductInfo.Sdk, framework = Environment.Version.ToString(),
        rid, http = 200, sqliteRows = 35, drivers = verified }));
}
finally { Directory.Delete(root, true); }

static T Require<T>(Result<T> result) => result.IsSuccess ? result.Value! :
    throw new InvalidOperationException($"{result.Code} ({result.Error?.Cause})");

static async Task Serve(TcpListener listener, CancellationToken cancellation)
{
    using var client = await listener.AcceptTcpClientAsync(cancellation);
    using var stream = client.GetStream();
    using var reader = new StreamReader(stream, leaveOpen: true);
    if (await reader.ReadLineAsync(cancellation) != "GET /acceptance HTTP/1.1")
        throw new InvalidOperationException("Unexpected request from the installed Worker");
    while (await reader.ReadLineAsync(cancellation) is { Length: > 0 }) { }
    await stream.WriteAsync(Encoding.ASCII.GetBytes("HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"), cancellation);
}

[SugarTable("fixture")]
public sealed class FixtureRow
{
    [SugarColumn(ColumnName = "id", IsPrimaryKey = true)]
    public int Id { get; set; }
    [SugarColumn(ColumnName = "label")]
    public string Label { get; set; } = "";
}


public static class InstalledFixture
{
    public static async Task Prepare(DataContext context, CancellationToken cancellation)
    {
        const string table = "CREATE TABLE fixture (id INTEGER NOT NULL PRIMARY KEY, label TEXT NOT NULL)";
        await context.Database("source").UseAsync(async (db, token) =>
        {
            token.ThrowIfCancellationRequested();
            await db.Ado.ExecuteCommandAsync(table);
            var rows = Enumerable.Range(1, 35).Select(id => new FixtureRow { Id = id, Label = $"row-{id}" }).ToArray();
            if (await db.Insertable(rows).ExecuteCommandAsync() != rows.Length)
                throw new InvalidOperationException("The installed SqlSugar provider did not consume the entire input");
            return true;
        }, cancellation);
        await context.Database("target").UseAsync(async (db, token) =>
        {
            token.ThrowIfCancellationRequested();
            await db.Ado.ExecuteCommandAsync(table);
            return await db.Ado.ExecuteCommandAsync("CREATE TABLE fixture_probe (fixture_value INTEGER NOT NULL)");
        }, cancellation);
    }

    public static Task Verify(DataContext context, CancellationToken cancellation) =>
        context.Database("target").UseAsync(async (db, token) =>
        {
            token.ThrowIfCancellationRequested();
            var rows = await db.Queryable<FixtureRow>().OrderBy(row => row.Id).ToListAsync();
            if (rows.Count != 35 || rows.Where((row, i) => row.Id != i + 1 || row.Label != $"row-{i + 1}").Any())
                throw new InvalidOperationException("Installed ETL output differs from the source");
            return true;
        }, cancellation);

    public static Task<long> TakeProbe(DataContext context, CancellationToken cancellation) =>
        context.Database("target").UseAsync(async (db, token) =>
        {
            token.ThrowIfCancellationRequested();
            var value = Convert.ToInt64(await db.Ado.GetScalarAsync("SELECT fixture_value FROM fixture_probe"));
            await db.Ado.ExecuteCommandAsync("DELETE FROM fixture_probe");
            return value;
        }, cancellation);
}
