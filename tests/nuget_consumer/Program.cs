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
    using (var input = Require(await SqlSugarClient.OpenAsync(Sqlite(source))))
    using (var output = Require(await SqlSugarClient.OpenAsync(Sqlite(target))))
    {
        const string table = "CREATE TABLE fixture (id INTEGER NOT NULL PRIMARY KEY, label TEXT NOT NULL)";
        Check(input.Ado.ExecuteCommand(table));
        Check(output.Ado.ExecuteCommand(table));
        var rows = Enumerable.Range(1, 35).Select(id => new FixtureRow { Id = id, Label = $"row-{id}" });
        if (Require(await input.Fastest<FixtureRow>().BulkCopyAsync(rows)) != 35)
            throw new InvalidOperationException("Installed SQLite connector did not consume the entire input");
    }

    using var listener = new TcpListener(IPAddress.Loopback, 0);
    listener.Start();
    using var httpDeadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
    var request = Serve(listener, httpDeadline.Token);
    var origin = $"http://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}";
    var rid = OperatingSystem.IsWindows() ? "win-x64" : "linux-x64";
    var runtime = Path.Combine(AppContext.BaseDirectory, "runtimes", rid, "worker");
    var plugin = Path.Combine(runtime, OperatingSystem.IsWindows() ? "ud_connector_sqlite.dll" : "ud_connector_sqlite.so");
    var credentials = new Dictionary<string, string> { ["source"] = Path.Combine(root, "source.conf"), ["target"] = Path.Combine(root, "target.conf") };
    await File.WriteAllTextAsync(credentials["source"], source);
    await File.WriteAllTextAsync(credentials["target"], target);
    var policy = Path.Combine(root, "policy.json");
    await File.WriteAllTextAsync(policy, JsonSerializer.Serialize(new
    {
        dataDirectory = Path.Combine(root, "state"), fileRoots = new[] { resources },
        networkOrigins = new[] { origin }, pluginRoots = new[] { runtime }, credentialFiles = credentials
    }));
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
    var client = new EtlClient(new(policy));
    await using (var run = Require(await client.StartAsync(package)))
    {
        if (run.WorkerProcessId == Environment.ProcessId)
            throw new InvalidOperationException("The SDK execution did not start a separate process");
        if (Require(await run.WaitAsync(httpDeadline.Token)).CommittedRows != 35)
            throw new InvalidOperationException("The installed ETL did not commit all 35 rows");
    }
    await request;
    using (var output = Require(await SqlSugarClient.OpenAsync(Sqlite(target))))
    {
        await using var cursor = Require(await output.Queryable<FixtureRow>().OrderBy(row => row.Id).OpenCursorAsync(4));
        int count = 0;
        while (true)
        {
            var batch = await cursor.ReadBatchAsync();
            if (batch.Code == StatusCode.End) break;
            foreach (var row in Require(batch))
                if (row.Id != ++count || row.Label != $"row-{count}")
                    throw new InvalidOperationException("Installed ETL output differs from the source");
        }
        if (count != 35)
            throw new InvalidOperationException("Installed cursor did not return all 35 rows");
    }
    var verified = new List<string> { "sqlite" };
    if (args is ["--complete"])
    {
        foreach (var (type, resource, sql) in new[]
        {
            (DbType.PostgreSQL, "POSTGRESQL", "SELECT 1"), (DbType.MySql, "MYSQL", "SELECT 1"),
            (DbType.MariaDb, "MARIADB", "SELECT 1"), (DbType.SqlServer, "ODBC", "SELECT 1"),
            (DbType.Oracle, "ORACLE_OCI", "SELECT 1 FROM dual")
        })
        {
            string fixture = Environment.GetEnvironmentVariable($"UD_TEST_{resource}_CONFIG")
                ?? throw new InvalidOperationException($"The isolated {resource} fixture is unavailable");
            using var database = Require(await SqlSugarClient.OpenAsync(new ConnectionConfig
                { DbType = type, ConnectionString = (await File.ReadAllTextAsync(fixture)).Trim() }));
            if (Convert.ToInt32(Require(database.Ado.GetScalar(sql))) != 1)
                throw new InvalidOperationException($"Unexpected scalar from the installed {resource} driver");
            verified.Add(resource.ToLowerInvariant());
        }
    }
    Console.WriteLine(JsonSerializer.Serialize(new { version = ProductInfo.Sdk, framework = Environment.Version.ToString(),
        rid, http = 200, sqliteRows = 35, drivers = verified }));
}
finally { Directory.Delete(root, true); }

static ConnectionConfig Sqlite(string path) => new() { DbType = DbType.Sqlite, ConnectionString = "Data Source=" + path };
static void Check<T>(Result<T> result) => _ = Require(result);
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
internal sealed class FixtureRow
{
    [SugarColumn(ColumnName = "id", IsPrimaryKey = true)]
    public int Id { get; set; }
    [SugarColumn(ColumnName = "label")]
    public string Label { get; set; } = "";
}
