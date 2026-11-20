using UniversalData;
using SqlSugar;
using System.Text.Json;

if (args.Length == 4 && args[0] is "--catalog" or "--explain" or "--execute" or "--instantiate")
{
    var package = Require(Package.FromJson(await File.ReadAllTextAsync(args[1])));
    var client = new EtlClient(new(Path.GetFullPath(args[3]), "fixture"));
    if (args[0] == "--execute")
    {
        await using var run = Require(await client.StartAsync(package));
        _ = Require(await run.WaitAsync());
        await File.WriteAllTextAsync(args[2], Require(await run.DiagnosticsAsync()).GetRawText());
    }
    else
    {
        var result = args[0] switch
        {
            "--catalog" => await client.ComponentCatalogAsync(package),
            "--instantiate" => await client.InstantiateAsync(package),
            _ => await client.ExplainAsync(package)
        };
        await File.WriteAllTextAsync(args[2], Require(result).GetRawText());
    }
    return;
}
if (args.Length == 5 && args[0] == "--compare")
{
    File.WriteAllText(args[4], Schema.Compare(File.ReadAllText(args[1]), File.ReadAllText(args[2]), File.ReadAllText(args[3])));
    return;
}
if (args.Length == 3 && args[0] == "--row-info")
{
    using var restored = Batch.FromWire(File.ReadAllBytes(args[1]));
    File.WriteAllText(args[2], restored.RowInfoJson(0));
    return;
}
if (args.Length == 6 && args[0] == "--discover")
{
    var package = Require(Package.FromJson(await File.ReadAllTextAsync(args[1])));
    var client = new EtlClient(new(Path.GetFullPath(args[5]), "fixture"));
    await File.WriteAllTextAsync(args[4], Require(await client.DiscoverSchemaAsync(package, args[2], args[3])).GetRawText());
    return;
}
if (args.Length == 4 && args[0] == "--sql-etl")
{
    using var source = Require(await SqlSugarClient.OpenAsync(ReadSqlConfig(args[1])));
    using var target = Require(await SqlSugarClient.OpenAsync(ReadSqlConfig(args[2])));
    await using var cursor = Require(await source.Queryable<ExampleOrder>().OrderBy(order => order.Id).OpenCursorAsync(512));
    var written = 0;
    Check(target.Ado.BeginTran());
    try
    {
        while (true)
        {
            var batch = await cursor.ReadBatchAsync();
            if (batch.Code == StatusCode.End)
                break;
            var transformed = Require(batch).Select(order => new ExampleOrder
                { Id = order.Id, Name = order.Name.Trim(), Amount = order.Amount });
            written += Require(target.Fastest<ExampleOrder>().BulkCopy(transformed));
        }
        Check(target.Ado.CommitTran());
    }
    catch
    {
        _ = target.Ado.RollbackTran();
        throw;
    }
    await File.WriteAllTextAsync(args[3], JsonSerializer.Serialize(new { rows = written, mode = "native-typed-stream" }));
    return;
}
if (args.Length != 4) throw new ArgumentException("schema.json data.json output.json output.wire");
using var batch = new Batch(File.ReadAllText(args[0]), File.ReadAllText(args[1]));
File.WriteAllText(args[2], batch.ToJson()); File.WriteAllBytes(args[3], batch.ToWire());

static void Check(Result result)
{
    if (!result.IsSuccess)
        throw new InvalidOperationException(result.Error?.Message ?? result.Code.ToString());
}

static T Require<T>(Result<T> result)
{
    Check(new Result(result.Code, result.Error));
    return result.Value!;
}

static ConnectionConfig ReadSqlConfig(string path)
{
    using var document = JsonDocument.Parse(File.ReadAllText(path));
    var root = document.RootElement;
    if (!root.TryGetProperty("dbType", out var dbType) || !Enum.TryParse<DbType>(dbType.GetString(), true, out var dialect))
        throw new ArgumentException("SQL config needs a dbType string.");
    return new ConnectionConfig
    {
        DbType = dialect,
        ConnectionString = root.GetProperty("connectionString").GetString() ?? throw new ArgumentException("SQL config needs connectionString."),
        PluginPath = root.GetProperty("pluginPath").GetString() ?? throw new ArgumentException("SQL config needs pluginPath.")
    };
}
