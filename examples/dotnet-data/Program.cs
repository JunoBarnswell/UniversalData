using UniversalData;
using System.Text.Json;

if (args.Length == 4 && args[0] is "--catalog" or "--explain" or "--execute" or "--instantiate")
{
    var package = Require(Package.FromJson(await File.ReadAllTextAsync(args[1])));
    await using var data = DataClient.CreateBuilder(args[3], "fixture").Build();
    var client = new EtlClient(data);
    if (args[0] == "--execute")
    {
        await using var run = await client.StartAsync(package);
        _ = await run.WaitAsync();
        await File.WriteAllTextAsync(args[2], (await run.DiagnosticsAsync()).GetRawText());
    }
    else
    {
        var result = args[0] switch
        {
            "--catalog" => await client.ComponentCatalogAsync(package),
            "--instantiate" => await client.InstantiateAsync(package),
            _ => await client.ExplainAsync(package)
        };
        await File.WriteAllTextAsync(args[2], result.GetRawText());
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
    await using var data = DataClient.CreateBuilder(args[5], "fixture").Build();
    var client = new EtlClient(data);
    await File.WriteAllTextAsync(args[4], (await client.DiscoverSchemaAsync(package, args[2], args[3])).GetRawText());
    return;
}
if (args.Length == 5 && args[0] == "--sql-etl")
{
    var target = ReadSource("target", args[3]);
    await using var data = DataClient.CreateBuilder(args[1], "fixture")
        .AddSource(ReadSource("source", args[2]))
        .AddSource(target)
        .AddModule(typeof(ExampleOrder).Assembly)
        .Build();
    var source = data.Etl("example-orders").FromTable<ExampleOrder>("source").Stream();
    var pipeline = target.Kind == DataSourceKind.Sqlite
        ? source.Map(ExampleOrderText.Trim).ToTable("target").Append()
        : source.Map(ExampleOrder.Trim).ToTable("target").Append();
    var metrics = await pipeline.RunAsync();
    await File.WriteAllTextAsync(args[4], JsonSerializer.Serialize(new { rows = metrics.CommittedRows, mode = "native-typed-stream" }));
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

static DataSourceDefinition ReadSource(string name, string path)
{
    using var document = JsonDocument.Parse(File.ReadAllText(path));
    var root = document.RootElement;
    if (!root.TryGetProperty("dbType", out var dbType) ||
        !Enum.TryParse<DataSourceKind>(dbType.GetString(), true, out var dialect) || dialect == DataSourceKind.Http)
        throw new ArgumentException("SQL config needs a database dbType string.");
    return new(name, dialect, Required("connectionString"), Optional("resource"),
        Optional("credentialReference"), nativeConnectionString: Optional("nativeConnectionString"));

    string Required(string key) => Optional(key) ?? throw new ArgumentException($"SQL config needs {key}.");
    string? Optional(string key) => root.TryGetProperty(key, out var value) ? value.GetString() : null;
}
