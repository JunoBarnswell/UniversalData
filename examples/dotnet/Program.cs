using System.Text.Json;
using UniversalData;

if (args.Length != 4)
    throw new ArgumentException("package.json parameters.json execution-id authorized-root");

string state = Directory.CreateTempSubdirectory("universaldata-consumer-").FullName;
try
{
    var policy = Path.Combine(state, "policy.json");
    await File.WriteAllTextAsync(policy, JsonSerializer.Serialize(new
    {
        dataDirectory = Path.Combine(state, "runtime"),
        fileRoots = new[] { Path.GetFullPath(args[3]) },
        networkOrigins = Array.Empty<string>(),
        pluginRoots = new[] { AppContext.BaseDirectory },
        credentialFiles = new Dictionary<string, string>()
    }));
    var package = Package.FromJson(await File.ReadAllTextAsync(args[0]));
    if (!package.IsSuccess)
        return Failure(package.Code, package.Error);
    var started = await new EtlClient(new(policy, "fixture")).StartNamedAsync(package.Value!,
        await File.ReadAllTextAsync(args[1]), args[2]);
    if (!started.IsSuccess)
        return Failure(started.Code, started.Error);
    await using var run = started.Value!;
    var result = await run.WaitAsync();
    if (!result.IsSuccess)
        return Failure(result.Code, result.Error);
    Console.WriteLine(JsonSerializer.Serialize(result.Value, new JsonSerializerOptions(JsonSerializerDefaults.Web)));
    return 0;
}
finally { Directory.Delete(state, recursive: true); }

static int Failure(StatusCode code, OperationError? error)
{
    Console.Error.WriteLine(JsonSerializer.Serialize(new Result(code, error),
        new JsonSerializerOptions(JsonSerializerDefaults.Web)));
    return (int)code;
}
