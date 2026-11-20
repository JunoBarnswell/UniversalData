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
    await using var data = DataClient.CreateBuilder(policy, "fixture").Build();
    await using var run = await new EtlClient(data).StartNamedAsync(package.Value!,
        await File.ReadAllTextAsync(args[1]), args[2]);
    var result = await run.WaitAsync();
    Console.WriteLine(JsonSerializer.Serialize(result, new JsonSerializerOptions(JsonSerializerDefaults.Web)));
    return 0;
}
catch (UniversalDataException error) { return Failure(error.Code, error.Detail); }
finally { Directory.Delete(state, recursive: true); }

static int Failure(StatusCode code, OperationError? error)
{
    Console.Error.WriteLine(JsonSerializer.Serialize(new Result(code, error),
        new JsonSerializerOptions(JsonSerializerDefaults.Web)));
    return (int)code;
}
