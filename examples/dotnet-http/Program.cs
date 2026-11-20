using System.Text.Json;
using UniversalData;

if (args.Length != 4)
    throw new ArgumentException("package.json exact-url ca-file permit|deny|resolver-deny");

string state = Directory.CreateTempSubdirectory("universaldata-http-").FullName;
try
{
    string policy = Path.Combine(state, "policy.json");
    string privateHeaders = Path.Combine(state, "header.json");
    await File.WriteAllTextAsync(privateHeaders, "{\"headers\":{\"Authorization\":\"Bearer fixture-token\"}}");
    var uri = new Uri(args[1]);
    await File.WriteAllTextAsync(policy, JsonSerializer.Serialize(new
    {
        dataDirectory = Path.Combine(state, "runtime"),
        fileRoots = new[] { Path.GetDirectoryName(Path.GetFullPath(args[2])) },
        networkOrigins = args[3] == "deny" ? Array.Empty<string>() : new[] { uri.GetLeftPart(UriPartial.Authority) },
        pluginRoots = Array.Empty<string>(),
        credentialFiles = args[3] == "resolver-deny" ? new Dictionary<string, string>()
            : new Dictionary<string, string> { ["token"] = privateHeaders }
    }));
    var package = Package.FromJson(await File.ReadAllTextAsync(args[0]));
    if (!package.IsSuccess)
        return (int)package.Code;
    var start = await new EtlClient(new(policy, "fixture")).StartAsync(package.Value!);
    if (!start.IsSuccess)
    {
        Console.WriteLine(JsonSerializer.Serialize(new { result = (int)start.Code, error = start.Error }));
        return 0;
    }
    await using var run = start.Value!;
    var finished = await run.WaitAsync();
    Console.WriteLine(JsonSerializer.Serialize(new { result = (int)finished.Code, error = finished.Error,
        state = finished.Value?.State, executedTasks = finished.Value?.ExecutedTasks }));
    return 0;
}
finally { Directory.Delete(state, recursive: true); }
