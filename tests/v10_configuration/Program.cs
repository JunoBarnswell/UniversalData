using System.Text.Json;
using UniversalData;

if (args is ["per-call", var configuration, var delta])
{
    await using var client = DataClient.Create(["--config", configuration]);
    var result = await client.RunAsync(V10ConfigurationJobs.Configured,
        new V10RunInput { Delta = int.Parse(delta, System.Globalization.CultureInfo.InvariantCulture) });
    Console.WriteLine(JsonSerializer.Serialize(result));
    return 0;
}

Console.Error.WriteLine("usage: V10ConfigurationJobs per-call CONFIG DELTA");
return 2;
