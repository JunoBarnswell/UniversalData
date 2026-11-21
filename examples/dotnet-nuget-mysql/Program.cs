using UniversalData;

var runEtl = args is ["--etl"];
if (args.Length != 0 && !runEtl)
{
    Console.Error.WriteLine("Usage: UniversalData.MySqlDemo [--etl]");
    return 2;
}

var connectionVariable = runEtl ? "UD_ETL_CONNECTION_STRING" : "UD_MYSQL_CONNECTION_STRING";
var connection = Environment.GetEnvironmentVariable(connectionVariable);
var policy = Environment.GetEnvironmentVariable("UD_POLICY_FILE");
if (string.IsNullOrWhiteSpace(connection) || string.IsNullOrWhiteSpace(policy))
{
    Console.Error.WriteLine($"Set {connectionVariable} and UD_POLICY_FILE before running the example.");
    return 2;
}

try
{
    await using var client = DataClient.CreateBuilder(policy, "demo")
        .AddMariaDb("demo", connection)
        .ConfigureWorkers(workers => workers.MaxConcurrentSessions(2).MaxBranchesPerSession(2))
        .Build();

    if (runEtl)
    {
        await MariaDbEtlDemo.RunAsync(client);
        return 0;
    }
    var health = await MariaDbEtlDemo.CheckAsync(client);
    if (health.Ping != 1 || string.IsNullOrWhiteSpace(health.Version))
        throw new InvalidOperationException("The MariaDB health response was invalid.");
    Console.WriteLine($"MariaDB {health.Version} returned {health.Ping}.");
    return 0;
}
catch (Exception error) when (error is UniversalDataException or InvalidOperationException)
{
    Console.Error.WriteLine(error.Message);
    return 1;
}
