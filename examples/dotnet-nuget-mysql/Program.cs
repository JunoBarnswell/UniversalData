using SqlSugar;
using UniversalData;

const string connectionVariable = "UD_MYSQL_CONNECTION_STRING";
var connectionString = Environment.GetEnvironmentVariable(connectionVariable);
if (string.IsNullOrWhiteSpace(connectionString))
{
    Console.Error.WriteLine($"Set {connectionVariable} to a local MySQL connection string before running the demo.");
    return 2;
}

var opened = await SqlSugarClient.OpenAsync(new ConnectionConfig
{
    DbType = DbType.MariaDb,
    ConnectionString = connectionString
});
if (!opened.IsSuccess || opened.Value is null)
{
    Console.Error.WriteLine($"MySQL connection failed ({opened.Code}).");
    return 1;
}

using var client = opened.Value;
var ping = client.Ado.GetScalar("SELECT 1");
if (!ping.IsSuccess || ping.Value is null || Convert.ToInt64(ping.Value) != 1)
{
    Console.Error.WriteLine($"MySQL health query failed ({ping.Code}).");
    return 1;
}

var version = client.Ado.GetScalar("SELECT VERSION()");
if (!version.IsSuccess || string.IsNullOrWhiteSpace(version.Value?.ToString()))
{
    Console.Error.WriteLine($"MySQL version query failed ({version.Code}).");
    return 1;
}

Console.WriteLine("UniversalData NuGet MySQL demo passed.");
Console.WriteLine($"SELECT 1 => {ping.Value}");
Console.WriteLine($"Server version => {version.Value}");
return 0;
