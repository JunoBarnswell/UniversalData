using System.Globalization;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using UniversalData;

namespace UniversalData.Dotnet10Hosting;

public static class GenericHostCommand
{
    public static bool IsRequested(string[] args) => args.Any(argument =>
        argument is "--generic-host" or "--command" || argument.StartsWith("--command=", StringComparison.Ordinal));

    public static async Task<int> RunAsync(string[] args, CancellationToken cancellationToken = default)
    {
        string command = Option(args, "--command") ?? "list";
        string configPath = Option(args, "--config") ??
            Environment.GetEnvironmentVariable("UD_UNIVERSALDATA_CONFIG") ?? ".local/sqlite/universaldata.yaml";
        configPath = Path.GetFullPath(configPath);

        try
        {
            var builder = Host.CreateApplicationBuilder([]);
            // Generic Host intentionally uses the direct path overload. ASP.NET Core uses the
            // configuration-section overload in Program.cs below.
            if (builder.Configuration["Orders:DefaultSource"] is null)
                builder.Configuration["Orders:DefaultSource"] =
                    configPath.Contains("sqlite", StringComparison.OrdinalIgnoreCase)
                        ? "orders-sqlite" : "orders-primary";
            builder.Services.AddUniversalData(configPath);
            builder.Services.AddSingleton<OrdersService>();
            using IHost host = builder.Build();
            await host.StartAsync(cancellationToken);
            var service = host.Services.GetRequiredService<OrdersService>();

            switch (command.ToLowerInvariant())
            {
                case "init":
                {
                    string source = Option(args, "--source") ?? "orders-sqlite";
                    var schema = DataJob.Define("orders.initialize")
                        .ExecuteSql(source, "CREATE TABLE IF NOT EXISTS orders (" +
                            "id INTEGER NOT NULL PRIMARY KEY, shop_id INTEGER NOT NULL, " +
                            "sequence INTEGER NOT NULL, total_cents BIGINT NOT NULL)")
                        .ExecuteSql(source, "CREATE INDEX IF NOT EXISTS orders_shop_sequence_id_idx " +
                            "ON orders (shop_id, sequence DESC, id ASC)");
                    await host.Services.GetRequiredService<IDataClient>().RunAsync(schema, cancellationToken);
                    break;
                }
                case "list":
                case "verify":
                {
                    var filter = new OrderFilter
                    {
                        ShopId = IntegerOption(args, "--shop-id", 1),
                        Offset = IntegerOption(args, "--offset", 0),
                        PageSize = IntegerOption(args, "--page-size", 20)
                    };
                    var rows = await service.ListAsync(filter, Option(args, "--source"), cancellationToken);
                    Console.WriteLine(JsonSerializer.Serialize(rows));
                    break;
                }
                case "write":
                {
                    var request = new OrderWriteRequest
                    {
                        Id = RequiredInteger(args, "--id"),
                        ShopId = IntegerOption(args, "--shop-id", 1),
                        Sequence = RequiredInteger(args, "--sequence"),
                        Total = RequiredDecimal(args, "--total")
                    };
                    var row = await service.InsertAndReadAsync(request, Option(args, "--source"), cancellationToken);
                    Console.WriteLine(JsonSerializer.Serialize(row));
                    break;
                }
                default:
                    throw new ArgumentException("Command must be init, list, verify, or write.", nameof(args));
            }

            await host.StopAsync(cancellationToken);
            return 0;
        }
        catch (Exception error) when (error is ArgumentException or InvalidOperationException or
                                       UniversalDataException)
        {
            Console.Error.WriteLine(error.Message);
            return 1;
        }
    }

    private static string? Option(string[] args, string name)
    {
        for (int index = 0; index < args.Length; index++)
        {
            if (args[index].Equals(name, StringComparison.OrdinalIgnoreCase))
                return index + 1 < args.Length ? args[index + 1] : throw new ArgumentException($"{name} requires a value.");
            if (args[index].StartsWith(name + "=", StringComparison.OrdinalIgnoreCase))
                return args[index][(name.Length + 1)..];
        }
        return null;
    }

    private static int IntegerOption(string[] args, string name, int fallback)
    {
        string? value = Option(args, name);
        return value is null ? fallback : int.Parse(value, CultureInfo.InvariantCulture);
    }

    private static int RequiredInteger(string[] args, string name) =>
        int.Parse(Option(args, name) ?? throw new ArgumentException($"{name} is required."),
            CultureInfo.InvariantCulture);

    private static decimal RequiredDecimal(string[] args, string name) =>
        decimal.Parse(Option(args, name) ?? throw new ArgumentException($"{name} is required."),
            CultureInfo.InvariantCulture);
}
