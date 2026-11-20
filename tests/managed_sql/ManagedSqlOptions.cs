using System.Globalization;
using System.Text.Json;
using SqlSugar;

namespace ManagedSqlAcceptance;

internal sealed class ManagedSqlOptions
{
    private const string TableName = "managed_sql_orders";

    internal DbType Provider { get; private init; }
    internal string ConnectionString { get; private init; } = string.Empty;
    internal string? TargetConnectionString { get; private init; }
    internal string PluginPath { get; private init; } = string.Empty;
    internal int RowCount { get; private init; } = 16;
    internal int BatchSize { get; private init; } = 4;
    internal string? BenchmarkPath { get; private init; }
    internal string? SetupSqlPath { get; private init; }

    internal static ManagedSqlOptions Parse(string[] args)
    {
        var fixturePath = RawArg(args, "--fixture");
        using var fixture = fixturePath is null ? null : JsonDocument.Parse(File.ReadAllText(fixturePath));

        string? FixtureValue(string property)
        {
            if (fixture is null || !fixture.RootElement.TryGetProperty(property, out var value) || value.ValueKind != JsonValueKind.String)
                return null;
            var result = value.GetString();
            if (result is null) return null;
            if ((property is "connectionFile" or "plugin" or "setupSql") && !Path.IsPathFullyQualified(result) && fixturePath is not null)
                result = Path.GetFullPath(Path.Combine(Path.GetDirectoryName(Path.GetFullPath(fixturePath))!, result));
            return result;
        }

        string Value(string name)
        {
            var result = OptionalNullable(name);
            if (string.IsNullOrWhiteSpace(result)) throw new ArgumentException($"Missing {name}.");
            return result;
        }

        if (!Enum.TryParse<DbType>(Value("--provider"), true, out var provider))
            throw new ArgumentException("Unknown --provider.");
        var connectionFile = RawArg(args, "--connection-file") ?? FixtureValue("connectionFile");
        var targetFile = RawArg(args, "--target-connection-file");
        var connection = connectionFile is not null
            ? File.ReadAllText(connectionFile).Trim()
            : RawArg(args, "--connection") ?? FixtureValue("connection") ?? throw new ArgumentException("Missing --connection-file or --connection.");
        if (connection.Length == 0) throw new ArgumentException("Connection file is empty.");
        var plugin = OptionalNullable("--plugin") ?? FixtureValue("plugin") ?? throw new ArgumentException("Missing --plugin.");
        return new ManagedSqlOptions
        {
            Provider = provider,
            ConnectionString = connection,
            TargetConnectionString = targetFile is null ? null : File.ReadAllText(targetFile).Trim(),
            PluginPath = plugin,
            RowCount = ParsePositive("--rows", 16),
            BatchSize = ParsePositive("--batch", 4),
            BenchmarkPath = OptionalNullable("--benchmark"),
            SetupSqlPath = OptionalNullable("--setup-sql") ?? FixtureValue("setupSql")
        };

        string? OptionalNullable(string name)
        {
            var property = name.TrimStart('-').Replace("-", string.Empty, StringComparison.Ordinal);
            return RawArg(args, name) ?? FixtureValue(char.ToLowerInvariant(property[0]) + property[1..]);
        }

        int ParsePositive(string name, int fallback)
        {
            var text = OptionalNullable(name);
            return text is null
                ? fallback
                : int.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out var number) && number > 0
                    ? number
                    : throw new ArgumentException($"{name} must be positive.");
        }
    }

    internal string QuotedTable() => Quote(TableName);
    internal string QuotedColumn(string name) => Quote(name);
    internal string QuotedColumns() => string.Join(", ", new[] { "Id", "Name", "Amount" }.Select(QuotedColumn));

    internal string CreateTableSql() => Provider switch
    {
        DbType.SqlServer => $"CREATE TABLE {QuotedTable()} ({QuotedColumn("Id")} BIGINT NOT NULL PRIMARY KEY, {QuotedColumn("Name")} NVARCHAR(200) NOT NULL, {QuotedColumn("Amount")} DECIMAL(38,28) NOT NULL)",
        DbType.Oracle => $"CREATE TABLE {QuotedTable()} ({QuotedColumn("Id")} NUMBER(19) PRIMARY KEY, {QuotedColumn("Name")} NVARCHAR2(200) NOT NULL, {QuotedColumn("Amount")} NUMBER(38,28) NOT NULL)",
        DbType.Sqlite => $"CREATE TABLE {QuotedTable()} ({QuotedColumn("Id")} INTEGER NOT NULL PRIMARY KEY, {QuotedColumn("Name")} TEXT NOT NULL, {QuotedColumn("Amount")} TEXT NOT NULL)",
        _ => $"CREATE TABLE {QuotedTable()} ({QuotedColumn("Id")} BIGINT NOT NULL PRIMARY KEY, {QuotedColumn("Name")} VARCHAR(200) NOT NULL, {QuotedColumn("Amount")} DECIMAL(38,28) NOT NULL)"
    };

    private string Quote(string identifier) => Provider switch
    {
        DbType.SqlServer => "[" + identifier + "]",
        DbType.MySql or DbType.MariaDb => "`" + identifier + "`",
        _ => "\"" + identifier + "\""
    };

    private static string? RawArg(string[] args, string name)
    {
        var index = Array.FindIndex(args, value => string.Equals(value, name, StringComparison.OrdinalIgnoreCase));
        return index >= 0 && index + 1 < args.Length && !args[index + 1].StartsWith("--", StringComparison.Ordinal)
            ? args[index + 1]
            : null;
    }
}
