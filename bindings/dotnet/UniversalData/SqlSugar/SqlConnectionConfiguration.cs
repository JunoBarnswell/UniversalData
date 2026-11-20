namespace SqlSugar;

internal static class SqlConnectionConfiguration
{
    internal static void Validate(ConnectionConfig value)
    {
        if (!Enum.IsDefined(value.DbType)) throw new ArgumentOutOfRangeException(nameof(value.DbType));
        if (value.InitKeyType != InitKeyType.Attribute)
            throw new NotSupportedException("Declare entity keys with SugarColumn attributes.");
        if (string.IsNullOrWhiteSpace(value.ConnectionString)) throw new ArgumentException("ConnectionString is required.", nameof(value));
    }
    internal static string Plugin(ConnectionConfig value)
    {
        if (!string.IsNullOrWhiteSpace(value.PluginPath))
        {
            if (!Path.IsPathFullyQualified(value.PluginPath)) throw new ArgumentException("PluginPath must be absolute.", nameof(value));
            if (!File.Exists(value.PluginPath)) throw new FileNotFoundException("Native SQL connector plugin was not found.", value.PluginPath);
            return value.PluginPath;
        }
        var stem = value.DbType switch
        {
            DbType.Sqlite => "ud_connector_sqlite",
            DbType.PostgreSQL => "ud_connector_postgresql",
            DbType.MySql => "ud_connector_mysql",
            DbType.MariaDb => "ud_connector_mariadb",
            DbType.SqlServer => "ud_connector_odbc",
            DbType.Oracle => "ud_connector_oracle_oci",
            _ => throw new ArgumentOutOfRangeException(nameof(value.DbType))
        };
        var extension = OperatingSystem.IsWindows() ? ".dll" : OperatingSystem.IsMacOS() ? ".dylib" : ".so";
        var names = OperatingSystem.IsWindows() ? new[] { stem + extension } : new[] { stem + extension, "lib" + stem + extension };
        var candidates = names.SelectMany(name => new[]
        {
            Path.Combine(AppContext.BaseDirectory, name),
            Path.Combine(AppContext.BaseDirectory, "UniversalData", "plugins", name)
        }).ToArray();
        var path = candidates.FirstOrDefault(File.Exists);
        return path ?? throw new FileNotFoundException($"Native SQL connector plugin '{stem}' was not found. Specify ConnectionConfig.PluginPath.");
    }

}
