using System.Collections.Concurrent;

namespace UniversalData;

internal static class SdkNativeSources
{
    private static readonly ConcurrentDictionary<(DataSourceKind Kind, string Root), string> installedDrivers = new();
    private static readonly Dictionary<DataSourceKind, string> driverNames = new()
    {
        [DataSourceKind.Sqlite] = "sqlite",
        [DataSourceKind.PostgreSql] = "postgresql",
        [DataSourceKind.MySql] = "mysql",
        [DataSourceKind.MariaDb] = "mariadb",
        [DataSourceKind.SqlServer] = "odbc",
        [DataSourceKind.Oracle] = "oracle_oci",
        [DataSourceKind.Kafka] = "kafka",
        [DataSourceKind.RabbitMq] = "rabbitmq",
        [DataSourceKind.RocketMq] = "rocketmq",
        [DataSourceKind.ActiveMq] = "activemq"
    };

    internal static IEnumerable<DataSourceKind> PluginKinds => driverNames.Keys;

    internal static DataSourceKind? LogicalDriver(string id)
    {
        if (!id.StartsWith("ud:", StringComparison.Ordinal)) return null;
        foreach (var (kind, name) in driverNames)
            if (id.AsSpan(3).SequenceEqual(name)) return kind;
        return null;
    }

    internal static string CredentialReference(SdkSourceBinding binding) => "sdk-source/" + binding.Definition.Name + "/" +
        binding.Revision.ToString(System.Globalization.CultureInfo.InvariantCulture);

    internal static string PluginPath(DataSourceKind kind, string? workerPath = null) =>
        FindPluginPath(kind, workerPath) ?? throw new UniversalDataException((int)StatusCode.DriverUnavailable,
            $"The Native {kind} connector was not found in the installed runtime");

    internal static string? FindPluginPath(DataSourceKind kind, string? workerPath = null)
    {
        var executable = workerPath ?? Environment.GetEnvironmentVariable("UD_WORKER_PATH");
        string root = string.IsNullOrWhiteSpace(executable) ? AppContext.BaseDirectory :
            Path.GetDirectoryName(Path.GetFullPath(executable))!;
        var key = (kind, root);
        if (installedDrivers.TryGetValue(key, out string? installed) && File.Exists(installed)) return installed;
        var path = LocatePlugin(kind, root);
        if (path is not null) installedDrivers[key] = path;
        return path;
    }

    private static string? LocatePlugin(DataSourceKind kind, string workerRoot)
    {
        var stem = "ud_connector_" + DriverName(kind);
        var name = stem + (OperatingSystem.IsWindows() ? ".dll" : ".so");
        var rid = OperatingSystem.IsWindows() ? "win-x64" : "linux-x64";
        string[] roots = [AppContext.BaseDirectory, workerRoot];
        foreach (string root in roots.Distinct())
        {
            foreach (string candidate in new[]
            {
                Path.Combine(root, name),
                Path.Combine(root, "runtimes", rid, "native", name),
                Path.Combine(root, "runtimes", rid, "worker", name),
                Path.Combine(root, "UniversalData", "plugins", name)
            })
                if (File.Exists(candidate)) return Path.GetFullPath(candidate);

            DirectoryInfo? parent = new(Path.GetFullPath(root));
            for (var level = 0; parent is not null && level < 4; level++, parent = parent.Parent)
            {
                foreach (string candidate in new[]
                {
                    Path.Combine(parent.FullName, "plugins", name),
                    Path.Combine(parent.FullName, "plugins", "Release", name),
                    Path.Combine(parent.FullName, "lib", "UniversalData", "plugins", name)
                })
                    if (File.Exists(candidate)) return Path.GetFullPath(candidate);
            }
        }
        return null;
    }

    internal static string DriverName(DataSourceKind kind) => driverNames.TryGetValue(kind, out var name)
        ? name : throw new ArgumentException("The named source has no Native plugin", nameof(kind));
}
