using System.Collections.Concurrent;

namespace UniversalData;

internal static class SdkNativeSources
{
    private static readonly ConcurrentDictionary<DataSourceKind, string> installedDrivers = new();

    internal static string CredentialReference(SdkSourceBinding binding) => "sdk-source/" + binding.Definition.Name + "/" +
        binding.Revision.ToString(System.Globalization.CultureInfo.InvariantCulture);

    internal static string PluginPath(DataSourceKind kind, string? workerPath = null) =>
        FindPluginPath(kind, workerPath) ?? throw new UniversalDataException((int)StatusCode.DriverUnavailable,
            $"The Native {kind} connector was not found in the installed runtime");

    internal static string? FindPluginPath(DataSourceKind kind, string? workerPath = null)
    {
        var executable = workerPath ?? Environment.GetEnvironmentVariable("UD_WORKER_PATH");
        if (!string.IsNullOrWhiteSpace(executable))
            return LocatePlugin(kind, Path.GetDirectoryName(Path.GetFullPath(executable))!);

        if (installedDrivers.TryGetValue(kind, out string? installed) && File.Exists(installed)) return installed;
        var path = LocatePlugin(kind, AppContext.BaseDirectory);
        if (path is not null) installedDrivers[kind] = path;
        return path;
    }

    private static string? LocatePlugin(DataSourceKind kind, string workerRoot)
    {
        var stem = kind switch
        {
            DataSourceKind.Sqlite => "ud_connector_sqlite",
            DataSourceKind.PostgreSql => "ud_connector_postgresql",
            DataSourceKind.MySql => "ud_connector_mysql",
            DataSourceKind.MariaDb => "ud_connector_mariadb",
            DataSourceKind.SqlServer => "ud_connector_odbc",
            DataSourceKind.Oracle => "ud_connector_oracle_oci",
            _ => throw new ArgumentException("The named source is not a database", nameof(kind))
        };
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

    internal static SdkNativeConnection Describe(SdkSourceBinding binding, string path) =>
        new(binding.Definition.Name, path, binding.Definition.Resource, CredentialReference(binding));
}
