using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace UniversalData;

/// <summary>Registers one application-wide, lazily initialized UniversalData client.</summary>
public static class UniversalDataServiceCollectionExtensions
{
    /// <summary>
    /// Registers the YAML configuration named by <c>ConfigPath</c> in a configuration section.
    /// The path is checked during registration; YAML is parsed when the singleton is first resolved.
    /// </summary>
    public static IServiceCollection AddUniversalData(this IServiceCollection services,
        IConfigurationSection section, string contentRootPath)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(section);
        EnsureNotRegistered(services);
        if (section.GetChildren().Any(child => !child.Key.Equals("ConfigPath", StringComparison.OrdinalIgnoreCase)))
            throw new ArgumentException("The UniversalData section accepts only ConfigPath.", nameof(section));
        string root = ExistingDirectory(contentRootPath, nameof(contentRootPath));
        string configuredPath = section["ConfigPath"]
            ?? throw new ArgumentException("The UniversalData section must define ConfigPath.", nameof(section));
        string configPath = ExistingFile(configuredPath, root, "ConfigPath");
        return AddClient(services, () => DataConfiguration.Load(configPath).CreateClient());
    }

    /// <summary>
    /// Registers a YAML configuration file relative to the current working directory when needed.
    /// The path is checked during registration; YAML is parsed when the singleton is first resolved.
    /// </summary>
    public static IServiceCollection AddUniversalData(this IServiceCollection services, string configPath)
    {
        ArgumentNullException.ThrowIfNull(services);
        EnsureNotRegistered(services);
        string fullPath = ExistingFile(configPath, Directory.GetCurrentDirectory(), nameof(configPath));
        return AddClient(services, () => DataConfiguration.Load(fullPath).CreateClient());
    }

    /// <summary>Registers a manually configured client built from the existing DataClientBuilder.</summary>
    public static IServiceCollection AddUniversalData(this IServiceCollection services, string policyFile,
        string tenant, Action<DataClientBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        EnsureNotRegistered(services);
        string fullPolicyPath = ExistingFile(policyFile, Directory.GetCurrentDirectory(), nameof(policyFile));
        ArgumentException.ThrowIfNullOrWhiteSpace(tenant);
        ArgumentNullException.ThrowIfNull(configure);

        return AddClient(services, () =>
        {
            var builder = DataClient.CreateBuilder(fullPolicyPath, tenant);
            configure(builder);
            return builder.Build();
        });
    }

    private static IServiceCollection AddClient(IServiceCollection services, Func<DataClient> factory)
    {
        services.Add(ServiceDescriptor.Singleton<IDataClient>(_ => factory()));
        return services;
    }

    private static void EnsureNotRegistered(IServiceCollection services)
    {
        if (services.Any(descriptor => descriptor.ServiceType == typeof(IDataClient)))
            throw new InvalidOperationException("UniversalData's default IDataClient is already registered.");
    }

    private static string ExistingDirectory(string path, string parameterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path, parameterName);
        string fullPath = Path.GetFullPath(path);
        if (!Directory.Exists(fullPath))
            throw new DirectoryNotFoundException($"The UniversalData content root does not exist: '{fullPath}'.");
        return fullPath;
    }

    private static string ExistingFile(string path, string baseDirectory, string parameterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path, parameterName);
        string fullPath = Path.GetFullPath(path, baseDirectory);
        if (!File.Exists(fullPath))
            throw new FileNotFoundException($"The UniversalData configuration entry does not exist: '{fullPath}'.",
                fullPath);
        return fullPath;
    }
}
