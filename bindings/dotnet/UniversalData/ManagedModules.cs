using System.Reflection;
using System.Text.Json;

namespace UniversalData;

[AttributeUsage(AttributeTargets.Assembly)]
public sealed class UniversalDataModuleAttribute(string sdkVersion, int protocolVersion) : Attribute
{ public string SdkVersion { get; } = sdkVersion; public int ProtocolVersion { get; } = protocolVersion; }
public interface IManagedModule { void Register(Registry registry); }
public sealed record ManagedAssembly(string Kind, string Path, string Sha256);

public sealed partial class Registry
{
    private readonly Dictionary<string, SdkModuleCatalog> modules = new(StringComparer.OrdinalIgnoreCase);
    private readonly List<SdkModuleCatalog> moduleCatalogs = [];
    private readonly List<IManagedModule> ownedModules = [];
    private bool failedModule;

    /// <summary>Loads the immutable assembly inventory used by Native package components.</summary>
    public void LoadPackageExtensions(string packageJson, string tenant, PackageAuthorizer authorize)
    {
        ArgumentNullException.ThrowIfNull(authorize);
        using var package = new NativePackage(packageJson);
        using var document = JsonDocument.Parse(package.ToJson());
        var root = document.RootElement;
        var entries = root.TryGetProperty("extensions", out var extensions)
            ? extensions.EnumerateArray().Select(entry => new SdkModule(entry.GetProperty("path").GetString()!,
                entry.GetProperty("sha256").GetString()!)).ToArray() : [];

        lock (sync)
        {
            Open();
            foreach (var entry in entries)
                if (!Path.IsPathFullyQualified(entry.Path) || !authorize(tenant, entry.Path, ConnectionAccess.Execute))
                    throw new UniversalDataException((int)StatusCode.Auth, "Managed assembly is outside host scope");
            if (entries.Length == 0) return;
            SdkModuleCatalog? catalog = null;
            try
            {
                foreach (var entry in entries)
                {
                    if (modules.ContainsKey(entry.Sha256)) continue;
                    if (catalog is null)
                    {
                        catalog = new SdkModuleCatalog(entries, (resource, access) => authorize(tenant, resource, access));
                        moduleCatalogs.Add(catalog);
                    }
                    var assembly = catalog.Load(entry.Sha256);
                    var registrations = assembly.GetExportedTypes().Where(type => !type.IsAbstract &&
                        typeof(IManagedModule).IsAssignableFrom(type)).ToArray();
                    if (registrations.Length == 0)
                        throw new ArgumentException("Assembly exports no Native Registry registrations");
                    var version = assembly.GetCustomAttribute<UniversalDataModuleAttribute>();
                    if (version?.ProtocolVersion != ProductInfo.Protocol || version.SdkVersion != ProductInfo.Sdk)
                        throw new UniversalDataException((int)StatusCode.Incompatible, "Managed module SDK/protocol differs from product");
                    foreach (var type in registrations)
                    {
                        var module = (IManagedModule)(Activator.CreateInstance(type) ?? throw new ArgumentException("Module construction failed"));
                        ownedModules.Add(module);
                        module.Register(this);
                    }
                    modules.Add(entry.Sha256, catalog);
                }
            }
            catch
            {
                // Native registrations already installed by a failing module remain owned until registry destruction.
                failedModule = true;
                throw;
            }
        }
    }

    private void ReleaseModules()
    {
        foreach (var registration in ownedModules)
            if (registration is IDisposable disposable)
            {
                try { disposable.Dispose(); }
                catch { /* Native teardown already completed. */ }
            }
        foreach (var catalog in moduleCatalogs) catalog.Dispose();
        ownedModules.Clear();
        moduleCatalogs.Clear();
        modules.Clear();
    }
}
