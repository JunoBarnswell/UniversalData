using System.Reflection;
using System.Runtime.Loader;
using System.Security.Cryptography;
using System.Text.Json;

namespace UniversalData;

[AttributeUsage(AttributeTargets.Assembly)]
public sealed class UniversalDataModuleAttribute(string sdkVersion, int protocolVersion) : Attribute
{ public string SdkVersion { get; } = sdkVersion; public int ProtocolVersion { get; } = protocolVersion; }
public interface IManagedModule { void Register(Registry registry); }
public sealed record ManagedAssembly(string Kind, string Path, string Sha256);

public sealed partial class Registry
{
    private sealed class ModuleContext() : AssemblyLoadContext(isCollectible: true)
    {
        protected override Assembly? Load(AssemblyName name) => name.Name == typeof(Registry).Assembly.GetName().Name ? typeof(Registry).Assembly : null;
    }
    private readonly Dictionary<string, (ModuleContext Context, IManagedModule[] Modules)> modules = new();
    private bool failedModule;
    private readonly List<(ModuleContext Context, List<IManagedModule> Modules)> failedLoads = new();
    /// <summary>Same fixed-assembly loading entry for SDK, CLI and product Worker.</summary>
    public void LoadPackageExtensions(string packageJson, string tenant, PackageAuthorizer authorize)
    {
        ArgumentNullException.ThrowIfNull(authorize);
        // The C17 package parser owns the allowed document and extension contract.
        using var package = new NativePackage(packageJson);
        using var document = JsonDocument.Parse(package.ToJson());
        if (!document.RootElement.TryGetProperty("extensions", out var entries)) return;
        lock (sync) {
            Open();
            foreach (var entry in entries.EnumerateArray()) {
                var path = entry.GetProperty("path").GetString()!; var digest = entry.GetProperty("sha256").GetString()!;
                if (!Path.IsPathFullyQualified(path) || !authorize(tenant, path, ConnectionAccess.Execute))
                    throw new UniversalDataException((int)StatusCode.Auth, "Managed assembly is outside host scope");
                path = Path.GetFullPath(path);
                // Loading uses verified frozen bytes, never a mutable deployment file.
                var info = new FileInfo(path); if (info.Length > 64 * 1024 * 1024) throw new ArgumentException("Assembly exceeds product limit");
                for (FileSystemInfo? part = info; part is not null; part = part is FileInfo file ? file.Directory : ((DirectoryInfo)part).Parent)
                    if (part.LinkTarget is not null) throw new UniversalDataException((int)StatusCode.Auth, "Assembly path contains a link");
                byte[] bytes = File.ReadAllBytes(path);
                if (!CryptographicOperations.FixedTimeEquals(SHA256.HashData(bytes), Convert.FromHexString(digest)))
                    throw new UniversalDataException((int)StatusCode.Corrupt, "Assembly digest differs from deployment");
                if (modules.ContainsKey(digest)) continue;
                var load = new ModuleContext();
                var registrations = new List<IManagedModule>();
                try {
                    using var stream = new MemoryStream(bytes, writable: false); var assembly = load.LoadFromStream(stream);
                    var version = assembly.GetCustomAttribute<UniversalDataModuleAttribute>();
                    if (version?.ProtocolVersion != ProductInfo.Protocol || version.SdkVersion != ProductInfo.Sdk)
                        throw new UniversalDataException((int)StatusCode.Incompatible, "Managed module SDK/protocol differs from product");
                    foreach (var type in assembly.GetExportedTypes().Where(type => !type.IsAbstract && typeof(IManagedModule).IsAssignableFrom(type)))
                        registrations.Add((IManagedModule)(Activator.CreateInstance(type) ?? throw new ArgumentException("Module construction failed")));
                    if (registrations.Count == 0) throw new ArgumentException("Assembly exports no Native Registry registrations");
                    foreach (var registration in registrations) registration.Register(this);
                    modules.Add(digest, (load, registrations.ToArray()));
                } catch {
                    // Native registrations already installed by a failing module
                    // remain owned until registry destruction. Fence further use.
                    failedModule = true; failedLoads.Add((load, registrations)); throw;
                }
            }
        }
    }
    private void ReleaseModules()
    {
        foreach (var module in modules.Values) {
            foreach (var registration in module.Modules) if (registration is IDisposable owned) {
                try { owned.Dispose(); } catch { /* Native teardown already completed. */ }
            }
            module.Context.Unload();
        }
        foreach (var load in failedLoads) {
            foreach (var registration in load.Modules) if (registration is IDisposable owned) {
                try { owned.Dispose(); } catch { /* Native teardown already completed. */ }
            }
            load.Context.Unload();
        }
        modules.Clear(); failedLoads.Clear();
    }
}
