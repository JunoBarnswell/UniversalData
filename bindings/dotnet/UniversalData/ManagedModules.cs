using System.Reflection;
using System.Runtime.Loader;
using System.Security.Cryptography;
using System.Text.Json;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

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
    private readonly List<ModuleContext> failedLoads = new();
    /// <summary>Same fixed-assembly loading entry for SDK, CLI and product Worker.</summary>
    public void LoadPackageExtensions(string packageJson, string tenant, PackageAuthorizer authorize)
    {
        ArgumentNullException.ThrowIfNull(authorize);
        // The C17 package parser owns the allowed document and extension contract.
        using var package = new Package(packageJson);
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
                try {
                    using var stream = new MemoryStream(bytes, writable: false); var assembly = load.LoadFromStream(stream);
                    var version = assembly.GetCustomAttribute<UniversalDataModuleAttribute>();
                    if (version?.ProtocolVersion != ProductInfo.Protocol || version.SdkVersion != ProductInfo.Sdk)
                        throw new UniversalDataException((int)StatusCode.Incompatible, "Managed module SDK/protocol differs from product");
                    var registrations = assembly.GetExportedTypes().Where(type => !type.IsAbstract && typeof(IManagedModule).IsAssignableFrom(type))
                        .Select(type => (IManagedModule)(Activator.CreateInstance(type) ?? throw new ArgumentException("Module construction failed"))).ToArray();
                    if (registrations.Length == 0) throw new ArgumentException("Assembly exports no Native Registry registrations");
                    foreach (var registration in registrations) registration.Register(this);
                    modules.Add(digest, (load, registrations));
                } catch {
                    // Native registrations already installed by a failing module
                    // remain owned until registry destruction. Fence further use.
                    failedModule = true; failedLoads.Add(load); throw;
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
        foreach (var load in failedLoads) load.Unload();
        modules.Clear(); failedLoads.Clear();
    }
}

public static class CSharpDeployment
{
    /// <summary>Compile once at deployment. Execution loads only the fixed digest-bound assembly.</summary>
    public static ManagedAssembly Compile(string source, string outputDirectory, string tenant, PackageAuthorizer authorize)
    {
        ArgumentNullException.ThrowIfNull(authorize);
        if (source.Length > 2 * 1024 * 1024 || !Path.IsPathFullyQualified(outputDirectory) ||
            !authorize(tenant, outputDirectory, ConnectionAccess.Write))
            throw new UniversalDataException((int)StatusCode.Auth, "Script deployment is outside host scope");
        Directory.CreateDirectory(outputDirectory);
        var code = CSharpSyntaxTree.ParseText(source, new CSharpParseOptions(LanguageVersion.Latest), path: "deployment.cs");
        var identity = CSharpSyntaxTree.ParseText($"[assembly: UniversalData.UniversalDataModule(\"{ProductInfo.Sdk}\", {ProductInfo.Protocol})]");
        var paths = ((string?)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES") ?? throw new PlatformNotSupportedException("Runtime references missing"))
            .Split(Path.PathSeparator).Append(typeof(Registry).Assembly.Location).Distinct(StringComparer.OrdinalIgnoreCase);
        var references = paths.Select(path => MetadataReference.CreateFromFile(path));
        var name = "UniversalData.Script." + Convert.ToHexString(SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(source))).ToLowerInvariant();
        var compilation = CSharpCompilation.Create(name, [code, identity], references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, optimizationLevel: OptimizationLevel.Release,
                allowUnsafe: false, deterministic: true));
        using var output = new MemoryStream(); var result = compilation.Emit(output);
        if (!result.Success) {
            // Compiler text can contain user code; expose only codes and locations.
            var errors = result.Diagnostics.Where(diagnostic => diagnostic.Severity == DiagnosticSeverity.Error)
                .Take(32).Select(diagnostic => diagnostic.Id + "@" + diagnostic.Location.GetLineSpan().StartLinePosition);
            throw new ArgumentException("C# deployment compilation failed: " + string.Join(", ", errors));
        }
        byte[] bytes = output.ToArray(); string digest = Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();
        var path = Path.Combine(outputDirectory, digest + ".dll");
        if (!File.Exists(path)) {
            var temporary = Path.Combine(outputDirectory, ".compile-" + Guid.NewGuid().ToString("N"));
            try { using (var file = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None)) { file.Write(bytes); file.Flush(true); }
                try { File.Move(temporary, path, overwrite: false); } catch (IOException) when (File.Exists(path)) { } }
            finally { if (File.Exists(temporary)) File.Delete(temporary); }
        }
        if (!CryptographicOperations.FixedTimeEquals(SHA256.HashData(File.ReadAllBytes(path)), SHA256.HashData(bytes)))
            throw new UniversalDataException((int)StatusCode.Corrupt, "Fixed assembly content differs");
        return new("managedAssembly", path, digest);
    }
}
