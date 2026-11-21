using System.Reflection;
using System.Runtime.Loader;
using System.Security.Cryptography;

namespace UniversalData;

internal sealed class SdkModuleCatalog : IDisposable
{
    private const int MaximumAssemblyBytes = 64 * 1024 * 1024;
    private Dictionary<string, SdkModule> modules = new(StringComparer.OrdinalIgnoreCase);
    private SdkModule[]? inventory;
    private readonly Dictionary<string, Assembly> loaded = new(StringComparer.OrdinalIgnoreCase);
    private readonly ModuleContext context;

    internal SdkModuleCatalog(SdkModule[] modules, Func<string, ConnectionAccess, bool> authorize)
        : this(authorize) => Bind(modules);

    internal SdkModuleCatalog(Func<string, ConnectionAccess, bool> authorize) =>
        context = new ModuleContext(this, authorize);

    internal void Bind(SdkModule[] modules)
    {
        lock (loaded)
        {
            if (inventory is not null)
            {
                if (!inventory.SequenceEqual(modules))
                    throw new UniversalDataException((int)StatusCode.Auth, "SDK session module inventory changed");
                return;
            }

            var registrations = new Dictionary<string, SdkModule>(StringComparer.OrdinalIgnoreCase);
            foreach (var module in modules)
            {
                if (!Path.IsPathFullyQualified(module.Path) || module.Sha256.Length != 64 ||
                    module.Sha256.Any(character => !Uri.IsHexDigit(character)))
                    throw new ArgumentException("Invalid SDK module registration");
                if (!registrations.TryAdd(module.Sha256, module))
                    throw new ArgumentException("Duplicate SDK module digest");
            }
            this.modules = registrations;
            inventory = modules.ToArray();
        }
    }

    internal Assembly Load(string digest)
    {
        lock (loaded)
        {
            if (loaded.TryGetValue(digest, out var existing)) return existing;
            if (!modules.TryGetValue(digest, out var module))
                throw new UniversalDataException((int)StatusCode.Auth, "SDK method module is outside the registered assembly inventory");
            var assembly = context.LoadModule(module);
            loaded.Add(digest, assembly);
            return assembly;
        }
    }

    public void Dispose() => context.Unload();

    private sealed class ModuleContext(SdkModuleCatalog catalog, Func<string, ConnectionAccess, bool> authorize)
        : AssemblyLoadContext(isCollectible: true)
    {
        private static readonly Assembly[] Contracts = [typeof(DataClient).Assembly, typeof(SqlSugar.ISqlSugarClient).Assembly];
        private readonly List<AssemblyDependencyResolver> dependencies = [];

        internal Assembly LoadModule(SdkModule module)
        {
            using var image = ReadAuthorizedImage(module.Path, authorize);
            if (!CryptographicOperations.FixedTimeEquals(SHA256.HashData(image), Convert.FromHexString(module.Sha256)))
                throw new UniversalDataException((int)StatusCode.Corrupt, "SDK module digest differs from its registration");
            image.Position = 0;
            lock (dependencies) dependencies.Add(new AssemblyDependencyResolver(module.Path));
            return LoadFromStream(image);
        }

        protected override Assembly? Load(AssemblyName name)
        {
            foreach (var contract in Contracts)
                if (AssemblyName.ReferenceMatchesDefinition(name, contract.GetName())) return contract;

            AssemblyDependencyResolver[] snapshot;
            lock (dependencies) snapshot = dependencies.ToArray();
            foreach (var resolver in snapshot)
            {
                var path = resolver.ResolveAssemblyToPath(name);
                if (path is null) continue;
                if (catalog.modules.Values.FirstOrDefault(module => SamePath(path, module.Path)) is { } registered)
                    return catalog.Load(registered.Sha256);
                using var image = ReadAuthorizedImage(path, authorize);
                return LoadFromStream(image);
            }
            return null;
        }

        private static bool SamePath(string left, string right) => string.Equals(Path.GetFullPath(left), Path.GetFullPath(right),
            OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal);

        private static MemoryStream ReadAuthorizedImage(string path, Func<string, ConnectionAccess, bool> authorize)
        {
            path = Path.GetFullPath(path);
            if (!authorize(path, ConnectionAccess.Execute))
                throw new UniversalDataException((int)StatusCode.Auth, "SDK module is outside the executable resources");
            var info = new FileInfo(path);
            for (FileSystemInfo? segment = info; segment is not null;
                segment = segment is FileInfo file ? file.Directory : ((DirectoryInfo)segment).Parent)
                if (segment.LinkTarget is not null)
                    throw new UniversalDataException((int)StatusCode.Auth, "SDK module path contains a link");
            using var source = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
            if (source.Length is <= 0 or > MaximumAssemblyBytes)
                throw new ArgumentException("SDK assembly exceeds the module size limit");
            var bytes = GC.AllocateUninitializedArray<byte>(checked((int)source.Length));
            source.ReadExactly(bytes);
            return new MemoryStream(bytes, writable: false);
        }
    }
}
