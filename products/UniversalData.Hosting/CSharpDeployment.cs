using System.Security.Cryptography;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using UniversalData;

namespace UniversalData.Hosting;

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
