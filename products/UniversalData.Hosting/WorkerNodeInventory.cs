using System.Text.Json;

namespace UniversalData.Hosting;

/// <summary>Installed inputs advertised by a registered node, scoped to a tenant.</summary>
public sealed record WorkerNodeInventory(string[] Plugins, string[] CredentialReferences, string[] ManagedAssemblies,
    string[] FileRoots, string[] FileResources, string[] TrustedAssemblies, string Platform, bool Sandbox)
{
    internal bool Valid => ValidList(Plugins) && ValidList(CredentialReferences) && ValidList(ManagedAssemblies) &&
        ValidList(FileRoots) && ValidList(FileResources) && ValidList(TrustedAssemblies) && Platform is "windows" or "linux";

    private static bool ValidList(string[] values) => values is { Length: <= 256 } &&
        values.All(value => value is { Length: > 0 and <= 4096 });

    internal bool Supports(JsonElement package)
    {
        foreach (string property in new[] { "connections", "extensions" })
        {
            if (!package.TryGetProperty(property, out var entries))
                continue;
            foreach (var entry in entries.EnumerateArray())
            {
                if (property == "extensions" && entry.TryGetProperty("sha256", out var hash))
                {
                    string? digest = hash.GetString();
                    if (!ManagedAssemblies.Contains(digest,StringComparer.OrdinalIgnoreCase) ||
                        !Sandbox && !TrustedAssemblies.Contains(digest,StringComparer.OrdinalIgnoreCase))
                        return false;
                }
                if (property == "connections" && !SupportsConnection(entry))
                    return false;
            }
        }
        return !package.TryGetProperty("project", out var project) || Supports(project);
    }

    private bool SupportsConnection(JsonElement entry)
    {
        var comparison = Platform == "windows" ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        if (entry.TryGetProperty("driver", out var driver) && driver.ValueKind == JsonValueKind.String &&
            driver.GetString() is { } name)
        {
            if ((AbsolutePath(name) || name.StartsWith("ud:", StringComparison.Ordinal)) &&
                !Plugins.Any(plugin => plugin.Equals(name, comparison))) return false;
            if (entry.TryGetProperty("resource",out var resource) && resource.ValueKind == JsonValueKind.String &&
                resource.GetString() is { } path && AbsolutePath(path) &&
                !FileResources.Any(file => file.Equals(path,comparison)) &&
                !FileRoots.Any(root => Within(path,root,comparison)))
                return false;
        }
        if (!HasReference(entry))
            return false;
        return !entry.TryGetProperty("oauth2", out var oauth) || HasReference(oauth);
    }

    private bool HasReference(JsonElement entry) => !entry.TryGetProperty("credentialReference", out var reference) ||
        reference.ValueKind != JsonValueKind.String || CredentialReferences.Contains(reference.GetString(), StringComparer.Ordinal);

    private static bool AbsolutePath(string value) => value.StartsWith('/') || value.StartsWith("\\\\",StringComparison.Ordinal) ||
        value.Length >= 3 && char.IsAsciiLetter(value[0]) && value[1] == ':' && value[2] is '/' or '\\';

    private static bool Within(string path,string root,StringComparison comparison)
    {
        root = root.TrimEnd('/','\\');
        return path.Equals(root,comparison) || path.Length > root.Length && path.StartsWith(root,comparison) &&
            path[root.Length] is '/' or '\\';
    }
}
