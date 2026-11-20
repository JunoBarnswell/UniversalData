using System.Text.Json;

namespace UniversalData.Hosting;

/// <summary>Installed inputs advertised by a registered node, scoped to a tenant.</summary>
public sealed record WorkerNodeInventory(string[] Plugins, string[] CredentialReferences, string[] ManagedAssemblies)
{
    internal bool Valid => ValidList(Plugins) && ValidList(CredentialReferences) && ValidList(ManagedAssemblies);

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
                if (property == "extensions" && entry.TryGetProperty("sha256", out var hash) &&
                    !ManagedAssemblies.Contains(hash.GetString(), StringComparer.OrdinalIgnoreCase))
                    return false;
                if (property == "connections" && !SupportsConnection(entry))
                    return false;
            }
        }
        return !package.TryGetProperty("project", out var project) || Supports(project);
    }

    private bool SupportsConnection(JsonElement entry)
    {
        if (entry.TryGetProperty("driver", out var driver) && driver.ValueKind == JsonValueKind.String &&
            driver.GetString() is { } name && Path.IsPathFullyQualified(name) &&
            !Plugins.Contains(name, OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal))
            return false;
        if (!HasReference(entry))
            return false;
        return !entry.TryGetProperty("oauth2", out var oauth) || HasReference(oauth);
    }

    private bool HasReference(JsonElement entry) => !entry.TryGetProperty("credentialReference", out var reference) ||
        reference.ValueKind != JsonValueKind.String || CredentialReferences.Contains(reference.GetString(), StringComparer.Ordinal);
}
