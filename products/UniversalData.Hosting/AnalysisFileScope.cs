namespace UniversalData.Hosting;

/// <summary>Exact private file grants for one Management-authored Native package.</summary>
public sealed record AnalysisFileScope(string DatasetId, string? Input = null, string? Asset = null,
    bool CreateAsset = false)
{
    internal static string DirectoryFor(HostPolicy policy, string datasetId)
    {
        if (!Guid.TryParseExact(datasetId, "N", out _))
            throw new ArgumentException("Invalid analysis identity");
        return Path.Combine(policy.DataDirectory, "analysis", policy.Identity.Subject, datasetId);
    }

    internal string? InputPath(HostPolicy policy) => Input is null ? null
        : Resolve(policy, Input, "input-", ".csv", ".xlsx");

    internal string? AssetPath(HostPolicy policy) => Asset is null ? null
        : Resolve(policy, Asset, "release-", ".ndjson");

    private string Resolve(HostPolicy policy, string name, string prefix, params string[] extensions)
    {
        if (name != Path.GetFileName(name) || !extensions.Contains(Path.GetExtension(name), StringComparer.Ordinal) ||
            !name.StartsWith(prefix, StringComparison.Ordinal) ||
            !Guid.TryParseExact(Path.GetFileNameWithoutExtension(name).AsSpan(prefix.Length), "N", out _))
            throw new ArgumentException("Invalid analysis file");
        return Path.Combine(DirectoryFor(policy, DatasetId), name);
    }

    internal bool Allows(HostPolicy policy, string resource, ConnectionAccess access)
    {
        string? input = InputPath(policy);
        string? asset = AssetPath(policy);
        var reads = access is ConnectionAccess.Open or ConnectionAccess.Read;
        if (reads && (resource == input || resource == asset))
            return true;
        if (!CreateAsset || asset is null || access != ConnectionAccess.Write)
            return false;
        return resource == asset ||
            (resource.StartsWith(asset + ".ud-generation-", StringComparison.Ordinal) &&
                Guid.TryParseExact(resource.AsSpan(asset.Length + ".ud-generation-".Length), "N", out _));
    }
}
