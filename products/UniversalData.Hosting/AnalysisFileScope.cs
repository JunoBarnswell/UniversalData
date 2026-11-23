namespace UniversalData.Hosting;

/// <summary>Exact private file grants for one Management-authored Native package.</summary>
public sealed record AnalysisFileScope(string DatasetId, string? Input = null, string? Asset = null,
    bool CreateAsset = false, string[]? Assets = null, string? ScratchTicket = null)
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
        : Resolve(policy, Asset, "release-", ".parquet", ".ndjson");

    internal string? ScratchPath(HostPolicy policy) => ScratchTicket is null ? null
        : Path.Combine(policy.ExecutionDirectory(ScratchTicket), "buffers");

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
        if (resource == ScratchPath(policy))
            return access is ConnectionAccess.Open or ConnectionAccess.Read or ConnectionAccess.Write or ConnectionAccess.Delete;
        string? input = InputPath(policy);
        string? asset = AssetPath(policy);
        string[] assets = (Assets ?? []).Select(name => Resolve(policy, name, "release-", ".parquet", ".ndjson"))
            .Append(asset).Where(path => path is not null).Select(path => path!).ToArray();
        var reads = access is ConnectionAccess.Open or ConnectionAccess.Read;
        if (reads && (resource == input || assets.Contains(resource, StringComparer.Ordinal)))
            return true;
        if (!CreateAsset || assets.Length == 0 || access != ConnectionAccess.Write)
            return false;
        return assets.Any(path => resource == path ||
            (resource.StartsWith(path + ".ud-generation-", StringComparison.Ordinal) &&
                Guid.TryParseExact(resource.AsSpan(path.Length + ".ud-generation-".Length), "N", out _)));
    }
}
