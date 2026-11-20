namespace UniversalData;

public enum DataSourceKind { PostgreSql, MySql, MariaDb, SqlServer, Oracle, Sqlite, Http }

public sealed class DataSourceDefinition
{
    public DataSourceDefinition(string name, DataSourceKind kind, string connectionString,
        string? resource = null, string? credentialReference = null, Uri? baseUri = null,
        string? nativeConnectionString = null, ulong initialRevision = 1, bool provisionTransactionReceipts = false)
    {
        if (string.IsNullOrWhiteSpace(name) || name.Length > 128 ||
            name.Any(c => !char.IsAsciiLetterOrDigit(c) && c is not ('_' or '-' or '.')))
            throw new ArgumentException("A source name must be an identifier", nameof(name));
        if (!Enum.IsDefined(kind)) throw new ArgumentOutOfRangeException(nameof(kind));
        if (initialRevision == 0) throw new ArgumentOutOfRangeException(nameof(initialRevision));
        if (kind == DataSourceKind.Http && (baseUri is null || !baseUri.IsAbsoluteUri ||
            baseUri.Scheme is not ("http" or "https") || baseUri.UserInfo.Length != 0 ||
            baseUri.Query.Length != 0 || baseUri.Fragment.Length != 0))
            throw new ArgumentException("An HTTP source needs an absolute HTTP base URI", nameof(baseUri));
        if (kind != DataSourceKind.Http && string.IsNullOrWhiteSpace(connectionString) &&
            string.IsNullOrEmpty(credentialReference))
            throw new ArgumentException("A database source needs a connection string or credential reference", nameof(connectionString));
        Name = name;
        Kind = kind;
        ConnectionString = connectionString ?? "";
        Resource = resource ?? DefaultResource(name, kind, ConnectionString, baseUri);
        CredentialReference = credentialReference;
        BaseUri = baseUri;
        NativeConnectionString = nativeConnectionString;
        InitialRevision = initialRevision;
        ProvisionTransactionReceipts = provisionTransactionReceipts;
    }

    public string Name { get; }
    public DataSourceKind Kind { get; }
    public string ConnectionString { get; }
    public string Resource { get; }
    public string? CredentialReference { get; }
    public Uri? BaseUri { get; }
    public string? NativeConnectionString { get; }
    public ulong InitialRevision { get; }
    public bool ProvisionTransactionReceipts { get; }

    private static string DefaultResource(string name, DataSourceKind kind, string connection, Uri? address)
    {
        if (kind == DataSourceKind.Http) return address!.GetLeftPart(UriPartial.Authority);
        if (kind != DataSourceKind.Sqlite || connection.Length == 0) return name;
        var builder = new System.Data.Common.DbConnectionStringBuilder { ConnectionString = connection };
        foreach (var key in new[] { "Data Source", "DataSource", "Filename" })
            if (builder.TryGetValue(key, out var value) && value is string path && Path.IsPathFullyQualified(path))
                return Path.GetFullPath(path);
        return name;
    }
}

public sealed class DataSourceOptions
{
    public string? Resource { get; set; }
    public string? CredentialReference { get; set; }
    public string? NativeConnectionString { get; set; }
    public ulong InitialRevision { get; set; } = 1;
    public bool ProvisionTransactionReceipts { get; set; }
}

public sealed record DataSourceInfo(string Name, DataSourceKind Kind, ulong Revision, bool Enabled);

public sealed record DataSourceProbe(DataSourceInfo Source, bool Connected, IReadOnlyList<string> Capabilities);
