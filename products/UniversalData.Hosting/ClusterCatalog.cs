using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace UniversalData.Hosting;

/// <summary>Host-owned credentials and Catalog leadership context. Worker requests never serialize this configuration.</summary>
internal sealed class ClusterCatalog
{
    internal const string SystemTenant = "__ud_cluster";
    internal const string LeadershipId = "leadership";
    internal const string IdentityId = "identity/directory";
    internal const string KeyPrefix = "key-ring/";
    private readonly string connection;
    private readonly byte[] key;
    private readonly AsyncLocal<CatalogLease?> scope = new();
    internal Func<CatalogLease?>? Authority { private get; set; }
    internal CatalogLease? PinnedAuthority => scope.Value;

    internal ClusterCatalog(string connectionFile, string keyFile)
    {
        HostFiles.NoLinks(connectionFile);
        HostFiles.NoLinks(keyFile);
        connection = File.ReadAllText(connectionFile).Trim();
        key = Convert.FromBase64String(File.ReadAllText(keyFile));
        if (connection.Length == 0 || key.Length != 32)
            throw new InvalidDataException("Invalid cluster Catalog secrets");
    }

    internal Catalog Open(string tenant, bool election = false, bool initialize = false)
    {
        var lease = election ? null : Authority?.Invoke();
        if (!election && scope.Value is { } pinned)
        {
            if (Authority is not null && (lease is null || lease.Generation != pinned.Generation))
                throw new UniversalDataException((int)StatusCode.Transient, "Catalog leadership is no longer confirmed",
                    new(ErrorSource.Catalog, StatusCode.Transient, "Catalog leadership is no longer confirmed"));
            lease = pinned;
        }
        var catalog = Catalog.OpenPostgreSql(connection, tenant,
            (scopeTenant, resource, _) => scopeTenant == tenant && resource == "ud-catalog", initialize);
        try
        {
            if (lease is not null)
                catalog.SetLeadership(lease);
            return catalog;
        }
        catch { catalog.Dispose(); throw; }
    }

    internal IDisposable Pin(CatalogLease lease)
    {
        var previous = scope.Value;
        scope.Value = lease;
        return new RestoreScope(scope, previous);
    }

    private sealed class RestoreScope(AsyncLocal<CatalogLease?> slot, CatalogLease? previous) : IDisposable
    {
        public void Dispose() => slot.Value = previous;
    }

    internal ulong Write(string tenant, string id, string value, ulong expected = 0)
    {
        using var catalog = Open(tenant);
        string document = Encode(tenant, id, value);
        catalog.CompareExchange(CatalogKind.Audit, id, expected, document);
        return checked(expected + 1);
    }

    internal (string? Value, ulong Revision) Read(string tenant, string id)
    {
        using var catalog = Open(tenant);
        ulong revision = catalog.LatestRevision(CatalogKind.Audit, id);
        return revision == 0 ? (null, 0) : (Decode(tenant, id, catalog.Get(CatalogKind.Audit, id, revision)!), revision);
    }

    internal string Encode(string tenant, string id, string value)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(value);
        byte[] encrypted = new byte[checked(28 + bytes.Length)];
        RandomNumberGenerator.Fill(encrypted.AsSpan(0,12));
        try
        {
            using var cipher = new AesGcm(key, 16);
            cipher.Encrypt(encrypted.AsSpan(0,12), bytes, encrypted.AsSpan(28), encrypted.AsSpan(12,16), Binding(tenant,id));
            return JsonSerializer.Serialize(new { version = ProductInfo.Protocol, id, encrypted = Convert.ToBase64String(encrypted) }, HostPolicy.JsonOptions);
        }
        finally { CryptographicOperations.ZeroMemory(bytes); }
    }

    internal string Decode(string tenant, string id, string document)
    {
        using var envelope = JsonDocument.Parse(document);
        byte[] encrypted = envelope.RootElement.GetProperty("encrypted").GetBytesFromBase64();
        if (encrypted.Length is < 28 or > 33554432)
            throw new InvalidDataException("Invalid protected cluster document");
        byte[] bytes = new byte[encrypted.Length - 28];
        try
        {
            using var cipher = new AesGcm(key,16);
            cipher.Decrypt(encrypted.AsSpan(0,12), encrypted.AsSpan(28), encrypted.AsSpan(12,16), bytes, Binding(tenant,id));
            return Encoding.UTF8.GetString(bytes);
        }
        finally { CryptographicOperations.ZeroMemory(bytes); }
    }

    internal string Fingerprint(string purpose, string value) =>
        Convert.ToHexString(HMACSHA256.HashData(key, Encoding.UTF8.GetBytes(purpose + "\0" + value)));

    private static byte[] Binding(string tenant, string id) =>
        Encoding.UTF8.GetBytes("UniversalData/catalog/v1/" + tenant + "/" + id);
}
