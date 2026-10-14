using System.Text.Json;

namespace UniversalData.Hosting;

/// <summary>Explicit offline historical credential migration; Native owns integrity and secure SQLite compaction.</summary>
public static class HistoryProtection
{
    public static ulong Protect(HostPolicy policy,string directory,string tenant)
    {
        if (!HostIdentity.ValidId(tenant)) throw new ArgumentException("Invalid tenant");
        directory = Path.GetFullPath(directory); HostFiles.NoLinks(directory);
        var scope = policy.Tenants is null ? policy with { Tenant = tenant } : policy.Bind(HostIdentity.Operator(tenant));
        using var catalog = new Catalog(directory,tenant,(actual,resource,_) => actual == tenant && resource == directory,createDirectory:false);
        var vault = new ParameterVault(scope); string? afterId = null; ulong afterRevision = 0,changed = 0;
        while(true) {
            var query = new Dictionary<string,object> { ["kind"] = (int)CatalogKind.Audit,["limit"] = 100 };
            if (afterId is not null) { query["afterId"] = afterId; query["afterRevision"] = afterRevision; }
            using var page = JsonDocument.Parse(catalog.Query(JsonSerializer.Serialize(query,HostPolicy.JsonOptions)));
            if (page.RootElement.GetArrayLength() == 0) break;
            foreach(var entry in page.RootElement.EnumerateArray()) {
                afterId = entry.GetProperty("id").GetString()!; afterRevision = entry.GetProperty("revision").GetUInt64();
                var document = entry.GetProperty("document");
                if (!document.TryGetProperty("parameters",out var parameters)) continue;
                if (!afterId.StartsWith("run/",StringComparison.Ordinal) && !afterId.StartsWith("job/",StringComparison.Ordinal))
                    throw new InvalidDataException("Unclassified legacy secret document requires explicit ownership review");
                string text = parameters.ValueKind == JsonValueKind.String ? parameters.GetString()! : parameters.GetRawText();
                string reference = vault.Store(text); catalog.ProtectLegacyParameters(afterId,afterRevision,reference); changed++;
            }
        }
        catalog.SecureCompact();
        string id = "credential-migration/" + Guid.NewGuid().ToString("N");
        catalog.Put(CatalogKind.Audit,id,1,JsonSerializer.Serialize(new { version = ProductInfo.Protocol,id,tenant,protectedRecords = changed,
            subject = "host-operator",timestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() },HostPolicy.JsonOptions));
        return changed;
    }
}
