using System.Text.Json;
using System.Xml.Linq;
using Microsoft.AspNetCore.DataProtection.Repositories;
using UniversalData.Hosting;

namespace UniversalData.Management;

/// <summary>Identity versions and the Cookie key ring are encrypted Catalog documents.</summary>
internal sealed class ClusterHostState(ClusterCatalog catalog) : IXmlRepository
{
    internal static void EnsureInitialized(ClusterCatalog catalog)
    {
        string? document = catalog.Read(ClusterCatalog.SystemTenant, ClusterCatalog.IdentityId).Value;
        if (document is null)
            throw new InvalidDataException("Cluster identity is unavailable");
        IdentityDirectory.Parse(document);
    }

    internal IdentityDirectory Identities() => new(
        () => catalog.Read(ClusterCatalog.SystemTenant, ClusterCatalog.IdentityId),
        (document, revision) => catalog.Write(ClusterCatalog.SystemTenant, ClusterCatalog.IdentityId, document, revision));

    public IReadOnlyCollection<XElement> GetAllElements()
    {
        using var store = catalog.Open(ClusterCatalog.SystemTenant);
        var result = new List<XElement>();
        string after = ClusterCatalog.KeyPrefix;
        while (true)
        {
            using var page = JsonDocument.Parse(store.Query(JsonSerializer.Serialize(new
            {
                kind = (int)CatalogKind.Audit, afterId = after,afterRevision=ulong.MaxValue, latestOnly = true, limit = 100
            }, HostPolicy.JsonOptions)));
            if (page.RootElement.GetArrayLength() == 0) break;
            foreach (var row in page.RootElement.EnumerateArray())
            {
                after = row.GetProperty("id").GetString()!;
                if (!after.StartsWith(ClusterCatalog.KeyPrefix, StringComparison.Ordinal)) return result;
                result.Add(XElement.Parse(catalog.Decode(ClusterCatalog.SystemTenant, after, row.GetProperty("document").GetRawText())));
            }
        }
        return result;
    }

    public void StoreElement(XElement element, string friendlyName)
    {
        if (!HostIdentity.ValidId(friendlyName))
            throw new ArgumentException("Invalid Cookie key identity");
        catalog.Write(ClusterCatalog.SystemTenant, ClusterCatalog.KeyPrefix + friendlyName, element.ToString(SaveOptions.DisableFormatting));
    }
}
