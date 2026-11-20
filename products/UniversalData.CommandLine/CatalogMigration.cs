using System.Security.Cryptography;
using System.Text.Json;
using System.Xml.Linq;
using UniversalData.Hosting;

namespace UniversalData.CommandLine;

internal static class CatalogMigration
{
    internal sealed record TenantSnapshot(string Tenant, CatalogSnapshot Snapshot, int ProtectedParameters);
    internal sealed record Receipt(string Mode, TenantSnapshot[] Tenants, int CookieKeys);

    internal static Receipt Run(string mode,string policyPath,string connectionFile,string keyFile)
    {
        if (mode is not ("check" or "import" or "verify"))
            throw new ArgumentException("Use check, import, or verify for Catalog migration");
        var policy = HostPolicy.Load(Path.GetFullPath(policyPath));
        if (policy.Tenants is not { Count: > 0 } || policy.Tenants.ContainsKey(ClusterCatalog.SystemTenant) ||
            policy.IdentityFile is not { } identityFile)
            throw new ArgumentException("Migration requires explicit tenants and a source identity file");
        HostFiles.NoLinks(identityFile);
        string identity = File.ReadAllText(identityFile);
        var identities = IdentityDirectory.Parse(identity);
        if (identities.Users.Any(user => !policy.Tenants.ContainsKey(user.Tenant)))
            throw new ArgumentException("Source identity tenant is outside policy");
        ClusterCatalog? cluster = mode == "check" ? null :
            new(Path.GetFullPath(connectionFile),Path.GetFullPath(keyFile));
        using var authority = mode == "import" ? cluster!.Open(ClusterCatalog.SystemTenant,election:true,initialize:true) : null;
        var lease = authority?.TryAcquireLease(CatalogKind.Audit,ClusterCatalog.LeadershipId,
            "migration." + Guid.NewGuid().ToString("N"),300000);
        if (mode == "import" && lease is null)
            throw new UniversalDataException((int)StatusCode.Busy,"Management must be stopped before migration");
        using var pinned = lease is null ? null : cluster!.Pin(lease);
        try
        {
            var tenants = new List<TenantSnapshot>();
            foreach (string tenant in policy.Tenants.Keys.Order(StringComparer.Ordinal))
            {
                if (lease is not null) authority!.RenewLease(lease,300000);
                tenants.Add(MigrateTenant(mode,policy.Bind(HostIdentity.Operator(tenant)),cluster));
            }
            CheckOrStore(cluster,mode,ClusterCatalog.SystemTenant,ClusterCatalog.IdentityId,identity);
            string keys = Path.Combine(policy.DataDirectory,"authentication","keys");
            HostFiles.NoLinks(keys);
            int count = 0;
            if (Directory.Exists(keys))
            {
                foreach (string file in Directory.EnumerateFiles(keys,"*.xml").Order(StringComparer.Ordinal))
                {
                    HostFiles.NoLinks(file);
                    string name = Path.GetFileNameWithoutExtension(file);
                    if (!HostIdentity.ValidId(name)) throw new InvalidDataException("Invalid Cookie key filename");
                    string document = XElement.Load(file).ToString(SaveOptions.DisableFormatting);
                    CheckOrStore(cluster,mode,ClusterCatalog.SystemTenant,ClusterCatalog.KeyPrefix + name,document);
                    count++;
                }
            }
            return new(mode,tenants.ToArray(),count);
        }
        finally
        {
            if (lease is not null)
                try { authority!.ReleaseLease(lease); }
                catch (UniversalDataException error) when (error.Code == StatusCode.Fenced) { }
        }
    }

    private static TenantSnapshot MigrateTenant(string mode,HostPolicy sourcePolicy,ClusterCatalog? cluster)
    {
        string directory = sourcePolicy.CatalogDirectory;
        string tenant = sourcePolicy.Tenant;
        if (!File.Exists(Path.Combine(directory,"catalog.sqlite3")))
            return new(tenant,new(0,Convert.ToHexString(SHA256.HashData([])).ToLowerInvariant()),0);
        using var source = new Catalog(directory,tenant,(scope,resource,_) => scope == tenant && resource == directory,
            createDirectory:false);
        var snapshot = source.CheckSnapshot();
        var vault = new ParameterVault(sourcePolicy);
        var references = References(source);
        foreach (string reference in references)
            CheckOrStore(cluster,mode,tenant,"protected/" + reference,vault.Resolve(reference));
        if (cluster is not null)
        {
            using var target = cluster.Open(tenant);
            if (mode == "import" && source.CheckSnapshot(target,import:true) != snapshot ||
                source.CheckSnapshot(target) != snapshot)
                throw new UniversalDataException((int)StatusCode.Corrupt,"Catalog snapshot verification differs");
        }
        return new(tenant,snapshot,references.Count);
    }

    private static HashSet<string> References(Catalog source)
    {
        var references = new HashSet<string>(StringComparer.Ordinal);
        foreach (string prefix in new[] { "job/","run/" })
        {
            string after = prefix;
            ulong revision = 0;
            bool more = true;
            while (more)
            {
                using var page = JsonDocument.Parse(source.Query(JsonSerializer.Serialize(new
                { kind=(int)CatalogKind.Audit,afterId=after,afterRevision=revision,limit=100 },HostPolicy.JsonOptions)));
                if (page.RootElement.GetArrayLength() == 0) break;
                foreach (var row in page.RootElement.EnumerateArray())
                {
                    after = row.GetProperty("id").GetString()!;
                    if (!after.StartsWith(prefix,StringComparison.Ordinal)) { more = false; break; }
                    revision = row.GetProperty("revision").GetUInt64();
                    var body = row.GetProperty("document");
                    if (body.TryGetProperty("parameters",out _))
                        throw new InvalidDataException("Migrate historical parameters into the vault before catalog migrate");
                    foreach (string name in new[] { "parametersReference","controlReference" })
                        if (body.TryGetProperty(name,out var value) && value.GetString() is { Length: > 0 } reference)
                            references.Add(reference);
                }
            }
        }
        return references;
    }

    private static void CheckOrStore(ClusterCatalog? cluster,string mode,string tenant,string id,string value)
    {
        if (cluster is null) return;
        var (existing,_) = cluster.Read(tenant,id);
        if (existing == value) return;
        if (existing is not null || mode != "import")
            throw new UniversalDataException((int)StatusCode.Corrupt,"Protected Catalog document differs from the source");
        cluster.Write(tenant,id,value);
    }
}
