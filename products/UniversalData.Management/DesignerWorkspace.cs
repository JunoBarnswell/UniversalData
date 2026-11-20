using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using UniversalData.Hosting;

namespace UniversalData.Management;

public sealed record DraftWrite(JsonElement Document, string ExpectedRevision, string BaseRevision = "0");
public sealed record PublicationRequest(JsonElement Document, string ExpectedPackageRevision);
public sealed record PreviewRequest(JsonElement Package, string PipelineId, string ComponentId, uint SourceRows = 1000, uint SampleRows = 32);
public sealed record FileInspectionRequest(JsonElement Package, string Connection, string Format, JsonElement? Options = null);
public sealed record MetadataRequest(JsonElement Package, string Connection, int Kind = 3, string Parent = "", ulong Offset = 0);
public sealed record TestRequest(JsonElement Package, string TargetConnection, Dictionary<string,Dictionary<string,string>> Tables);

/// <summary>Catalog owns versions and draft concurrency. Workers own every Native
/// discovery/validation/preview. Publication never dispatches execution.</summary>
public static class DesignerWorkspace
{
    private static readonly SemaphoreSlim uploads = new(1,1);
    public static JsonObject InspectionPackage(JsonElement package,bool preserveCaches=false) {
        var value = JsonNode.Parse(package.GetRawText())!.AsObject();
        value["controlFlow"] = new JsonObject { ["tasks"] = new JsonArray(), ["constraints"] = new JsonArray() };
        value["dataFlows"] = new JsonArray(); value.Remove("layout"); value.Remove("eventHandlers"); value.Remove("policy");value.Remove("extensions");if(!preserveCaches)value.Remove("caches");
        // Native preview binds the selected component's dependencies using the built-in registry.
        return value;
    }
    private static string Digest(string value)=>Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();
    private static string DraftPrefix(HostIdentity identity)=>"designer-draft/"+Digest(identity.Subject)+"/";
    private static string DraftId(HostIdentity identity,string id) => DraftPrefix(identity)+Digest(id);
    public static JsonElement[] ListDrafts(HostPolicy policy,HostIdentity identity) {
        string prefix=DraftPrefix(identity);using var catalog=policy.OpenCatalog();
        using var rows=JsonDocument.Parse(catalog.Query(JsonSerializer.Serialize(new {kind=(int)CatalogKind.Audit,afterId=prefix,latestOnly=true,limit=1000},HostPolicy.JsonOptions)));
        return rows.RootElement.EnumerateArray().TakeWhile(row=>row.GetProperty("id").GetString()!.StartsWith(prefix,StringComparison.Ordinal))
            .Select(row=>row.GetProperty("document")).Where(row=>row.GetProperty("subject").GetString()==identity.Subject&&identity.Allows("package.read",row.GetProperty("packageId").GetString()!)).Select(row=>row.Clone()).ToArray();
    }
    public static object? ReadDraft(HostPolicy policy,HostIdentity identity,string id) {
        identity.Require("package.read",id);using var catalog=policy.OpenCatalog();string key=DraftId(identity,id);
        ulong revision=catalog.LatestRevision(CatalogKind.Audit,key);if(revision==0)return null;
        using var doc=JsonDocument.Parse(catalog.Get(CatalogKind.Audit,key,revision)!);
        return doc.RootElement.Clone();
    }
    public static object WriteDraft(HostPolicy policy,HostIdentity identity,string id,DraftWrite request) {
        identity.Require("package.write",id);
        if(request.Document.GetProperty("id").GetString()!=id)throw new ArgumentException("Package identity differs");
        if(request.Document.GetProperty("version").GetInt32()!=ProductInfo.Protocol || request.Document.GetProperty("dataFlows").ValueKind!=JsonValueKind.Array)throw new ArgumentException("Incompatible draft");
        string text=request.Document.GetRawText(),key=DraftId(identity,id);
        ulong expected=ulong.Parse(request.ExpectedRevision);string hash=Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(text))).ToLowerInvariant();
        var record=new {version=ProductInfo.Protocol,id=key,packageId=id,subject=identity.Subject,revision=(expected+1).ToString(),baseRevision=request.BaseRevision,hash,document=JsonSerializer.Deserialize<JsonElement>(text),timestampMs=DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()};
        using var catalog=policy.OpenCatalog();try{catalog.CompareExchange(CatalogKind.Audit,key,expected,JsonSerializer.Serialize(record,HostPolicy.JsonOptions));}catch(UniversalDataException error)when(error.Code is StatusCode.Busy or StatusCode.Fenced){throw new UniversalDataException((int)StatusCode.Fenced,"Draft revision changed");}return record;
    }
    public static object SaveVersion(HostPolicy policy,HostIdentity identity,JsonElement document,string expected) {
        string id=document.GetProperty("id").GetString()!;identity.Require("package.write",id);
        using var package=new NativePackage(document.GetRawText());string text=package.ToJson();ulong revision=ulong.Parse(expected);
        using var catalog=policy.OpenCatalog();try{catalog.CompareExchange(CatalogKind.Package,id,revision,text);}catch(UniversalDataException error)when(error.Code is StatusCode.Busy or StatusCode.Fenced){throw new UniversalDataException((int)StatusCode.Fenced,"Package revision changed");}
        return new {id,revision=(revision+1).ToString(),subject=identity.Subject,document=JsonSerializer.Deserialize<JsonElement>(text)};
    }
    public static async Task<object> Upload(HostPolicy policy,HttpRequest request,CancellationToken cancellation) {
        policy.Identity.Require("resource.upload","*");
        string name=Path.GetFileName(Uri.UnescapeDataString(request.Headers["X-File-Name"].ToString()));
        string extension=Path.GetExtension(name).ToLowerInvariant();
        if(extension is not (".csv" or ".xlsx") || request.ContentLength is > 67108864)throw new ArgumentException("CSV/XLSX upload exceeds the 64 MiB limit");
        string root=policy.FileRoots.FirstOrDefault() ?? throw new UniversalDataException((int)StatusCode.Auth,"No import directory is configured");
        string directory=Path.Combine(root,".ud-imports",policy.Tenant,policy.Identity.Subject);
        HostFiles.NoLinks(directory);string path=Path.Combine(directory,Guid.NewGuid().ToString("N")+extension);
        if(!policy.Authorize(policy.Tenant,path,ConnectionAccess.Write))throw new UniversalDataException((int)StatusCode.Auth,"Import directory is outside the authenticated scope");
        await uploads.WaitAsync(cancellation);
        string temporary=path+".upload";
        try {
            Directory.CreateDirectory(directory);if(!OperatingSystem.IsWindows())File.SetUnixFileMode(directory,UnixFileMode.UserRead|UnixFileMode.UserWrite|UnixFileMode.UserExecute);
            long used=Directory.EnumerateFiles(directory).Sum(file=>new FileInfo(file).Length);
            if(used>=536870912||Directory.EnumerateFiles(directory).Take(513).Count()>=512)throw new IOException("Import quota exceeded");
            long size=0;byte[] buffer=new byte[65536];
            var options=new FileStreamOptions { Mode=FileMode.CreateNew,Access=FileAccess.Write,Share=FileShare.None,Options=FileOptions.Asynchronous };
            if(!OperatingSystem.IsWindows())options.UnixCreateMode=UnixFileMode.UserRead|UnixFileMode.UserWrite;
            await using(var target=new FileStream(temporary,options)) {
                for(int read;(read=await request.Body.ReadAsync(buffer,cancellation))!=0;) {
                    size+=read;if(size>67108864||used+size>536870912)throw new IOException("Import quota exceeded");await target.WriteAsync(buffer.AsMemory(0,read),cancellation);
                }
                await target.FlushAsync(cancellation);target.Flush(true);
            }
            if(size==0)throw new ArgumentException("Empty import");
            File.Move(temporary,path);return new {name,resource=path,size,format=extension==".xlsx"?"xlsx":"csv"};
        } finally {if(File.Exists(temporary))File.Delete(temporary);uploads.Release();}
    }
    public static JsonObject IsolatedTest(HostPolicy policy,HostIdentity identity,TestRequest request) {
        string id=request.Package.GetProperty("id").GetString()!;identity.Require("package.test",id);
        var package=JsonNode.Parse(request.Package.GetRawText())!.AsObject();
        if(package.ContainsKey("extensions")||package.ContainsKey("eventHandlers"))throw new ArgumentException("Isolated tests cannot load modules or event handlers");
        var tasks=package["controlFlow"]!["tasks"]!.AsArray();
        if(tasks.Any(task=>task!["kind"]!.GetValue<string>()!="dataFlow"))throw new ArgumentException("Isolated tests require data-flow tasks");
        var connections=package["connections"]!.AsArray();var selected=connections.Single(connection=>connection!["id"]!.GetValue<string>()==request.TargetConnection)!;
        string resource=selected["resource"]!.GetValue<string>();
        if(!(policy.TestResources??[]).Contains(resource,StringComparer.Ordinal))throw new UniversalDataException((int)StatusCode.Auth,"Target is not a host-approved isolated test resource");
        identity.Require("resource.write",resource);
        var targets=package["dataFlows"]!.AsArray().SelectMany(flow=>flow!["components"]!.AsArray().Select(node=>(Flow:flow["id"]!.GetValue<string>(),Node:node)))
            .Where(target=>target.Node!["component"]!.GetValue<string>().EndsWith("destination",StringComparison.Ordinal)).ToArray();
        var destinations=targets.Select(target=>target.Node).ToArray();
        if(destinations.Length==0||destinations.Any(node=>node!["component"]!.GetValue<string>()!="sql.destination"))throw new ArgumentException("Isolated tests require SQL destinations");
        foreach(var node in package["dataFlows"]!.AsArray().SelectMany(flow=>flow!["components"]!.AsArray()))
            if(node!["component"]!.GetValue<string>() is "sql.command" or "importColumn" or "exportColumn" or "cacheBuild" or "mq.source" or "mq.destination")throw new ArgumentException("Unsupported test side effect");
        var targetIds=destinations.Select(node=>node!["config"]!["connection"]!.GetValue<string>()).ToHashSet(StringComparer.Ordinal);
        var otherReaders=package["dataFlows"]!.AsArray().SelectMany(flow=>flow!["components"]!.AsArray()).Where(node=>!destinations.Contains(node));
        if(otherReaders.Any(node=>node!["config"]?["connection"] is JsonValue connection && targetIds.Contains(connection.GetValue<string>())))
            throw new ArgumentException("Use separate source and destination connections for isolated testing");
        var replacements=targetIds.ToDictionary(id=>id,id=>"test-target-"+Digest(id)[..16],StringComparer.Ordinal);
        foreach(var original in connections.Where(node=>targetIds.Contains(node!["id"]!.GetValue<string>())).ToArray()) {
            var connection=original!.DeepClone();connection["id"]=replacements[original["id"]!.GetValue<string>()];
            foreach(string key in new[]{"driver","resource","credentialReference"})connection[key]=selected[key]?.DeepClone();
            connections.Add(connection);
        }
        void RebindTransaction(JsonNode? transaction) {
            if(transaction?["participants"] is not JsonArray participants)return;
            for(int i=0;i<participants.Count;i++) {
                string original=participants[i]!.GetValue<string>();
                if(!replacements.TryGetValue(original,out string? target))throw new ArgumentException("Test transaction participants must be isolated destinations");
                participants[i]=target;
            }
        }
        RebindTransaction(package["policy"]?["transaction"]);
        foreach(var task in tasks)RebindTransaction(task?["transaction"]);
        foreach(var target in targets){var destination=target.Node!;string component=destination["id"]!.GetValue<string>();if(!request.Tables.TryGetValue(target.Flow,out var tables)||!tables.TryGetValue(component,out var table)||string.IsNullOrWhiteSpace(table))throw new ArgumentException("Every test destination needs an explicit test table");destination["config"]!["table"]=table;destination["config"]!["connection"]=replacements[destination["config"]!["connection"]!.GetValue<string>()];}
        return package;
    }
}
