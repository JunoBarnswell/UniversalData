using System.Text.Json;
using UniversalData.Hosting;

namespace UniversalData.CommandLine;

internal static class JobUpgradeAudit
{
    internal static int Run(string policyFile, string tenant)
    {
        var policy = HostPolicy.Load(Path.GetFullPath(policyFile));
        if (policy.Tenants is not null) policy = policy.Bind(HostIdentity.Operator(tenant));
        else if (tenant != policy.Tenant) throw new ArgumentException("The audit tenant differs from the host policy");
        using var catalog = policy.OpenCatalog();
        using var output = new Utf8JsonWriter(Console.OpenStandardOutput(),
            new JsonWriterOptions { Indented = true });
        output.WriteStartArray();
        string after = "";
        ulong afterRevision = 0;
        while (true)
        {
            using var page = JsonDocument.Parse(catalog.Query(JsonSerializer.Serialize(new
            {
                kind = (int)CatalogKind.Package, afterId = after, afterRevision, limit = 100
            }, HostPolicy.JsonOptions)));
            if (page.RootElement.GetArrayLength() == 0) break;
            foreach (var row in page.RootElement.EnumerateArray())
            {
                after = row.GetProperty("id").GetString()!;
                afterRevision = row.GetProperty("revision").GetUInt64();
                var document = row.GetProperty("document");
                var managed = new List<string>();
                Inspect(document, managed);
                if (managed.Count == 0) continue;
                output.WriteStartObject();
                output.WriteString("id", after);
                output.WritePropertyName("revision");
                row.GetProperty("revision").WriteTo(output);
                output.WritePropertyName("steps");
                JsonSerializer.Serialize(output, managed.Distinct(StringComparer.Ordinal).ToArray());
                output.WriteEndObject();
            }
        }
        output.WriteEndArray();
        return 0;
    }

    private static void Inspect(JsonElement value, List<string> managed)
    {
        if (value.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in value.EnumerateArray()) Inspect(item, managed);
            return;
        }
        if (value.ValueKind != JsonValueKind.Object) return;
        if (value.TryGetProperty("extensions", out var extensions) && extensions.ValueKind == JsonValueKind.Array &&
            extensions.EnumerateArray().Any(item => item.TryGetProperty("kind", out var kind) &&
                kind.ValueKind == JsonValueKind.String && kind.GetString() == "managedAssembly"))
            managed.Add("Managed extensions");
        string? component = value.TryGetProperty("component", out var type) && type.ValueKind == JsonValueKind.String
            ? type.GetString() : null;
        if (component == "sdk.map" || value.TryGetProperty("moduleSha256", out _))
            managed.Add(value.TryGetProperty("id", out var id) && id.ValueKind == JsonValueKind.String
                ? id.GetString()! : "Managed business method");
        foreach (var property in value.EnumerateObject()) Inspect(property.Value, managed);
    }
}
