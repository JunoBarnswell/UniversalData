using System.Text.Json;
using System.Text.Json.Nodes;

namespace UniversalData.Hosting;

/// <summary>Product preview/inspection never returns parameter string values. Native receives the unchanged typed parameters.</summary>
public sealed class SecretFilter
{
    private readonly string[] values;
    public SecretFilter(string parameters)
    {
        using var document = JsonDocument.Parse(parameters); var found = new HashSet<string>(StringComparer.Ordinal);
        void Collect(JsonElement value) {
            if (value.ValueKind == JsonValueKind.String && value.GetString() is { Length: > 0 } text) found.Add(text);
            else if (value.ValueKind == JsonValueKind.Object) foreach(var property in value.EnumerateObject()) Collect(property.Value);
            else if (value.ValueKind == JsonValueKind.Array) foreach(var item in value.EnumerateArray()) Collect(item);
        }
        Collect(document.RootElement); values = found.OrderByDescending(value => value.Length).ToArray();
    }
    public string Text(string value)
    {
        foreach(string secret in values) if (value == secret || secret.Length >= 8 && value.Contains(secret,StringComparison.Ordinal))
            value = value.Replace(secret,"[redacted]",StringComparison.Ordinal);
        return value;
    }
    public string Json(string json)
    {
        var root = JsonNode.Parse(json);
        JsonNode? Visit(JsonNode? node) {
            if (node is JsonValue scalar && scalar.TryGetValue<string>(out var text)) return JsonValue.Create(Text(text));
            if (node is JsonObject obj) foreach(string key in obj.Select(pair => pair.Key).ToArray()) { var child = obj[key]; var next = Visit(child); if (!ReferenceEquals(child,next)) obj[key] = next; }
            else if (node is JsonArray array) for(int at = 0; at < array.Count; at++) { var child = array[at]; var next = Visit(child); if (!ReferenceEquals(child,next)) array[at] = next; }
            return node;
        }
        return Visit(root)?.ToJsonString() ?? "null";
    }
}
