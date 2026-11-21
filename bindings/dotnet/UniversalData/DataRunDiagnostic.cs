using System.Text.Json;

namespace UniversalData;

/// <summary>Typed projection of one Native diagnostic fact.</summary>
public sealed record DataRunDiagnostic(JsonElement Document)
{
    public string Id => String("id");
    public string Component => String("component");
    public string Message => String("message");
    public StatusCode Result => Document.TryGetProperty("result", out var value) &&
        value.ValueKind == JsonValueKind.Number && value.TryGetInt32(out var code)
            ? (StatusCode)code : StatusCode.Ok;
    public string GetRawText() => Document.GetRawText();
    public bool TryGetProperty(string name, out JsonElement value) => Document.TryGetProperty(name, out value);

    internal static DataRunDiagnostic Read(JsonElement value) => new(value.Clone());

    private string String(string name) => Document.TryGetProperty(name, out var value) &&
        value.ValueKind == JsonValueKind.String ? value.GetString() ?? "" : "";
}
