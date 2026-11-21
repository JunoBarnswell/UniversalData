using System.Text.Json;

namespace UniversalData;

/// <summary>Native binding/inspection result shared by SDK and CLI.</summary>
public sealed record JobValidationResult(JsonElement Document)
{
    public bool IsValid => !Document.TryGetProperty("result", out var result) ||
        result.ValueKind != JsonValueKind.Number || !result.TryGetInt32(out var code) || code == 0;
    public string PackageId => Document.TryGetProperty("id", out var id) ? id.GetString() ?? "" :
        Document.TryGetProperty("packageId", out var package) ? package.GetString() ?? "" : "";
    public IReadOnlyList<DataRunDiagnostic> Diagnostics => ReadDiagnostics(Document);
    public string GetRawText() => Document.GetRawText();
    public bool TryGetProperty(string name, out JsonElement value) => Document.TryGetProperty(name, out value);
    public JsonElement Raw => Document;

    internal static JobValidationResult Read(JsonElement value) => new(value.Clone());

    private static IReadOnlyList<DataRunDiagnostic> ReadDiagnostics(JsonElement document)
    {
        if (document.TryGetProperty("diagnostics", out var diagnostics) && diagnostics.ValueKind == JsonValueKind.Array)
            return diagnostics.EnumerateArray().Select(DataRunDiagnostic.Read).ToArray();
        if (document.TryGetProperty("errors", out var errors) && errors.ValueKind == JsonValueKind.Array)
            return errors.EnumerateArray().Select(DataRunDiagnostic.Read).ToArray();
        return [];
    }
}
