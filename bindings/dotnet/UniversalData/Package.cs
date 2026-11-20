using System.Text.Json;

namespace UniversalData;

/// <summary>An immutable Package document shared by the Worker, management service and Studio.</summary>
public sealed class Package
{
    private readonly string document;
    private Package(string document) => this.document = document;

    public string ToJson() => document;

    public static Result<Package> FromJson(string document)
    {
        try
        {
            using var parsed = JsonDocument.Parse(document);
            var root = parsed.RootElement;
            if (root.ValueKind != JsonValueKind.Object ||
                !root.TryGetProperty("version", out var version) || !version.TryGetInt32(out int protocol) ||
                protocol != ProductInfo.Protocol)
                return Failure(StatusCode.Incompatible, "The Package protocol differs from the Worker protocol");
            if (!root.TryGetProperty("id", out var id) || id.ValueKind != JsonValueKind.String ||
                string.IsNullOrWhiteSpace(id.GetString()))
                return Failure(StatusCode.Schema, "The Package identity is missing");
            return Result<Package>.Success(new(document));
        }
        catch (Exception error) when (error is JsonException or ArgumentException)
        {
            return Failure(StatusCode.Parse, "Invalid Package JSON");
        }
    }

    private static Result<Package> Failure(StatusCode code, string message) =>
        new(code, null, new(ErrorSource.Native, code, message));
}
