using System.Text.Json;

namespace UniversalData;

/// <summary>An immutable Package document shared by the Worker, management service and Studio.</summary>
public sealed class Package
{
    private readonly string document;
    private Package(string document)
    {
        this.document = document;
        Definition = JsonSerializer.Deserialize<JsonElement>(document);
    }

    public string ToJson() => document;
    internal JsonElement Definition { get; }

    public static Result<Package> FromJson(string document)
    {
        try
        {
            using var package = new NativePackage(document);
            return Result<Package>.Success(new(package.ToJson()));
        }
        catch (UniversalDataException error)
        {
            return new(error.Code, null, error.Detail ?? new(ErrorSource.Native, error.Code, error.Message));
        }
    }
}
