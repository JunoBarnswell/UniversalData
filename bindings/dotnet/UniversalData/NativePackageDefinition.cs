using System.Text.Json;

namespace UniversalData;

internal sealed record NativePackageDefinition(string Json, JsonElement Document, bool RequiresJournal)
{
    internal static NativePackageDefinition Read(string document, ulong memoryLimit)
    {
        using var package = new NativePackage(document, memoryLimit);
        string canonical = package.ToJson();
        return new(canonical, JsonSerializer.Deserialize<JsonElement>(canonical), package.RequiresJournal);
    }
}
