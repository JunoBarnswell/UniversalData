using System.Text.Json;

namespace UniversalData;

public sealed record SqlParameterBatch(JsonElement Schema, IReadOnlyList<JsonElement> Rows);
