using System.Text.Json;
using System.Text.Json.Nodes;

namespace UniversalData;

internal sealed class SqlTableQueryParameters
{
    private readonly JsonArray fields = [];
    private readonly JsonObject row = [];

    internal JsonObject Literal(JsonObject literal)
    {
        var field = new JsonObject { ["type"] = literal["type"]!.DeepClone(),
            ["nullable"] = literal["literal"] is null };
        if (literal["precision"] is { } precision) field["precision"] = precision.DeepClone();
        if (literal["scale"] is { } scale) field["scale"] = scale.DeepClone();
        return Add(field, literal["literal"]?.DeepClone());
    }

    internal JsonObject Parameter(EntityMember member)
    {
        var field = new JsonObject { ["type"] = SqlValueCodec.NativeTypeName(member.NativeType),
            ["nullable"] = member.Nullable };
        if (member.NativeType == DataType.Decimal128)
        {
            field["precision"] = member.Precision;
            field["scale"] = member.Scale;
        }
        return Add(field, new JsonObject { ["$parameter"] = member.Property.Name });
    }

    private JsonObject Add(JsonObject field, JsonNode? value)
    {
        int ordinal = fields.Count;
        string name = "argument" + ordinal.ToString(System.Globalization.CultureInfo.InvariantCulture);
        field["name"] = name;
        fields.Add(field);
        row.Add(name, value);
        return new() { ["param"] = ordinal };
    }

    internal SqlParameterBatch? Build() => fields.Count == 0 ? null : new(
        JsonSerializer.SerializeToElement(new JsonObject { ["version"] = ProductInfo.Protocol, ["fields"] = fields }),
        [JsonSerializer.SerializeToElement(row)]);
}
