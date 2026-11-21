namespace UniversalData;

internal sealed class EtlDataFlowDefinition(string id)
{
    internal string Id { get; } = id;
    internal List<object> Components { get; } = [];
    internal List<object> Paths { get; } = [];
    private readonly Dictionary<string, Dictionary<string, System.Text.Json.Nodes.JsonNode?>> extensions = new(StringComparer.Ordinal);
    internal object? Delivery { get; set; }

    internal void Extend(string component, string property, object? value)
    {
        if (!extensions.TryGetValue(component, out var values))
            extensions.Add(component, values = new(StringComparer.Ordinal));
        values[property] = value is null ? null :
            System.Text.Json.JsonSerializer.SerializeToNode(value, EtlPackageBuilder.Json);
    }

    internal void SetComponentType(string component, string type) => Extend(component, "component", type);

    internal object Describe()
    {
        if (extensions.Count == 0) return new { Id, Components, Paths, Delivery };
        var components = Components.Select(component =>
        {
            var node = System.Text.Json.JsonSerializer.SerializeToNode(component, EtlPackageBuilder.Json)!.AsObject();
            string componentId = node["id"]?.GetValue<string>() ?? "";
            if (!extensions.TryGetValue(componentId, out var values)) return node;
            foreach (var (property, value) in values)
            {
                if (property == "component") node[property] = value?.DeepClone();
                else if (node["config"] is System.Text.Json.Nodes.JsonObject config)
                {
                    if (value is null) config.Remove(property);
                    else config[property] = value.DeepClone();
                }
            }
            return node;
        }).ToArray();
        return new { Id, Components = components, Paths, Delivery };
    }
}
