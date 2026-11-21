namespace UniversalData;

internal sealed class EtlDataFlowDefinition(string id)
{
    internal string Id { get; } = id;
    internal List<object> Components { get; } = [];
    internal List<object> Paths { get; } = [];
    internal object? Delivery { get; set; }
    internal object Describe() => new { Id, Components, Paths, Delivery };
}
