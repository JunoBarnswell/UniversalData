using System.Text.Json.Serialization;

namespace UniversalData.Hosting;

/// <summary>Coordinates the consumer lanes assigned to one execution member.</summary>
public sealed record WorkerShard(uint Member, uint Members, uint Lanes,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] string? RunId = null);
