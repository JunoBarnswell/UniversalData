using System.Text.Json;

namespace UniversalData;

/// <summary>Typed projection of the Native execution progress document.</summary>
public sealed record DataRunProgress
{
    private readonly JsonElement document;

    internal DataRunProgress(JsonElement value) => document = value;

    public ulong InputRows => Number("inputRows");
    public ulong OutputRows => Number("outputRows");
    public ulong CommittedRows => Number("committedRows");
    public ulong RejectedRows => Number("rejectedRows");
    public ulong Batches => Number("batches");
    public ulong ConfirmedEpoch => Number("confirmedEpoch");
    public IReadOnlyList<DataRunProgressPipeline> Pipelines =>
        document.TryGetProperty("pipelines", out var value) && value.ValueKind == JsonValueKind.Array
            ? value.EnumerateArray().Select(static item => new DataRunProgressPipeline(item)).ToArray()
            : [];

    internal static DataRunProgress Read(JsonElement value) => new(value.Clone());

    private ulong Number(string name)
    {
        if (!document.TryGetProperty(name, out var value)) return 0;
        return value.ValueKind == JsonValueKind.Number && value.TryGetUInt64(out var number) ? number : 0;
    }
}

public sealed record DataRunProgressPipeline
{
    private readonly JsonElement Document;

    internal DataRunProgressPipeline(JsonElement value) => Document = value;

    public string Id => Document.TryGetProperty("id", out var value) ? value.GetString() ?? "" : "";
    public ulong SampledAtMilliseconds => Number("sampledAtMs");
    public ulong SampleWindowMilliseconds => Number("sampleWindowMs");
    public ulong ConfirmedEpoch => Number("confirmedEpoch");
    public ulong InputRows => Number("inputRows");
    public ulong OutputRows => Number("outputRows");
    public ulong CommittedRows => Number("committedRows");
    public IReadOnlyList<DataRunProgressNode> Nodes =>
        Document.TryGetProperty("nodes", out var value) && value.ValueKind == JsonValueKind.Array
            ? value.EnumerateArray().Select(static item => new DataRunProgressNode(item)).ToArray()
            : [];
    private ulong Number(string name) => Document.TryGetProperty(name, out var value) &&
        value.ValueKind == JsonValueKind.Number && value.TryGetUInt64(out var number) ? number : 0;
}

/// <summary>One component's bounded, numeric progress facts.</summary>
public sealed record DataRunProgressNode
{
    private readonly JsonElement Document;

    internal DataRunProgressNode(JsonElement value) => Document = value;

    public string Id => Document.TryGetProperty("id", out var value) ? value.GetString() ?? "" : "";
    public ulong ProcessedRows => Number("processedRows");
    public ulong ProducedRows => Number("producedRows");
    public ulong RejectedRows => Number("rejectedRows");
    public ulong CallbackMilliseconds => Number("callbackMs");
    public ulong IdleMilliseconds => Number("idleMs");
    public ulong StagedEpoch => Number("stagedEpoch");
    public long Watermark => Document.TryGetProperty("watermark", out var value) &&
        value.ValueKind == JsonValueKind.Number && value.TryGetInt64(out var number) ? number : 0;
    public bool Idle => Document.TryGetProperty("idle", out var value) && value.ValueKind == JsonValueKind.True;
    private ulong Number(string name) => Document.TryGetProperty(name, out var value) &&
        value.ValueKind == JsonValueKind.Number && value.TryGetUInt64(out var number) ? number : 0;
}
