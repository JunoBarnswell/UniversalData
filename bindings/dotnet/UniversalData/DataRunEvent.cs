using System.Text.Json;

namespace UniversalData;

/// <summary>Typed projection of a Native execution event.</summary>
public sealed record DataRunEvent(JsonElement Document)
{
    public string PackageId => String("packageId");
    public string ExecutionId => String("executionId");
    public string TaskId => String("taskId");
    public string PipelineId => String("pipelineId");
    public string ComponentId => String("componentId");
    public ExecutionState State => EnumValue<ExecutionState>("state");
    public StatusCode Result => EnumValue<StatusCode>("result");
    public ExecutionEventKind Kind => EnumValue<ExecutionEventKind>("kind");
    public ulong Sequence => Number("sequence");
    public ulong TimestampMilliseconds => Number("timestampMilliseconds", "timestampMs", "timestamp");
    public string ErrorChain => String("errorChain");
    public string GetRawText() => Document.GetRawText();
    public bool TryGetProperty(string name, out JsonElement value) => Document.TryGetProperty(name, out value);

    internal static DataRunEvent Read(JsonElement value) => new(value.Clone());

    private string String(string name) => Document.TryGetProperty(name, out var value) &&
        value.ValueKind == JsonValueKind.String ? value.GetString() ?? "" : "";

    private ulong Number(params string[] names)
    {
        foreach (string name in names)
            if (Document.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.Number &&
                value.TryGetUInt64(out var number)) return number;
        return 0;
    }

    private T EnumValue<T>(string name) where T : struct, Enum => Document.TryGetProperty(name, out var value)
        ? value.ValueKind == JsonValueKind.Number && value.TryGetInt32(out var number) ? (T)(object)number :
          value.ValueKind == JsonValueKind.String && Enum.TryParse<T>(value.GetString(), true, out var parsed) ? parsed : default
        : default;
}
