using System.Text.Json;

namespace UniversalData;

internal sealed record WorkerResponseFrame(int Revision, long Sequence, string Kind, StatusCode Code,
    JsonElement? Value = null, OperationError? Error = null, uint Capacity = 0)
{
    internal Result Result => new(Code, Error);
}
