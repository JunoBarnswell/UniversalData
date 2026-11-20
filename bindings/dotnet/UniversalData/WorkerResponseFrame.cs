using System.Text.Json;

namespace UniversalData;

internal sealed record WorkerResponseFrame(int Revision, long Sequence, string Kind, StatusCode Code,
    JsonElement? Value = null, OperationError? Error = null, uint Capacity = 0,
    SdkRemoteFailure? BusinessError = null, DataRunReference? RecoveryReference = null)
{
    internal Result Result => new(Code, Error);
}

internal sealed record SdkRemoteFailure(string Type, string Message);
