namespace UniversalData;

internal sealed record WorkerBatchFrame(int Revision, long Sequence, StatusCode Code, bool Last,
    OperationError? Error = null);
