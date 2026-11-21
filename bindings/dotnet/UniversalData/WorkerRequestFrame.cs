using System.Text.Json;

namespace UniversalData;

internal sealed record WorkerRequestFrame(int Revision, string SessionId, long Sequence, string Operation, JsonElement Values);
