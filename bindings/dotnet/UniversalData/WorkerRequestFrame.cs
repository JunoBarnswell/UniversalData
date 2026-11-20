using System.Text.Json;

namespace UniversalData;

internal sealed record WorkerRequestFrame(int Revision, long Sequence, string Operation, JsonElement Values);
