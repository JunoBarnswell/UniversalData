using System.Text.Json;

namespace UniversalData;

public sealed record DataTransactionRecovery(string TransactionId, DataRecoveryState State);
