using System.Text.Json;

namespace UniversalData;

public sealed record DataRecoveryResult(DataRunReference Reference, DataRecoveryState State,
    IReadOnlyList<DataTransactionRecovery> Transactions);
