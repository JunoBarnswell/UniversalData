using System.Text.Json;

namespace UniversalData;

internal sealed record SdkHostOptions(int MaxConcurrentSessions, int MaxBranchesPerSession, int QueueCapacity,
    TimeSpan QueueTimeout, TimeSpan ExecutionTimeout, TimeSpan CancellationGrace, ulong MemoryPerSession,
    int MaxConnectionsPerSource, bool RequireIsolation);

internal sealed record SdkSourceBinding(DataSourceDefinition Definition, ulong Revision, bool Enabled);
internal sealed record SdkModule(string Path, string Sha256);
internal sealed record SdkMethod(string ModuleSha256, string DeclaringType, string Name,
    string[] ParameterTypes, bool HasArgument);
internal sealed record SdkInvocation(string RunId, SdkMethod Method, JsonElement? Argument,
    SdkSourceBinding[] Sources, SdkModule[] Modules, string? DefaultSource = null);
internal sealed record SdkRecoveryInvocation(DataRunReference Reference, SdkSourceBinding[] Sources);
internal sealed record SdkExecutionEnvironment(string Tenant, string JournalRoot, int MaxBranches,
    int MaxConnectionsPerSource, Func<string, ConnectionAccess, bool> Authorize,
    Func<string, string, string>? Resolve = null, Func<string, ulong, bool>? CanBorrow = null,
    Func<string, CancellationToken, Task<IAsyncDisposable>>? ReserveConnection = null);

public sealed record DataRunReference(string RunId, string? TransactionId = null, string? JournalTicket = null);
public enum DataRecoveryState { NoDecision, Committed, RolledBack, RecoveryRequired }
public sealed record DataTransactionRecovery(string TransactionId, DataRecoveryState State);
public sealed record DataRecoveryResult(DataRunReference Reference, DataRecoveryState State,
    IReadOnlyList<DataTransactionRecovery> Transactions);

public sealed class DataBusinessException : Exception
{
    public DataBusinessException(string remoteType, string message, DataRunReference reference)
        : base(message) { RemoteType = remoteType; Reference = reference; }
    public string RemoteType { get; }
    public DataRunReference Reference { get; }
}

public sealed class DataExecutionException : UniversalDataException
{
    public DataExecutionException(int status, string message, DataRunReference reference, OperationError? detail = null)
        : base(status, message, detail) => Reference = reference;
    public DataRunReference Reference { get; }
    public ExecutionMetrics? Metrics { get; init; }
}
