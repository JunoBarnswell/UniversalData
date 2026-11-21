using System.Text.Json;

namespace UniversalData;

internal sealed record SdkExecutionEnvironment(string Tenant, string JournalRoot, int MaxBranches,
    int MaxConnectionsPerSource, Func<string, ConnectionAccess, bool> Authorize, ConnectionPool Pool,
    Func<string, string, string>? Resolve = null, Func<string, ulong, bool>? CanBorrow = null,
    Func<string, CancellationToken, Task<IAsyncDisposable>>? ReserveConnection = null,
    ulong MemoryLimit = 268435456);
