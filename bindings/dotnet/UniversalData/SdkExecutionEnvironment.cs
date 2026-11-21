namespace UniversalData;

internal sealed record SdkExecutionEnvironment(string Tenant, Func<string, ConnectionAccess, bool> Authorize,
    ConnectionPool Pool, uint ConnectionWaitMilliseconds, Func<string, string, string>? Resolve = null,
    Func<string, ulong, bool>? CanBorrow = null);
