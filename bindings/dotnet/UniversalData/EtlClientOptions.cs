namespace UniversalData;

public sealed record EtlClientOptions(string PolicyFile, string Tenant = "local", string? WorkerPath = null);
