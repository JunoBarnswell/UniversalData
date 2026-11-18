namespace UniversalData.Management;

internal sealed class WorkerFailureException(RunFailure failure) : Exception("Worker lifecycle failed") { public RunFailure Failure { get; }=failure; }
