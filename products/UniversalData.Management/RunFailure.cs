namespace UniversalData.Management;

public sealed record RunFailure(int Result,string Phase,string ErrorType, OperationError? Detail = null);
