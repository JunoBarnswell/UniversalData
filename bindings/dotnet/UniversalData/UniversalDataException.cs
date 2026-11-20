namespace UniversalData;

public class UniversalDataException(int status, string message, OperationError? detail = null) : Exception(message)
{
    public OperationError? Detail { get; } = detail;
    public int Status { get; } = status;
    public StatusCode Code => (StatusCode)Status;
}
