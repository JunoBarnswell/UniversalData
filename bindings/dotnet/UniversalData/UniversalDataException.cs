namespace UniversalData;

public sealed class UniversalDataException(int status, string message) : Exception(message)
{
    public int Status { get; } = status;
}
