using System.Text.Json;

namespace UniversalData;

public sealed class DataExecutionException : UniversalDataException
{
    public DataExecutionException(int status, string message, DataRunReference reference, OperationError? detail = null)
        : base(status, message, detail) => Reference = reference;
    public DataRunReference Reference { get; }
    public ExecutionMetrics? Metrics { get; init; }
}
