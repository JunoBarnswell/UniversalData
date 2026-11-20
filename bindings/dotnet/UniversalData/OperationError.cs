namespace UniversalData;

public enum ErrorSource
{
    Native = 0,
    Database = 1,
    Http = 2,
    Transport = 3,
    Worker = 4,
    Component = 5,
    Catalog = 6
}

public sealed record OperationError(ErrorSource Source, StatusCode Cause, string Message,
    long? NativeCode = null, string? SqlState = null, int? HttpStatus = null, int? ExitCode = null);
