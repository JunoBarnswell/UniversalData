using UniversalData;

namespace SqlSugar;

internal static class SqlResult
{
    internal static bool Expected(Exception error) => error is UniversalDataException or
        OperationCanceledException or ArgumentException or InvalidOperationException or InvalidCastException or
        NotSupportedException or OverflowException or IOException or InvalidDataException or DllNotFoundException;

    internal static Result Error(Exception error)
    {
        var status = error switch
        {
            UniversalDataException native => native.Code,
            OperationCanceledException => StatusCode.Cancelled,
            OverflowException => StatusCode.Overflow,
            DllNotFoundException => StatusCode.DriverUnavailable,
            InvalidDataException => StatusCode.Protocol,
            IOException => StatusCode.Io,
            _ => StatusCode.Invalid
        };
        return new(status, (error as UniversalDataException)?.Detail ??
            new(ErrorSource.Native, status, status.ToString()));
    }

    internal static Result<T> Run<T>(Func<T> operation)
    {
        try { return Result<T>.Success(operation()); }
        catch (Exception error) when (Expected(error)) { return Result<T>.Failure(Error(error)); }
    }

    internal static Result Run(Action operation)
    {
        try { operation(); return Result.Success; }
        catch (Exception error) when (Expected(error)) { return Error(error); }
    }
}
