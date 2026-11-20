using System.Text.Json.Serialization;

namespace UniversalData;

public readonly record struct Result<T>(StatusCode Code, T? Value, OperationError? Error = null)
{
    [JsonIgnore] public bool IsSuccess => Code == StatusCode.Ok;

    public static Result<T> Success(T value) => new(StatusCode.Ok, value);

    public static Result<T> Failure(Result result) => new(result.Code, default, result.Error);
}
