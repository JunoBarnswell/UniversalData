using System.Text.Json.Serialization;

namespace UniversalData;

public readonly record struct Result(StatusCode Code, OperationError? Error = null)
{
    [JsonIgnore] public bool IsSuccess => Code == StatusCode.Ok;
    public static Result Success => new(StatusCode.Ok);
}
