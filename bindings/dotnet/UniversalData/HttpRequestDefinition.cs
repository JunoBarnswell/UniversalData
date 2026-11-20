namespace UniversalData;

public sealed record HttpRequestDefinition(string Connection, string Method = "GET", string? Body = null,
    string? ContentType = null, ulong? TimeoutMs = null, uint? ExpectedStatus = null,
    HttpHeaderDefinition[]? Headers = null, string[]? ResponseHeaders = null,
    string? StatusVariable = null, string? BodyVariable = null, string? HeadersVariable = null,
    string? CaConnection = null);
