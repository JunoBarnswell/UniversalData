namespace UniversalData;

public sealed record OAuth2Connection(string TokenEndpoint, string ClientId, string CredentialReference,
    string? Scope = null, string Authentication = "basic");
