namespace UniversalData;

public sealed record HttpHeaderDefinition(string Name, string? Value = null, string? Variable = null,
    string? CredentialReference = null);
