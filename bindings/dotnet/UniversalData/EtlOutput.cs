namespace UniversalData;

internal sealed record EtlOutput(string Target, string Table, string Mode, EntityMapping Mapping, string[] Keys,
    string? Namespace = null);
