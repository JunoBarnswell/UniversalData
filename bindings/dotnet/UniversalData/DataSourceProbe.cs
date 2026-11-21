namespace UniversalData;

public sealed record DataSourceProbe(DataSourceInfo Source, bool Connected, IReadOnlyList<string> Capabilities);
