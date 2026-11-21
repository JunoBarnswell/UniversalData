namespace UniversalData;

public sealed record DataSourceInfo(string Name, DataSourceKind Kind, ulong Revision, bool Enabled);
