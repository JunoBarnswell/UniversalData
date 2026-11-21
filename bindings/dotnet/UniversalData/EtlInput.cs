namespace UniversalData;

internal sealed record EtlInput(string Id, string Source, Type Type, string? Table = null, string? Sql = null,
    SqlParameterBatch? Parameters = null, bool Stream = false, uint? BatchSize = null, SdkMethod? Map = null,
    EtlChangeOptions? Changes = null, string? IcebergNamespace = null, long? SnapshotId = null);
