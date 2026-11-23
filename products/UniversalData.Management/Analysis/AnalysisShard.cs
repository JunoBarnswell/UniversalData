namespace UniversalData.Management;

/// <summary>Immutable physical member of a release snapshot manifest.</summary>
internal sealed record AnalysisShard(string File, string ContentHash, int? Rows);
