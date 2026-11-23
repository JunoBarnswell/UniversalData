namespace UniversalData;

public sealed record AnalysisPage<T>(
    T[] Items, string? NextCursor);
