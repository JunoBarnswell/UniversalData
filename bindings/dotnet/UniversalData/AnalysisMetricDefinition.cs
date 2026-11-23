namespace UniversalData;

/// <summary>
/// A typed, persisted analysis metric definition. Base metrics use <see cref="FieldId"/>;
/// derived metrics reference other metric IDs through <see cref="Left"/> and <see cref="Right"/>.
/// </summary>
public sealed record AnalysisMetricDefinition(string Id, string Label, string Operation,
    string? FieldId = null, string? Left = null, string? Right = null, string? Unit = null);
