namespace UniversalData;

public sealed record AnalysisSource(string Id, string Name, string Kind, string? SnapshotId = null);
public sealed record AnalysisField(string Id, string Label, string PhysicalType, string SemanticRole,
    string ConfidenceBand, string Reason);
public sealed record AnalysisRule(string FieldId, string Label, string Role);
public sealed record AnalysisModel(string Grain, AnalysisRule[] Fields, string DateFormat = "iso",
    int OffsetMinutes = 0, string AmountFormat = "point", string? Currency = null,
    bool AllowEmptyAmount = true, bool ConfirmSingleCurrency = false, bool ConfirmLineAmount = false,
    AnalysisMetricDefinition[]? Metrics = null);

public sealed record AnalysisDataset(string Id, string Name, string Revision, string SourceName, string Kind,
    string? Sheet, AnalysisModel? Draft, string? PublishedReleaseId, AnalysisSavedReport? Report);
public sealed record AnalysisPreview(AnalysisField[] Fields, Dictionary<string, string?>[] Rows,
    string[] Sheets, string? Sheet, AnalysisModel? Suggestion);
public sealed record AnalysisDraftWrite(string ExpectedRevision, string Name, AnalysisModel Model, string? Sheet = null);

public sealed record AnalysisMeasure(string Id, string Label, string Calculation, string? FieldId = null, string? Unit = null);
public sealed record AnalysisRelease(string Id, string ModelRevisionId, AnalysisModel Model, AnalysisMeasure[] Measures,
    int Rows, DateTimeOffset ValidatedAt, DateTimeOffset DataAsOf, string SchemaFingerprint, string? SourceSnapshotId,
    string SourceName, AnalysisSavedReport StartingReport);
public sealed record AnalysisPublish(string OperationId, string ExpectedRevision, string? ExpectedReleaseId);
public sealed record AnalysisIssue(int Row, string FieldId, string Message);
public sealed record AnalysisPublication(string OperationId, string State, int ScannedRows = 0, int AffectedRows = 0,
    AnalysisIssue[]? Issues = null, AnalysisRelease? Release = null, string? Error = null);

public sealed record AnalysisDimension(string FieldId, string Level = "value");
public sealed record AnalysisFilter(string FieldId, string Operator, string? Value = null);
public sealed record AnalysisQuery(string ReleaseId, AnalysisDimension[] Dimensions, string[]? Measures = null,
    AnalysisFilter[]? Filters = null, string? SortMeasure = null, bool Descending = false, int Limit = 300);
public sealed record AnalysisCell(string?[] Keys, Dictionary<string, string?> Values);
public sealed record AnalysisGrouping(AnalysisDimension[] Dimensions, AnalysisCell[] Cells);
public sealed record AnalysisResult(string Context, string ReleaseId, AnalysisMeasure[] Measures,
    Dictionary<string, string?> Total, AnalysisGrouping[] Groups, bool Empty);
public sealed record AnalysisDrill(string Context, string?[]? Cell = null, string? Cursor = null);
public sealed record AnalysisDetails(string ReleaseId, AnalysisRule[] Fields, Dictionary<string, string?>[] Rows,
    string? NextCursor);
public sealed record AnalysisExport(string Context, string?[]? Cell = null, bool Details = false);
public sealed record AnalysisSavedReport(string Name, AnalysisQuery Query, string Chart, string? ChartMeasure = null);
public sealed record AnalysisReportWrite(string ExpectedRevision, AnalysisSavedReport Report);
