namespace SqlSugar;

internal sealed class SqlQueryPlan
{
    internal required string QueryJson { get; init; }
    internal required string SchemaJson { get; init; }
    internal required IReadOnlyList<SugarParameter> Parameters { get; init; }
}
