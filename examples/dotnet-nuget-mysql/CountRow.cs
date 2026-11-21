using SqlSugar;

[SugarTable("count_row")]
public sealed class CountRow
{
    [SugarColumn(ColumnName = "row_count")]
    public long Count { get; init; }
}
