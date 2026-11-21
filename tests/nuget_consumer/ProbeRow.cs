using SqlSugar;

public sealed class ProbeRow
{
    [SugarColumn(ColumnName = "fixture_value")]
    public long Value { get; set; }
}
