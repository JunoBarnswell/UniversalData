using SqlSugar;

public sealed class V10OutputRow
{
    [SugarColumn(ColumnName = "id")]
    public int Id { get; set; }

    [SugarColumn(ColumnName = "value")]
    public int Value { get; set; }
}
