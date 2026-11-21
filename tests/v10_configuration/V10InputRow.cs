using SqlSugar;

public sealed class V10InputRow
{
    [SugarColumn(ColumnName = "id", IsPrimaryKey = true)]
    public int Id { get; set; }

    [SugarColumn(ColumnName = "value")]
    public int Value { get; set; }
}
