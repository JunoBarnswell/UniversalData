using SqlSugar;

public sealed class StoredPaidOrder
{
    [SugarColumn(ColumnName = "id", IsPrimaryKey = true)]
    public int Id { get; set; }

    [SugarColumn(ColumnName = "label")]
    public string? Label { get; set; }

    [SugarColumn(ColumnName = "amount")]
    public string Amount { get; set; } = "";
}
