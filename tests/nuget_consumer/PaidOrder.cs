using SqlSugar;

public sealed class PaidOrder
{
    [SugarColumn(ColumnName = "id", IsPrimaryKey = true)]
    public int Id { get; set; }
    [SugarColumn(ColumnName = "label")]
    public string? Label { get; set; }
    [SugarColumn(ColumnName = "amount", Length = 18, DecimalDigits = 3)]
    public decimal Amount { get; set; }
}
