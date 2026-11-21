using SqlSugar;

public sealed class Order
{
    [SugarColumn(ColumnName = "id")]
    public int Id { get; set; }
    [SugarColumn(ColumnName = "label")]
    public string? Label { get; set; }
    [SugarColumn(ColumnName = "amount", Length = 18, DecimalDigits = 3)]
    public decimal Amount { get; set; }
    [SugarColumn(ColumnName = "is_paid")]
    public bool IsPaid { get; set; }
}
