using SqlSugar;

[SugarTable("example_orders")]
sealed class ExampleOrder
{
    [SugarColumn(IsPrimaryKey = true)]
    public int Id { get; set; }

    public string Name { get; set; } = string.Empty;
    public decimal Amount { get; set; }
}
