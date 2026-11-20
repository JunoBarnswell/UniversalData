using System.Globalization;
using SqlSugar;

[SugarTable("example_orders")]
public sealed class ExampleOrder
{
    [SugarColumn(IsPrimaryKey = true)]
    public int Id { get; set; }

    public string Name { get; set; } = string.Empty;
    [SugarColumn(Length = 18, DecimalDigits = 2)]
    public decimal Amount { get; set; }

    public static ExampleOrder Trim(ExampleOrder order) =>
        new() { Id = order.Id, Name = order.Name.Trim(), Amount = order.Amount };
}


[SugarTable("example_orders")]
public sealed class ExampleOrderText
{
    [SugarColumn(IsPrimaryKey = true)]
    public int Id { get; set; }

    public string Name { get; set; } = string.Empty;
    public string Amount { get; set; } = string.Empty;

    public static ExampleOrderText Trim(ExampleOrder order) =>
        new() { Id = order.Id, Name = order.Name.Trim(), Amount = order.Amount.ToString(CultureInfo.InvariantCulture) };
}
