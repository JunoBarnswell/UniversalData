using SqlSugar;

[SugarTable("ud_demo_etl_target")]
public sealed class EtlTargetRow
{
    public string RunId { get; init; } = string.Empty;

    [SugarColumn(IsPrimaryKey = true)]
    public int Id { get; init; }

    public string Name { get; init; } = string.Empty;
    [SugarColumn(Length = 38, DecimalDigits = 28)]
    public decimal Amount { get; init; }
}
