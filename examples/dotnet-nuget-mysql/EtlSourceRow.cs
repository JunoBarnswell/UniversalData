using SqlSugar;

[SugarTable("ud_demo_etl_source")]
internal sealed class EtlSourceRow
{
    [SugarColumn(IsPrimaryKey = true)]
    public string RunId { get; init; } = string.Empty;

    [SugarColumn(IsPrimaryKey = true)]
    public int Id { get; init; }

    public string Name { get; init; } = string.Empty;
    public decimal Amount { get; init; }
}
