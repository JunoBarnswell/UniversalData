using SqlSugar;

namespace ManagedSqlAcceptance;

[SugarTable("managed_sql_orders")]
internal sealed class ManagedSqlOrder
{
    [SugarColumn(IsPrimaryKey = true)]
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public decimal Amount { get; set; }
}
