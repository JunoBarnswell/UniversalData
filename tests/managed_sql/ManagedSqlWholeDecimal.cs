using SqlSugar;

namespace ManagedSqlAcceptance;

[SugarTable("managed_sql_whole_decimal")]
internal sealed class ManagedSqlWholeDecimal
{
    public int Id { get; set; }

    [SugarColumn(Length = 38, DecimalDigits = 0)]
    public decimal Amount { get; set; }
}
