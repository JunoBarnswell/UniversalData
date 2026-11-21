using SqlSugar;

namespace ManagedSqlAcceptance;

public sealed class StreamScaleRow
{
    [SugarColumn(ColumnName = "id", IsPrimaryKey = true)]
    public long Id { get; set; }

    [SugarColumn(ColumnName = "label")]
    public string Label { get; set; } = "";

    [SugarColumn(ColumnName = "amount", Length = 18, DecimalDigits = 2)]
    public decimal Amount { get; set; }
}
