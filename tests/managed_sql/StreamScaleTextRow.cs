using SqlSugar;

namespace ManagedSqlAcceptance;

public sealed class StreamScaleTextRow
{
    [SugarColumn(ColumnName = "id", IsPrimaryKey = true)]
    public long Id { get; set; }

    [SugarColumn(ColumnName = "label")]
    public string Label { get; set; } = "";

    [SugarColumn(ColumnName = "amount")]
    public string Amount { get; set; } = "";
}
