using SqlSugar;

namespace ManagedSqlAcceptance;

[SugarTable("etl_rows")]
internal sealed class ManagedEtlRow
{
    [SugarColumn(ColumnName = "id", IsPrimaryKey = true)]
    public long Id { get; set; }

    [SugarColumn(ColumnName = "label")]
    public string Label { get; set; } = string.Empty;
}
