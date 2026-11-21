using SqlSugar;

namespace FluentAcceptance;

public sealed class OracleOrmRow
{
    [SugarColumn(IsPrimaryKey = true, ColumnName = "OWNER_ID", Length = 18, DecimalDigits = 0)]
    public decimal Owner { get; set; }

    [SugarColumn(IsPrimaryKey = true, ColumnName = "ENTRY_ID", Length = 18, DecimalDigits = 0)]
    public decimal Id { get; set; }

    [SugarColumn(ColumnName = "LABEL", IsNullable = true)]
    public string? Label { get; set; }
}
