using SqlSugar;

namespace ManagedSqlAcceptance;

[SugarTable("managed_sql_composite")]
internal sealed class ManagedSqlCompositeRow
{
    [SugarColumn(IsPrimaryKey = true)]
    public int GroupId { get; set; }

    [SugarColumn(IsPrimaryKey = true)]
    public int Id { get; set; }

    public string? Label { get; set; }
}
