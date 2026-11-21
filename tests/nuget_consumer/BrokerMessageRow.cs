using SqlSugar;

[SugarTable("nuget_mq_inbox")]
public sealed class BrokerMessageRow
{
    [SugarColumn(ColumnName = "id", IsPrimaryKey = true)]
    public long Id { get; set; }

    [SugarColumn(ColumnName = "label", IsNullable = true)]
    public string? Label { get; set; }
}
