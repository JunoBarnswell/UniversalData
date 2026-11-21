using SqlSugar;

namespace FluentAcceptance;

public sealed class NativeIdentityRow
{
    [SugarColumn(ColumnName = "physical_id")]
    public string PhysicalId { get; set; } = "";
}
