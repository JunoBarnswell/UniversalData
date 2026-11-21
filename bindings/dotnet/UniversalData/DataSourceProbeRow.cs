using SqlSugar;

namespace UniversalData;

internal sealed class DataSourceProbeRow
{
    [SugarColumn(ColumnName = "value")]
    public long Value { get; set; }
}
