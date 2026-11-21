using SqlSugar;

namespace ManagedSqlAcceptance;

public sealed class WorkerFailureCount
{
    [SugarColumn(ColumnName = "row_count")]
    public long Count { get; init; }
}
