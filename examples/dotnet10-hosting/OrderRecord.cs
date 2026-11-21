using SqlSugar;

namespace UniversalData.Dotnet10Hosting;

[SugarTable("orders")]
public sealed class OrderRecord
{
    [SugarColumn(ColumnName = "id", IsPrimaryKey = true)]
    public int Id { get; init; }

    [SugarColumn(ColumnName = "shop_id")]
    public int ShopId { get; init; }

    [SugarColumn(ColumnName = "sequence")]
    public int Sequence { get; init; }

    [SugarColumn(ColumnName = "total_cents")]
    public long TotalCents { get; init; }
}
