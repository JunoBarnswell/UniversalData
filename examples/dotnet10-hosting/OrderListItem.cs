using System.Text.Json.Serialization;
using SqlSugar;

namespace UniversalData.Dotnet10Hosting;

public sealed class OrderListItem
{
    public int Id { get; init; }
    public int ShopId { get; init; }
    public int Sequence { get; init; }
    [JsonIgnore]
    public long TotalCents { get; init; }

    [SugarColumn(IsIgnore = true)]
    public decimal Total => TotalCents / 100m;
}
