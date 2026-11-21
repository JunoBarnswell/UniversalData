using System.Text.Json.Serialization;
using SqlSugar;

namespace UniversalData.Dotnet10Hosting;

public sealed class OrderWriteRequest
{
    public int Id { get; init; }
    public int ShopId { get; init; }
    public int Sequence { get; init; }
    [SugarColumn(IsIgnore = true)]
    public decimal Total { get; init; }

    [JsonIgnore]
    public long TotalCents => decimal.ToInt64(Total * 100m);
}
