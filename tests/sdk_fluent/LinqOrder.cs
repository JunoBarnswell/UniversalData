using SqlSugar;

namespace FluentAcceptance;

public sealed record LinqOrder<T>(
    [property: SugarColumn(ColumnName = "entry_id", Length = 18, DecimalDigits = 0)] T Id,
    [property: SugarColumn(ColumnName = "shop_id", Length = 18, DecimalDigits = 0)] T Shop,
    [property: SugarColumn(ColumnName = "score", Length = 18, DecimalDigits = 0)] T? Score,
    [property: SugarColumn(ColumnName = "payload")] string? Payload) where T : struct;
