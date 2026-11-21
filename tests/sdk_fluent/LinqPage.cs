using SqlSugar;

namespace FluentAcceptance;

public sealed record LinqPage<T>(
    [property: SugarColumn(ColumnName = "entry_alias", Length = 18, DecimalDigits = 0)] T Id,
    [property: SugarColumn(ColumnName = "score_alias", Length = 18, DecimalDigits = 0)] T? Score) where T : struct;
