using SqlSugar;

namespace FluentAcceptance;

public sealed record LinqPageRequest<T>(
    [property: SugarColumn(Length = 18, DecimalDigits = 0)] T Shop,
    [property: SugarColumn(Length = 18, DecimalDigits = 0)] T? MinimumScore,
    int Offset, int Size) where T : struct;
