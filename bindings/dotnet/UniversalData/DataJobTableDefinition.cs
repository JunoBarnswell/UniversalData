using System.Linq.Expressions;

namespace UniversalData;

internal sealed record DataJobTableDefinition(string Source, string Table, string? Step)
{
    internal IReadOnlyDictionary<string, string>? Projection { get; init; }
    internal (LambdaExpression Rule, IReadOnlyDictionary<string, string>? Projection)[] Filters { get; init; } = [];
    internal (string Column, bool Descending)[] Order { get; init; } = [];
    internal DataJobRange[] Ranges { get; init; } = [];
}
