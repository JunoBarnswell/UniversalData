namespace SqlSugar;

public sealed record SqlRow(IReadOnlyDictionary<string, object?> Values)
{
    public object? this[string name] => Values[name];
}
