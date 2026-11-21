using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;

namespace UniversalData;

internal sealed record DataJobRange(string Op, JsonElement Count)
{
    internal static DataJobRange Constant(string op, long count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        return new(op, JsonSerializer.SerializeToElement(count));
    }

    internal static DataJobRange Parameter<TInput>(string op, Expression<Func<TInput, long>> selector,
        string job, string step)
    {
        ArgumentNullException.ThrowIfNull(selector);
        Expression value = NativeRuleExpression.StripLosslessConvert(selector.Body);
        if (value is not MemberExpression { Expression: var owner, Member: PropertyInfo property } ||
            owner != selector.Parameters[0] ||
            !EntityMapping.For<TInput>().Members.Any(member => member.Property == property))
            throw new ArgumentException($"Job '{job}', step '{step}': {op} must select an integer input member.",
                nameof(selector));
        return new(op, JsonSerializer.SerializeToElement(new Dictionary<string, string>
        {
            ["$parameter"] = property.Name
        }));
    }
}
