using System.Linq.Expressions;

namespace UniversalData;

internal static class WriteKeyResolver
{
    internal static IReadOnlyList<EntityMember> Resolve<T>(EntityMapping mapping, Expression<Func<T, object>>[] expressions)
    {
        if (expressions.Length == 0) return mapping.Keys;
        var result = new List<EntityMember>(expressions.Length);
        foreach (var expression in expressions)
        {
            var body = expression.Body;
            if (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary) body = unary.Operand;
            if (body is not MemberExpression member) throw new ArgumentException("WhereColumns requires mapped entity members.", nameof(expressions));
            var mapped = mapping.Members.FirstOrDefault(value => value.Property.Name == member.Member.Name)
                ?? throw new ArgumentException($"Entity member '{member.Member.Name}' is not mapped.", nameof(expressions));
            if (!result.Contains(mapped)) result.Add(mapped);
        }
        return result;
    }
}
