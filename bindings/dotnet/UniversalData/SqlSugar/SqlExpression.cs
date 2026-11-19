using System.Linq.Expressions;
using System.Text.Json;

namespace SqlSugar;

internal static class SqlExpression
{
    internal static SqlQueryPlan Build<T>(EntityMapping mapping,
        IReadOnlyList<Expression<Func<T, bool>>> filters,
        IReadOnlyList<(LambdaExpression Expression, bool Descending)> orderings,
        int? page, int? size)
    {
        var parameters = new List<SugarParameter>();
        var where = filters.Count switch
        {
            0 => null,
            1 => BuildNode(filters[0].Body, mapping, parameters),
            _ => new Dictionary<string, object?> { ["op"] = "and", ["args"] = filters.Select(filter => BuildNode(filter.Body, mapping, parameters)).ToArray() }
        };
        var order = orderings.Select(ordering => new Dictionary<string, object?>
        {
            ["column"] = ResolveColumn(ordering.Expression.Body, mapping),
            ["descending"] = ordering.Descending
        }).ToArray();
        var document = new Dictionary<string, object?> { ["table"] = mapping.TableName };
        if (where is not null) document["where"] = where;
        if (order.Length != 0) document["order"] = order;
        if (page is not null || size is not null)
        {
            if (page is null || size is null || page <= 0 || size <= 0) throw new ArgumentOutOfRangeException(nameof(page), "Page number and size must both be positive.");
            document["page"] = page.Value;
            document["size"] = size.Value;
            document["keys"] = mapping.Keys.Select(key => key.ColumnName).ToArray();
        }
        return new SqlQueryPlan { QueryJson = JsonSerializer.Serialize(document), SchemaJson = mapping.SchemaJson(), Parameters = parameters };
    }

    private static object BuildNode(Expression expression, EntityMapping mapping, List<SugarParameter> parameters)
    {
        expression = Unwrap(expression);
        if (expression is BinaryExpression binary && binary.NodeType is ExpressionType.AndAlso or ExpressionType.OrElse)
            return new Dictionary<string, object?> { ["op"] = binary.NodeType == ExpressionType.AndAlso ? "and" : "or", ["args"] = new[] { BuildNode(binary.Left, mapping, parameters), BuildNode(binary.Right, mapping, parameters) } };
        if (expression is UnaryExpression unary && unary.NodeType == ExpressionType.Not)
            return new Dictionary<string, object?> { ["op"] = "not", ["args"] = new[] { BuildNode(unary.Operand, mapping, parameters) } };
        if (expression is BinaryExpression comparison && TryOperator(comparison.NodeType, out var operation))
        {
            var leftColumn = TryResolveColumn(comparison.Left, mapping, out var left);
            var rightColumn = TryResolveColumn(comparison.Right, mapping, out var right);
            if (leftColumn == rightColumn) throw new NotSupportedException("A query comparison must contain one entity column and one value.");
            var column = leftColumn ? left! : right!;
            var valueExpression = leftColumn ? comparison.Right : comparison.Left;
            var value = Evaluate(valueExpression);
            if (value is null)
            {
                if (operation is not ("eq" or "ne")) throw new NotSupportedException("Only equality comparisons can test SQL NULL.");
                var nullNode = new Dictionary<string, object?> { ["op"] = "isnull", ["args"] = new[] { (object)new Dictionary<string, object?> { ["col"] = column } } };
                return operation == "eq" ? nullNode : new Dictionary<string, object?> { ["op"] = "not", ["args"] = new[] { (object)nullNode } };
            }
            var index = parameters.Count;
            parameters.Add(new SugarParameter("p" + index, value));
            var parameterNode = new Dictionary<string, object?> { ["param"] = index };
            var args = leftColumn ? new object[] { new Dictionary<string, object?> { ["col"] = column }, parameterNode } : new object[] { parameterNode, new Dictionary<string, object?> { ["col"] = column } };
            return new Dictionary<string, object?> { ["op"] = operation, ["args"] = args };
        }
        throw new NotSupportedException($"Expression node '{expression.NodeType}' is not supported by the Native SQL query contract.");
    }

    private static bool TryOperator(ExpressionType type, out string operation)
    {
        operation = type switch
        {
            ExpressionType.Equal => "eq",
            ExpressionType.NotEqual => "ne",
            ExpressionType.LessThan => "lt",
            ExpressionType.LessThanOrEqual => "le",
            ExpressionType.GreaterThan => "gt",
            ExpressionType.GreaterThanOrEqual => "ge",
            _ => string.Empty
        };
        return operation.Length != 0;
    }

    private static bool TryResolveColumn(Expression expression, EntityMapping mapping, out string? column)
    {
        expression = Unwrap(expression);
        if (expression is not MemberExpression member || member.Expression is not ParameterExpression) { column = null; return false; }
        var entityMember = mapping.Members.FirstOrDefault(value => value.Property.Name == member.Member.Name);
        if (entityMember is null) throw new InvalidOperationException($"Entity member '{member.Member.Name}' is not mapped.");
        column = entityMember.ColumnName;
        return true;
    }

    private static string ResolveColumn(Expression expression, EntityMapping mapping)
    {
        if (!TryResolveColumn(expression, mapping, out var column)) throw new NotSupportedException("OrderBy must select a mapped entity member.");
        return column!;
    }

    private static object? Evaluate(Expression expression)
    {
        expression = Unwrap(expression);
        if (expression is ConstantExpression constant) return constant.Value;
        return Expression.Lambda(expression).Compile().DynamicInvoke();
    }

    private static Expression Unwrap(Expression expression)
    {
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary) expression = unary.Operand;
        return expression;
    }
}
