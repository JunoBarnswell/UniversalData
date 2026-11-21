using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace UniversalData;

internal sealed class NativeRuleExpression(string job, string step, LambdaExpression rule)
{
    private readonly ParameterExpression row = rule.Parameters[0];
    private readonly ParameterExpression? argument = rule.Parameters.Count == 2 ? rule.Parameters[1] : null;

    internal JsonNode Predicate() => Bind(rule.Body);

    internal JsonArray Projection(Type output)
    {
        var fields = new Dictionary<string, Expression>(StringComparer.OrdinalIgnoreCase);
        if (rule.Body is MemberInitExpression init)
        {
            ReadConstructor(init.NewExpression, fields);
            foreach (var member in init.Bindings)
            {
                if (member is not MemberAssignment assignment)
                    throw Error(member.Member.Name);
                fields[member.Member.Name] = assignment.Expression;
            }
        }
        else if (rule.Body is NewExpression create)
            ReadConstructor(create, fields);
        else if (rule.Body == row && output == row.Type)
        {
            foreach (var member in EntityMapping.For(output).Members)
                fields.Add(member.Property.Name, Expression.Property(row, member.Property));
        }
        else throw Error(rule.Body.NodeType.ToString());

        var result = new JsonArray();
        foreach (var member in EntityMapping.For(output).Members)
        {
            if (!fields.Remove(member.Property.Name, out var value))
                throw Error("unassigned output member " + member.Property.Name);
            result.Add(new JsonObject { ["name"] = member.ColumnName, ["expression"] = Bind(value) });
        }
        if (fields.Count != 0)
            throw Error("unmapped output member " + fields.Keys.First());
        return result;
    }

    internal string Key()
    {
        var body = StripConvert(rule.Body);
        if (body is not MemberExpression { Expression: var owner, Member: PropertyInfo property } || owner != row)
            throw Error("key selector " + body.NodeType);
        return Column(row.Type, property).ColumnName;
    }

    private void ReadConstructor(NewExpression constructor, Dictionary<string, Expression> fields)
    {
        if (constructor.Arguments.Count == 0) return;
        var names = constructor.Members?.Select(member => member.Name).ToArray() ??
            constructor.Constructor?.GetParameters().Select(parameter => parameter.Name!).ToArray();
        if (names is null || names.Length != constructor.Arguments.Count)
            throw Error("output constructor");
        for (int i = 0; i < names.Length; i++)
            fields.Add(names[i], constructor.Arguments[i]);
    }

    private JsonNode Bind(Expression value)
    {
        switch (value)
        {
            case ConstantExpression literal:
                return Literal(literal.Value, literal.Type);
            case MemberExpression { Member: PropertyInfo property } member when member.Expression == row:
                return new JsonObject { ["column"] = Column(row.Type, property).ColumnName };
            case MemberExpression { Member: PropertyInfo property } member when
                argument is not null && member.Expression == argument:
                return new JsonObject { ["ref"] = Column(argument.Type, property).Property.Name };
            case MemberExpression { Member.Name: "HasValue", Expression: { } nullable } when
                Nullable.GetUnderlyingType(nullable.Type) is not null:
                return Operation("not", Operation("isnull", Bind(nullable)));
            case MemberExpression { Member.Name: "Value", Expression: { } nullable } when
                Nullable.GetUnderlyingType(nullable.Type) is not null:
                return Bind(nullable);
            case BinaryExpression binary:
                return Binary(binary);
            case ConditionalExpression conditional:
                return Operation("conditional", Bind(conditional.Test), Bind(conditional.IfTrue), Bind(conditional.IfFalse));
            case UnaryExpression unary when unary.NodeType == ExpressionType.Not && unary.Type == typeof(bool):
                return Operation("not", Bind(unary.Operand));
            case UnaryExpression unary when unary.NodeType is ExpressionType.Negate or ExpressionType.NegateChecked:
                return Operation("neg", Bind(unary.Operand));
            case UnaryExpression unary when unary.NodeType is ExpressionType.Convert or ExpressionType.ConvertChecked:
                if (SqlValueCodec.TryGetNativeType(unary.Type, out var target) &&
                    SqlValueCodec.TryGetNativeType(unary.Operand.Type, out var source) && target == source)
                    return Bind(unary.Operand);
                break;
            case MethodCallExpression method:
                return Method(method);
        }
        throw Error(value is MemberExpression memberAccess
            ? memberAccess.Member.DeclaringType?.FullName + "." + memberAccess.Member.Name
            : value.NodeType.ToString());
    }

    private JsonNode Binary(BinaryExpression value)
    {
        // A user-defined operator is still a CLR method, including overloads on value types.
        if (value.Method is { } method && method.DeclaringType != typeof(string) &&
            method.DeclaringType != typeof(decimal))
            throw Error(method.DeclaringType?.FullName + "." + method.Name);
        var left = value.Left;
        var right = value.Right;
        bool eq = value.NodeType == ExpressionType.Equal;
        if (eq || value.NodeType == ExpressionType.NotEqual)
        {
            if (left is ConstantExpression { Value: null } || right is ConstantExpression { Value: null })
            {
                var isNull = Operation("isnull", Bind(left is ConstantExpression { Value: null } ? right : left));
                return eq ? isNull : Operation("not", isNull);
            }
            var equal = Operation("eq", Bind(left), Bind(right));
            if (CanBeNull(left.Type) || CanBeNull(right.Type))
                equal = Operation("conditional", Operation("isnull", Bind(left)),
                    Operation("isnull", Bind(right)), Function("coalesce", equal, Literal(false, typeof(bool))));
            return eq ? equal : Operation("not", equal);
        }
        if (value.NodeType == ExpressionType.AndAlso)
            return Operation("conditional", Bind(left), Bind(right), Literal(false, typeof(bool)));
        if (value.NodeType == ExpressionType.OrElse)
            return Operation("conditional", Bind(left), Literal(true, typeof(bool)), Bind(right));
        if (value.NodeType == ExpressionType.Coalesce && value.Conversion is null)
            return Operation("conditional", Operation("isnull", Bind(left)), Bind(right), Bind(left));
        string op = value.NodeType switch
        {
            ExpressionType.Add or ExpressionType.AddChecked => "add",
            ExpressionType.Subtract or ExpressionType.SubtractChecked => "sub",
            ExpressionType.Multiply or ExpressionType.MultiplyChecked => "mul",
            ExpressionType.Divide => "div",
            ExpressionType.Modulo => "mod",
            ExpressionType.GreaterThan => "gt",
            ExpressionType.GreaterThanOrEqual => "ge",
            ExpressionType.LessThan => "lt",
            ExpressionType.LessThanOrEqual => "le",
            ExpressionType.And when value.Type == typeof(bool) => "and",
            ExpressionType.Or when value.Type == typeof(bool) => "or",
            _ => throw Error(value.NodeType.ToString())
        };
        return Operation(op, Bind(left), Bind(right));
    }

    private JsonNode Method(MethodCallExpression call)
    {
        if (call.Method.DeclaringType == typeof(string) && call.Object is { } value && call.Arguments.Count == 0)
        {
            if (call.Method.Name == nameof(string.Trim)) return Function("trim_unicode", Bind(value));
            if (call.Method.Name == nameof(string.ToLowerInvariant)) return Function("lower", Bind(value));
            if (call.Method.Name == nameof(string.ToUpperInvariant)) return Function("upper", Bind(value));
        }
        if (call.Method.DeclaringType == typeof(Math) && call.Method.Name == nameof(Math.Abs) &&
            call.Arguments.Count == 1)
            return Function("abs", Bind(call.Arguments[0]));
        if (call.Method.DeclaringType == typeof(decimal) && call.Method.Name == nameof(decimal.ToString) &&
            call.Object is { } amount && call.Arguments.Count == 1 &&
            StripConvert(call.Arguments[0]) is MemberExpression
                { Member.Name: nameof(CultureInfo.InvariantCulture), Member.DeclaringType: var owner } &&
            owner == typeof(CultureInfo))
            return new JsonObject
            {
                ["cast"] = new JsonObject { ["name"] = "decimalText", ["type"] = "utf8", ["nullable"] = false },
                ["value"] = Bind(amount)
            };
        throw Error(call.Method.DeclaringType?.FullName + "." + call.Method.Name);
    }

    private JsonObject Literal(object? value, Type type)
    {
        if (!SqlValueCodec.TryGetNativeType(type, out var native))
            throw Error(type.FullName ?? type.Name);
        var literal = new JsonObject { ["type"] = SqlValueCodec.NativeTypeName(native) };
        if (native == DataType.Decimal128)
        {
            literal["scale"] = value is decimal number ? (decimal.GetBits(number)[3] >> 16) & 0xff : 28;
            literal["precision"] = 38;
            literal["literal"] = value is decimal amount ? amount.ToString(CultureInfo.InvariantCulture) : null;
        }
        else literal["literal"] = value is null ? null : JsonSerializer.SerializeToNode(value, value.GetType());
        return literal;
    }

    private EntityMember Column(Type type, PropertyInfo property) => EntityMapping.For(type).Members
        .SingleOrDefault(member => member.Property == property) ?? throw Error(property.Name);

    private static bool CanBeNull(Type type) => !type.IsValueType || Nullable.GetUnderlyingType(type) is not null;
    private static Expression StripConvert(Expression body) => body is UnaryExpression
        { NodeType: ExpressionType.Convert, Operand: var operand } ? operand : body;
    private static JsonObject Operation(string op, params JsonNode[] arguments) =>
        new() { ["op"] = op, ["args"] = new JsonArray(arguments) };
    private static JsonObject Function(string name, params JsonNode[] arguments) =>
        new() { ["function"] = name, ["args"] = new JsonArray(arguments) };
    private ArgumentException Error(string rulePart) =>
        new($"Job '{job}', step '{step}': '{rulePart}' has no Native rule translation.");
}
