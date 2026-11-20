using System.Linq.Expressions;

namespace UniversalData;

public sealed class EtlDestination<T>
{
    private readonly DataClient client;
    private readonly EtlInput input;
    private readonly string target;
    private readonly string table;

    internal EtlDestination(DataClient client, EtlInput input, string target, string table)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(target);
        ArgumentException.ThrowIfNullOrWhiteSpace(table);
        this.client = client;
        this.input = input;
        this.target = target;
        this.table = table;
    }

    public EtlPipeline Append() => new(client, input, new(target, table, "append", EntityMapping.For<T>(), []));

    public EtlPipeline UpsertBy(Expression<Func<T, object>> key, params Expression<Func<T, object>>[] additional)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(additional);
        Expression<Func<T, object>>[] selectors = [key, .. additional];
        foreach (var selector in selectors)
        {
            ArgumentNullException.ThrowIfNull(selector);
            var body = selector.Body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } convert
                ? convert.Operand : selector.Body;
            if (body is not MemberExpression member || member.Expression != selector.Parameters[0])
                throw new ArgumentException("Select a mapped member of the target row", nameof(key));
        }
        var mapping = EntityMapping.For<T>();
        var keys = WriteKeyResolver.Resolve(mapping, selectors).Select(member => member.ColumnName).ToArray();
        return new(client, input, new(target, table, "upsert", mapping, keys));
    }
}

internal sealed record EtlOutput(string Target, string Table, string Mode, EntityMapping Mapping, string[] Keys);
