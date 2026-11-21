using System.Linq.Expressions;

namespace UniversalData;

public sealed class EtlDestination<T>
{
    private readonly DataClient client;
    private readonly EtlInput input;
    private readonly string target;
    private readonly string table;
    private readonly string? tableNamespace;

    internal EtlDestination(DataClient client, EtlInput input, string target, string table, string? tableNamespace = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(target);
        ArgumentException.ThrowIfNullOrWhiteSpace(table);
        this.client = client;
        this.input = input;
        this.target = target;
        this.table = table;
        this.tableNamespace = tableNamespace;
    }

    internal static EtlDestination<T> Iceberg(DataClient client, EtlInput input, string target,
        string tableNamespace, string? table = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableNamespace);
        if (input.Stream)
            throw new UniversalDataException((int)StatusCode.Unsupported, "An Iceberg commit requires a replayable input");
        return new(client, input, target, table ?? EntityMapping.For<T>().TableName, tableNamespace);
    }

    public EtlPipeline Append()
    {
        if (input.Changes is not null)
            throw new UniversalDataException((int)StatusCode.Unsupported, "A CDC destination requires declared keys");
        return new(client, input, new(target, table, "append", EntityMapping.For<T>(), [], tableNamespace));
    }

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
        return new(client, input, new(target, table, input.Changes is null ? "upsert" : "changeset", mapping, keys,
            tableNamespace));
    }
}
