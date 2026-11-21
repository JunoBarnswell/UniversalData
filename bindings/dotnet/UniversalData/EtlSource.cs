using System.Linq.Expressions;

namespace UniversalData;

public sealed class EtlSource<T>
{
    private readonly DataClient client;
    private readonly EtlInput input;

    internal EtlSource(DataClient client, EtlInput input)
    {
        this.client = client;
        this.input = input;
    }

    /// <summary>Read directly from the database cursor. A direct stream cannot resume its Native source snapshot.</summary>
    public EtlSource<T> Stream()
    {
        if (input.IcebergNamespace is not null)
            throw new UniversalDataException((int)StatusCode.Unsupported, "An Iceberg source reads a snapshot");
        return new(client, input with { Stream = true });
    }

    public EtlSource<T> BatchSize(uint rows)
    {
        if (rows is 0 or > 4096) throw new ArgumentOutOfRangeException(nameof(rows));
        return new(client, input with { BatchSize = rows });
    }

    public EtlMapped<TOutput> Select<TOutput>(Expression<Func<T, TOutput>> rule)
    {
        ArgumentNullException.ThrowIfNull(rule);
        var projection = new NativeRuleExpression(input.Id, "select", rule).Projection(typeof(TOutput));
        return new(client, input with { Projection = projection });
    }

    public EtlDestination<T> ToTable(string target) => ToTable(target, EntityMapping.For<T>().TableName);

    public EtlDestination<T> ToTable(string target, string table) => new(client, input, target, table);

    public EtlDestination<T> ToIceberg(string target, string tableNamespace) =>
        EtlDestination<T>.Iceberg(client, input, target, tableNamespace);

    public EtlDestination<T> ToIceberg(string target, string tableNamespace, string table) =>
        EtlDestination<T>.Iceberg(client, input, target, tableNamespace, table);
}
