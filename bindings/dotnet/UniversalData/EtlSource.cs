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

    /// <summary>Bind a deterministic public static method in a registered assembly to the Native Data Flow.</summary>
    public EtlMapped<TOutput> Map<TOutput>(Func<T, TOutput> method)
    {
        ArgumentNullException.ThrowIfNull(method);
        var output = typeof(TOutput);
        if (typeof(Task).IsAssignableFrom(output) || output == typeof(ValueTask) ||
            output.IsGenericType && output.GetGenericTypeDefinition() == typeof(ValueTask<>))
            throw new ArgumentException("An ETL mapping must synchronously return a row", nameof(method));
        return new(client, input with { Map = client.DescribeMethod(method) });
    }

    public EtlDestination<T> ToTable(string target) => ToTable(target, EntityMapping.For<T>().TableName);

    public EtlDestination<T> ToTable(string target, string table) => new(client, input, target, table);

    public EtlDestination<T> ToIceberg(string target, string tableNamespace) =>
        EtlDestination<T>.Iceberg(client, input, target, tableNamespace);

    public EtlDestination<T> ToIceberg(string target, string tableNamespace, string table) =>
        EtlDestination<T>.Iceberg(client, input, target, tableNamespace, table);
}
