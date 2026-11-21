namespace UniversalData;

public sealed class EtlChangeSource<T>
{
    private readonly DataClient client;
    private readonly EtlInput input;

    internal EtlChangeSource(DataClient client, EtlInput input)
    {
        this.client = client;
        this.input = input;
    }

    public EtlDestination<T> ToTable(string target) => ToTable(target, EntityMapping.For<T>().TableName);

    public EtlDestination<T> ToTable(string target, string table) => new(client, input, target, table);

    public EtlDestination<T> ToIceberg(string target, string tableNamespace) =>
        EtlDestination<T>.Iceberg(client, input, target, tableNamespace);

    public EtlDestination<T> ToIceberg(string target, string tableNamespace, string table) =>
        EtlDestination<T>.Iceberg(client, input, target, tableNamespace, table);
}
