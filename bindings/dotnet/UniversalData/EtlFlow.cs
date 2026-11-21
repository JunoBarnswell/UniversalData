namespace UniversalData;

/// <summary>Entry point for compiling a typed SQL transfer into a Native Package.</summary>
public sealed class EtlFlow
{
    private readonly DataClient client;
    private readonly string id;

    internal EtlFlow(DataClient client, string id)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(id);
        this.client = client;
        this.id = id;
    }

    public EtlSource<T> FromTable<T>(string source) => FromTable<T>(source, EntityMapping.For<T>().TableName);

    public EtlSource<T> FromTable<T>(string source, string table)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(source);
        ArgumentException.ThrowIfNullOrWhiteSpace(table);
        return new(client, new(id, source, typeof(T), Table: table));
    }

    public EtlSource<T> FromSql<T>(string source, string sql) => FromSql<T>(source, sql, null);

    public EtlSource<T> FromSql<T>(string source, string sql, SqlParameterBatch? parameters)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(source);
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);
        return new(client, new(id, source, typeof(T), Sql: sql, Parameters: parameters));
    }

    public EtlChangeSource<T> FromChanges<T>(string source) => FromChanges<T>(source, EntityMapping.For<T>().TableName);

    public EtlChangeSource<T> FromChanges<T>(string source, string table) => FromChanges<T>(source, table, new());

    public EtlChangeSource<T> FromChanges<T>(string source, string table, EtlChangeOptions options)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(source);
        ArgumentException.ThrowIfNullOrWhiteSpace(table);
        ArgumentNullException.ThrowIfNull(options);
        return new(client, new(id, source, typeof(T), Table: table, Changes: options));
    }

    public EtlSource<T> FromIceberg<T>(string source, string tableNamespace) =>
        FromIceberg<T>(source, tableNamespace, EntityMapping.For<T>().TableName);

    public EtlSource<T> FromIceberg<T>(string source, string tableNamespace, string table, long? snapshotId = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(source);
        ArgumentException.ThrowIfNullOrWhiteSpace(tableNamespace);
        ArgumentException.ThrowIfNullOrWhiteSpace(table);
        if (snapshotId is <= 0) throw new ArgumentOutOfRangeException(nameof(snapshotId));
        return new(client, new(id, source, typeof(T), Table: table, IcebergNamespace: tableNamespace, SnapshotId: snapshotId));
    }
}
