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
}

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
    public EtlSource<T> Stream() => new(client, input with { Stream = true });

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
}

public sealed class EtlMapped<T>
{
    private readonly DataClient client;
    private readonly EtlInput input;

    internal EtlMapped(DataClient client, EtlInput input)
    {
        this.client = client;
        this.input = input;
    }

    public EtlDestination<T> ToTable(string target) => ToTable(target, EntityMapping.For<T>().TableName);

    public EtlDestination<T> ToTable(string target, string table) => new(client, input, target, table);
}

internal sealed record EtlInput(string Id, string Source, Type Type, string? Table = null, string? Sql = null,
    SqlParameterBatch? Parameters = null, bool Stream = false, uint? BatchSize = null, SdkMethod? Map = null);
