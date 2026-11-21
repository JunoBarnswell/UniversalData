using System.Linq.Expressions;

namespace UniversalData;

/// <summary>Iceberg destination declaration with an explicit storage connection.</summary>
public sealed class DataJobIceberg<TInput, TRow>
{
    private readonly DataJobRows<TInput, TRow> input;
    private readonly string target;
    private readonly string tableNamespace;
    private readonly string table;
    private readonly string? storageConnection;
    private readonly string? step;

    internal DataJobIceberg(DataJobRows<TInput, TRow> input, string target, string tableNamespace,
        string table, string? storageConnection, string? step)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(target);
        ArgumentException.ThrowIfNullOrWhiteSpace(tableNamespace);
        ArgumentException.ThrowIfNullOrWhiteSpace(table);
        if (storageConnection is not null) ArgumentException.ThrowIfNullOrWhiteSpace(storageConnection);
        this.input = input;
        this.target = target;
        this.tableNamespace = tableNamespace;
        this.table = table;
        this.storageConnection = storageConnection;
        this.step = step;
    }

    public DataJob<TInput> Append() => Write("append", []);

    public DataJob<TInput> UpsertBy(Expression<Func<TRow, object?>> key,
        params Expression<Func<TRow, object?>>[] additional) =>
        Write("upsert", input.KeyOrdinals(step ?? "iceberg-upsert", key, additional));

    /// <summary>Apply Native CDC operation and before-image metadata to an Iceberg sink.</summary>
    public DataJob<TInput> ApplyChangesBy(Expression<Func<TRow, object?>> key,
        params Expression<Func<TRow, object?>>[] additional) =>
        ApplyChangesBy(key, "before", additional);

    public DataJob<TInput> ApplyChangesBy(Expression<Func<TRow, object?>> key, string beforeField,
        params Expression<Func<TRow, object?>>[] additional)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(beforeField);
        return Write("changeset", input.KeyOrdinals(step ?? "iceberg-changeset", key, additional), beforeField);
    }

    private DataJob<TInput> Write(string mode, int[] keys, string? beforeField = null)
    {
        var graph = input.Job.Graph;
        string name = graph.Step(step, mode == "changeset" ? "changeset" : "iceberg-write");
        string storage = storageConnection ?? target + "-storage";
        graph.Use(target, DataJobSourceRole.Iceberg);
        graph.Plan.IcebergDestination(name, target, storage, tableNamespace, table, mode,
            keys.Length == 0 ? null : keys, null, beforeField, input.Flow)
            .Connect(input.Flow, input.Step, name, input.Port);
        graph.StorageComponent(target, name, input.Flow, storageConnection);
        return input.Job;
    }
}
