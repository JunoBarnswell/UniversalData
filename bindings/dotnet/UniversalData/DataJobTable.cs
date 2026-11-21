using System.Linq.Expressions;

namespace UniversalData;

public sealed class DataJobTable<TInput, TRow>
{
    private readonly DataJobRows<TInput, TRow> input;
    private readonly string target;
    private readonly string table;
    private readonly string? step;
    private readonly bool durable;
    private readonly bool provisionReceipts;

    internal DataJobTable(DataJobRows<TInput, TRow> input, string target, string table, string? step,
        bool durable, bool provisionReceipts)
    {
        this.input = input;
        this.target = target;
        this.table = table;
        this.step = step;
        this.durable = durable;
        this.provisionReceipts = provisionReceipts;
    }

    public DataJob<TInput> Append() => Write("append", []);

    public DataJob<TInput> UpsertBy(Expression<Func<TRow, object?>> key,
        params Expression<Func<TRow, object?>>[] additional) =>
        Write("upsert", input.KeyOrdinals(step ?? "upsert", key, additional));

    /// <summary>Write Native CDC operations and before images as a changeset.</summary>
    public DataJob<TInput> ApplyChangesBy(Expression<Func<TRow, object?>> key,
        params Expression<Func<TRow, object?>>[] additional) =>
        ApplyChangesBy(key, "before", additional);

    public DataJob<TInput> ApplyChangesBy(Expression<Func<TRow, object?>> key, string beforeField,
        params Expression<Func<TRow, object?>>[] additional)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(beforeField);
        return Write("changeset", input.KeyOrdinals(step ?? "changeset", key, additional), beforeField);
    }

    private DataJob<TInput> Write(string mode, int[] keys, string? beforeField = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(table);
        var graph = input.Job.Graph;
        graph.Use(target, DataJobSourceRole.Database);
        string name = graph.Step(step, mode);
        graph.Plan.SqlDestination(name, target, table, mode, null,
            keys.Length == 0 ? null : keys, durable ? "receipt" : "response", durable && provisionReceipts, input.Flow);
        if (beforeField is not null)
            graph.ComponentConfig(input.Flow, name, "beforeField", beforeField);
        graph.Plan
            .Connect(input.Flow, input.Step, name, input.Port);
        return input.Job;
    }
}
