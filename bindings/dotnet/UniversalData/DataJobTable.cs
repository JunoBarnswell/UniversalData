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
        params Expression<Func<TRow, object?>>[] additional)
    {
        var selectors = additional.Prepend(key);
        var keys = selectors.Select(selector => new NativeRuleExpression(input.Job.Id,
            step ?? "upsert", selector).Key()).ToArray();
        if (keys.Distinct(StringComparer.Ordinal).Count() != keys.Length)
            throw new ArgumentException("Upsert keys must be distinct.", nameof(additional));
        var fields = EntityMapping.For<TRow>().Members;
        return Write("upsert", keys.Select(name => Enumerable.Range(0, fields.Count)
            .Single(index => fields[index].ColumnName == name)).ToArray());
    }

    private DataJob<TInput> Write(string mode, int[] keys)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(table);
        var graph = input.Job.Graph;
        graph.Use(target, DataJobSourceRole.Database);
        string name = graph.Step(step, mode);
        graph.Plan.SqlDestination(name, target, table, mode, null,
            keys.Length == 0 ? null : keys, durable ? "receipt" : "response", durable && provisionReceipts, input.Flow)
            .Connect(input.Flow, input.Step, name, input.Port);
        return input.Job;
    }
}
