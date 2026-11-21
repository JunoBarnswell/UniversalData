using System.Linq.Expressions;

namespace UniversalData;

public sealed class DataJobRows<TInput, TRow>
{
    internal DataJobRows(DataJob<TInput> job, string flow, string step, uint port = 0)
    {
        Job = job;
        Flow = flow;
        Step = step;
        Port = port;
    }

    internal DataJob<TInput> Job { get; }
    internal string Flow { get; }
    internal string Step { get; }
    internal uint Port { get; }
    private DataJobGraph Graph => Job.Graph;

    public DataJobRows<TInput, TRow> Where(Expression<Func<TRow, TInput, bool>> predicate, string? step = null)
    {
        var (matched, rejected) = Split(predicate, step);
        rejected.Discard();
        return matched;
    }

    public DataJobRows<TInput, TRow> Where(Expression<Func<TRow, bool>> predicate, string? step = null)
    {
        var matched = SplitRule(predicate, step);
        new DataJobRows<TInput, TRow>(Job, Flow, matched.Step, 1).Discard();
        return matched;
    }

    public (DataJobRows<TInput, TRow> Matched, DataJobRows<TInput, TRow> Otherwise) Split(
        Expression<Func<TRow, TInput, bool>> predicate, string? step = null)
    {
        var matched = SplitRule(predicate, step);
        return (matched, new(Job, Flow, matched.Step, 1));
    }

    private DataJobRows<TInput, TRow> SplitRule(LambdaExpression predicate, string? step)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        string name = Graph.Step(step, "split");
        var condition = new NativeRuleExpression(Job.Id, name, predicate).Predicate();
        Graph.Plan.Component(name, "split", new { condition }, Flow).Connect(Flow, Step, name, Port);
        return new(Job, Flow, name);
    }

    public DataJobRows<TInput, TOutput> Select<TOutput>(Expression<Func<TRow, TInput, TOutput>> rule,
        string? step = null) => Project<TOutput>(rule, step);

    public DataJobRows<TInput, TOutput> Select<TOutput>(Expression<Func<TRow, TOutput>> rule,
        string? step = null) => Project<TOutput>(rule, step);

    private DataJobRows<TInput, TOutput> Project<TOutput>(LambdaExpression rule, string? step)
    {
        ArgumentNullException.ThrowIfNull(rule);
        string name = Graph.Step(step, "select");
        var columns = new NativeRuleExpression(Job.Id, name, rule).Projection(typeof(TOutput));
        Graph.Plan.Component(name, "derive", new
        {
            columns, project = true, schema = DataJob<TInput>.Schema<TOutput>()
        }, Flow).Connect(Flow, Step, name, Port);
        return new(Job, Flow, name);
    }

    public (DataJobRows<TInput, TRow> First, DataJobRows<TInput, TRow> Second) Fork(string? step = null)
    {
        string name = Graph.Step(step, "fork");
        Graph.Plan.Component(name, "multicast", new { }, Flow).Connect(Flow, Step, name, Port);
        return (new(Job, Flow, name), new(Job, Flow, name, 1));
    }

    public DataJobRows<TInput, TRow> UnionAll(DataJobRows<TInput, TRow> other, string? step = null)
    {
        RequireSameJob(other);
        string name = Graph.Step(step, "union");
        Graph.Plan.Component(name, "unionAll", new { }, Flow).Connect(Flow, Step, name, Port)
            .Connect(Flow, other.Step, name, other.Port, 1);
        return new(Job, Flow, name);
    }

    public DataJobRows<TInput, TJoined> Join<TRight, TJoined>(DataJobRows<TInput, TRight> other,
        Expression<Func<TRow, object?>> leftKey, Expression<Func<TRight, object?>> rightKey,
        IReadOnlyDictionary<string, string>? rightAliases = null, string? step = null)
    {
        RequireSameJob(other);
        string name = Graph.Step(step, "join");
        string[] leftKeys = [new NativeRuleExpression(Job.Id, name, leftKey).Key()];
        string[] rightKeys = [new NativeRuleExpression(Job.Id, name, rightKey).Key()];
        Graph.Plan.Component(name, "join", new { leftKeys, rightKeys, rightAliases }, Flow).Connect(Flow, Step, name, Port)
            .Connect(Flow, other.Step, name, other.Port, 1);
        return new(Job, Flow, name);
    }

    public DataJobTable<TInput, TRow> ToTable(string target, string? table = null, string? step = null,
        bool durable = true, bool provisionReceipts = true) =>
        new(this, target, table ?? EntityMapping.For<TRow>().TableName, step, durable, provisionReceipts);

    public DataJob<TInput> ToMessages(string target, string address, bool queue = false, string? step = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(address);
        Graph.Use(target, DataJobSourceRole.MessageBroker);
        string name = Graph.Step(step, "publish");
        Graph.Plan.Component(name, "mq.destination", new
        {
            connection = target, address, targetType = queue ? "queue" : "topic", format = "json"
        }, Flow).Connect(Flow, Step, name, Port);
        return Job;
    }

    public DataJob<TInput> ToHttp(string target, string path, string? step = null, string? receiptPath = null,
        HttpHeaderDefinition[]? headers = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        Graph.Use(target, DataJobSourceRole.Http);
        string name = Graph.Step(step, "send-http");
        Graph.Plan.Component(name, "rest.destination", new
        {
            connection = target, url = path, delivery = receiptPath is null ? "response" : "receipt",
            receiptUrl = receiptPath, headers
        }, Flow).Connect(Flow, Step, name, Port);
        return Job;
    }

    public DataJobQuery<TInput, TRow> Return(string? step = null)
    {
        Graph.Return();
        string name = Graph.Step(step, "result");
        Graph.Plan.Component(name, "result.destination", new { }, Flow).Connect(Flow, Step, name, Port);
        return new(Graph);
    }

    public DataJob<TInput> Discard(string? step = null)
    {
        string name = Graph.Step(step, "discard");
        Graph.Plan.Component(name, "discard", new { }, Flow).Connect(Flow, Step, name, Port);
        return Job;
    }

    private void RequireSameJob<TOther>(DataJobRows<TInput, TOther> other)
    {
        ArgumentNullException.ThrowIfNull(other);
        if (!ReferenceEquals(Job, other.Job) || Flow != other.Flow)
            throw new ArgumentException("Data flow inputs must belong to the same pipeline.", nameof(other));
    }
}
