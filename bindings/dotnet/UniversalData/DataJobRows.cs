using System.Linq.Expressions;
using System.Text.Json;

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

    public DataJobIceberg<TInput, TRow> ToIceberg(string target, string tableNamespace,
        string? table = null, string? storageConnection = null, string? step = null) =>
        new(this, target, tableNamespace, table ?? EntityMapping.For<TRow>().TableName,
            storageConnection, step);

    internal int[] KeyOrdinals(string step, Expression<Func<TRow, object?>> key,
        Expression<Func<TRow, object?>>[] additional)
    {
        var fields = EntityMapping.For<TRow>().Members;
        var keys = additional.Prepend(key).Select(selector => new NativeRuleExpression(Job.Id, step, selector).Key())
            .Select(name => Enumerable.Range(0, fields.Count).FirstOrDefault(index => fields[index].ColumnName == name, -1))
            .ToArray();
        if (keys.Any(index => index < 0) || keys.Distinct().Count() != keys.Length)
            throw new ArgumentException("Destination keys must be distinct columns in the mapped row", nameof(additional));
        return keys;
    }

    /// <summary>
    /// Adds the Native cross-run incremental fence to a SQL source.  The first
    /// selector is the monotonic watermark; additional selectors are stable
    /// tie-breakers for equal watermark values.
    /// </summary>
    public DataJobRows<TInput, TRow> IncrementalBy(Expression<Func<TRow, object?>> watermark,
        params Expression<Func<TRow, object?>>[] keys)
    {
        return IncrementalBy(watermark, (JsonElement?)null, keys);
    }

    public DataJobRows<TInput, TRow> IncrementalBy(Expression<Func<TRow, object?>> watermark,
        JsonElement? after, params Expression<Func<TRow, object?>>[] keys)
    {
        ArgumentNullException.ThrowIfNull(watermark);
        ArgumentNullException.ThrowIfNull(keys);
        string watermarkName = new NativeRuleExpression(Job.Id, Step + " incremental", watermark).Key();
        var names = new List<string> { watermarkName };
        foreach (var key in keys)
        {
            ArgumentNullException.ThrowIfNull(key);
            names.Add(new NativeRuleExpression(Job.Id, Step + " incremental", key).Key());
        }
        if (names.Distinct(StringComparer.Ordinal).Count() != names.Count)
            throw new ArgumentException("Incremental columns must be distinct", nameof(keys));
        var config = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["mode"] = names.Count == 1 ? "watermark" : "composite",
            ["keys"] = names,
            ["unique"] = true,
            ["monotonic"] = true
        };
        if (after is { } value)
        {
            if (value.ValueKind != JsonValueKind.Array)
                throw new ArgumentException("Incremental after must be a JSON array", nameof(after));
            config["after"] = value.Clone();
        }
        Graph.Incremental(Flow, Step, config);
        return this;
    }

    public DataJobRows<TInput, TRow> IncrementalBy(Expression<Func<TRow, object?>> watermark,
        object?[] after, params Expression<Func<TRow, object?>>[] keys)
    {
        ArgumentNullException.ThrowIfNull(after);
        return IncrementalBy(watermark, JsonSerializer.SerializeToElement(after, WorkerProtocol.Json), keys);
    }

    public DataJobRows<TInput, TRow> EventTime(Expression<Func<TRow, object?>> field,
        TimeSpan allowedLateness = default, string? step = null)
    {
        ArgumentNullException.ThrowIfNull(field);
        if (allowedLateness < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(allowedLateness));
        string name = Graph.Step(step, "event-time");
        string timeField = new NativeRuleExpression(Job.Id, name, field).Key();
        Graph.Plan.Component(name, "eventTime", new { field = timeField,
            lagMicros = checked((long)allowedLateness.TotalMicroseconds) }, Flow)
            .Connect(Flow, Step, name, Port);
        return new(Job, Flow, name, Port);
    }

    public DataJobRows<TInput, TRow> EventTime(Expression<Func<TRow, object?>> field,
        long lagMicros, string? step = null)
    {
        if (lagMicros < 0) throw new ArgumentOutOfRangeException(nameof(lagMicros));
        return EventTime(field, TimeSpan.FromMicroseconds(lagMicros), step);
    }

    public DataJobRows<TInput, TRow> Deduplicate(Expression<Func<TRow, object?>> key,
        TimeSpan ttl, uint stateBytes = 65536, Expression<Func<TRow, object?>>? timeField = null,
        string? step = null)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (ttl <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(ttl));
        if (stateBytes < 4096) throw new ArgumentOutOfRangeException(nameof(stateBytes));
        string name = Graph.Step(step, "deduplicate");
        string[] keys = [new NativeRuleExpression(Job.Id, name, key).Key()];
        string time = timeField is null ? "event_time" : new NativeRuleExpression(Job.Id, name, timeField).Key();
        Graph.Plan.Component(name, "eventDeduplicate", new { keys, timeField = time,
            ttlMicros = checked((long)ttl.TotalMicroseconds), stateBytes }, Flow)
            .Connect(Flow, Step, name, Port);
        return new(Job, Flow, name, Port);
    }

    public DataJobRows<TInput, TRow> Deduplicate(Expression<Func<TRow, object?>> key,
        long ttlMicros, uint stateBytes = 65536, Expression<Func<TRow, object?>>? timeField = null,
        string? step = null) => Deduplicate(key, TimeSpan.FromMicroseconds(ttlMicros), stateBytes, timeField, step);

    public DataJobRows<TInput, TJoined> EventJoin<TRight, TJoined>(DataJobRows<TInput, TRight> other,
        Expression<Func<TRow, object?>> leftKey, Expression<Func<TRight, object?>> rightKey,
        TimeSpan within, Expression<Func<TRow, object?>>? leftTime = null,
        Expression<Func<TRight, object?>>? rightTime = null, uint stateBytes = 65536,
        string? step = null)
    {
        RequireSameJob(other);
        ArgumentNullException.ThrowIfNull(leftKey);
        ArgumentNullException.ThrowIfNull(rightKey);
        if (within <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(within));
        if (stateBytes < 4096) throw new ArgumentOutOfRangeException(nameof(stateBytes));
        string name = Graph.Step(step, "event-join");
        string[] leftKeys = [new NativeRuleExpression(Job.Id, name, leftKey).Key()];
        string[] rightKeys = [new NativeRuleExpression(Job.Id, name, rightKey).Key()];
        Graph.Plan.Component(name, "eventJoin", new
        {
            leftKeys, rightKeys,
            leftTimeField = leftTime is null ? "event_time" : new NativeRuleExpression(Job.Id, name, leftTime).Key(),
            rightTimeField = rightTime is null ? "event_time" : new NativeRuleExpression(Job.Id, name, rightTime).Key(),
            withinMicros = checked((long)within.TotalMicroseconds), stateBytes
        }, Flow).Connect(Flow, Step, name, Port).Connect(Flow, other.Step, name, other.Port, 1);
        return new(Job, Flow, name);
    }

    public DataJobRows<TInput, TEnriched> CurrentLookup<TEnriched>(string cache,
        Expression<Func<TRow, object?>> key, string errorPolicy = "fail", string? step = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(cache);
        ArgumentException.ThrowIfNullOrWhiteSpace(errorPolicy);
        string name = Graph.Step(step, "lookup");
        int[] keys = [FieldIndex(key, name, nameof(key))];
        Graph.Plan.Component(name, "cacheLookup", new { cache, keys, errorPolicy }, Flow)
            .Connect(Flow, Step, name, Port);
        return new(Job, Flow, name);
    }

    public DataJobRows<TInput, TWindow> EventWindow<TWindow>(string mode, Expression<Func<TRow, object?>> timeField,
        TimeSpan size, JsonElement plan, TimeSpan? slide = null, TimeSpan? allowedLateness = null,
        string emit = "final", string? step = null, string? beforeField = null)
    {
        if (mode is not ("tumbling" or "sliding" or "session"))
            throw new ArgumentException("Event window mode must be tumbling, sliding, or session", nameof(mode));
        ArgumentNullException.ThrowIfNull(timeField);
        if (size <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(size));
        if (slide is { } windowSlide && windowSlide <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(slide));
        if (allowedLateness is { } lateness && lateness < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(allowedLateness));
        if (emit is not ("final" or "changes"))
            throw new ArgumentException("Event window emit must be final or changes", nameof(emit));
        if (plan.ValueKind != JsonValueKind.Object)
            throw new ArgumentException("Event window plan must be a JSON object", nameof(plan));
        if (mode == "sliding" && slide is null)
            throw new ArgumentException("Sliding windows require a slide interval", nameof(slide));
        string name = Graph.Step(step, "event-window");
        string field = new NativeRuleExpression(Job.Id, name, timeField).Key();
        Graph.Plan.Component(name, "eventWindow", new
        {
            mode, timeField = field, sizeMicros = checked((long)size.TotalMicroseconds),
            slideMicros = slide is null ? (long?)null : checked((long)slide.Value.TotalMicroseconds),
            allowedLatenessMicros = allowedLateness is null ? 0 : checked((long)allowedLateness.Value.TotalMicroseconds),
            latePolicy = "fail", emit, beforeField, plan = plan.Clone()
        }, Flow).Connect(Flow, Step, name, Port);
        return new(Job, Flow, name, Port);
    }

    public DataJobRows<TInput, TWindow> TumblingWindow<TWindow>(Expression<Func<TRow, object?>> timeField,
        TimeSpan size, JsonElement plan, TimeSpan? allowedLateness = null, string emit = "final",
        string? step = null) => EventWindow<TWindow>("tumbling", timeField, size, plan,
            allowedLateness: allowedLateness, emit: emit, step: step);

    public DataJobRows<TInput, TWindow> SlidingWindow<TWindow>(Expression<Func<TRow, object?>> timeField,
        TimeSpan size, TimeSpan slide, JsonElement plan, TimeSpan? allowedLateness = null,
        string emit = "final", string? step = null) =>
        EventWindow<TWindow>("sliding", timeField, size, plan, slide, allowedLateness, emit, step: step);


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

    private int FieldIndex(Expression<Func<TRow, object?>> selector, string step, string parameter)
    {
        ArgumentNullException.ThrowIfNull(selector);
        string column = new NativeRuleExpression(Job.Id, step, selector).Key();
        var fields = EntityMapping.For<TRow>().Members;
        int index = Array.FindIndex(fields.ToArray(), field => field.ColumnName == column);
        if (index < 0) throw new ArgumentException("Lookup key is not in the mapped row", parameter);
        return index;
    }
}
