using System.Linq.Expressions;
using System.Text.Json;

namespace UniversalData;

public sealed class DataJobRows<TInput, TRow>
{
    private (string Column, bool Descending)[]? order;

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

    public DataJobRows<TInput, TRow> OrderBy(Expression<Func<TRow, object?>> key, string? step = null) =>
        Sort(key, false, step);

    public DataJobRows<TInput, TRow> OrderByDescending(Expression<Func<TRow, object?>> key, string? step = null) =>
        Sort(key, true, step);

    public DataJobRows<TInput, TRow> ThenBy(Expression<Func<TRow, object?>> key) => AddOrder(key, false);

    public DataJobRows<TInput, TRow> ThenByDescending(Expression<Func<TRow, object?>> key) => AddOrder(key, true);

    public DataJobRows<TInput, TRow> Skip(long count, string? step = null) =>
        Range(DataJobRange.Constant("skip", count), step);

    public DataJobRows<TInput, TRow> Skip(Expression<Func<TInput, long>> count, string? step = null) =>
        Range(DataJobRange.Parameter("skip", count, Job.Id, step ?? "skip"), step);

    public DataJobRows<TInput, TRow> Take(long count, string? step = null) =>
        Range(DataJobRange.Constant("take", count), step);

    public DataJobRows<TInput, TRow> Take(Expression<Func<TInput, long>> count, string? step = null) =>
        Range(DataJobRange.Parameter("take", count, Job.Id, step ?? "take"), step);

    private DataJobRows<TInput, TRow> Sort(Expression<Func<TRow, object?>> key, bool descending, string? step)
    {
        ArgumentNullException.ThrowIfNull(key);
        string name = Graph.Step(step, "sort");
        string column = new NativeRuleExpression(Job.Id, name, key).Key(direct: true);
        Graph.Plan.Component(name, "sort", new { keys = new[] { column }, keyDirections = new[] { descending } },
            Flow).Connect(Flow, Step, name, Port);
        return new(Job, Flow, name) { order = [(column, descending)] };
    }

    private DataJobRows<TInput, TRow> AddOrder(Expression<Func<TRow, object?>> key, bool descending)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (order is null || Graph.Plan.HasOutputConnection(Flow, Step))
            throw new ArgumentException($"Job '{Job.Id}', step '{Step}': ThenBy requires an unconsumed OrderBy.");
        string column = new NativeRuleExpression(Job.Id, Step, key).Key(direct: true);
        if (order.Any(item => item.Column == column))
            throw new ArgumentException($"Job '{Job.Id}', step '{Step}': ordering keys must be distinct.");
        order = [.. order, (column, descending)];
        Graph.ComponentConfig(Flow, Step, "keys", order.Select(item => item.Column).ToArray());
        Graph.ComponentConfig(Flow, Step, "keyDirections", order.Select(item => item.Descending).ToArray());
        return this;
    }

    private DataJobRows<TInput, TRow> Range(DataJobRange range, string? step)
    {
        string name = Graph.Step(step, range.Op);
        Graph.Plan.Component(name, "rowRange", new { ranges = new[] { range } }, Flow)
            .Connect(Flow, Step, name, Port);
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
        var fields = EntityMapping.For<TRow>().ColumnOrdinals;
        var keys = new int[additional.Length + 1];
        var used = new HashSet<int>();
        for (int index = 0; index < keys.Length; index++)
        {
            var selector = index == 0 ? key : additional[index - 1];
            ArgumentNullException.ThrowIfNull(selector);
            string name = new NativeRuleExpression(Job.Id, step, selector).Key();
            if (!fields.TryGetValue(name, out var ordinal) || !used.Add(ordinal))
                throw new ArgumentException("Destination keys must be distinct columns in the mapped row", nameof(additional));
            keys[index] = ordinal;
        }
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
        if (!EntityMapping.For<TRow>().ColumnOrdinals.TryGetValue(column, out int index))
            throw new ArgumentException("Lookup key is not in the mapped row", parameter);
        return index;
    }
}
