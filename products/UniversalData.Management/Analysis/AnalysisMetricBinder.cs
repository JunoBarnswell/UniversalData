using System.Text.Json;
using System.Text.Json.Nodes;
using UniversalData.Hosting;

namespace UniversalData.Management;

/// <summary>
/// Binds the persisted product metric model to Native aggregate and derive components.
/// The binder owns metric semantics; dashboard code owns date levels and presentation.
/// </summary>
internal static class AnalysisMetricBinder
{
    private const int MaximumMetricCount = 128;
    private const int ExactDistinctBudget = 30_000_000;
    private static readonly string[] BuiltinMetricIds = ["records", "amount", "orders", "average"];

    internal static void Validate(AnalysisModel model)
    {
        ArgumentNullException.ThrowIfNull(model);
        AnalysisMetricDefinition[] metrics = model.Metrics ?? [];
        if (metrics.Length > MaximumMetricCount)
            Invalid(StatusCode.Schema, "指标数量超过支持上限。");

        AnalysisRule[] modelFields = model.Fields
            ?? throw new UniversalDataException((int)StatusCode.Schema, "分析字段设置缺失。");
        var fields = new Dictionary<string, AnalysisRule>(StringComparer.Ordinal);
        foreach (var field in modelFields)
        {
            Text(field.FieldId, "字段标识", 128);
            Text(field.Label, "字段名称", 256);
            if (!fields.TryAdd(field.FieldId, field))
                Invalid(StatusCode.Schema, $"字段标识「{field.FieldId}」重复。");
        }
        var ids = new HashSet<string>(BuiltinMetricIds, StringComparer.Ordinal);
        var definitions = new Dictionary<string, AnalysisMetricDefinition>(StringComparer.Ordinal);
        foreach (var metric in metrics)
        {
            Text(metric.Id, "指标标识", 128);
            Text(metric.Label, "指标名称", 128);
            if (!ids.Add(metric.Id))
                Invalid(StatusCode.Schema, $"指标标识「{metric.Id}」重复或保留给内置指标。");
            if (metric.Unit is not null)
                Text(metric.Unit, "指标单位", 64);

            switch (metric.Operation)
            {
                case "count":
                    if (metric.Left is not null || metric.Right is not null)
                        Invalid(StatusCode.Schema, $"计数指标「{metric.Id}」不能引用其他指标。");
                    if (metric.FieldId is not null)
                        Field(fields, metric.FieldId, metric.Id, allowOmitted: false);
                    break;
                case "sum":
                    if (metric.Left is not null || metric.Right is not null || metric.FieldId is null)
                        Invalid(StatusCode.Schema, $"求和指标「{metric.Id}」需要一个金额字段。");
                    if (Field(fields, metric.FieldId, metric.Id, allowOmitted: false).Role != "amount")
                        Invalid(StatusCode.Schema, $"求和指标「{metric.Id}」只能使用金额字段。");
                    if (string.IsNullOrWhiteSpace(model.Currency))
                        Invalid(StatusCode.Schema, $"求和指标「{metric.Id}」缺少金额单位。");
                    if (metric.Unit is not null &&
                        !StringComparer.Ordinal.Equals(metric.Unit, model.Currency))
                        Invalid(StatusCode.Schema, $"求和指标「{metric.Id}」的单位必须与金额单位一致。");
                    break;
                case "distinct":
                    if (metric.Left is not null || metric.Right is not null || metric.FieldId is null)
                        Invalid(StatusCode.Schema, $"去重指标「{metric.Id}」需要一个字段。");
                    Field(fields, metric.FieldId, metric.Id, allowOmitted: false);
                    break;
                case "add":
                case "subtract":
                case "divide":
                    if (metric.FieldId is not null || metric.Left is null || metric.Right is null)
                        Invalid(StatusCode.Schema, $"派生指标「{metric.Id}」需要 Left 和 Right 指标引用。");
                    Text(metric.Left, "指标引用", 128);
                    Text(metric.Right, "指标引用", 128);
                    break;
                default:
                    Invalid(StatusCode.Unsupported, $"指标「{metric.Id}」的操作不受支持。");
                    break;
            }
            definitions.Add(metric.Id, metric);
        }

        foreach (var metric in metrics)
        {
            if (metric.Operation is not ("add" or "subtract" or "divide"))
                continue;
            RequireReference(metric.Left!, definitions, metric.Id);
            RequireReference(metric.Right!, definitions, metric.Id);
            if (metric.Operation is "add" or "subtract")
            {
                string? leftUnit = UnitOf(metric.Left!, definitions, model.Currency);
                string? rightUnit = UnitOf(metric.Right!, definitions, model.Currency);
                if ((leftUnit is null) != (rightUnit is null) ||
                    leftUnit is not null && !StringComparer.Ordinal.Equals(leftUnit, rightUnit))
                    Invalid(StatusCode.Schema, $"指标「{metric.Id}」的两个操作数单位不一致。");
                if (metric.Unit is not null && leftUnit is not null &&
                    !StringComparer.Ordinal.Equals(metric.Unit, leftUnit))
                    Invalid(StatusCode.Schema, $"指标「{metric.Id}」的结果单位与操作数不一致。");
            }
            // Division changes dimensionality (for example CNY/order or %),
            // so an explicit result unit is authoritative rather than being
            // required to equal either operand.  When omitted, Describe
            // infers the left unit only for a unitless denominator.
        }

        var visiting = new HashSet<string>(StringComparer.Ordinal);
        var visited = new HashSet<string>(StringComparer.Ordinal);
        foreach (var metric in metrics)
            Visit(metric.Id, definitions, visiting, visited);
    }

    internal static AnalysisMeasure[] Describe(AnalysisModel model, AnalysisMeasure[] defaults)
    {
        ArgumentNullException.ThrowIfNull(model);
        ArgumentNullException.ThrowIfNull(defaults);
        Validate(model);
        var measures = new List<AnalysisMeasure>(defaults.Length + (model.Metrics?.Length ?? 0));
        var labels = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var measure in defaults)
        {
            if (!labels.TryAdd(measure.Id, measure.Label))
                Invalid(StatusCode.Schema, $"指标标识「{measure.Id}」重复。");
            measures.Add(measure);
        }
        foreach (var field in model.Fields ?? [])
            labels.TryAdd(field.FieldId, field.Label);

        var definitions = (model.Metrics ?? []).ToDictionary(metric => metric.Id, StringComparer.Ordinal);
        foreach (var metric in model.Metrics ?? [])
        {
            if (!labels.TryAdd(metric.Id, metric.Label))
                Invalid(StatusCode.Schema, $"指标标识「{metric.Id}」重复。");
        }
        foreach (var metric in model.Metrics ?? [])
        {
            string calculation = Calculation(metric, labels);
            string? unit = metric.Unit ?? UnitOf(metric.Id, definitions, model.Currency);
            if (unit is null && metric.Operation == "sum" &&
                (model.Fields ?? []).FirstOrDefault(field => field.FieldId == metric.FieldId)?.Role == "amount")
                unit = model.Currency;
            measures.Add(new(metric.Id, metric.Label, calculation, metric.Operation is "count" or "sum" or "distinct"
                ? metric.FieldId : null, unit));
            labels[metric.Id] = metric.Label;
        }
        return measures.ToArray();
    }

    internal static string Build(EtlPackageBuilder plan, string aggregateId, string input, uint port,
        string[] keys, AnalysisAsset asset, string[] measures)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentException.ThrowIfNullOrEmpty(aggregateId);
        ArgumentException.ThrowIfNullOrEmpty(input);
        ArgumentNullException.ThrowIfNull(keys);
        ArgumentNullException.ThrowIfNull(asset);
        ArgumentNullException.ThrowIfNull(measures);
        AnalysisModel model = asset.Release.Model;
        Validate(model);
        var custom = (model.Metrics ?? []).ToDictionary(metric => metric.Id, StringComparer.Ordinal);
        AnalysisMeasure[] defaults = asset.Release.Measures
            .Where(measure => !custom.ContainsKey(measure.Id))
            .ToArray();
        var described = Describe(model, defaults);
        var available = described.ToDictionary(measure => measure.Id, StringComparer.Ordinal);
        foreach (string id in measures)
            if (!available.ContainsKey(id))
                Invalid(StatusCode.Invalid, $"所选版本中没有指标「{id}」。");

        var needed = new HashSet<string>(StringComparer.Ordinal) { "records" };
        var order = new List<string>();
        var visiting = new HashSet<string>(StringComparer.Ordinal);
        foreach (string id in measures)
            Expand(id, custom, available, needed, order, visiting);

        var aggregateIds = new List<string> { "records" };
        foreach (string id in order)
        {
            if (id != "records" && !aggregateIds.Contains(id, StringComparer.Ordinal))
                aggregateIds.Add(id);
        }
        var aggregates = new List<object>();
        foreach (string id in aggregateIds)
            TryAggregate(id, model, custom, available, aggregates);
        if (!aggregates.Any(aggregate => AggregateName(aggregate) == "records"))
            aggregates.Insert(0, new { function = "count", column = "*", name = "records" });

        plan.Component(aggregateId, "aggregate", new
        {
            plan = new { version = ProductInfo.Protocol, group = keys, aggregates = aggregates.ToArray() }
        }).Connect(input, aggregateId, port);

        string current = aggregateId;
        var built = new HashSet<string>(StringComparer.Ordinal);
        foreach (string id in order)
        {
            AnalysisMetricDefinition? definition = custom.GetValueOrDefault(id);
            if (id is "records" or "amount" or "orders" ||
                definition is not null && definition.Operation is not ("add" or "subtract" or "divide") ||
                !built.Add(id))
                continue;
            if (definition is null && id != "average")
                continue;
            string left = definition?.Left ?? "amount";
            string right = definition?.Right ?? "orders";
            string operation = definition?.Operation ?? "divide";
            var shapes = new Dictionary<string, MetricShape>(StringComparer.Ordinal);
            MetricShape shape = Shape(id, model, asset, custom, available, shapes);
            object expression = DerivedExpression(operation, left, right, shape, model, asset, custom,
                available, shapes);
            string component = aggregateId + "-metric-" + id;
            plan.Component(component, "derive", new
            {
                columns = new[] { new { name = id, expression } }
            }).Connect(current, component);
            current = component;
        }
        return current;
    }

    private static void Expand(string id, IReadOnlyDictionary<string, AnalysisMetricDefinition> custom,
        IReadOnlyDictionary<string, AnalysisMeasure> available, ISet<string> needed, ICollection<string> order,
        ISet<string> visiting)
    {
        if (!needed.Add(id))
            return;
        if (!visiting.Add(id))
            Invalid(StatusCode.Schema, $"指标「{id}」存在循环引用。");
        if (custom.GetValueOrDefault(id) is { } definition)
        {
            if (definition.Operation is "add" or "subtract" or "divide")
            {
                Expand(definition.Left!, custom, available, needed, order, visiting);
                Expand(definition.Right!, custom, available, needed, order, visiting);
            }
        }
        else if (id == "average")
        {
            Expand("amount", custom, available, needed, order, visiting);
            Expand("orders", custom, available, needed, order, visiting);
        }
        visiting.Remove(id);
        order.Add(id);
    }

    private static bool TryAggregate(string id, AnalysisModel model,
        IReadOnlyDictionary<string, AnalysisMetricDefinition> custom,
        IReadOnlyDictionary<string, AnalysisMeasure> available, ICollection<object> aggregates)
    {
        if (id == "records")
        {
            AddAggregate(aggregates, "count", "*", id);
            return true;
        }
        if (id == "average")
            return false;
        string? field = null;
        string operation;
        bool distinct = false;
        if (custom.GetValueOrDefault(id) is { } definition)
        {
            operation = definition.Operation;
            field = definition.FieldId;
            distinct = operation == "distinct";
        }
        else if (id == "amount")
        {
            operation = "sum";
            field = available.GetValueOrDefault(id)?.FieldId ?? model.Fields.FirstOrDefault(f => f.Role == "amount")?.FieldId;
        }
        else if (id == "orders")
        {
            operation = "count";
            field = available.GetValueOrDefault(id)?.FieldId ?? model.Fields.FirstOrDefault(f => f.Role == "identifier")?.FieldId;
            distinct = true;
        }
        else
            return false;

        if (operation is not ("count" or "sum" or "distinct"))
            return false;
        if (field is null && operation != "count")
            Invalid(StatusCode.Schema, $"指标「{id}」没有可用字段。");
        AddAggregate(aggregates, operation == "distinct" ? "count" : operation, field ?? "*", id,
            distinct);
        return true;
    }

    private static void AddAggregate(ICollection<object> aggregates, string function, string column, string name,
        bool distinct = false)
    {
        if (aggregates.Any(aggregate => AggregateName(aggregate) == name))
            return;
        aggregates.Add(distinct
            ? new { function, column, name, distinct = true, spill = true, maxDistinct = ExactDistinctBudget }
            : new { function, column, name });
    }

    private static string AggregateName(object aggregate) =>
        (string)aggregate.GetType().GetProperty("name")!.GetValue(aggregate)!;

    private static MetricShape Shape(string id, AnalysisModel model, AnalysisAsset asset,
        IReadOnlyDictionary<string, AnalysisMetricDefinition> custom,
        IReadOnlyDictionary<string, AnalysisMeasure> available,
        IDictionary<string, MetricShape> cache)
    {
        if (cache.TryGetValue(id, out MetricShape shape))
            return shape;
        shape = default;
        if (custom.GetValueOrDefault(id) is { } definition)
        {
            shape = definition.Operation switch
            {
                "count" or "distinct" => new("int64", 0, 0),
                "sum" => FieldShape(asset, definition.FieldId!, model, id),
                "add" or "subtract" => CommonShape(Shape(definition.Left!, model, asset, custom, available, cache),
                    Shape(definition.Right!, model, asset, custom, available, cache)),
                "divide" => DivideShape(Shape(definition.Left!, model, asset, custom, available, cache)),
                _ => throw new UniversalDataException((int)StatusCode.Unsupported, "Unsupported metric operation")
            };
        }
        else if (id is "records" or "orders")
            shape = new("int64", 0, 0);
        else if (id == "amount")
            shape = FieldShape(asset, available.GetValueOrDefault(id)?.FieldId ??
                model.Fields.FirstOrDefault(field => field.Role == "amount")?.FieldId, model, id);
        else if (id == "average")
            shape = DivideShape(Shape("amount", model, asset, custom, available, cache));
        else
            Invalid(StatusCode.Schema, $"指标「{id}」未定义。");
        cache[id] = shape;
        return shape;
    }

    private static MetricShape CommonShape(MetricShape left, MetricShape right)
    {
        if (left.Type == right.Type && (left.Type != "decimal128" || left.Scale == right.Scale))
            return left;
        if (left.Type == "decimal128" || right.Type == "decimal128")
            return new("decimal128", 38, Math.Clamp(Math.Max(left.Scale, right.Scale), -38, 38));
        return left;
    }

    private static MetricShape DivideShape(MetricShape left) => new("decimal128", 38,
        Math.Max(6, left.Type == "decimal128" ? left.Scale : 0));

    private static MetricShape FieldShape(AnalysisAsset asset, string? fieldId, AnalysisModel model, string metric)
    {
        if (fieldId is null)
        {
            Invalid(StatusCode.Schema, $"指标「{metric}」没有可用字段。");
            return default;
        }
        if (asset.Schema.ValueKind == JsonValueKind.Object && asset.Schema.TryGetProperty("fields", out var fields))
        {
            foreach (var field in fields.EnumerateArray())
            {
                if (field.GetProperty("name").GetString() != fieldId)
                    continue;
                string type = field.GetProperty("type").GetString() ?? "";
                int precision = field.TryGetProperty("precision", out var p) ? p.GetInt32() : 38;
                int scale = field.TryGetProperty("scale", out var s) ? s.GetInt32() : 0;
                return new(type, precision, scale);
            }
        }
        if (model.Fields.FirstOrDefault(field => field.FieldId == fieldId)?.Role == "amount")
            return new("decimal128", 38, 2);
        Invalid(StatusCode.Schema, $"指标「{metric}」引用的字段不存在。");
        return default;
    }

    private static object DerivedExpression(string operation, string leftId, string rightId, MetricShape result,
        AnalysisModel model, AnalysisAsset asset, IReadOnlyDictionary<string, AnalysisMetricDefinition> custom,
        IReadOnlyDictionary<string, AnalysisMeasure> available, IDictionary<string, MetricShape> cache)
    {
        object left = Col(leftId);
        object right = Col(rightId);
        MetricShape leftShape = Shape(leftId, model, asset, custom, available, cache);
        MetricShape rightShape = Shape(rightId, model, asset, custom, available, cache);
        if (operation is "add" or "subtract")
        {
            MetricShape target = CommonShape(leftShape, rightShape);
            left = Cast(left, leftShape, target);
            right = Cast(right, rightShape, target);
        }
        else if (operation == "divide")
        {
            // DIV promotes its result to decimal128 p38 with at least six
            // fractional digits.  Its operands still need their original
            // exact scale while the Native binder computes the common type;
            // widening a p38s2 source to p38s6 would itself overflow.
            MetricShape numerator = DecimalShape(leftShape);
            left = Cast(left, leftShape, numerator);
            // Keep the denominator's native decimal width (or the exact
            // 64-bit integer width) instead of widening it to p38s0.  The
            // Native scalar common type is calculated before DIV promotes
            // the result; p38s0 combined with a fractional numerator would
            // require more than 38 digits and correctly reports overflow.
            MetricShape denominator = DecimalShape(rightShape);
            right = Cast(right, rightShape, denominator);
            object zero = LiteralZero(denominator);
            object quotient = Op("div", left, right);
            return Op("conditional", Op("eq", right, zero), NullLiteral(result), quotient);
        }
        return Op(operation == "subtract" ? "sub" : operation, left, right);
    }

    private static object Cast(object value, MetricShape source, MetricShape target)
    {
        if (source.Type == target.Type && (source.Type != "decimal128" || source.Scale == target.Scale))
            return value;
        return new
        {
            cast = new { name = target.Type == "decimal128" ? "decimal128" : target.Type,
                type = target.Type, nullable = true, precision = target.Precision, scale = target.Scale },
            value
        };
    }

    private static object LiteralZero(MetricShape shape) => shape.Type == "decimal128"
        ? new JsonObject { ["literal"] = "0", ["type"] = shape.Type, ["precision"] = shape.Precision, ["scale"] = shape.Scale }
        : new JsonObject { ["literal"] = 0, ["type"] = shape.Type };

    private static MetricShape DecimalShape(MetricShape shape) => shape.Type == "decimal128"
        ? shape
        : new("decimal128", shape.Type == "uint64" ? 20 : 19, 0);

    private static object NullLiteral(MetricShape shape) =>
        new JsonObject { ["literal"] = null, ["type"] = shape.Type, ["precision"] = shape.Precision, ["scale"] = shape.Scale };

    private static object Col(string name) => new { column = name };
    private static object Op(string operation, params object[] args) => new { op = operation, args };

    private static string Calculation(AnalysisMetricDefinition metric, IReadOnlyDictionary<string, string> labels)
    {
        string Label(string id) => labels.GetValueOrDefault(id, id);
        return metric.Operation switch
        {
            "count" => metric.FieldId is null ? "按记录计数" : $"按「{Label(metric.FieldId)}」计数",
            "sum" => $"对「{Label(metric.FieldId!)}」求和",
            "distinct" => $"按「{Label(metric.FieldId!)}」精确去重",
            "add" => $"「{Label(metric.Left!)}」+「{Label(metric.Right!)}」",
            "subtract" => $"「{Label(metric.Left!)}」−「{Label(metric.Right!)}」",
            "divide" => $"「{Label(metric.Left!)}」÷「{Label(metric.Right!)}」，分母为零时为空",
            _ => metric.Operation
        };
    }

    private static AnalysisRule Field(IReadOnlyDictionary<string, AnalysisRule> fields, string? fieldId,
        string metric, bool allowOmitted)
    {
        if (fieldId is null || !fields.TryGetValue(fieldId, out var field))
        {
            Invalid(StatusCode.Schema, $"指标「{metric}」引用的字段不存在或已停用。");
            return default!;
        }
        if (!allowOmitted && field.Role == "omit")
        {
            Invalid(StatusCode.Schema, $"指标「{metric}」引用的字段不存在或已停用。");
            return default!;
        }
        return field;
    }

    private static void RequireReference(string id, IReadOnlyDictionary<string, AnalysisMetricDefinition> definitions,
        string owner)
    {
        if (!definitions.ContainsKey(id) && !BuiltinMetricIds.Contains(id, StringComparer.Ordinal))
            Invalid(StatusCode.Schema, $"指标「{owner}」引用了未知指标「{id}」。");
    }

    private static string? UnitOf(string id, IReadOnlyDictionary<string, AnalysisMetricDefinition> definitions,
        string? currency)
    {
        return UnitOf(id, definitions, currency, new HashSet<string>(StringComparer.Ordinal));
    }

    private static string? UnitOf(string id, IReadOnlyDictionary<string, AnalysisMetricDefinition> definitions,
        string? currency, ISet<string> visiting)
    {
        if (!visiting.Add(id))
            return null;
        try
        {
            if (definitions.GetValueOrDefault(id) is { } definition)
            {
                if (definition.Unit is not null)
                    return definition.Unit;
                if (definition.Operation == "sum")
                    return currency;
                if (definition.Operation is "add" or "subtract")
                {
                    string? left = UnitOf(definition.Left!, definitions, currency, visiting);
                    string? right = UnitOf(definition.Right!, definitions, currency, visiting);
                    return left is not null && StringComparer.Ordinal.Equals(left, right) ? left : null;
                }
                if (definition.Operation == "divide")
                {
                    string? left = UnitOf(definition.Left!, definitions, currency, visiting);
                    string? right = UnitOf(definition.Right!, definitions, currency, visiting);
                    return right is null ? left : null;
                }
                return null;
            }
            return id is "amount" or "average" ? currency : null;
        }
        finally
        {
            visiting.Remove(id);
        }
    }

    private static void Visit(string id, IReadOnlyDictionary<string, AnalysisMetricDefinition> definitions,
        ISet<string> visiting, ISet<string> visited)
    {
        if (BuiltinMetricIds.Contains(id, StringComparer.Ordinal) || visited.Contains(id))
            return;
        if (!visiting.Add(id))
        {
            Invalid(StatusCode.Schema, $"指标「{id}」存在循环引用。");
            return;
        }
        if (!definitions.TryGetValue(id, out var definition))
        {
            Invalid(StatusCode.Schema, $"指标引用了未知指标「{id}」。");
            return;
        }
        if (definition.Operation is "add" or "subtract" or "divide")
        {
            Visit(definition.Left!, definitions, visiting, visited);
            Visit(definition.Right!, definitions, visiting, visited);
        }
        visiting.Remove(id);
        visited.Add(id);
    }

    private static void Text(string? value, string label, int max)
    {
        if (string.IsNullOrWhiteSpace(value) || value.Length > max || value.Any(char.IsControl))
            Invalid(StatusCode.Invalid, $"{label}无效。");
    }

    private static void Invalid(StatusCode code, string message) =>
        throw new UniversalDataException((int)code, message);

    private readonly record struct MetricShape(string Type, int Precision, int Scale);
}
