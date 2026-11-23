#include "internal.h"

static int named(ud_string value, const char *literal) {
    size_t length = strlen(literal);
    return value.length == length && !memcmp(value.data, literal, length);
}

static int component_suffix(ud_string value, const char *suffix) {
    size_t length = strlen(suffix);
    return value.length >= length && !memcmp(value.data + value.length - length, suffix, length);
}

ud_status ud_data_flow_is_continuous(const ud_json *flow, int32_t *continuous) {
    *continuous = 0;
    const ud_json *nodes = ud_member(flow, "components");
    for (uint32_t node = 0; node < ud_json_size(nodes); node++) {
        const ud_json *component = ud_json_at(nodes, node);
        ud_string kind;
        UD_TRY(ud_json_string(ud_member(component, "component"), &kind));
        const ud_json *config = ud_member(component, "config");
        if (component_suffix(kind, ".cdc.source")) {
            uint64_t maximum = 0;
            const ud_json *limit = ud_member(config, "maxTransactions");
            if (limit)
                UD_TRY(ud_json_uint(limit, &maximum));
            *continuous = maximum == 0;
        } else if (named(kind, "mq.source")) {
            *continuous = !ud_member(config, "endOffsets") && !ud_member(config, "fromStage");
        }
        if (*continuous)
            break;
    }
    return UD_OK;
}

ud_status ud_package_is_continuous(const ud_package *package, int32_t *continuous) {
    if (!package || !package->document || !continuous)
        return UD_INVALID;
    *continuous = 0;
    const ud_json *flows = ud_member(package->document, "dataFlows");
    for (uint32_t index = 0; index < ud_json_size(flows) && !*continuous; index++) {
        UD_TRY(ud_data_flow_is_continuous(ud_json_at(flows, index), continuous));
    }
    return UD_OK;
}

/* A complete analysis snapshot consumes one database cursor. The file becomes
 * visible to readers only after its validation receipt and the Catalog CAS.
 * A failed attempt starts over with a new output file, never by resuming SQL.
 *
 * This predicate is intentionally an exact graph recognizer. It is used by
 * both the journal decision and the live-SQL contract, so a permissive match
 * here would silently allow a stream cursor to feed an arbitrary file write.
 * The graph has one SQL source, one prepare component, eight deterministic
 * row-key shards, one create destination and one count/receipt chain per
 * shard, plus the validation and error sinks. */
#define UD_ANALYSIS_SNAPSHOT_SHARDS 8

typedef struct analysis_snapshot_node {
    ud_string id, kind;
    const ud_json *config;
} analysis_snapshot_node;

typedef struct analysis_snapshot_edge {
    ud_string from, to;
    uint64_t output, input;
} analysis_snapshot_edge;

static int analysis_snapshot_parse_node(const ud_json *value, analysis_snapshot_node *out) {
    static const char *keys[] = {"id", "component", "config"};
    if (!value || ud_json_keys(value, keys, sizeof(keys) / sizeof(*keys)) ||
        ud_json_string(ud_member(value, "id"), &out->id) ||
        ud_json_string(ud_member(value, "component"), &out->kind) ||
        ud_json_kind(ud_member(value, "config")) != UD_JSON_OBJECT)
        return 0;
    out->config = ud_member(value, "config");
    return out->id.length && out->kind.length;
}

static int analysis_snapshot_distinct(const ud_json *const *nodes, uint32_t count) {
    for (uint32_t i = 0; i < count; i++)
        for (uint32_t j = 0; j < i; j++)
            if (!nodes[i] || nodes[i] == nodes[j])
                return 0;
    return 1;
}

static int analysis_snapshot_parse_edge(const ud_json *value, analysis_snapshot_edge *out) {
    static const char *keys[] = {"from", "to", "output", "input"};
    if (!value || ud_json_keys(value, keys, sizeof(keys) / sizeof(*keys)) ||
        ud_json_string(ud_member(value, "from"), &out->from) ||
        ud_json_string(ud_member(value, "to"), &out->to))
        return 0;
    if (ud_json_uint(ud_member(value, "output"), &out->output) ||
        ud_json_uint(ud_member(value, "input"), &out->input))
        return 0;
    return out->from.length && out->to.length;
}

static int analysis_snapshot_target(const ud_json *flow, ud_string from, uint64_t output,
                                    uint64_t input, ud_string *target) {
    const ud_json *paths = ud_member(flow, "paths");
    uint32_t matches = 0;
    for (uint32_t i = 0; i < ud_json_size(paths); i++) {
        analysis_snapshot_edge edge;
        if (!analysis_snapshot_parse_edge(ud_json_at(paths, i), &edge))
            return 0;
        if (!ud_same(edge.from, from) || edge.output != output)
            continue;
        if (edge.input != input || ++matches != 1)
            return 0;
        *target = edge.to;
    }
    return matches == 1;
}

static int analysis_snapshot_node_allowed(ud_string kind) {
    return ud_named(kind, "sql.source") || ud_named(kind, "analysis.prepare") ||
           ud_named(kind, "split") || ud_named(kind, "multicast") ||
           ud_named(kind, "file.parquet.destination") || ud_named(kind, "aggregate") ||
           ud_named(kind, "derive") || ud_named(kind, "result.destination") ||
           ud_named(kind, "discard");
}

static int analysis_snapshot_graph_shape(const ud_json *flow) {
    const ud_json *components = ud_member(flow, "components");
    const uint32_t count = ud_json_size(components);
    for (uint32_t i = 0; i < count; i++) {
        analysis_snapshot_node node;
        if (!analysis_snapshot_parse_node(ud_json_at(components, i), &node) ||
            !analysis_snapshot_node_allowed(node.kind))
            return 0;
        for (uint32_t prior = 0; prior < i; prior++) {
            analysis_snapshot_node other;
            if (!analysis_snapshot_parse_node(ud_json_at(components, prior), &other) ||
                ud_same(node.id, other.id))
                return 0;
        }
    }
    return 1;
}

static uint32_t analysis_snapshot_kind_count(const ud_json *flow, const char *kind) {
    const ud_json *components = ud_member(flow, "components");
    uint32_t count = 0;
    for (uint32_t i = 0; i < ud_json_size(components); i++) {
        analysis_snapshot_node node;
        if (analysis_snapshot_parse_node(ud_json_at(components, i), &node) &&
            ud_named(node.kind, kind))
            count++;
    }
    return count;
}

static const ud_json *analysis_snapshot_find_id(const ud_json *flow, ud_string id) {
    const ud_json *components = ud_member(flow, "components");
    for (uint32_t i = 0; i < ud_json_size(components); i++) {
        analysis_snapshot_node node;
        if (analysis_snapshot_parse_node(ud_json_at(components, i), &node) &&
            ud_same(node.id, id))
            return ud_json_at(components, i);
    }
    return NULL;
}

static const ud_json *analysis_snapshot_find_kind(const ud_json *flow, const char *kind) {
    const ud_json *components = ud_member(flow, "components");
    for (uint32_t i = 0; i < ud_json_size(components); i++) {
        analysis_snapshot_node node;
        if (analysis_snapshot_parse_node(ud_json_at(components, i), &node) &&
            ud_named(node.kind, kind))
            return ud_json_at(components, i);
    }
    return NULL;
}

static int analysis_snapshot_paths_valid(const ud_json *flow) {
    const ud_json *paths = ud_member(flow, "paths");
    for (uint32_t i = 0; i < ud_json_size(paths); i++) {
        analysis_snapshot_edge edge;
        if (!analysis_snapshot_parse_edge(ud_json_at(paths, i), &edge))
            return 0;
    }
    return 1;
}

static int analysis_snapshot_stream_source(const analysis_snapshot_node *source) {
    ud_string mode = {0}, table = {0};
    if (ud_json_string(ud_member(source->config, "readMode"), &mode) ||
        !ud_named(mode, "stream") ||
        ud_json_string(ud_member(source->config, "table"), &table) || !table.length ||
        ud_member(source->config, "sql") || ud_member(source->config, "incremental") ||
        ud_member(source->config, "partitionCount") || ud_member(source->config, "partitionId") ||
        ud_member(source->config, "partitionBegin") || ud_member(source->config, "partitionEnd"))
        return 0;
    return 1;
}

static int analysis_snapshot_create_file(const analysis_snapshot_node *file) {
    const ud_json *mode = ud_member(file->config, "fileMode");
    ud_string value = {0};
    return !mode || (!ud_json_string(mode, &value) && ud_named(value, "create"));
}

/* The graph is intentionally structural: Native binders already validate the
 * component schemas, expression types, and resolved port identities. This
 * predicate only decides whether the live SQL cursor is allowed to publish the
 * one frozen analysis snapshot shape. */
int ud_analysis_snapshot_flow(const ud_json *flow) {
    const ud_json *components, *paths;
    const ud_json *source_component, *model_component, *split_component, *validation_component;
    const ud_json *multicasts[UD_ANALYSIS_SNAPSHOT_SHARDS] = {0};
    const ud_json *files[UD_ANALYSIS_SNAPSHOT_SHARDS] = {0};
    const ud_json *aggregates[UD_ANALYSIS_SNAPSHOT_SHARDS] = {0};
    const ud_json *derives[UD_ANALYSIS_SNAPSHOT_SHARDS] = {0};
    const ud_json *results[UD_ANALYSIS_SNAPSHOT_SHARDS] = {0};
    const ud_json *discards[2] = {0};
    uint32_t expected_components, expected_paths;
    ud_string source_id = {0}, model_id = {0}, split_id = {0}, target = {0};

    if (!flow || ud_member(flow, "transaction") || ud_member(flow, "delivery") ||
        ud_json_kind(components = ud_member(flow, "components")) != UD_JSON_ARRAY ||
        ud_json_kind(paths = ud_member(flow, "paths")) != UD_JSON_ARRAY)
        return 0;
    expected_components = 3 + 4 * UD_ANALYSIS_SNAPSHOT_SHARDS +
                          (UD_ANALYSIS_SNAPSHOT_SHARDS + 1) + 2;
    expected_paths = 3 + 5 * UD_ANALYSIS_SNAPSHOT_SHARDS + 2;
    if (ud_json_size(components) != expected_components ||
        ud_json_size(paths) != expected_paths ||
        !analysis_snapshot_graph_shape(flow) || !analysis_snapshot_paths_valid(flow))
        return 0;
    if (analysis_snapshot_kind_count(flow, "sql.source") != 1 ||
        analysis_snapshot_kind_count(flow, "analysis.prepare") != 1 ||
        analysis_snapshot_kind_count(flow, "split") != 1 ||
        analysis_snapshot_kind_count(flow, "multicast") != UD_ANALYSIS_SNAPSHOT_SHARDS ||
        analysis_snapshot_kind_count(flow, "file.parquet.destination") !=
            UD_ANALYSIS_SNAPSHOT_SHARDS ||
        analysis_snapshot_kind_count(flow, "aggregate") != UD_ANALYSIS_SNAPSHOT_SHARDS ||
        analysis_snapshot_kind_count(flow, "derive") != UD_ANALYSIS_SNAPSHOT_SHARDS ||
        analysis_snapshot_kind_count(flow, "result.destination") !=
            UD_ANALYSIS_SNAPSHOT_SHARDS + 1 ||
        analysis_snapshot_kind_count(flow, "discard") != 2)
        return 0;

    analysis_snapshot_node source_node, model_node, split_node;
    source_component = analysis_snapshot_find_kind(flow, "sql.source");
    model_component = analysis_snapshot_find_kind(flow, "analysis.prepare");
    split_component = analysis_snapshot_find_kind(flow, "split");
    if (!source_component || !model_component || !split_component ||
        !analysis_snapshot_parse_node(source_component, &source_node) ||
        !analysis_snapshot_parse_node(model_component, &model_node) ||
        !analysis_snapshot_parse_node(split_component, &split_node) ||
        !analysis_snapshot_stream_source(&source_node))
        return 0;
    source_id = source_node.id;
    model_id = model_node.id;
    split_id = split_node.id;

    if (!analysis_snapshot_target(flow, source_id, 0, 0, &target) ||
        !ud_same(target, model_id) ||
        !analysis_snapshot_target(flow, model_id, 0, 0, &target) ||
        !ud_same(target, split_id) ||
        !analysis_snapshot_target(flow, model_id, 1, 0, &target))
        return 0;
    validation_component = analysis_snapshot_find_id(flow, target);
    analysis_snapshot_node validation_node;
    if (!validation_component ||
        !analysis_snapshot_parse_node(validation_component, &validation_node) ||
        !ud_named(validation_node.kind, "result.destination"))
        return 0;

    for (uint32_t i = 0; i < UD_ANALYSIS_SNAPSHOT_SHARDS; i++) {
        analysis_snapshot_node fan_node, file_node, aggregate_node, derive_node, result_node;
        const ud_json *fan, *file, *aggregate, *derive, *result;
        if (!analysis_snapshot_target(flow, split_id, i, 0, &target))
            return 0;
        fan = analysis_snapshot_find_id(flow, target);
        if (!fan || !analysis_snapshot_parse_node(fan, &fan_node) ||
            !ud_named(fan_node.kind, "multicast"))
            return 0;
        multicasts[i] = fan;

        if (!analysis_snapshot_target(flow, fan_node.id, 0, 0, &target))
            return 0;
        file = analysis_snapshot_find_id(flow, target);
        if (!file || !analysis_snapshot_parse_node(file, &file_node) ||
            !ud_named(file_node.kind, "file.parquet.destination") ||
            !analysis_snapshot_create_file(&file_node))
            return 0;
        files[i] = file;

        if (!analysis_snapshot_target(flow, fan_node.id, 1, 0, &target))
            return 0;
        aggregate = analysis_snapshot_find_id(flow, target);
        if (!aggregate || !analysis_snapshot_parse_node(aggregate, &aggregate_node) ||
            !ud_named(aggregate_node.kind, "aggregate"))
            return 0;
        aggregates[i] = aggregate;

        if (!analysis_snapshot_target(flow, aggregate_node.id, 0, 0, &target))
            return 0;
        derive = analysis_snapshot_find_id(flow, target);
        if (!derive || !analysis_snapshot_parse_node(derive, &derive_node) ||
            !ud_named(derive_node.kind, "derive"))
            return 0;
        derives[i] = derive;

        if (!analysis_snapshot_target(flow, derive_node.id, 0, 0, &target))
            return 0;
        result = analysis_snapshot_find_id(flow, target);
        if (!result || !analysis_snapshot_parse_node(result, &result_node) ||
            !ud_named(result_node.kind, "result.destination"))
            return 0;
        results[i] = result;
    }
    for (uint32_t i = 0; i < 2; i++) {
        analysis_snapshot_node discard_node;
        if (!analysis_snapshot_target(flow, split_id, UD_ANALYSIS_SNAPSHOT_SHARDS + i, 0,
                                      &target))
            return 0;
        discards[i] = analysis_snapshot_find_id(flow, target);
        if (!discards[i] || !analysis_snapshot_parse_node(discards[i], &discard_node) ||
            !ud_named(discard_node.kind, "discard"))
            return 0;
    }
    if (!analysis_snapshot_distinct(multicasts, UD_ANALYSIS_SNAPSHOT_SHARDS) ||
        !analysis_snapshot_distinct(files, UD_ANALYSIS_SNAPSHOT_SHARDS) ||
        !analysis_snapshot_distinct(aggregates, UD_ANALYSIS_SNAPSHOT_SHARDS) ||
        !analysis_snapshot_distinct(derives, UD_ANALYSIS_SNAPSHOT_SHARDS) ||
        !analysis_snapshot_distinct(results, UD_ANALYSIS_SNAPSHOT_SHARDS) ||
        !analysis_snapshot_distinct(discards, 2))
        return 0;
    for (uint32_t i = 0; i < UD_ANALYSIS_SNAPSHOT_SHARDS; i++)
        if (results[i] == validation_component)
            return 0;
    return 1;
}

/* A stream-only SQL flow has no replay boundary: the live provider cursor is
 * the source of truth and the SQL response sink does not need a receipt table.
 * Every other package keeps the existing Journal contract.  This is deliberately
 * conservative; returning true selects recovery, while returning false is only
 * allowed for the narrow, validated stream shape. */
ud_status ud_package_requires_journal(const ud_package *package, int32_t *required) {
    if (!package || !package->document || !required)
        return UD_INVALID;
    *required = 1;
    const ud_json *flows = ud_member(package->document, "dataFlows");
    if (ud_json_kind(flows) != UD_JSON_ARRAY || !ud_json_size(flows))
        return UD_OK;
    int stream_source = 0, client_results = 0, external_response_sink = 0;
    for (uint32_t flow_index = 0; flow_index < ud_json_size(flows); flow_index++) {
        const ud_json *flow = ud_json_at(flows, flow_index);
        const ud_json *components = ud_member(flow, "components");
        if (ud_json_kind(components) != UD_JSON_ARRAY)
            return UD_SCHEMA;
        if (ud_member(flow, "transaction"))
            return UD_OK;
        const ud_json *delivery = ud_member(flow, "delivery");
        if (delivery)
            return UD_OK; /* An explicit delivery policy keeps the durable execution contract. */
        int flow_source = 0;
        int analysis_snapshot = ud_analysis_snapshot_flow(flow);
        for (uint32_t component_index = 0; component_index < ud_json_size(components);
             component_index++) {
            const ud_json *component = ud_json_at(components, component_index);
            ud_string kind = {0};
            UD_TRY(ud_json_string(ud_member(component, "component"), &kind));
            const ud_json *config = ud_member(component, "config");
            if (ud_json_kind(config) != UD_JSON_OBJECT)
                return UD_SCHEMA;
            if (named(kind, "sql.source")) {
                ud_string mode = {"snapshot", 8};
                if (ud_member(config, "readMode"))
                    UD_TRY(ud_json_string(ud_member(config, "readMode"), &mode));
                if (!named(mode, "stream"))
                    return UD_OK;
                if (ud_member(config, "incremental") || ud_member(config, "partitionCount") ||
                    ud_member(config, "partitionId") || ud_member(config, "partitionBegin") ||
                    ud_member(config, "partitionEnd"))
                    return UD_OK;
                stream_source = 1;
                flow_source = 1;
                continue;
            }
            if (component_suffix(kind, ".source"))
                return UD_OK;
            if (named(kind, "result.destination")) {
                client_results = 1;
                continue;
            }
            if (named(kind, "sql.destination")) {
                external_response_sink = 1;
                const ud_json *delivery_setting = ud_member(config, "delivery");
                if (delivery_setting) {
                    ud_string delivery_mode = {0};
                    UD_TRY(ud_json_string(delivery_setting, &delivery_mode));
                    if (!named(delivery_mode, "response"))
                        return UD_OK;
                } else {
                    const ud_json *provision = ud_member(config, "provisionReceipts");
                    const char *boolean = NULL;
                    uint32_t length = 0;
                    if (ud_json_kind(provision) == UD_JSON_BOOL &&
                        !ud_json_text(provision, &boolean, &length) && length == 4)
                        return UD_OK;
                }
                continue;
            }
            if (analysis_snapshot && (named(kind, "file.parquet.destination") || named(kind, "aggregate"))) continue;
            if (component_suffix(kind, ".destination") || named(kind, "sort") ||
                named(kind, "join") || named(kind, "aggregate") || named(kind, "window"))
                return UD_OK;
            /* The actual Registry descriptor in the Native binding step decides
             * whether a named transform is stateless and free of external effects. */
        }
        if (!flow_source)
            return UD_OK;
    }
    if (client_results && external_response_sink)
        return UD_OK;
    const ud_json *policy = ud_member(package->document, "policy");
    if (policy && (ud_member(policy, "transaction") || ud_member(policy, "transactions") ||
                   ud_member(policy, "recovery") || ud_member(policy, "durable")))
        return UD_OK;
    const ud_json *control = ud_member(package->document, "controlFlow");
    const ud_json *tasks = ud_member(control, "tasks");
    if (ud_json_kind(tasks) == UD_JSON_ARRAY) {
        for (uint32_t index = 0; index < ud_json_size(tasks); index++) {
            const ud_json *task = ud_json_at(tasks, index);
            ud_string kind = {0};
            UD_TRY(ud_json_string(ud_member(task, "kind"), &kind));
            if (ud_member(task, "children") || ud_member(task, "transaction"))
                return UD_OK;
            if (named(kind, "http.request")) {
                const ud_json *config = ud_member(task, "config");
                const ud_json *method = ud_member(config, "method");
                ud_string verb = {"GET", 3};
                if ((method && ud_json_string(method, &verb)) ||
                    (!named(verb, "GET") && !named(verb, "HEAD")))
                    return UD_OK;
                continue;
            }
            if (!named(kind, "dataFlow"))
                return UD_OK;
        }
    }
    *required = stream_source ? 0 : 1;
    return UD_OK;
}

static ud_status append(ud_bytes *b, const ud_json *value) {
    char *text = NULL;
    uint32_t length = 0;
    ud_status s = ud_json_serialize(value, &text, &length);
    if (!s)
        s = ud_bytes_put(b, text, length);
    ud_free(text);
    return s;
}
static ud_status ports(ud_bytes *b, const ud_json *paths, const ud_json *bound) {
    if (!bound)
        return append(b, paths);
    if (ud_json_size(paths) != ud_json_size(bound))
        return UD_CORRUPT;
    UD_TRY(ud_bytes_put(b, "[", 1));
    for (uint32_t i = 0; i < ud_json_size(paths); i++) {
        const ud_json *path = ud_json_at(paths, i), *plan = ud_json_at(bound, i);
        ud_string from = {0}, to = {0}, a = {0}, z = {0};
        UD_TRY(ud_json_string(ud_member(path, "from"), &from));
        UD_TRY(ud_json_string(ud_member(path, "to"), &to));
        UD_TRY(ud_json_string(ud_member(plan, "from"), &a));
        UD_TRY(ud_json_string(ud_member(plan, "to"), &z));
        if (!ud_same(from, a) || !ud_same(to, z))
            return UD_CORRUPT;
        if (i)
            UD_TRY(ud_bytes_put(b, ",", 1));
        UD_TRY(ud_bytes_put(b, "{\"from\":", 8));
        UD_TRY(ud_json_quote(b, from.data, from.length));
        UD_TRY(ud_bytes_put(b, ",\"to\":", 6));
        UD_TRY(ud_json_quote(b, to.data, to.length));
        UD_TRY(ud_bytes_put(b, ",\"output\":", 10));
        UD_TRY(append(b, ud_member(plan, "output")));
        UD_TRY(ud_bytes_put(b, ",\"input\":", 9));
        UD_TRY(append(b, ud_member(plan, "input")));
        UD_TRY(ud_bytes_put(b, "}", 1));
    }
    return ud_bytes_put(b, "]", 1);
}
static ud_status flow(ud_bytes *b, const ud_json *definition, const ud_json *plans) {
    ud_string id = {0};
    UD_TRY(ud_json_string(ud_member(definition, "id"), &id));
    const ud_json *bound = NULL;
    for (uint32_t i = 0; i < ud_json_size(plans); i++) {
        const ud_json *plan = ud_json_at(plans, i);
        ud_string name = {0};
        UD_TRY(ud_json_string(ud_member(plan, "id"), &name));
        if (!ud_same(id, name))
            continue;
        const ud_json *next = ud_member(plan, "paths");
        if (bound) {
            /* Reused flows with scope-dependent physical port assignments may
             * not be silently fixed to one task's configuration. */
            char *a = NULL, *z = NULL;
            uint32_t an = 0, zn = 0;
            ud_status s = ud_json_serialize(bound, &a, &an);
            if (!s)
                s = ud_json_serialize(next, &z, &zn);
            if (!s && (an != zn || memcmp(a, z, an)))
                s = UD_UNSUPPORTED;
            ud_free(a);
            ud_free(z);
            if (s)
                return s;
        } else
            bound = next;
    }
    if (!bound)
        for (uint32_t i = 0; i < ud_json_size(ud_member(definition, "paths")); i++) {
            const ud_json *path = ud_json_at(ud_member(definition, "paths"), i);
            const char *keys[] = {"output", "input"};
            for (uint32_t k = 0; k < 2; k++) {
                const ud_json *reference = ud_member(path, keys[k]);
                ud_string text = {0};
                ud_id identity;
                if (ud_json_kind(reference) == UD_JSON_STRING &&
                    !ud_json_string(reference, &text) && !ud_id_parse(text, &identity))
                    return UD_UNSUPPORTED;
            }
        }
    UD_TRY(ud_bytes_put(b, "{", 1));
    for (uint32_t i = 0; i < ud_json_size(definition); i++) {
        const char *key = NULL;
        uint32_t n = 0;
        UD_TRY(ud_json_key(definition, i, &key, &n));
        if (i)
            UD_TRY(ud_bytes_put(b, ",", 1));
        UD_TRY(ud_json_quote(b, key, n));
        UD_TRY(ud_bytes_put(b, ":", 1));
        const ud_json *value = ud_json_find(definition, key, n);
        if (n == 5 && !memcmp(key, "paths", 5))
            UD_TRY(ports(b, value, bound));
        else
            UD_TRY(append(b, value));
    }
    return ud_bytes_put(b, "}", 1);
}
ud_status ud_execution_reidentify_package(ud_execution *e, ud_string id, char **out,
                                          uint32_t *length) {
    if (!e || !out || !length || !id.data || !id.length || id.length > 255)
        return UD_INVALID;
    *out = NULL;
    *length = 0;
    if (atomic_load(&e->state) != UD_EXEC_READY)
        return UD_BUSY;
    if (e->external || ud_member(e->package->document, "extensions"))
        return UD_UNSUPPORTED;
    char *plan = NULL;
    uint32_t size = 0;
    ud_json *report = NULL;
    ud_status s = ud_execution_explain(e, &plan, &size);
    if (!s)
        s = ud_json_parse(e->ctx, plan, size, &report, NULL);
    ud_bytes b = {.ctx = e->ctx, .limit = 67108864};
    if (!s)
        s = ud_bytes_put(&b, "{", 1);
    for (uint32_t i = 0; !s && i < ud_json_size(e->package->document); i++) {
        const char *key = NULL;
        uint32_t n = 0;
        s = ud_json_key(e->package->document, i, &key, &n);
        if (!s && i)
            s = ud_bytes_put(&b, ",", 1);
        if (!s)
            s = ud_json_quote(&b, key, n);
        if (!s)
            s = ud_bytes_put(&b, ":", 1);
        const ud_json *value = !s ? ud_json_find(e->package->document, key, n) : NULL;
        if (!s && n == 2 && !memcmp(key, "id", 2))
            s = ud_json_quote(&b, id.data, id.length);
        else if (!s && n == 9 && !memcmp(key, "dataFlows", 9)) {
            s = ud_bytes_put(&b, "[", 1);
            for (uint32_t j = 0; !s && j < ud_json_size(value); j++) {
                if (j)
                    s = ud_bytes_put(&b, ",", 1);
                if (!s)
                    s = flow(&b, ud_json_at(value, j), ud_member(report, "pipelines"));
            }
            if (!s)
                s = ud_bytes_put(&b, "]", 1);
        } else if (!s)
            s = append(&b, value);
    }
    if (!s)
        s = ud_bytes_put(&b, "}", 1);
    ud_package *validated = NULL;
    if (!s)
        s = ud_package_create(e->ctx, (ud_string){(const char *)b.data, (uint32_t)b.size},
                              &validated);
    ud_package_destroy(validated);
    if (!s) {
        *out = ud_strdup_n(e->ctx, (const char *)b.data, (uint32_t)b.size);
        if (!*out)
            s = UD_RESOURCE;
        else
            *length = (uint32_t)b.size;
    }
    ud_bytes_clear(&b);
    ud_free(plan);
    ud_json_destroy(report);
    return s;
}
