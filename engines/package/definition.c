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
            return UD_OK; /* Kafka stage/transaction delivery is durable by definition. */
        int flow_source = 0;
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
