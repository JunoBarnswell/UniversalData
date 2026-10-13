#include "advanced_internal.h"
static int text_is(const ud_json *j, ud_string name) {
    const char *p;
    uint32_t n;
    return j && !ud_json_text(j, &p, &n) && n == name.length && !memcmp(p, name.data, n);
}
static ud_status join_config(ud_advanced *a, const ud_json *entry, ud_string *out) {
    ud_bytes bytes = {.ctx = a->ctx};
    ud_status s = ud_bytes_put(&bytes, "{", 1);
    uint32_t written = 0;
    for (uint32_t i = 0; !s && i < ud_json_size(entry); i++) {
        const char *key;
        uint32_t n;
        s = ud_json_key(entry, i, &key, &n);
        if (s)
            break;
        if (n == 5 && !memcmp(key, "input", 5))
            continue;
        if (written++)
            s = ud_bytes_put(&bytes, ",", 1);
        if (!s)
            s = ud_json_quote(&bytes, key, n);
        if (!s)
            s = ud_bytes_put(&bytes, ":", 1);
        char *value = NULL;
        uint32_t length = 0;
        if (!s)
            s = ud_json_serialize(ud_json_at(entry, i), &value, &length);
        if (!s)
            s = ud_bytes_put(&bytes, value, length);
        ud_free(value);
    }
    if (!s)
        s = ud_bytes_put(&bytes, "}", 1);
    if (!s)
        s = ud_bytes_put(&bytes, "", 1);
    if (!s) {
        *out = (ud_string){(char *)bytes.data, (uint32_t)bytes.size - 1};
        bytes.data = NULL;
    }
    ud_bytes_clear(&bytes);
    return s;
}
/* Freeze the logical definition through the same Native Join binder. Physical
 * order may change; columns, aliases, lineage and nullability remain logical. */
static ud_status logical_schema(ud_advanced *a, const ud_json *names, const ud_json *joins,
                                ud_batch **out) {
    *out = NULL;
    ud_batch *left = NULL;
    UD_TRY(ud_advanced_clone(a->ctx, a->inputs[0], 0, &left));
    ud_status s = UD_OK;
    uint64_t used = 1;
    for (uint32_t level = 0; !s && level < a->ports - 1; level++) {
        const ud_json *entry = ud_json_at(joins, level);
        ud_string name = {0}, config = {0};
        s = ud_json_text(ud_advanced_member(entry, "input"), &name.data, &name.length);
        uint32_t input = UINT32_MAX;
        for (uint32_t p = 1; !s && p < a->ports; p++)
            if (text_is(ud_json_at(names, p), name)) input = p;
        if (!s && (input == UINT32_MAX || (used & (UINT64_C(1) << input)))) s = UD_SCHEMA;
        if (!s) s = join_config(a, entry, &config);
        ud_operator_options options = a->options;
        options.kind = UD_OPERATOR_JOIN;
        options.configuration = config;
        options.record_state = 0;
        options.input_count = 2;
        options.input_order = NULL;
        options.partition_count = 1;
        const ud_batch *inputs[] = {left, input == UINT32_MAX ? NULL : a->inputs[input]};
        ud_operator *bound = NULL;
        if (!s) s = ud_operator_create(a->ctx, &options, inputs, &bound);
        ud_batch *next = NULL;
        if (!s) s = ud_operator_schema(bound, 0, &next);
        ud_operator_destroy(bound);
        ud_free((void *)config.data);
        ud_batch_destroy(left);
        left = next;
        if (!s) used |= UINT64_C(1) << input;
    }
    if (s) ud_batch_destroy(left); else *out = left;
    return s;
}
ud_status ud_multiway_bind(ud_advanced *a) {
    const ud_json *names = ud_advanced_member(a->config, "inputs"),
                  *joins = ud_advanced_member(a->config, "joins");
    if (a->ports < 2 || a->ports > 64 || ud_json_size(names) != a->ports ||
        ud_json_size(joins) != a->ports - 1)
        return UD_SCHEMA;
    UD_TRY(logical_schema(a, names, joins, &a->schema[0]));
    int reorder = 1;
    ud_bytes tree = {.ctx = a->ctx};
    ud_status initialized = ud_bytes_put(&tree, "[", 1);
    if (initialized)
        return initialized;
    for (uint32_t i = 0; i < ud_json_size(joins); i++) {
        const ud_json *type = ud_advanced_member(ud_json_at(joins, i), "joinType");
        if (type && !ud_advanced_named(type, "inner"))
            reorder = 0;
        const ud_json *left_keys = ud_advanced_member(ud_json_at(joins, i), "leftKeys");
        for (uint32_t k = 0; k < ud_json_size(left_keys); k++)
            if (ud_json_kind(ud_json_at(left_keys, k)) != UD_JSON_STRING)
                reorder = 0;
    }
    uint64_t used = 1;
    ud_batch *left = NULL;
    UD_TRY(ud_advanced_clone(a->ctx, a->inputs[0], 0, &left));
    ud_status status = UD_OK;
    for (uint32_t level = 0; !status && level < a->ports - 1; level++) {
        const ud_json *selected = NULL;
        uint32_t input = UINT32_MAX;
        uint64_t cost = UINT64_MAX;
        for (uint32_t i = 0; i < ud_json_size(joins); i++) {
            if (!reorder && i != level)
                continue;
            const ud_json *entry = ud_json_at(joins, i);
            ud_string name;
            const ud_json *named = ud_advanced_member(entry, "input");
            if (ud_json_text(named, &name.data, &name.length)) {
                status = UD_SCHEMA;
                break;
            }
            uint32_t port = UINT32_MAX;
            for (uint32_t p = 1; p < a->ports; p++)
                if (text_is(ud_json_at(names, p), name))
                    port = p;
            if (port == UINT32_MAX) {
                status = UD_SCHEMA;
                break;
            }
            if (used & (UINT64_C(1) << port))
                continue;
            const ud_json *keys = ud_advanced_member(entry, "leftKeys");
            int ready = 1;
            for (uint32_t k = 0; k < ud_json_size(keys) && ready; k++) {
                uint32_t col;
                ready = !ud_compute_field_index(left, ud_json_at(keys, k), &col);
            }
            if (!ready)
                continue;
            uint64_t estimate =
                a->options.estimated_rows[port] ? a->options.estimated_rows[port] : UINT64_MAX - 1;
            if (!selected || estimate < cost || (estimate == cost && port < input)) {
                selected = entry;
                input = port;
                cost = estimate;
            }
        }
        if (status)
            break;
        if (!selected) {
            status = UD_SCHEMA;
            break;
        }
        ud_string config = {0};
        status = join_config(a, selected, &config);
        ud_operator_options o = a->options;
        o.kind = UD_OPERATOR_JOIN;
        o.configuration = config;
        o.input_count = 2;
        o.input_order = NULL;
        o.partition_count = 1;
        o.estimated_rows[1] = a->options.estimated_rows[input];
        const ud_batch *in[] = {left, a->inputs[input]};
        if (!status)
            status = ud_operator_create(a->ctx, &o, in, &a->joins[level]);
        ud_free((void *)config.data);
        ud_batch *next = NULL;
        if (!status)
            status = ud_operator_schema(a->joins[level], 0, &next);
        ud_batch_destroy(left);
        left = next;
        if (!status) {
            char *child = NULL;
            uint32_t length = 0;
            status = ud_operator_explain(a->joins[level], &child, &length);
            if (!status && level)
                status = ud_bytes_put(&tree, ",", 1);
            char header[96];
            int n = snprintf(header, sizeof(header),
                             "{\"level\":%u,\"left\":%d,\"rightInput\":%u,\"plan\":", level,
                             level ? (int)level - 1 : -1, input);
            if (!status)
                status = ud_bytes_put(&tree, header, (size_t)n);
            if (!status)
                status = ud_bytes_put(&tree, child, length);
            if (!status)
                status = ud_bytes_put(&tree, "}", 1);
            ud_free(child);
            a->join_inputs[level] = input;
            used |= UINT64_C(1) << input;
            a->join_count++;
        }
    }
    if (!status)
        status = ud_bytes_put(&tree, "]", 1);
    if (!status) {
        a->tree_length = (uint32_t)tree.size;
        status = ud_bytes_put(&tree, "", 1);
    }
    if (!status) {
        a->tree_json = (char *)tree.data;
        tree.data = NULL;
        uint32_t columns = a->schema[0]->columns;
        if (left->columns != columns)
            status = UD_SCHEMA;
        a->join_projection = !status ? ud_alloc(a->ctx, columns * sizeof(uint32_t)) : NULL;
        if (!status && !a->join_projection)
            status = UD_RESOURCE;
        for (uint32_t i = 0; !status && i < columns; i++) {
            int found = 0;
            for (uint32_t j = 0; j < columns; j++)
                if (!ud_id_compare(a->schema[0]->column[i].field.identity,
                                   left->column[j].field.identity) &&
                    ud_field_matches(&a->schema[0]->column[i].field, &left->column[j].field,
                                     UD_FIELD_VALUE_TYPE)) {
                    a->join_projection[i] = j;
                    found++;
                }
            if (found != 1)
                status = UD_SCHEMA;
        }
    }
    ud_batch_destroy(left);
    ud_bytes_clear(&tree);
    return status;
}
ud_status ud_multiway_push(ud_advanced *a, uint32_t port, const ud_batch *b) {
    uint32_t node = 0, input = 0;
    if (port) {
        input = 1;
        node = UINT32_MAX;
        for (uint32_t i = 0; i < a->join_count; i++)
            if (a->join_inputs[i] == port)
                node = i;
        if (node == UINT32_MAX)
            return UD_SCHEMA;
    }
    ud_string name = {0};
    const ud_json *names = ud_advanced_member(a->config, "inputs");
    UD_TRY(ud_json_text(ud_json_at(names, port), &name.data, &name.length));
    ud_id scope;
    UD_TRY(ud_id_make(a->ctx, (ud_string){"multiway.input", 14}, name, &scope));
    ud_batch *tagged = NULL;
    ud_status s = ud_batch_join_origin(a->ctx, b, scope, &tagged);
    if (!s) s = ud_operator_push(a->joins[node], input, tagged);
    ud_batch_destroy(tagged);
    return s;
}
ud_status ud_multiway_end(ud_advanced *a, uint32_t port) {
    if (!port)
        return ud_operator_finish_input(a->joins[0], 0);
    for (uint32_t i = 0; i < a->join_count; i++)
        if (a->join_inputs[i] == port)
            return ud_operator_finish_input(a->joins[i], 1);
    return UD_SCHEMA;
}
ud_status ud_multiway_pull(ud_advanced *a, ud_batch **out, uint32_t *port) {
    *out = NULL;
    if (port)
        *port = 0;
    for (;;) {
        UD_TRY(ud_advanced_check(a));
        if (a->join_at >= a->join_count)
            return UD_END;
        ud_batch *batch = NULL;
        ud_status s = ud_operator_pull(a->joins[a->join_at], &batch, NULL);
        if (s == UD_BUSY)
            return UD_BUSY;
        if (s == UD_END) {
            uint32_t finished = a->join_at++;
            if (a->join_at < a->join_count)
                UD_TRY(ud_operator_finish_input(a->joins[a->join_at], 0));
            ud_operator_destroy(a->joins[finished]);
            a->joins[finished] = NULL;
            continue;
        }
        if (s)
            return s;
        if (a->join_at + 1 == a->join_count) {
            ud_status projected = ud_batch_project(a->ctx, batch, a->join_projection,
                                                   a->schema[0]->schema->fields,
                                                   a->schema[0]->columns, out);
            ud_batch_destroy(batch);
            return projected;
        }
        s = ud_operator_push(a->joins[a->join_at + 1], 0, batch);
        ud_batch_destroy(batch);
        if (s)
            return s;
    }
}
