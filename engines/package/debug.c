#include "internal.h"
typedef struct ud_debug {
    ud_mutex mutex;
    ud_condition changed;
    ud_json *configuration;
    uint32_t preview_rows;
    uint64_t retained_bytes, byte_limit;
    uint64_t steps;
    int requested, paused, boundary;
    struct {
        ud_string pipeline, component;
        uint32_t port;
        ud_id identity;
        ud_string name;
        ud_batch *sample;
        uint64_t total_rows;
        int truncated;
    } edges[64];
    uint32_t edge_count, next_edge;
} ud_debug;
int ud_debug_enabled(const ud_execution *execution) {
    return execution && execution->debug != NULL;
}
ud_status ud_execution_debug_configure(ud_execution *execution, ud_string text) {
    if (!execution)
        return UD_INVALID;
    if (atomic_load(&execution->state) != UD_EXEC_READY || execution->debug)
        return UD_BUSY;
    ud_json *config = NULL;
    UD_TRY(ud_json_parse(execution->ctx, text.data, text.length, &config, NULL));
    static const char *keys[] = {"previewRows", "breakpoints", "definitionBreakpoints"};
    ud_status status = ud_json_keys(config, keys, 3);
    uint64_t rows = 0;
    if (!status && ud_member(config, "previewRows"))
        status = ud_json_uint(ud_member(config, "previewRows"), &rows);
    if (!status && rows > 32)
        status = UD_SCHEMA;
    const ud_json *breakpoints = ud_member(config, "breakpoints");
    if (!status && breakpoints &&
        (ud_json_kind(breakpoints) != UD_JSON_ARRAY || ud_json_size(breakpoints) > 1024))
        status = UD_SCHEMA;
    for (uint32_t at = 0; !status && at < ud_json_size(breakpoints); at++) {
        ud_string id;
        status = ud_json_string(ud_json_at(breakpoints, at), &id);
    }
    const ud_json *definitions = ud_member(config, "definitionBreakpoints");
    if (!status && definitions &&
        (ud_json_kind(definitions) != UD_JSON_ARRAY || ud_json_size(definitions) > 1024))
        status = UD_SCHEMA;
    for (uint32_t i = 0; !status && i < ud_json_size(definitions); i++) {
        const ud_json *path = ud_json_at(definitions, i);
        if (ud_json_kind(path) != UD_JSON_ARRAY || !ud_json_size(path) || ud_json_size(path) > 64)
            status = UD_SCHEMA;
        for (uint32_t j = 0; !status && j < ud_json_size(path); j++) {
            ud_string id = {0};
            status = ud_json_string(ud_json_at(path, j), &id);
            if (!status && !id.length)
                status = UD_SCHEMA;
        }
    }
    ud_debug *debug = !status ? ud_alloc(execution->ctx, sizeof(*debug)) : NULL;
    if (!status && !debug)
        status = UD_RESOURCE;
    if (status) {
        ud_json_destroy(config);
        return status;
    }
    ud_mutex_init(&debug->mutex);
    ud_condition_init(&debug->changed);
    debug->configuration = config;
    debug->preview_rows = (uint32_t)rows;
    debug->byte_limit = execution->options.context.memory_limit
                            ? execution->options.context.memory_limit / 32
                            : 2097152;
    if (debug->byte_limit > 2097152)
        debug->byte_limit = 2097152;
    execution->debug = debug;
    return UD_OK;
}
ud_status ud_execution_debug_control(ud_execution *execution, ud_string text) {
    if (!execution || !execution->debug)
        return UD_UNSUPPORTED;
    ud_json *command = NULL;
    UD_TRY(ud_json_parse(execution->ctx, text.data, text.length, &command, NULL));
    static const char *keys[] = {"action"};
    ud_status status = ud_json_keys(command, keys, 1);
    ud_string action = {0};
    if (!status)
        status = ud_json_string(ud_member(command, "action"), &action);
    ud_debug *debug = execution->debug;
    ud_mutex_lock(&debug->mutex);
    if (!status && ud_named(action, "pause")) {
        debug->requested = 1;
        debug->steps = 0;
    } else if (!status && ud_named(action, "continue")) {
        debug->requested = debug->paused = 0;
        debug->steps = 0;
    } else if (!status && ud_named(action, "step")) {
        if (!debug->paused)
            status = UD_BUSY;
        else {
            debug->requested = debug->paused = 0;
            debug->steps = 1;
        }
    } else if (!status)
        status = UD_SCHEMA;
    ud_condition_wake(&debug->changed);
    ud_mutex_unlock(&debug->mutex);
    ud_json_destroy(command);
    return status;
}
static int definition_matches(const ud_activation *activation, const ud_json *path) {
    uint32_t remaining = ud_json_size(path);
    const ud_json *previous = NULL;
    for (const ud_activation *frame = activation; frame; frame = frame->parent) {
        const ud_json *definition = frame->task_definition;
        if (!definition || definition == previous)
            continue;
        previous = definition;
        if (!remaining)
            return 0;
        ud_string actual = {0}, expected = {0};
        if (ud_json_string(ud_member(definition, "id"), &actual) ||
            ud_json_string(ud_json_at(path, --remaining), &expected) || !ud_same(actual, expected))
            return 0;
    }
    return remaining == 0;
}
ud_status ud_debug_boundary(ud_activation *activation, int32_t kind) {
    ud_debug *debug = activation->execution->debug;
    if (!debug || ud_transaction_active(activation))
        return UD_OK;
    ud_mutex_lock(&debug->mutex);
    if (kind == UD_DEBUG_TASK) {
        const ud_json *points = ud_member(debug->configuration, "breakpoints");
        for (uint32_t at = 0; at < ud_json_size(points); at++) {
            ud_string id;
            if (!ud_json_string(ud_json_at(points, at), &id) && ud_same(id, activation->task_id))
                debug->requested = 1;
        }
        points = ud_member(debug->configuration, "definitionBreakpoints");
        for (uint32_t at = 0; at < ud_json_size(points); at++)
            if (definition_matches(activation, ud_json_at(points, at)))
                debug->requested = 1;
    }
    if (debug->requested && atomic_load(&activation->execution->queued) &&
        !ud_activation_stopping(activation)) {
        ud_mutex_unlock(&debug->mutex);
        return UD_BUSY; /* Drain queued batches cooperatively. */
    }
    ud_status status = UD_OK;
    while (debug->requested && !status && !ud_activation_stopping(activation)) {
        debug->paused = 1;
        debug->boundary = kind;
        ud_condition_wait_ms(&debug->changed, &debug->mutex, 50);
        status = ud_activation_check(activation);
    }
    debug->paused = 0;
    if (ud_activation_stopping(activation)) {
        debug->requested = 0;
        debug->steps = 0;
    }
    ud_mutex_unlock(&debug->mutex);
    return status;
}
void ud_debug_advance(ud_activation *activation) {
    ud_debug *debug = activation->execution->debug;
    if (!debug)
        return;
    ud_mutex_lock(&debug->mutex);
    if (debug->steps) {
        debug->steps--;
        debug->requested = 1;
    }
    ud_mutex_unlock(&debug->mutex);
}
static ud_status write_edge(ud_bytes *json, const ud_debug *debug, uint32_t at) {
    if (!debug->edges[at].pipeline.data || !debug->edges[at].component.data ||
        !debug->edges[at].name.data)
        return UD_RESOURCE;
    UD_TRY(ud_bytes_put(json, "{\"pipeline\":", sizeof("{\"pipeline\":") - 1));
    UD_TRY(ud_json_quote(json, debug->edges[at].pipeline.data, debug->edges[at].pipeline.length));
    UD_TRY(ud_bytes_put(json, ",\"component\":", sizeof(",\"component\":") - 1));
    UD_TRY(ud_json_quote(json, debug->edges[at].component.data, debug->edges[at].component.length));
    char port_id[33];
    ud_id_format(debug->edges[at].identity, port_id);
    UD_TRY(ud_bytes_put(json, ",\"portId\":", sizeof(",\"portId\":") - 1));
    UD_TRY(ud_json_quote(json, port_id, 32));
    UD_TRY(ud_bytes_put(json, ",\"portName\":", sizeof(",\"portName\":") - 1));
    UD_TRY(ud_json_quote(json, debug->edges[at].name.data, debug->edges[at].name.length));
    char header[160];
    int n = snprintf(header, sizeof(header),
                     ",\"port\":%u,\"totalRows\":%llu,\"truncated\":%s,\"schema\":",
                     debug->edges[at].port, (unsigned long long)debug->edges[at].total_rows,
                     debug->edges[at].truncated ? "true" : "false");
    UD_TRY(ud_bytes_put(json, header, (size_t)n));
    const ud_batch *batch = debug->edges[at].sample;
    if (!batch)
        return ud_bytes_put(json, "null,\"rows\":[]}", sizeof("null,\"rows\":[]}") - 1);
    char *schema = NULL;
    uint32_t length = 0;
    ud_status status = ud_schema_to_json(batch, &schema, &length);
    if (!status)
        status = ud_bytes_put(json, schema, length);
    ud_free(schema);
    if (!status)
        status = ud_bytes_put(json, ",\"rows\":[", sizeof(",\"rows\":[") - 1);
    for (uint32_t row = 0; !status && row < batch->rows; row++) {
        if (row)
            status = ud_bytes_put(json, ",", sizeof(",") - 1);
        if (!status)
            status = ud_bytes_put(json, "{\"identity\":", sizeof("{\"identity\":") - 1);
        char *info = NULL;
        uint32_t size = 0;
        if (!status)
            status = ud_batch_row_info_json(batch, row, &info, &size);
        if (!status)
            status = ud_bytes_put(json, info, size);
        ud_free(info);
        if (!status)
            status = ud_bytes_put(json, ",\"fields\":[", sizeof(",\"fields\":[") - 1);
        for (uint32_t column = 0; !status && column < batch->columns; column++) {
            ud_value value;
            if (column)
                status = ud_bytes_put(json, ",", sizeof(",") - 1);
            if (!status)
                status = ud_batch_get(batch, row, column, &value);
            if (!status && value.is_absent)
                status =
                    ud_bytes_put(json, "{\"present\":false}", sizeof("{\"present\":false}") - 1);
            else if (!status) {
                const char *prefix = "{\"present\":true,\"value\":";
                status = ud_bytes_put(json, prefix, strlen(prefix));
                if (!status)
                    status = ud_json_value_encode(json, &batch->column[column].field, &value);
                if (!status)
                    status = ud_bytes_put(json, "}", sizeof("}") - 1);
            }
        }
        if (!status)
            status = ud_bytes_put(json, "]}", sizeof("]}") - 1);
    }
    if (!status)
        status = ud_bytes_put(json, "]}", sizeof("]}") - 1);
    return status;
}
ud_status ud_debug_preview(ud_activation *activation, const ud_component_port *port,
                           const ud_batch *batch) {
    ud_debug *debug = activation->execution->debug;
    if (!debug || !debug->preview_rows)
        return UD_OK;
    ud_mutex_lock(&debug->mutex);
    uint32_t at = 0;
    for (; at < debug->edge_count; at++)
        if (debug->edges[at].port == port->index && debug->edges[at].pipeline.data &&
            debug->edges[at].component.data &&
            ud_same(debug->edges[at].pipeline, activation->pipeline_id) &&
            ud_same(debug->edges[at].component, activation->component_id))
            break;
    if (at == debug->edge_count) {
        if (at == 64) {
            at = debug->next_edge++ % 64;
            ud_free((void *)debug->edges[at].pipeline.data);
            ud_free((void *)debug->edges[at].component.data);
            ud_free((void *)debug->edges[at].name.data);
            ud_batch_destroy(debug->edges[at].sample);
            memset(&debug->edges[at], 0, sizeof(debug->edges[at]));
        } else
            debug->edge_count++;
        debug->edges[at].pipeline =
            (ud_string){ud_strdup_n(activation->execution->ctx, activation->pipeline_id.data,
                                    activation->pipeline_id.length),
                        activation->pipeline_id.length};
        debug->edges[at].component =
            (ud_string){ud_strdup_n(activation->execution->ctx, activation->component_id.data,
                                    activation->component_id.length),
                        activation->component_id.length};
        debug->edges[at].port = port->index;
        debug->edges[at].identity = port->id;
        debug->edges[at].name =
            (ud_string){ud_strdup_n(activation->execution->ctx, port->name.data, port->name.length),
                        port->name.length};
    }
    debug->edges[at].total_rows += batch->rows;
    ud_batch *sample = debug->edges[at].sample;
    ud_status status = UD_OK;
    if (sample) {
        if (sample->columns != batch->columns)
            status = UD_INCOMPATIBLE;
        for (uint32_t c = 0; !status && c < sample->columns; c++)
            if (!ud_field_matches(&sample->column[c].field, &batch->column[c].field,
                                  UD_FIELD_LAYOUT))
                status = UD_INCOMPATIBLE;
    } else
        status = ud_batch_create(activation->execution->ctx, batch->schema->fields, batch->columns,
                                 debug->preview_rows, &sample);
    debug->edges[at].sample = sample;
    for (uint32_t row = 0; !status && row < batch->rows && sample->rows < debug->preview_rows;
         row++) {
        /* A single large LOB must not turn observation into an unbounded copy. */
        size_t bytes = 0;
        for (uint32_t c = 0; c < batch->columns; c++) {
            ud_value v;
            if (!ud_batch_get(batch, row, c, &v) && !v.is_null && !v.is_absent &&
                (v.type == UD_UTF8 || v.type == UD_BINARY))
                bytes += v.text_length;
        }
        if (bytes > 65536) {
            status = UD_RESOURCE;
            break;
        }
        ud_bytes encoded = {.ctx = activation->execution->ctx, .limit = 65536};
        for (uint32_t c = 0; !status && c < batch->columns; c++) {
            ud_value v;
            status = ud_batch_get(batch, row, c, &v);
            if (!status && !v.is_absent)
                status = ud_json_value_encode(&encoded, &batch->column[c].field, &v);
        }
        size_t estimate = encoded.size + batch->columns * 16 + sizeof(ud_row_info);
        if (estimate > debug->byte_limit - debug->retained_bytes)
            status = UD_RESOURCE;
        ud_bytes_clear(&encoded);
        if (status)
            break;
        status = ud_batch_append_projected(sample, batch, row, NULL);
        if (!status)
            debug->retained_bytes += estimate;
    }
    if (status)
        debug->edges[at].truncated = 1;
    ud_mutex_unlock(&debug->mutex);
    return UD_OK; /* Observability cannot change business delivery. */
}
ud_status ud_execution_debug_snapshot(ud_execution *execution, char **out, uint32_t *length) {
    if (!execution || !out || !length)
        return UD_INVALID;
    *out = NULL;
    *length = 0;
    if (!execution->debug)
        return UD_UNSUPPORTED;
    ud_debug *debug = execution->debug;
    ud_bytes json = {.ctx = execution->ctx};
    ud_mutex_lock(&debug->mutex);
    char header[160];
    int size = snprintf(header, sizeof(header),
                        "{\"enabled\":true,\"maxConcurrency\":1,\"pauseRequested\":%s,\"paused\":%"
                        "s,\"boundary\":%d,\"preview\":[",
                        debug->requested ? "true" : "false", debug->paused ? "true" : "false",
                        debug->boundary);
    ud_status status = ud_bytes_put(&json, header, (size_t)size);
    for (uint32_t at = 0; !status && at < debug->edge_count; at++) {
        if (at)
            status = ud_bytes_put(&json, ",", sizeof(",") - 1);
        if (!status) {
            ud_bytes edge = {.ctx = execution->ctx, .limit = 262144};
            ud_status encoded = write_edge(&edge, debug, at);
            status = encoded ? ud_bytes_put(&json, "{\"truncated\":true}",
                                            sizeof("{\"truncated\":true}") - 1)
                             : ud_bytes_put(&json, edge.data, edge.size);
            ud_bytes_clear(&edge);
        }
    }
    if (!status)
        status = ud_bytes_put(&json, "]}", sizeof("]}") - 1);
    ud_mutex_unlock(&debug->mutex);
    if (!status && json.size <= UINT32_MAX) {
        *out = ud_strdup_n(execution->ctx, (const char *)json.data, json.size);
        if (!*out)
            status = UD_RESOURCE;
        else
            *length = (uint32_t)json.size;
    } else if (!status)
        status = UD_RESOURCE;
    ud_bytes_clear(&json);
    return status;
}
void ud_debug_destroy(ud_execution *execution) {
    if (!execution->debug)
        return;
    ud_debug *debug = execution->debug;
    ud_json_destroy(debug->configuration);
    for (uint32_t at = 0; at < debug->edge_count; at++) {
        ud_free((void *)debug->edges[at].pipeline.data);
        ud_free((void *)debug->edges[at].component.data);
        ud_free((void *)debug->edges[at].name.data);
        ud_batch_destroy(debug->edges[at].sample);
    }
    ud_condition_destroy(&debug->changed);
    ud_mutex_destroy(&debug->mutex);
    ud_free(debug);
    execution->debug = NULL;
}
