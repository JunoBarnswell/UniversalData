#include "internal.h"
typedef struct ud_debug {
    ud_mutex mutex;
    ud_condition changed;
    ud_json *configuration;
    uint32_t preview_rows;
    uint64_t steps;
    int requested, paused, boundary;
    struct {
        ud_string pipeline, component;
        uint32_t port;
        ud_bytes data;
    } edges[16];
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
    static const char *keys[] = {"previewRows", "breakpoints"};
    ud_status status = ud_json_keys(config, keys, 2);
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
    }
    if (debug->requested && atomic_load(&activation->execution->queued)) {
        ud_mutex_unlock(&debug->mutex);
        return UD_BUSY; /* Drain queued batches cooperatively. */
    }
    ud_status status = UD_OK;
    while (debug->requested && !status) {
        debug->paused = 1;
        debug->boundary = kind;
        ud_condition_wait_ms(&debug->changed, &debug->mutex, 50);
        status = ud_activation_check(activation);
    }
    debug->paused = 0;
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
ud_status ud_debug_preview(ud_activation *activation, uint32_t port, const ud_batch *batch) {
    ud_debug *debug = activation->execution->debug;
    if (!debug || !debug->preview_rows)
        return UD_OK;
    ud_bytes json = {.ctx = activation->execution->ctx, .limit = 262144};
    ud_status status = ud_bytes_put(&json, "{\"pipeline\":", 12);
    if (!status)
        status = ud_json_quote(&json, activation->pipeline_id.data, activation->pipeline_id.length);
    if (!status)
        status = ud_bytes_put(&json, ",\"component\":", 13);
    if (!status)
        status =
            ud_json_quote(&json, activation->component_id.data, activation->component_id.length);
    char header[96];
    uint32_t rows = batch->rows > debug->preview_rows ? debug->preview_rows : batch->rows;
    int length = snprintf(header, sizeof(header), ",\"port\":%u,\"totalRows\":%u,\"rows\":[", port,
                          batch->rows);
    if (!status)
        status = ud_bytes_put(&json, header, (size_t)length);
    for (uint32_t row = 0; !status && row < rows; row++) {
        if (row)
            status = ud_bytes_put(&json, ",", 1);
        if (!status)
            status = ud_bytes_put(&json, "{\"identity\":", 12);
        char *info = NULL;
        uint32_t size = 0;
        if (!status)
            status = ud_batch_row_info_json(batch, row, &info, &size);
        if (!status)
            status = ud_bytes_put(&json, info, size);
        ud_free(info);
        if (!status)
            status = ud_bytes_put(&json, ",\"fields\":[", 11);
        for (uint32_t column = 0; !status && column < batch->columns; column++) {
            ud_value value;
            if (column)
                status = ud_bytes_put(&json, ",", 1);
            if (!status)
                status = ud_batch_get(batch, row, column, &value);
            if (!status && value.is_absent)
                status = ud_bytes_put(&json, "{\"present\":false}", 17);
            else if (!status) {
                static const char prefix[] = "{\"present\":true,\"value\":";
                status = ud_bytes_put(&json, prefix, sizeof(prefix) - 1);
                if (!status)
                    status = ud_json_value_encode(&json, &batch->column[column].field, &value);
                if (!status)
                    status = ud_bytes_put(&json, "}", 1);
            }
        }
        if (!status)
            status = ud_bytes_put(&json, "]}", 2);
    }
    if (!status)
        status = ud_bytes_put(&json, "]}", 2);
    ud_mutex_lock(&debug->mutex);
    uint32_t at = 0;
    for (; at < debug->edge_count; at++)
        if (debug->edges[at].port == port &&
            ud_same(debug->edges[at].pipeline, activation->pipeline_id) &&
            ud_same(debug->edges[at].component, activation->component_id))
            break;
    if (at == debug->edge_count) {
        if (at == 16) {
            at = debug->next_edge++ % 16;
            ud_free((void *)debug->edges[at].pipeline.data);
            ud_free((void *)debug->edges[at].component.data);
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
        debug->edges[at].port = port;
    }
    ud_bytes_clear(&debug->edges[at].data);
    if (!status) {
        debug->edges[at].data = json;
        json.data = NULL;
    } else {
        const char *bounded = "{\"truncated\":true}";
        debug->edges[at].data.ctx = activation->execution->ctx;
        debug->edges[at].data.limit = 262144;
        (void)ud_bytes_put(&debug->edges[at].data, bounded, strlen(bounded));
    }
    ud_mutex_unlock(&debug->mutex);
    ud_bytes_clear(&json);
    /* Inspection never alters data delivery or confirmation. Encoding failure
     * is represented by a bounded snapshot rather than an execution error. */
    return UD_OK;
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
            status = ud_bytes_put(&json, ",", 1);
        if (!status)
            status = ud_bytes_put(&json,
                                  debug->edges[at].data.size ? debug->edges[at].data.data
                                                             : (const uint8_t *)"null",
                                  debug->edges[at].data.size ? debug->edges[at].data.size : 4);
    }
    if (!status)
        status = ud_bytes_put(&json, "]}", 2);
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
        ud_bytes_clear(&debug->edges[at].data);
    }
    ud_condition_destroy(&debug->changed);
    ud_mutex_destroy(&debug->mutex);
    ud_free(debug);
    execution->debug = NULL;
}
