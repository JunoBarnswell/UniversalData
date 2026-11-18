#include "pipeline_frontier.h"
#include "../package/internal.h"

ud_status ud_pipeline_frontier_observe(ud_pipeline_frontier *frontier, uint32_t port, int32_t kind,
                                       int64_t watermark) {
    uint64_t bit = UINT64_C(1) << port;
    if (kind == UD_COMPONENT_WATERMARK) {
        if ((frontier->observed & bit) && watermark < frontier->inputs[port])
            return UD_FENCED;
        frontier->inputs[port] = watermark;
        frontier->observed |= bit;
    }
    if (kind == UD_COMPONENT_IDLE)
        frontier->idle |= bit;
    else
        frontier->idle &= ~bit;
    return UD_OK;
}

int ud_pipeline_frontier_reduce(ud_pipeline_frontier *frontier, uint64_t live,
                                int64_t *watermark) {
    uint64_t active = live & ~frontier->idle;
    if (!active || (active & frontier->observed) != active)
        return 0;
    int64_t minimum = INT64_MAX;
    for (uint32_t port = 0; active; port++, active >>= 1)
        if ((active & 1) && frontier->inputs[port] < minimum)
            minimum = frontier->inputs[port];
    if (minimum <= frontier->emitted)
        return 0;
    *watermark = frontier->emitted = minimum;
    return 1;
}

ud_status ud_pipeline_frontier_emit(ud_pipeline_frontier *frontier, int64_t watermark) {
    if (watermark < frontier->emitted)
        return UD_FENCED;
    frontier->emitted = watermark;
    return UD_OK;
}

ud_status ud_pipeline_frontier_write(ud_bytes *payload, const ud_pipeline_frontier *frontier,
                                    uint32_t input_count) {
    char text[96];
    int length = snprintf(text, sizeof(text), "{\"idleMask\":%llu,\"emitted\":%lld,\"inputs\":[",
                          (unsigned long long)frontier->idle, (long long)frontier->emitted);
    UD_TRY(ud_bytes_put(payload, text, (size_t)length));
    for (uint32_t port = 0; port < input_count; port++) {
        if (port)
            UD_TRY(ud_bytes_put(payload, ",", 1));
        if (frontier->observed & (UINT64_C(1) << port)) {
            length = snprintf(text, sizeof(text), "%lld", (long long)frontier->inputs[port]);
            UD_TRY(ud_bytes_put(payload, text, (size_t)length));
        } else
            UD_TRY(ud_bytes_put(payload, "null", 4));
    }
    return ud_bytes_put(payload, "]}", 2);
}

static ud_status read_watermark(ud_context *ctx, const ud_json *json, int64_t *out) {
    static const ud_field field = {.type = UD_INT64};
    if (ud_json_kind(json) != UD_JSON_NUMBER)
        return UD_CORRUPT;
    ud_value value;
    UD_TRY(ud_json_value_decode(ctx, &field, json, UD_ROUND_EXACT, &value));
    *out = value.integer;
    return UD_OK;
}

ud_status ud_pipeline_frontier_read(ud_context *ctx, const ud_json *json, uint32_t input_count,
                                    uint64_t connected, ud_pipeline_frontier *frontier) {
    static const char *const keys[] = {"idleMask", "emitted", "inputs"};
    const ud_json *inputs = ud_member(json, "inputs");
    if (ud_json_keys(json, keys, 3) || ud_json_kind(inputs) != UD_JSON_ARRAY ||
        ud_json_size(inputs) != input_count)
        return UD_CORRUPT;
    ud_pipeline_frontier saved = {0};
    UD_TRY(ud_json_uint(ud_member(json, "idleMask"), &saved.idle));
    if (saved.idle & ~connected)
        return UD_CORRUPT;
    UD_TRY(read_watermark(ctx, ud_member(json, "emitted"), &saved.emitted));
    for (uint32_t port = 0; port < input_count; port++) {
        const ud_json *value = ud_json_at(inputs, port);
        if (ud_json_kind(value) != UD_JSON_NULL) {
            uint64_t bit = UINT64_C(1) << port;
            if (!(connected & bit))
                return UD_CORRUPT;
            UD_TRY(read_watermark(ctx, value, saved.inputs + port));
            saved.observed |= bit;
        }
    }
    *frontier = saved;
    return UD_OK;
}
