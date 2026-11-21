#include "builtins_internal.h"
#include "../../components/data/row_range.h"

typedef struct row_range {
    ud_component_context context;
    uint64_t offset, limit, seen;
} row_range;

static ud_status bind(const ud_component_context *context, ud_string text,
                      uint64_t *offset, uint64_t *limit) {
    ud_json *config = NULL;
    UD_TRY(ud_builtin_parse(context, text, &config));
    ud_status status = ud_row_range_bind(ud_member(config, "ranges"), offset, limit);
    ud_json_destroy(config);
    return status;
}

static ud_status configure(void *user, const ud_component_context *context, ud_string text,
                           const ud_batch *const *inputs, ud_batch **out) {
    (void)user;
    const ud_component_plan *input = context->input_plans;
    if (input->boundedness != UD_SOURCE_BOUNDED || input->ordering != UD_ORDER_GLOBAL ||
        !input->sort.stable)
        return ud_fail(context->memory, UD_UNSUPPORTED, "rowRange requires a bounded, stably ordered input");
    uint64_t offset, limit;
    UD_TRY(bind(context, text, &offset, &limit));
    return ud_builtin_clone_schema(context->memory, inputs[0], out);
}

static ud_status prepare(void *user, const ud_component_context *context, ud_string text,
                         const ud_batch *const *inputs, const ud_batch *const *outputs, void **out) {
    (void)user;
    (void)inputs;
    (void)outputs;
    uint64_t offset, limit;
    UD_TRY(bind(context, text, &offset, &limit));
    row_range *range = ud_alloc(context->memory, sizeof(*range));
    if (!range)
        return UD_RESOURCE;
    *range = (row_range){.context = *context, .offset = offset, .limit = limit};
    *out = range;
    return UD_OK;
}

static ud_status step(void *instance, const ud_component_event *event) {
    if (event->kind == UD_COMPONENT_POLL)
        return UD_END;
    if (event->kind != UD_COMPONENT_DATA)
        return UD_OK;
    row_range *range = instance;
    uint32_t rows = ud_batch_rows(event->batch);
    if (range->seen > UINT64_MAX - rows)
        return UD_OVERFLOW;
    uint64_t begin = range->seen, end = begin + rows;
    range->seen = end;
    if (!rows || !range->limit || end <= range->offset)
        return UD_OK;
    uint64_t start = begin > range->offset ? begin : range->offset;
    uint64_t used = start - range->offset;
    if (used >= range->limit)
        return UD_OK;
    uint64_t remaining = range->limit - used;
    uint32_t take = (uint32_t)(remaining < end - start ? remaining : end - start);
    if (take == rows)
        return range->context.emit(range->context.emit_user, 0, event->batch);
    ud_batch *slice = NULL;
    ud_status status = ud_batch_slice(range->context.memory, event->batch, (uint32_t)(start - begin),
                                     take, &slice);
    if (!status)
        status = range->context.emit(range->context.emit_user, 0, slice);
    ud_batch_destroy(slice);
    return status;
}

static ud_status checkpoint(void *instance, ud_batch **out) {
    const row_range *range = instance;
    const ud_field field = {.name = "seen", .name_length = 4, .type = UD_UINT64};
    UD_TRY(ud_batch_create(range->context.memory, &field, 1, 1, out));
    const ud_value value = {.type = UD_UINT64, .unsigned_integer = range->seen};
    ud_status status = ud_batch_append(*out, &value, 1);
    if (!status)
        status = ud_batch_seal(*out);
    if (status) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return status;
}

static ud_status restore(void *instance, const ud_batch *state) {
    row_range *range = instance;
    ud_value value;
    if (ud_batch_columns(state) != 1 || ud_batch_rows(state) != 1 || ud_batch_get(state, 0, 0, &value) ||
        value.type != UD_UINT64 || value.is_null || value.is_absent)
        return UD_CORRUPT;
    range->seen = value.unsigned_integer;
    return UD_OK;
}

static ud_status finish(void *instance) {
    (void)instance;
    return UD_OK;
}

static void dispose(void *instance, ud_status result) {
    (void)result;
    ud_free(instance);
}

ud_status ud_row_range_register(ud_registry *registry) {
    static const char schema[] = "{\"type\":\"object\",\"properties\":{\"ranges\":{\"type\":\"array\","
        "\"minItems\":1,\"items\":{\"type\":\"object\",\"properties\":{\"op\":{\"enum\":[\"skip\",\"take\"]},"
        "\"count\":{\"type\":\"integer\"}},\"required\":[\"op\",\"count\"],\"additionalProperties\":false}}},"
        "\"required\":[\"ranges\"],\"additionalProperties\":false}";
    const ud_component_descriptor descriptor = {
        .struct_size = sizeof(descriptor), .abi_version = UD_ABI_VERSION,
        .id = {"rowRange", 8}, .name = {"Row Range", 9},
        .version = {UD_SDK_VERSION, sizeof(UD_SDK_VERSION) - 1},
        .configuration_schema = {schema, sizeof(schema) - 1}, .category = UD_COMPONENT_TRANSFORM,
        .inputs = 1, .outputs = 1, .synchronous_input = 0, .error_output = -1,
        .capabilities = UD_COMPONENT_REPLAY_SAFE | UD_COMPONENT_THREAD_SAFE |
                        UD_COMPONENT_STATEFUL | UD_COMPONENT_PREVIEW_SAFE,
        .configure = configure, .prepare = prepare, .step = step, .finish = finish, .dispose = dispose,
        .state_version = 1, .checkpoint = checkpoint, .restore = restore};
    return ud_registry_register(registry, &descriptor);
}
