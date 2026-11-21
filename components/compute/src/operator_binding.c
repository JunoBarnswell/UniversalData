#include "../internal.h"
#include "ud/operator.h"
/* Native parameter grammar and field binding are shared by direct C callers
 * and DataFlow descriptors. This is the binding part of the existing operator. */
static const ud_json *member(const ud_json *j, const char *n) {
    return ud_json_find(j, n, (uint32_t)strlen(n));
}
static ud_status integer(const ud_json *j, uint64_t *out) {
    const char *p;
    uint32_t n;
    UD_TRY(ud_json_text(j, &p, &n));
    if (ud_json_kind(j) != UD_JSON_NUMBER || !n)
        return UD_SCHEMA;
    *out = 0;
    for (uint32_t i = 0; i < n; i++) {
        if (p[i] < '0' || p[i] > '9' || *out > (UINT64_MAX - (unsigned)(p[i] - '0')) / 10)
            return UD_SCHEMA;
        *out = *out * 10 + (unsigned)(p[i] - '0');
    }
    return UD_OK;
}
static int named(const ud_json *j, const char *n) {
    const char *p;
    uint32_t len;
    return j && !ud_json_text(j, &p, &len) && len == strlen(n) && !memcmp(p, n, len);
}
ud_status ud_operator_bind_options(ud_context *ctx, const ud_operator_options *input,
                                   const ud_batch *schema, ud_operator_options *out,
                                   ud_json **document, char **configuration, uint32_t keys[16],
                                   uint32_t *directions, ud_plan **plan) {
    *out = *input;
    *document = NULL;
    *configuration = NULL;
    *directions = 0;
    *plan = NULL;
    ud_string declaration;
    UD_TRY(ud_operator_configuration_schema(input->kind, &declaration));
    ud_json *rule = NULL, *raw = NULL;
    ud_status status = ud_json_parse(ctx, declaration.data, declaration.length, &rule, NULL);
    if (!status)
        status =
            ud_json_parse(ctx, input->configuration.data, input->configuration.length, &raw, NULL);
    if (!status)
        status = ud_json_bind_schema(ctx, rule, raw, document);
    ud_json_destroy(rule);
    ud_json_destroy(raw);
    if (status)
        return status;
    uint32_t length = 0;
    status = ud_json_serialize(*document, configuration, &length);
    if (status)
        return status;
    out->configuration = (ud_string){*configuration, length};
    if (member(*document, "partitions")) {
        uint64_t n;
        UD_TRY(integer(member(*document, "partitions"), &n));
        out->partition_count = (uint32_t)n;
    }
    if (input->kind == UD_OPERATOR_AGGREGATE || input->kind == UD_OPERATOR_EVENT_WINDOW) {
        char *text = NULL;
        uint32_t n;
        UD_TRY(ud_json_serialize(member(*document, "plan"), &text, &n));
        status =
            ud_plan_create_fields(ctx, UD_PLAN_VERSION, schema->schema->fields, schema->columns,
                                  (ud_string){text, n}, input->expression_bindings, plan);
        ud_free(text);
        out->aggregation = *plan;
        return status;
    }
    const ud_json *selectors = member(*document, "keys");
    out->key_count = ud_json_size(selectors);
    if (out->key_count > 16)
        return UD_SCHEMA;
    for (uint32_t i = 0; i < out->key_count; i++)
        UD_TRY(ud_compute_field_index(schema, ud_json_at(selectors, i), keys + i));
    out->keys = keys;
    if (input->kind == UD_OPERATOR_SORT) {
        const ud_json *per_key = member(*document, "keyDirections");
        const ud_json *descending = member(*document, "descending");
        if (per_key && (descending || ud_json_size(per_key) != out->key_count))
            return UD_SCHEMA;
        out->descending = named(descending, "true");
        for (uint32_t i = 0; per_key && i < out->key_count; i++)
            if (named(ud_json_at(per_key, i), "true"))
                *directions |= UINT32_C(1) << i;
        return UD_OK;
    }
    uint64_t partition = 0;
    UD_TRY(integer(member(*document, "partitionKeys"), &partition));
    if (partition > out->key_count)
        return UD_SCHEMA;
    out->partition_key_count = (uint32_t)partition;
    const ud_json *name = member(*document, "name");
    UD_TRY(ud_json_text(name, &out->window_name.data, &out->window_name.length));
    static const char *functions[] = {"rowNumber", "runningSum",   "rank",  "denseRank",
                                      "lag",       "lead",         "first", "last",
                                      "movingSum", "movingAverage"};
    out->window_function = 0;
    for (uint32_t i = 0; i < 10; i++)
        if (named(member(*document, "function"), functions[i]))
            out->window_function = (int32_t)i + 1;
    if (!out->window_function)
        return UD_SCHEMA;
    if (out->window_function == UD_WINDOW_RUNNING_SUM || out->window_function >= UD_WINDOW_LAG)
        UD_TRY(ud_compute_field_index(schema, member(*document, "column"), &out->window_column));
    uint64_t offset = 1;
    if (member(*document, "offset"))
        UD_TRY(integer(member(*document, "offset"), &offset));
    out->window_offset = (uint32_t)offset;
    const ud_json *frame = member(*document, "frame");
    out->frame_kind = 0;
    if (frame) {
        out->frame_kind = named(member(frame, "type"), "range") ? UD_FRAME_RANGE : UD_FRAME_ROWS;
        UD_TRY(integer(member(frame, "preceding"), &out->frame_preceding));
        UD_TRY(integer(member(frame, "following"), &out->frame_following));
    }
    if (!frame && out->window_function >= UD_WINDOW_MOVING_SUM)
        return UD_SCHEMA;
    out->average_rounding =
        named(member(*document, "averageRounding"), "exact") ? UD_ROUND_EXACT : UD_ROUND_HALF_EVEN;
    return UD_OK;
}
