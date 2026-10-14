#include "../internal.h"
ud_status ud_function_argument_schema(ud_context *ctx, const ud_field *fields, uint32_t count,
                                      ud_batch **out) {
    if (!ctx || !out || count > 16 || (count && !fields))
        return UD_INVALID;
    *out = NULL;
    if (!count)
        return UD_OK;
    ud_field copy[16] = {{0}};
    ud_status status = UD_OK;
    char names[16][24];
    for (uint32_t j = 0; !status && j < count; j++) {
        int n = snprintf(names[j], 24, "argument%u", j);
        status = ud_field_derive(ctx, fields + j, (ud_string){"function.arguments", 18},
                                 (ud_string){names[j], (uint32_t)n}, copy + j);
        if (!status)
            copy[j].key_order = 0;
    }
    if (!status)
        status = ud_batch_create(ctx, copy, count, 0, out);
    for (uint32_t j = 0; j < count; j++)
        ud_field_clear(copy + j);
    return status;
}
ud_status ud_function_arguments(ud_context *ctx, const ud_field *fields, const ud_value *values,
                                uint32_t count, ud_batch **out) {
    if (count && !values)
        return UD_INVALID;
    ud_batch *schema = NULL;
    UD_TRY(ud_function_argument_schema(ctx, fields, count, &schema));
    *out = NULL;
    if (!count)
        return UD_OK;
    ud_status s = ud_batch_create(ctx, schema->schema->fields, count, 1, out);
    if (!s)
        s = ud_batch_append(*out, values, count);
    if (!s)
        s = ud_batch_seal(*out);
    ud_batch_destroy(schema);
    if (s) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}
ud_status ud_function_result(ud_context *ctx, const ud_batch *batch, const ud_field *expected,
                             ud_value *out) {
    if (!ctx || !batch || !expected || !out)
        return UD_INVALID;
    memset(out, 0, sizeof(*out));
    if (batch->columns != 1 || batch->rows != 1 || batch->ctx != ctx ||
        !ud_field_matches(expected, batch->schema->fields, UD_FIELD_VALUE_TYPE))
        return UD_SCHEMA;
    ud_value value;
    UD_TRY(ud_batch_get(batch, 0, 0, &value));
    UD_TRY(ud_value_validate_field(ctx, expected, &value));
    return ud_copy_value(ctx, &value, out, 0);
}
