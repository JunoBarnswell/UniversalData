#include "../internal.h"
/* Pre-discovery consumes an already typed, authorized input. It returns frozen
 * binding values; configure and runtime never discover or mutate columns. The
 * shared Compute sort and canonical value comparator define the domain. */
static ud_status canonical_bytes(void *user, const void *data, size_t length) {
    return ud_bytes_put(user, data, length);
}
ud_status ud_pivot_discover_values(ud_context *ctx, const ud_batch *input, uint32_t column,
                                   ud_string prefix, uint32_t maximum, char **out,
                                   uint32_t *length) {
    if (!ctx || !input || !out || !length || ctx != input->ctx || !input->sealed ||
        column >= input->columns || !maximum || maximum > 4096 || !prefix.data || !prefix.length ||
        prefix.length > 128 || !ud_utf8_valid(prefix.data, prefix.length) ||
        memchr(prefix.data, 0, prefix.length))
        return UD_INVALID;
    *out = NULL;
    *length = 0;
    if (input->column[column].field.type >= UD_STRUCT)
        return UD_UNSUPPORTED;
    ud_batch *projected = NULL, *ordered = NULL;
    ud_status status =
        ud_batch_project(ctx, input, &column, &input->column[column].field, 1, &projected);
    if (!status)
        status = ud_compute_sort(ctx, projected, 0, 0, &ordered);
    ud_batch_destroy(projected);
    ud_bytes result = {.ctx = ctx, .limit = 16777216};
    if (!status)
        status = ud_bytes_put(&result, "[", 1);
    ud_value previous = {0};
    int have = 0;
    uint32_t count = 0;
    for (uint32_t row = 0; !status && row < ordered->rows; row++) {
        if (ud_is_cancelled(ctx)) {
            status = UD_CANCELLED;
            break;
        }
        ud_value value;
        status = ud_batch_get(ordered, row, 0, &value);
        if (status)
            break;
        if (value.is_null || (have && !ud_value_compare(&previous, &value)))
            continue;
        if (count == maximum) {
            status = UD_RESOURCE;
            break;
        }
        ud_bytes key = {.ctx = ctx};
        uint8_t digest[32];
        status = ud_put_u64(&key, value.type);
        if (!status)
            status = ud_value_canonical(&value, canonical_bytes, &key, 0);
        if (!status)
            ud_hash(key.data, key.size, digest);
        ud_bytes_clear(&key);
        if (status)
            break;
        char suffix[34];
        suffix[0] = '_';
        for (uint32_t i = 0; i < 16; i++)
            snprintf(suffix + 1 + i * 2, 3, "%02x", digest[i]);
        ud_bytes name = {.ctx = ctx};
        if (!status)
            status = ud_bytes_put(&name, prefix.data, prefix.length);
        if (!status)
            status = ud_bytes_put(&name, suffix, 33);
        if (!status && count)
            status = ud_bytes_put(&result, ",", 1);
        if (!status)
            status = ud_bytes_put(&result, "{\"value\":", 9);
        if (!status)
            status = ud_json_value_encode(&result, &ordered->column[0].field, &value);
        if (!status)
            status = ud_bytes_put(&result, ",\"name\":", 8);
        if (!status)
            status = ud_json_quote(&result, (char *)name.data, (uint32_t)name.size);
        if (!status)
            status = ud_bytes_put(&result, "}", 1);
        ud_bytes_clear(&name);
        previous = value;
        have = 1;
        count++;
    }
    if (!status && !count)
        status = ud_fail(ctx, UD_SCHEMA, "empty pivot domain requires explicit frozen values");
    if (!status)
        status = ud_bytes_put(&result, "]", 1);
    if (!status) {
        *length = (uint32_t)result.size;
        status = ud_bytes_put(&result, "", 1);
    }
    if (!status) {
        *out = (char *)result.data;
        result.data = NULL;
    }
    ud_bytes_clear(&result);
    ud_batch_destroy(ordered);
    return status;
}
