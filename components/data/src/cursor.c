#include "../cursor_internal.h"
#include "ud/format.h"
static int equal(ud_string text, const char *word) {
    return text.length == strlen(word) && !memcmp(text.data, word, text.length);
}
static ud_status text(const ud_json *json, ud_string *out) {
    return ud_json_text(json, &out->data, &out->length);
}
static int column(const ud_batch *schema, ud_string name) {
    ud_id id;
    int by_id = ud_id_parse(name, &id) == UD_OK;
    for (uint32_t i = 0; i < schema->columns; i++) {
        const ud_field *f = &schema->column[i].field;
        if (by_id ? !ud_id_compare(f->identity, id)
                  : name.length == f->name_length && !memcmp(name.data, f->name, name.length))
            return (int)i;
    }
    return -1;
}
void ud_cursor_destroy(ud_cursor *cursor) {
    if (!cursor)
        return;
    for (uint32_t j = 0; j < cursor->count; j++)
        if (cursor->last)
            ud_clear_value(cursor->last + j);
    ud_free(cursor->last);
    ud_free(cursor->keys);
    ud_batch_destroy(cursor->schema);
    ud_batch_destroy(cursor->after);
    ud_free(cursor);
}
ud_status ud_cursor_create(ud_context *ctx, const ud_batch *schema, ud_string policy,
                           ud_cursor **out) {
    if (!ctx || !schema || !out || !policy.data)
        return UD_INVALID;
    *out = NULL;
    ud_json *json = NULL;
    UD_TRY(ud_json_parse(ctx, policy.data, policy.length, &json, NULL));
    static const char *allowed[] = {"mode", "keys", "after", "unique", "monotonic", "overlap"};
    ud_status status = ud_json_keys(json, allowed, 6);
    ud_string mode = {0};
    if (!status)
        status = text(ud_json_find(json, "mode", 4), &mode);
    ud_cursor *cursor = ud_alloc(ctx, sizeof(*cursor));
    if (!cursor) {
        ud_json_destroy(json);
        return UD_RESOURCE;
    }
    cursor->ctx = ctx;
    cursor->mode = equal(mode, "snapshot")    ? 0
                   : equal(mode, "watermark") ? 1
                   : equal(mode, "composite") ? 2
                                              : -1;
    if (!status && cursor->mode < 0)
        status = UD_UNSUPPORTED;
    const ud_json *keys = ud_json_find(json, "keys", 4);
    cursor->count = ud_json_size(keys);
    if (!status &&
        ((keys && ud_json_kind(keys) != UD_JSON_ARRAY) || (!cursor->count && cursor->mode != 0) ||
         cursor->count > schema->columns || (cursor->mode == 1 && cursor->count != 1) ||
         (cursor->mode == 2 && cursor->count < 2)))
        status = UD_SCHEMA;
    if (!status && cursor->mode &&
        (ud_json_kind(ud_json_find(json, "unique", 6)) != UD_JSON_BOOL ||
         ud_json_kind(ud_json_find(json, "monotonic", 9)) != UD_JSON_BOOL))
        status = UD_SCHEMA;
    if (!status && cursor->mode) {
        ud_string unique = {0}, monotonic = {0};
        status = text(ud_json_find(json, "unique", 6), &unique);
        if (!status)
            status = text(ud_json_find(json, "monotonic", 9), &monotonic);
        if (!status && (!equal(unique, "true") || !equal(monotonic, "true")))
            status = UD_UNSUPPORTED;
    }
    if (!status) {
        cursor->keys = ud_alloc(ctx, (size_t)cursor->count * sizeof(*cursor->keys));
        cursor->last = ud_alloc(ctx, (size_t)cursor->count * sizeof(*cursor->last));
        if (!cursor->keys || !cursor->last)
            status = UD_RESOURCE;
    }
    ud_field *fields = !status ? ud_alloc(ctx, (size_t)cursor->count * sizeof(*fields)) : NULL;
    if (!status && !fields)
        status = UD_RESOURCE;
    for (uint32_t j = 0; !status && j < cursor->count; j++) {
        ud_string name;
        status = text(ud_json_at(keys, j), &name);
        int at = !status ? column(schema, name) : -1;
        if (at < 0)
            status = UD_SCHEMA;
        if (!status) {
            cursor->keys[j] = (uint32_t)at;
            fields[j] = schema->column[at].field;
            if (fields[j].nullable)
                status = UD_SCHEMA;
        }
        for (uint32_t k = 0; !status && k < j; k++)
            if (cursor->keys[k] == cursor->keys[j])
                status = UD_SCHEMA;
    }
    const ud_json *after = ud_json_find(json, "after", 5);
    if (!status && after &&
        (ud_json_kind(after) != UD_JSON_ARRAY || ud_json_size(after) != cursor->count))
        status = UD_SCHEMA;
    if (!status)
        status = cursor->count ? ud_batch_create(ctx, fields, cursor->count, 0, &cursor->schema)
                               : ud_batch_create(ctx, schema->schema->fields, schema->columns, 0,
                                                 &cursor->schema);
    if (!status && after && !cursor->count)
        status = UD_UNSUPPORTED;
    if (!status && after) {
        status = ud_batch_create(ctx, fields, cursor->count, 1, &cursor->after);
        ud_value *values = !status ? ud_alloc(ctx, (size_t)cursor->count * sizeof(*values)) : NULL;
        if (!status && !values)
            status = UD_RESOURCE;
        for (uint32_t j = 0; !status && j < cursor->count; j++)
            status = ud_json_value_decode(ctx, fields + j, ud_json_at(after, j), UD_ROUND_EXACT,
                                          values + j);
        if (!status)
            status = ud_batch_append(cursor->after, values, cursor->count);
        for (uint32_t j = 0; values && j < cursor->count; j++)
            ud_clear_value(values + j);
        ud_free(values);
    }
    if (!status && ud_json_find(json, "overlap", 7)) {
        ud_string number = {0};
        status = text(ud_json_find(json, "overlap", 7), &number);
        for (uint32_t j = 0; !status && j < number.length; j++) {
            unsigned d = (unsigned)(number.data[j] - '0');
            if (d > 9 || cursor->overlap > (UINT64_MAX - d) / 10)
                status = UD_SCHEMA;
            else
                cursor->overlap = cursor->overlap * 10 + d;
        }
        if (!status && cursor->overlap &&
            (!cursor->after || cursor->overlap > INT64_MAX ||
             (fields[0].type != UD_INT64 && fields[0].type != UD_TIMESTAMP &&
              fields[0].type != UD_DATE32 && fields[0].type != UD_TIME64)))
            status = UD_UNSUPPORTED;
        if (!status && cursor->overlap) {
            ud_value value;
            status = ud_batch_get(cursor->after, 0, 0, &value);
            if (!status && value.integer < INT64_MIN + (int64_t)cursor->overlap)
                status = UD_OVERFLOW;
            if (!status) {
                ud_value *values = ud_alloc(ctx, (size_t)cursor->count * sizeof(*values));
                if (!values)
                    status = UD_RESOURCE;
                for (uint32_t j = 0; !status && j < cursor->count; j++)
                    status = ud_batch_get(cursor->after, 0, j, values + j);
                if (!status) {
                    values[0].integer -= (int64_t)cursor->overlap;
                    ud_batch *lower = NULL;
                    status = ud_batch_create(ctx, fields, cursor->count, 1, &lower);
                    if (!status)
                        status = ud_batch_append(lower, values, cursor->count);
                    if (!status) {
                        ud_batch_destroy(cursor->after);
                        cursor->after = lower;
                        lower = NULL;
                    }
                    ud_batch_destroy(lower);
                }
                ud_free(values);
            }
        }
    }
    ud_bytes signature = {.ctx = ctx};
    if (!status)
        status = ud_put_u64(&signature, (uint64_t)cursor->mode);
    if (!status)
        status = ud_put_u64(&signature, cursor->overlap);
    if (!status)
        status = ud_encode_batch(&signature, cursor->schema);
    if (!status)
        ud_hash(signature.data, signature.size, cursor->fingerprint);
    ud_bytes_clear(&signature);
    ud_free(fields);
    ud_json_destroy(json);
    if (status)
        ud_cursor_destroy(cursor);
    else
        *out = cursor;
    return status;
}
static ud_status validate_row(ud_cursor *cursor, const ud_batch *input, uint32_t row) {
    if (!cursor->count)
        return UD_OK;
    int order = 0;
    for (uint32_t j = 0; j < cursor->count; j++) {
        ud_value value;
        UD_TRY(ud_batch_get(input, row, cursor->keys[j], &value));
        if (value.is_null)
            return UD_SCHEMA;
        if (cursor->have_last && !order)
            order = ud_value_compare(&value, cursor->last + j);
    }
    if (cursor->have_last && order <= 0)
        return UD_SCHEMA;
    for (uint32_t j = 0; j < cursor->count; j++) {
        ud_value value;
        UD_TRY(ud_batch_get(input, row, cursor->keys[j], &value));
        ud_clear_value(cursor->last + j);
        UD_TRY(ud_copy_value(cursor->ctx, &value, cursor->last + j, 0));
    }
    cursor->have_last = 1;
    return UD_OK;
}
ud_status ud_cursor_validate(ud_cursor *cursor, const ud_batch *input) {
    for (uint32_t row = 0; row < input->rows; row++)
        UD_TRY(validate_row(cursor, input, row));
    return UD_OK;
}
void ud_cursor_reset(ud_cursor *cursor) {
    for (uint32_t j = 0; j < cursor->count; j++)
        ud_clear_value(cursor->last + j);
    cursor->have_last = 0;
    cursor->read = cursor->confirmed = 0;
}
ud_status ud_cursor_filter(ud_cursor *cursor, const ud_batch *input, ud_batch **out) {
    if (!cursor || !input || !out)
        return UD_INVALID;
    if (!cursor->count) {
        if (input->columns != cursor->schema->columns)
            return UD_INCOMPATIBLE;
        for (uint32_t j = 0; j < input->columns; j++)
            if (!ud_field_matches(&input->column[j].field, &cursor->schema->column[j].field,
                                  UD_FIELD_PORT))
                return UD_INCOMPATIBLE;
    }
    *out = NULL;
    for (uint32_t j = 0; j < cursor->count; j++)
        if (cursor->keys[j] >= input->columns ||
            !ud_field_matches(&cursor->schema->column[j].field,
                              &input->column[cursor->keys[j]].field, UD_FIELD_PORT))
            return UD_INCOMPATIBLE;
    ud_field *fields = ud_alloc(cursor->ctx, (size_t)input->columns * sizeof(*fields));
    if (!fields)
        return UD_RESOURCE;
    for (uint32_t j = 0; j < input->columns; j++)
        fields[j] = input->column[j].field;
    ud_status status = ud_batch_create(cursor->ctx, fields, input->columns, input->rows, out);
    ud_free(fields);
    for (uint32_t row = 0; !status && row < input->rows; row++) {
        int after = cursor->after ? 0 : 1;
        status = validate_row(cursor, input, row);
        for (uint32_t j = 0; !status && cursor->after && !after && j < cursor->count; j++) {
            ud_value value, lower;
            status = ud_batch_get(input, row, cursor->keys[j], &value);
            if (!status)
                status = ud_batch_get(cursor->after, 0, j, &lower);
            if (!status)
                after = ud_value_compare(&value, &lower);
        }
        if (!status && after > 0) {
            status = ud_batch_append_from(*out, input, row);
            if (!status) {
                if (cursor->read == UINT64_MAX)
                    status = UD_OVERFLOW;
                else
                    cursor->read++;
            }
        }
    }
    if (!status)
        status = ud_batch_seal(*out);
    if (status) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return status;
}
static ud_status encode(const ud_cursor *cursor, int detailed, char **out, uint32_t *length) {
    if (!cursor || !out || !length)
        return UD_INVALID;
    *out = NULL;
    *length = 0;
    ud_bytes bytes = {.ctx = cursor->ctx};
    char fingerprint[65];
    static const char hex[] = "0123456789abcdef";
    for (uint32_t j = 0; j < 32; j++) {
        fingerprint[j * 2] = hex[cursor->fingerprint[j] >> 4];
        fingerprint[j * 2 + 1] = hex[cursor->fingerprint[j] & 15];
    }
    fingerprint[64] = 0;
    char header[384];
    int size = snprintf(header, sizeof(header),
                        "{\"version\":%u,\"policyFingerprint\":\"%s\",\"lastRead\":%llu,"
                        "\"lastConfirmed\":%llu,\"uncommitted\":%llu,\"commitUnknown\":%s",
                        UD_CONFIG_VERSION, fingerprint, (unsigned long long)cursor->read,
                        (unsigned long long)cursor->confirmed,
                        (unsigned long long)(cursor->read - cursor->confirmed),
                        cursor->commit_unknown ? "true" : "false");
    ud_status status = ud_bytes_put(&bytes, header, (size_t)size);
    if (!status && detailed) {
        const char *modes[] = {"snapshot", "watermark", "composite"};
        status = ud_bytes_put(&bytes, ",\"mode\":", 8);
        if (!status)
            status = ud_json_quote(&bytes, modes[cursor->mode], strlen(modes[cursor->mode]));
        if (!status)
            status = ud_bytes_put(&bytes, ",\"ordering\":\"ascending\",\"schema\":", 33);
        char *schema = NULL;
        uint32_t n = 0;
        if (!status)
            status = ud_schema_to_json(cursor->schema, &schema, &n);
        if (!status)
            status = ud_bytes_put(&bytes, schema, n);
        ud_free(schema);
        if (!status)
            status = ud_bytes_put(&bytes, ",\"keys\":[", 9);
        for (uint32_t j = 0; !status && j < cursor->count; j++) {
            char id[33];
            ud_id_format(cursor->schema->column[j].field.identity, id);
            if (j)
                status = ud_bytes_put(&bytes, ",", 1);
            if (!status)
                status = ud_json_quote(&bytes, id, 32);
        }
        if (!status)
            status = ud_bytes_put(&bytes, "]", 1);
    }
    if (!status && detailed && cursor->have_last) {
        status = ud_bytes_put(&bytes, ",\"values\":[", 11);
        for (uint32_t j = 0; !status && j < cursor->count; j++) {
            if (j)
                status = ud_bytes_put(&bytes, ",", 1);
            if (!status)
                status = ud_json_value_encode(&bytes, &cursor->schema->column[j].field,
                                              cursor->last + j);
        }
        if (!status)
            status = ud_bytes_put(&bytes, "]", 1);
    }
    if (!status)
        status = ud_bytes_put(&bytes, "}", 1);
    if (!status && bytes.size > UINT32_MAX)
        status = UD_RESOURCE;
    if (!status) {
        *out = ud_strdup_n(cursor->ctx, (char *)bytes.data, bytes.size);
        if (!*out)
            status = UD_RESOURCE;
        else
            *length = (uint32_t)bytes.size;
    }
    ud_bytes_clear(&bytes);
    return status;
}
ud_status ud_cursor_serialize(const ud_cursor *cursor, char **out, uint32_t *length) {
    return encode(cursor, 1, out, length);
}
ud_status ud_cursor_diagnostics(const ud_cursor *cursor, char **out, uint32_t *length) {
    return encode(cursor, 0, out, length);
}
ud_status ud_cursor_confirm(ud_cursor *cursor) {
    if (!cursor)
        return UD_INVALID;
    cursor->confirmed = cursor->read;
    return UD_OK;
}
