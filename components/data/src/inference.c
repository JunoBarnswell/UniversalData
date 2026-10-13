#include "../internal.h"
/* Sampling is a declared discovery policy. Runtime parsing never changes to
 * accommodate a later value; it uses the ordinary record error contract. */
static int numeric(ud_type type) {
    return type == UD_INT64 || type == UD_UINT64 || type == UD_DECIMAL128;
}
ud_status ud_infer_scalar(ud_context *ctx, ud_field *field, const char *text, uint32_t length,
                          int json_kind) {
    if (json_kind == UD_JSON_NULL) {
        field->nullable = 1;
        return UD_OK;
    }
    if ((json_kind >= 0 && json_kind != UD_JSON_STRING && json_kind != UD_JSON_NUMBER &&
         json_kind != UD_JSON_BOOL) ||
        !ud_utf8_valid(text, length))
        return UD_UNSUPPORTED;
    ud_field candidate = {.type = UD_UTF8};
    ud_value value;
    int textual = json_kind < 0 || json_kind == UD_JSON_STRING;
    if (json_kind == UD_JSON_BOOL ||
        (json_kind < 0 &&
         ((length == 4 && !memcmp(text, "true", 4)) || (length == 5 && !memcmp(text, "false", 5)))))
        candidate.type = UD_BOOL;
    else if (!textual || json_kind < 0) {
        uint32_t first = length && (text[0] == '-' || text[0] == '+') ? 1 : 0;
        int leading = json_kind < 0 && length > first + 1 && text[first] == '0' &&
                      text[first + 1] >= '0' && text[first + 1] <= '9';
        if (!leading && !ud_value_parse(ctx, UD_INT64, text, length, &value))
            candidate.type = UD_INT64;
        else if (!leading && !ud_value_parse(ctx, UD_UINT64, text, length, &value))
            candidate.type = UD_UINT64;
        else if (!leading) {
            int32_t scale = 0;
            int decimal = 0, valid = length != 0;
            for (uint32_t at = first; valid && at < length; at++) {
                if (text[at] == '.' && !decimal)
                    decimal = 1;
                else if (text[at] < '0' || text[at] > '9')
                    valid = 0;
                else if (decimal)
                    scale++;
            }
            candidate.precision = 38;
            candidate.scale = scale;
            if (valid && scale <= 38 &&
                !ud_decimal_parse(text, length, 38, scale, UD_ROUND_EXACT, &value.decimal))
                candidate.type = UD_DECIMAL128;
            else if (json_kind == UD_JSON_NUMBER)
                return UD_UNSUPPORTED;
        }
    }
    if (candidate.type == UD_UTF8 && textual && length) {
        const ud_type domains[] = {UD_DATE32, UD_TIMESTAMP, UD_INTERVAL};
        for (uint32_t index = 0; index < sizeof(domains) / sizeof(*domains); index++) {
            ud_field probe = {.type = domains[index], .timezone = "UTC", .timezone_length = 3};
            if (!ud_value_parse_field(ctx, &probe, text, length, UD_ROUND_EXACT, &value)) {
                candidate.type = domains[index];
                break;
            }
        }
    }
    if (!field->type) {
        field->type = candidate.type;
        field->precision = candidate.type == UD_DECIMAL128 ? 38 : 0;
        field->scale = candidate.type == UD_DECIMAL128 ? candidate.scale : 0;
    } else if (field->type != candidate.type) {
        if (numeric(field->type) && numeric(candidate.type)) {
            field->type = UD_DECIMAL128;
            field->precision = 38;
            if (candidate.scale > field->scale)
                field->scale = candidate.scale;
        } else if (json_kind == UD_JSON_NUMBER || json_kind == UD_JSON_BOOL ||
                   numeric(field->type) || field->type == UD_BOOL) {
            if (json_kind >= 0)
                return UD_INCOMPATIBLE;
            field->type = UD_UTF8;
            field->precision = field->scale = 0;
        } else {
            field->type = UD_UTF8;
            field->precision = field->scale = 0;
        }
    } else if (field->type == UD_DECIMAL128 && candidate.scale > field->scale)
        field->scale = candidate.scale;
    if (field->type == UD_TIMESTAMP) {
        field->timezone = "UTC";
        field->timezone_length = 3;
    } else {
        field->timezone = NULL;
        field->timezone_length = 0;
    }
    return UD_OK;
}
ud_status ud_infer_schema(ud_context *ctx, ud_field *fields, uint32_t count, ud_string scope,
                          ud_batch **out) {
    if (!count)
        return UD_SCHEMA;
    ud_field *derived = ud_alloc(ctx, count * sizeof(*derived));
    if (!derived)
        return UD_RESOURCE;
    ud_status status = UD_OK;
    for (uint32_t at = 0; !status && at < count; at++) {
        if (!fields[at].type) {
            fields[at].type = UD_UTF8;
            fields[at].nullable = 1;
        }
        status =
            ud_field_derive(ctx, fields + at, scope,
                            (ud_string){fields[at].name, fields[at].name_length}, derived + at);
    }
    if (!status)
        status = ud_batch_create(ctx, derived, count, 0, out);
    for (uint32_t at = 0; at < count; at++)
        ud_field_clear(derived + at);
    ud_free(derived);
    return status;
}
