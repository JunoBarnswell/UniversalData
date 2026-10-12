#include "../internal.h"
static int space(unsigned char ch) {
    return ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n' || ch == '\v' || ch == '\f';
}
static ud_field literal_field(const ud_mapping *mapping, const ud_value *v) {
    ud_field f = mapping->target;
    f.type = v->type;
    f.nullable = 1;
    f.precision = v->type == UD_DECIMAL128 ? v->precision : 0;
    f.scale = v->type == UD_DECIMAL128 ? v->scale : 0;
    if (v->type != UD_TIMESTAMP) {
        f.timezone = NULL;
        f.timezone_length = 0;
    } else if (mapping->target.type != UD_TIMESTAMP) {
        f.timezone = "UTC";
        f.timezone_length = 3;
    }
    if (v->type < UD_STRUCT) {
        f.children = NULL;
        f.child_count = 0;
    }
    return f;
}
ud_status ud_compute_map(ud_context *ctx, const ud_batch *input, const ud_mapping *mapping,
                         uint32_t columns, int32_t rounding, ud_batch **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!ctx || !input || !mapping || !columns || columns > 65536 || rounding < UD_ROUND_EXACT ||
        rounding > UD_ROUND_CEILING)
        return UD_INVALID;
    for (uint32_t j = 0; j < columns; j++) {
        const ud_mapping *m = mapping + j;
        if (m->struct_size != sizeof(*m) || m->version != UD_CONFIG_VERSION)
            return UD_INCOMPATIBLE;
        if (m->source_column < -1 ||
            (m->source_column >= 0 && (uint32_t)m->source_column >= input->columns) ||
            (m->flags & ~15u) ||
            ((m->flags & UD_MAP_UPPER_ASCII) && (m->flags & UD_MAP_LOWER_ASCII)))
            return UD_INVALID;
    }
    ud_field *fields = ud_alloc(ctx, columns * sizeof(*fields));
    ud_value *values = ud_alloc(ctx, columns * sizeof(*values));
    if (!fields || !values) {
        ud_free(fields);
        ud_free(values);
        return UD_RESOURCE;
    }
    for (uint32_t j = 0; j < columns; j++)
        fields[j] = mapping[j].target;
    ud_status s = ud_batch_create(ctx, fields, columns, input->rows, out);
    for (uint32_t row = 0; !s && row < input->rows; row++) {
        if (ud_is_cancelled(ctx)) {
            s = UD_CANCELLED;
            break;
        }
        for (uint32_t j = 0; !s && j < columns; j++) {
            const ud_mapping *m = mapping + j;
            ud_value value;
            ud_field from;
            if (m->source_column < 0) {
                value = m->constant;
                from = literal_field(m, &value);
            } else {
                s = ud_batch_get(input, row, (uint32_t)m->source_column, &value);
                from = input->column[m->source_column].field;
            }
            if (s)
                break;
            if (value.is_null && (m->flags & UD_MAP_DEFAULT)) {
                value = m->default_value;
                from = literal_field(m, &value);
            }
            if (!value.is_null && from.type >= UD_STRUCT &&
                (from.type != m->target.type || value.type != from.type)) {
                s = UD_SCHEMA;
                break;
            }
            char *normalized = NULL;
            if (!s && !value.is_null && value.type == UD_UTF8 && (m->flags & 7)) {
                if (!value.text && value.text_length) {
                    s = UD_INVALID;
                    break;
                }
                uint32_t first = 0, last = value.text_length;
                if (m->flags & UD_MAP_TRIM_ASCII) {
                    while (first < last && space((unsigned char)value.text[first]))
                        first++;
                    while (last > first && space((unsigned char)value.text[last - 1]))
                        last--;
                }
                normalized = ud_strdup_n(ctx, value.text ? value.text + first : "", last - first);
                if (!normalized)
                    s = UD_RESOURCE;
                else {
                    for (uint32_t k = 0; k < last - first; k++) {
                        unsigned char ch = (unsigned char)normalized[k];
                        if ((m->flags & UD_MAP_UPPER_ASCII) && ch >= 'a' && ch <= 'z')
                            normalized[k] = (char)(ch - 'a' + 'A');
                        if ((m->flags & UD_MAP_LOWER_ASCII) && ch >= 'A' && ch <= 'Z')
                            normalized[k] = (char)(ch - 'A' + 'a');
                    }
                    value.text = normalized;
                    value.text_length = last - first;
                }
            }
            if (!s)
                s = ud_cast_value(ctx, &from, &value, &m->target, rounding, values + j, 0);
            ud_free(normalized);
        }
        if (!s)
            s = ud_batch_append(*out, values, columns);
        for (uint32_t j = 0; j < columns; j++)
            ud_clear_value(values + j);
    }
    ud_free(fields);
    ud_free(values);
    if (s) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}