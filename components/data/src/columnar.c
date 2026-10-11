#include "../internal.h"
#include <math.h>
ud_status ud_batch_create(ud_context *c, const ud_field *fields, uint32_t columns,
                          uint32_t capacity, ud_batch **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!c || !fields || !columns || columns > 65536 || capacity > INT32_MAX ||
        (size_t)columns > SIZE_MAX / sizeof(ud_column))
        return ud_fail(c, UD_INVALID, "invalid batch dimensions");
    ud_batch *b = ud_alloc(c, sizeof(*b));
    if (!b)
        return UD_RESOURCE;
    b->ctx = c;
    b->columns = columns;
    b->capacity = capacity;
    b->column = ud_alloc(c, (size_t)columns * sizeof(*b->column));
    if (!b->column) {
        ud_batch_destroy(b);
        return UD_RESOURCE;
    }
    ud_status s = UD_OK;
    for (uint32_t i = 0; i < columns; i++) {
        ud_column *col = b->column + i;
        const ud_field *f = fields + i;
        if (!f->name || !f->name_length || memchr(f->name, 0, f->name_length) ||
            !ud_utf8_valid(f->name, f->name_length) || f->type < UD_BOOL || f->type > UD_UTF8 ||
            (f->nullable != 0 && f->nullable != 1)) {
            s = UD_SCHEMA;
            break;
        }
        for (uint32_t j = 0; j < i; j++)
            if (f->name_length == fields[j].name_length &&
                !memcmp(f->name, fields[j].name, f->name_length)) {
                s = UD_SCHEMA;
                break;
            }
        if (s)
            break;
        col->field = *f;
        col->field.name = ud_strdup_n(c, f->name, f->name_length);
        col->validity = ud_alloc(c, ((size_t)capacity + 7) / 8 + 1);
        size_t bytes = f->type == UD_BOOL ? ((size_t)capacity + 7) / 8 + 1 : (size_t)capacity * 8;
        if (f->type == UD_UTF8)
            col->offsets = ud_alloc(c, ((size_t)capacity + 1) * sizeof(int32_t));
        else
            col->values = ud_alloc(c, bytes ? bytes : 1);
        if (!col->field.name || !col->validity ||
            (f->type == UD_UTF8 ? !col->offsets : !col->values)) {
            s = UD_RESOURCE;
            break;
        }
    }
    if (s) {
        ud_batch_destroy(b);
        return ud_fail(c, s, "batch schema or allocation failed");
    }
    *out = b;
    return UD_OK;
}
void ud_batch_destroy(ud_batch *b) {
    if (!b)
        return;
    if (b->column)
        for (uint32_t i = 0; i < b->columns; i++) {
            ud_column *v = b->column + i;
            ud_free((void *)v->field.name);
            ud_free(v->validity);
            ud_free(v->values);
            ud_free(v->offsets);
            ud_free(v->strings);
        }
    ud_free(b->column);
    ud_free(b);
}
uint32_t ud_batch_rows(const ud_batch *b) {
    return b ? b->rows : 0;
}
uint32_t ud_batch_columns(const ud_batch *b) {
    return b ? b->columns : 0;
}
ud_status ud_batch_field(const ud_batch *b, uint32_t col, ud_field *out) {
    if (!b || !out || col >= b->columns)
        return UD_INVALID;
    *out = b->column[col].field;
    return UD_OK;
}
ud_status ud_batch_get(const ud_batch *b, uint32_t row, uint32_t col, ud_value *out) {
    if (!b || !out || row >= b->rows || col >= b->columns)
        return UD_INVALID;
    const ud_column *v = b->column + col;
    memset(out, 0, sizeof(*out));
    out->type = v->field.type;
    out->is_null = !(v->validity[row / 8] & (1u << (row % 8)));
    if (out->is_null)
        return UD_OK;
    switch (out->type) {
    case UD_BOOL:
        out->integer = !!(((uint8_t *)v->values)[row / 8] & (1u << (row % 8)));
        break;
    case UD_INT64:
        out->integer = ((int64_t *)v->values)[row];
        break;
    case UD_FLOAT64:
        out->real = ((double *)v->values)[row];
        break;
    case UD_UTF8:
        out->text = v->strings ? v->strings + v->offsets[row] : "";
        out->text_length = (uint32_t)(v->offsets[row + 1] - v->offsets[row]);
        break;
    }
    return UD_OK;
}
ud_status ud_batch_append(ud_batch *b, const ud_value *v, uint32_t n) {
    if (!b || !v || n != b->columns)
        return UD_INVALID;
    if (b->rows == b->capacity)
        return ud_fail(b->ctx, UD_RESOURCE, "batch capacity reached");
    /* A caller may append a row borrowed from this same batch. Preserve aliased
     * strings before any column growth invalidates their backing allocations. */
    int alias = 0;
    for (uint32_t i = 0; i < n && !alias; i++)
        if (v[i].type == UD_UTF8 && !v[i].is_null && v[i].text)
            for (uint32_t j = 0; j < n; j++) {
                uintptr_t base = (uintptr_t)b->column[j].strings, ptr = (uintptr_t)v[i].text;
                if (base && ptr >= base && ptr - base < b->column[j].string_capacity) {
                    alias = 1;
                    break;
                }
            }
    if (alias) {
        ud_value *copy = ud_alloc(b->ctx, n * sizeof(*copy));
        if (!copy)
            return UD_RESOURCE;
        ud_status s = UD_OK;
        for (uint32_t i = 0; i < n; i++) {
            copy[i] = v[i];
            if (v[i].type == UD_UTF8 && !v[i].is_null) {
                copy[i].text = ud_strdup_n(b->ctx, v[i].text, v[i].text_length);
                if (!copy[i].text) {
                    s = UD_RESOURCE;
                    break;
                }
            }
        }
        if (!s)
            s = ud_batch_append(b, copy, n);
        for (uint32_t i = 0; i < n; i++)
            if (copy[i].type == UD_UTF8 && !copy[i].is_null)
                ud_free((void *)copy[i].text);
        ud_free(copy);
        return s;
    }
    /* Validate and reserve all columns before mutating row/offset/validity. */
    for (uint32_t i = 0; i < n; i++) {
        ud_column *c = b->column + i;
        if (v[i].type != c->field.type || (v[i].is_null && !c->field.nullable))
            return ud_fail(b->ctx, UD_SCHEMA, "column %u type or nullability mismatch", i);
        if (v[i].is_null)
            continue;
        if (v[i].type == UD_FLOAT64 && !isfinite(v[i].real))
            return UD_OVERFLOW;
        if (v[i].type == UD_BOOL && v[i].integer != 0 && v[i].integer != 1)
            return UD_PARSE;
        if (v[i].type == UD_UTF8) {
            if ((!v[i].text && v[i].text_length) || !ud_utf8_valid(v[i].text, v[i].text_length))
                return UD_PARSE;
            if (v[i].text_length > (size_t)INT32_MAX - c->string_bytes)
                return UD_OVERFLOW;
            size_t need = c->string_bytes + v[i].text_length;
            if (need > c->string_capacity) {
                size_t cap = c->string_capacity ? c->string_capacity : 64;
                while (cap < need) {
                    if (cap > INT32_MAX / 2) {
                        cap = need;
                        break;
                    }
                    cap *= 2;
                }
                char *p = ud_alloc(b->ctx, cap);
                if (!p)
                    return UD_RESOURCE;
                if (c->string_bytes)
                    memcpy(p, c->strings, c->string_bytes);
                ud_free(c->strings);
                c->strings = p;
                c->string_capacity = cap;
            }
        }
    }
    uint32_t r = b->rows;
    for (uint32_t i = 0; i < n; i++) {
        ud_column *c = b->column + i;
        if (!v[i].is_null) {
            c->validity[r / 8] |= (uint8_t)(1u << (r % 8));
            switch (v[i].type) {
            case UD_BOOL:
                if (v[i].integer)
                    ((uint8_t *)c->values)[r / 8] |= (uint8_t)(1u << (r % 8));
                break;
            case UD_INT64:
                ((int64_t *)c->values)[r] = v[i].integer;
                break;
            case UD_FLOAT64:
                ((double *)c->values)[r] = v[i].real;
                break;
            case UD_UTF8:
                if (v[i].text_length)
                    memcpy(c->strings + c->string_bytes, v[i].text, v[i].text_length);
                c->string_bytes += v[i].text_length;
                break;
            }
        }
        if (c->offsets)
            c->offsets[r + 1] = (int32_t)c->string_bytes;
    }
    b->rows++;
    return UD_OK;
}
ud_status ud_batch_clone(const ud_batch *b, ud_batch **out) {
    ud_field *fields = ud_alloc(b->ctx, b->columns * sizeof(*fields));
    ud_value *v = ud_alloc(b->ctx, b->columns * sizeof(*v));
    if (!fields || !v) {
        ud_free(fields);
        ud_free(v);
        return UD_RESOURCE;
    }
    for (uint32_t i = 0; i < b->columns; i++)
        fields[i] = b->column[i].field;
    ud_status s = ud_batch_create(b->ctx, fields, b->columns, b->rows, out);
    for (uint32_t r = 0; !s && r < b->rows; r++) {
        for (uint32_t i = 0; i < b->columns; i++)
            ud_batch_get(b, r, i, v + i);
        s = ud_batch_append(*out, v, b->columns);
    }
    ud_free(fields);
    ud_free(v);
    if (s && *out) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}
