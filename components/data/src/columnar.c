#include "../internal.h"
#include <math.h>
static int variable(ud_type t) { return t == UD_UTF8 || t == UD_BINARY; }
static size_t width(ud_type t) { return t == UD_DATE32 ? 4 : t == UD_DECIMAL128 ? 16 : 8; }
static ud_status create_depth(ud_context *c, const ud_field *fields, uint32_t columns,
                          uint32_t capacity, ud_batch **out, uint32_t depth) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (depth > 32 || !c || !fields || !columns || columns > 65536 || capacity > INT32_MAX ||
        (size_t)columns > SIZE_MAX / sizeof(ud_column) || (size_t)capacity > SIZE_MAX / sizeof(void *) - 1)
        return ud_fail(c, UD_INVALID, "invalid batch dimensions");
    ud_batch *b = ud_alloc(c, sizeof(*b));
    if (!b)
        return UD_RESOURCE;
    b->ctx = c;
    atomic_init(&b->references, 1);
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
            !ud_utf8_valid(f->name, f->name_length) || f->type < UD_BOOL || f->type > UD_MAP ||
            (f->nullable != 0 && f->nullable != 1) ||
            (f->type == UD_DECIMAL128 && (f->precision < 1 || f->precision > 38 || f->scale < -38 || f->scale > 38)) ||
            (f->type != UD_DECIMAL128 && (f->precision || f->scale)) ||
            (f->type == UD_TIMESTAMP && (!f->timezone || !f->timezone_length ||
             memchr(f->timezone, 0, f->timezone_length) || !ud_utf8_valid(f->timezone, f->timezone_length))) ||
            (f->type != UD_TIMESTAMP && f->timezone_length) ||
            (f->type < UD_STRUCT && f->child_count) ||
            (f->type >= UD_STRUCT && (!f->children || !f->child_count)) ||
            (f->type >= UD_LIST && f->child_count != 1) ||
            (f->type == UD_MAP && (f->children[0].type != UD_STRUCT || f->children[0].nullable ||
             f->children[0].child_count != 2 || !f->children[0].children || f->children[0].children[0].nullable))) {
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
        col->field.timezone = NULL;
        col->field.children = NULL;
        if (f->timezone_length)
            col->field.timezone = ud_strdup_n(c, f->timezone, f->timezone_length);
        col->validity = ud_alloc(c, ((size_t)capacity + 7) / 8 + 1);
        if ((size_t)capacity > SIZE_MAX / width(f->type)) { s = UD_OVERFLOW; break; }
        size_t bytes = f->type == UD_BOOL ? ((size_t)capacity + 7) / 8 + 1 : (size_t)capacity * width(f->type);
        if (f->type >= UD_STRUCT) {
            s = create_depth(c, f->children, f->child_count, f->type == UD_STRUCT ? capacity : 0,
                              &col->nested, depth + 1);
            if (s) break;
            ud_field *children = ud_alloc(c, f->child_count * sizeof(*children));
            col->field.children = children;
            col->views = ud_alloc(c, (size_t)capacity * sizeof(*col->views) + 1);
            if (!children || !col->views) { s = UD_RESOURCE; break; }
            for (uint32_t j = 0; j < f->child_count; j++) children[j] = col->nested->column[j].field;
        }
        if (variable(f->type) || f->type >= UD_LIST)
            col->offsets = ud_alloc(c, ((size_t)capacity + 1) * sizeof(int32_t));
        else if (f->type < UD_STRUCT)
            col->values = ud_alloc(c, bytes ? bytes : 1);
        if (!col->field.name || !col->validity || (f->timezone_length && !col->field.timezone) ||
            ((variable(f->type) || f->type >= UD_LIST) ? !col->offsets : f->type == UD_STRUCT ? !col->nested : !col->values)) {
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
ud_status ud_batch_create(ud_context *c, const ud_field *f, uint32_t n, uint32_t capacity, ud_batch **out) {
    return create_depth(c, f, n, capacity, out, 0);
}
void ud_batch_destroy(ud_batch *b) {
    if (!b)
        return;
    if (atomic_fetch_sub_explicit(&b->references, 1, memory_order_acq_rel) != 1)
        return;
    if (b->column)
        for (uint32_t i = 0; i < b->columns; i++) {
            ud_column *v = b->column + i;
            ud_free((void *)v->field.name);
            ud_free((void *)v->field.timezone);
            ud_free((void *)v->field.children);
            if (v->views) for (uint32_t r = 0; r < b->capacity; r++) ud_free(v->views[r]);
            ud_free(v->views);
            ud_batch_destroy(v->nested);
            ud_free(v->validity);
            ud_free(v->values);
            ud_free(v->offsets);
            ud_free(v->strings);
        }
    ud_free(b->column);
    ud_free(b);
}
ud_status ud_batch_seal(ud_batch *b) {
    if (!b) return UD_INVALID;
    b->sealed = 1;
    return UD_OK;
}
ud_status ud_batch_retain(ud_batch *b) {
    if (!b || !b->sealed) return UD_INVALID;
    unsigned current = atomic_load_explicit(&b->references, memory_order_relaxed);
    do {
        if (!current || current == UINT_MAX) return UD_RESOURCE;
    } while (!atomic_compare_exchange_weak_explicit(&b->references, &current, current + 1,
               memory_order_relaxed, memory_order_relaxed));
    return UD_OK;
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
    out->precision = v->field.precision;
    out->scale = v->field.scale;
    out->is_null = !(v->validity[row / 8] & (1u << (row % 8)));
    if (out->is_null)
        return UD_OK;
    if (out->type >= UD_STRUCT) {
        uint32_t start = out->type == UD_STRUCT ? row : (uint32_t)v->offsets[row];
        uint32_t count = out->type == UD_STRUCT ? v->field.child_count : (uint32_t)(v->offsets[row + 1] - v->offsets[row]);
        for (uint32_t j = 0; j < count; j++) {
            ud_status s = ud_batch_get(v->nested, out->type == UD_STRUCT ? row : start + j,
                                       out->type == UD_STRUCT ? j : 0, v->views[row] + j);
            if (s) return s;
        }
        out->children = v->views[row]; out->child_count = count;
        return UD_OK;
    }
    switch (out->type) {
    case UD_BOOL:
        out->integer = !!(((uint8_t *)v->values)[row / 8] & (1u << (row % 8)));
        break;
    case UD_INT64:
    case UD_TIME64:
    case UD_TIMESTAMP:
        out->integer = ((int64_t *)v->values)[row];
        break;
    case UD_FLOAT64:
        out->real = ((double *)v->values)[row];
        break;
    case UD_DATE32:
        out->integer = ((int32_t *)v->values)[row];
        break;
    case UD_DECIMAL128:
        out->decimal = ((ud_decimal128 *)v->values)[row];
        break;
    case UD_UTF8:
    case UD_BINARY:
        out->text = v->strings ? v->strings + v->offsets[row] : "";
        out->text_length = (uint32_t)(v->offsets[row + 1] - v->offsets[row]);
        break;
    }
    return UD_OK;
}
static void truncate_batch(ud_batch *b, uint32_t rows) {
    for (uint32_t i = 0; i < b->columns; i++) {
        ud_column *c = b->column + i;
        if (c->nested) truncate_batch(c->nested, c->field.type == UD_STRUCT ? rows : (uint32_t)c->offsets[rows]);
        if (variable(c->field.type)) c->string_bytes = (size_t)c->offsets[rows];
        if (c->views) for (uint32_t j = rows; j < b->capacity; j++) { ud_free(c->views[j]); c->views[j] = NULL; }
    }
    b->rows = rows;
}
static ud_status grow_nested(ud_column *c, uint32_t need) {
    if (need <= c->nested->capacity) return UD_OK;
    uint32_t cap = c->nested->capacity ? c->nested->capacity : 8;
    while (cap < need) { if (cap > INT32_MAX / 2) { cap = need; break; } cap *= 2; }
    ud_batch *replacement = NULL;
    UD_TRY(ud_batch_create(c->nested->ctx, c->field.children, c->field.child_count, cap, &replacement));
    ud_value *v = ud_alloc(c->nested->ctx, c->field.child_count * sizeof(*v));
    if (!v) { ud_batch_destroy(replacement); return UD_RESOURCE; }
    ud_status s = UD_OK;
    for (uint32_t r = 0; !s && r < c->nested->rows; r++) {
        for (uint32_t j = 0; j < c->field.child_count; j++) s = ud_batch_get(c->nested, r, j, v + j);
        if (!s) s = ud_batch_append(replacement, v, c->field.child_count);
    }
    ud_free(v);
    if (s) { ud_batch_destroy(replacement); return s; }
    ud_batch_destroy(c->nested); c->nested = replacement;
    for (uint32_t j = 0; j < c->field.child_count; j++) ((ud_field *)c->field.children)[j] = replacement->column[j].field;
    return UD_OK;
}
static ud_status validate_value(const ud_field *f, const ud_value *v, uint32_t depth, int masked) {
    if (depth > 32 || f->type != v->type || v->is_null < 0 || v->is_null > 1 ||
        (v->is_null && !f->nullable && !masked) || (f->type < UD_STRUCT && v->child_count)) return UD_SCHEMA;
    if (v->is_null) return UD_OK;
    if (f->type >= UD_STRUCT) {
        if ((v->child_count && !v->children) || (f->type == UD_STRUCT && v->child_count != f->child_count)) return UD_SCHEMA;
        for (uint32_t j = 0; j < v->child_count; j++)
            UD_TRY(validate_value(f->children + (f->type == UD_STRUCT ? j : 0), v->children + j, depth + 1, 0));
    }
    return UD_OK;
}
static ud_status append_core(ud_batch *b, const ud_value *v, uint32_t n, int masked) {
    if (!b || !v || n != b->columns) return UD_INVALID;
    if (b->rows == b->capacity) return ud_fail(b->ctx, UD_RESOURCE, "batch capacity reached");
    /* Validate and reserve all columns before mutating row/offset/validity. */
    for (uint32_t i = 0; i < n; i++) {
        ud_column *c = b->column + i;
        ud_status validated = validate_value(&c->field, v + i, 0, masked);
        if (validated) return ud_fail(b->ctx, validated, "column %u type or nullability mismatch", i);
        if (v[i].is_null)
            continue;
        if (v[i].type == UD_BOOL && v[i].integer != 0 && v[i].integer != 1)
            return UD_PARSE;
        if (v[i].type == UD_DATE32 && (v[i].integer < INT32_MIN || v[i].integer > INT32_MAX))
            return UD_OVERFLOW;
        if (v[i].type == UD_TIME64 && (v[i].integer < 0 || v[i].integer >= INT64_C(86400000000)))
            return UD_OVERFLOW;
        if (v[i].type == UD_DECIMAL128) {
            ud_decimal128 checked;
            if (v[i].precision != c->field.precision || v[i].scale != c->field.scale)
                return UD_SCHEMA;
            ud_status s = ud_decimal_rescale(v[i].decimal, v[i].scale, v[i].precision,
                                             v[i].scale, UD_ROUND_EXACT, &checked);
            if (s) return s;
        }
        if (variable(v[i].type)) {
            if ((!v[i].text && v[i].text_length) ||
                (v[i].type == UD_UTF8 && !ud_utf8_valid(v[i].text, v[i].text_length)))
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
        if (!c->nested) continue;
        uint32_t count = v[i].is_null ? 0 : v[i].child_count;
        ud_status s = UD_OK;
        if (count) {
            if ((size_t)count > SIZE_MAX / sizeof(ud_value)) s = UD_RESOURCE;
            else { c->views[r] = ud_alloc(b->ctx, (size_t)count * sizeof(ud_value)); if (!c->views[r]) s = UD_RESOURCE; }
        }
        if (!s && c->field.type == UD_STRUCT) {
            if (!v[i].is_null) s = append_core(c->nested, v[i].children, c->field.child_count, 0);
            else {
                ud_value *nulls = ud_alloc(b->ctx, c->field.child_count * sizeof(*nulls));
                if (!nulls) s = UD_RESOURCE;
                else {
                    for (uint32_t j = 0; j < c->field.child_count; j++) { nulls[j].type = c->field.children[j].type; nulls[j].is_null = 1; }
                    s = append_core(c->nested, nulls, c->field.child_count, 1); ud_free(nulls);
                }
            }
        } else if (!s) {
            if (count > (uint32_t)INT32_MAX - c->nested->rows) s = UD_OVERFLOW;
            if (!s) s = grow_nested(c, c->nested->rows + count);
            for (uint32_t j = 0; !s && j < count; j++) s = append_core(c->nested, v[i].children + j, 1, 0);
            if (!s) c->offsets[r + 1] = (int32_t)c->nested->rows;
        }
        if (s) { truncate_batch(b, r); return s; }
    }
    for (uint32_t i = 0; i < n; i++) {
        ud_column *c = b->column + i;
        c->validity[r / 8] &= (uint8_t)~(1u << (r % 8));
        if (!v[i].is_null) {
            c->validity[r / 8] |= (uint8_t)(1u << (r % 8));
            switch (v[i].type) {
            case UD_BOOL:
                ((uint8_t *)c->values)[r / 8] &= (uint8_t)~(1u << (r % 8));
                if (v[i].integer) ((uint8_t *)c->values)[r / 8] |= (uint8_t)(1u << (r % 8));
                break;
            case UD_INT64:
            case UD_TIME64:
            case UD_TIMESTAMP:
                ((int64_t *)c->values)[r] = v[i].integer;
                break;
            case UD_FLOAT64:
                ((double *)c->values)[r] = v[i].real;
                break;
            case UD_DATE32:
                ((int32_t *)c->values)[r] = (int32_t)v[i].integer;
                break;
            case UD_DECIMAL128:
                ((ud_decimal128 *)c->values)[r] = v[i].decimal;
                break;
            case UD_UTF8:
            case UD_BINARY:
                if (v[i].text_length)
                    memcpy(c->strings + c->string_bytes, v[i].text, v[i].text_length);
                c->string_bytes += v[i].text_length;
                break;
            }
        }
        if (variable(c->field.type))
            c->offsets[r + 1] = (int32_t)c->string_bytes;
    }
    b->rows++;
    return UD_OK;
}
void ud_clear_value(ud_value *v) {
    if (!v) return;
    if (!v->is_null && variable(v->type)) ud_free((void *)v->text);
    if (v->type >= UD_STRUCT) {
        for (uint32_t j = 0; j < v->child_count; j++) ud_clear_value((ud_value *)v->children + j);
        ud_free((void *)v->children);
    }
    memset(v, 0, sizeof(*v));
}
ud_status ud_copy_value(ud_context *ctx, const ud_value *v, ud_value *out, uint32_t depth) {
    if (!v || !out || depth > 32) return UD_SCHEMA;
    *out = *v;
    if (variable(v->type)) {
        out->text = NULL;
        if (!v->is_null) {
            if (!v->text && v->text_length) return UD_INVALID;
            out->text = ud_strdup_n(ctx, v->text ? v->text : "", v->text_length);
            if (!out->text) return UD_RESOURCE;
        }
    } else if (v->type >= UD_STRUCT) {
        out->children = NULL; out->child_count = 0;
        if (!v->is_null && v->child_count) {
            if (!v->children || (size_t)v->child_count > SIZE_MAX / sizeof(ud_value)) return UD_INVALID;
            ud_value *children = ud_alloc(ctx, (size_t)v->child_count * sizeof(*children));
            if (!children) return UD_RESOURCE;
            out->children = children; out->child_count = v->child_count;
            for (uint32_t j = 0; j < v->child_count; j++) {
                ud_status s = ud_copy_value(ctx, v->children + j, children + j, depth + 1);
                if (s) return s;
            }
        }
    }
    return UD_OK;
}
ud_status ud_batch_append(ud_batch *b, const ud_value *v, uint32_t n) {
    if (!b || !v || n != b->columns) return UD_INVALID;
    if (b->sealed) return UD_BUSY;
    /* Deep-copy before reserves: nested / same-batch views can alias any child buffer. */
    ud_value *copy = ud_alloc(b->ctx, (size_t)n * sizeof(*copy));
    if (!copy) return UD_RESOURCE;
    ud_status s = UD_OK;
    for (uint32_t i = 0; !s && i < n; i++) s = ud_copy_value(b->ctx, v + i, copy + i, 0);
    if (!s) s = append_core(b, copy, n, 0);
    for (uint32_t i = 0; i < n; i++) ud_clear_value(copy + i);
    ud_free(copy); return s;
}
int ud_field_equal(const ud_field *a, const ud_field *b, int names) {
    if (!a || !b || a->type != b->type || a->nullable != b->nullable || a->precision != b->precision ||
        a->scale != b->scale || a->timezone_length != b->timezone_length || a->child_count != b->child_count ||
        (names && (a->name_length != b->name_length || memcmp(a->name, b->name, a->name_length))) ||
        (a->timezone_length && memcmp(a->timezone, b->timezone, a->timezone_length))) return 0;
    for (uint32_t j = 0; j < a->child_count; j++) if (!ud_field_equal(a->children + j, b->children + j, 1)) return 0;
    return 1;
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
