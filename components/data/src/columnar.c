#include "../internal.h"
#include <math.h>
static int variable(ud_type t) {
    return t == UD_UTF8 || t == UD_BINARY;
}
static size_t width(ud_type t) {
    return t == UD_DATE32 ? 4 : (t == UD_DECIMAL128 || t == UD_INTERVAL) ? 16 : 8;
}
static ud_status create_depth(ud_context *ctx, ud_schema_owner *schema, const ud_field *fields,
                              uint32_t columns, uint32_t capacity, ud_batch **out, uint32_t depth) {
    *out = NULL;
    if (depth > 32 || !ctx || !schema || schema->ctx != ctx || !fields || !columns ||
        columns > 65536 || capacity > INT32_MAX ||
        (uint64_t)capacity + 1 > SIZE_MAX / sizeof(void *))
        return UD_INVALID;
    ud_batch *b = ud_alloc(ctx, sizeof(*b));
    if (!b)
        return UD_RESOURCE;
    b->ctx = ctx;
    b->columns = columns;
    b->capacity = capacity;
    atomic_init(&b->references, 1);
    ud_status s = ud_schema_owner_retain(schema);
    if (!s)
        b->schema = schema;
    if (!s) {
        b->column = ud_alloc(ctx, columns * sizeof(*b->column));
        if (!b->column)
            s = UD_RESOURCE;
    }
    for (uint32_t j = 0; !s && j < columns; j++) {
        ud_column *col = b->column + j;
        const ud_field *f = fields + j;
        col->field = *f; /* Immutable views owned by one retained schema tree. */
        col->validity = ud_alloc(ctx, ((size_t)capacity + 7) / 8 + 1);
        if (!col->validity) {
            s = UD_RESOURCE;
            break;
        }
        if ((size_t)capacity > SIZE_MAX / width(f->type)) {
            s = UD_OVERFLOW;
            break;
        }
        size_t bytes =
            f->type == UD_BOOL ? ((size_t)capacity + 7) / 8 + 1 : (size_t)capacity * width(f->type);
        if (f->type >= UD_STRUCT) {
            s = create_depth(ctx, schema, f->children, f->child_count,
                             f->type == UD_STRUCT ? capacity : 0, &col->nested, depth + 1);
            if (!s) {
                col->views = ud_alloc(ctx, (size_t)capacity * sizeof(*col->views) + 1);
                if (!col->views)
                    s = UD_RESOURCE;
            }
        }
        if (!s && (variable(f->type) || f->type >= UD_LIST)) {
            col->offsets = ud_alloc(ctx, ((size_t)capacity + 1) * sizeof(int32_t));
            if (!col->offsets)
                s = UD_RESOURCE;
        } else if (!s && f->type < UD_STRUCT) {
            col->values = ud_alloc(ctx, bytes ? bytes : 1);
            if (!col->values)
                s = UD_RESOURCE;
        }
    }
    if (s) {
        ud_batch_destroy(b);
        return ud_fail(ctx, s, "batch allocation failed");
    }
    *out = b;
    return UD_OK;
}
ud_status ud_batch_create_named(ud_context *ctx, const ud_field *fields, uint32_t columns,
                                uint32_t capacity, ud_string scope, ud_batch **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (capacity > INT32_MAX)
        return UD_INVALID;
    ud_schema_owner *schema = NULL;
    UD_TRY(ud_schema_owner_create(ctx, fields, columns, scope, &schema));
    ud_status s = create_depth(ctx, schema, schema->fields, columns, capacity, out, 0);
    ud_schema_owner_release(schema);
    return s;
}
ud_status ud_batch_create(ud_context *ctx, const ud_field *fields, uint32_t columns,
                          uint32_t capacity, ud_batch **out) {
    return ud_batch_create_named(ctx, fields, columns, capacity, (ud_string){0}, out);
}
void ud_batch_destroy(ud_batch *b) {
    if (!b)
        return;
    if (atomic_fetch_sub_explicit(&b->references, 1, memory_order_acq_rel) != 1)
        return;
    if (b->column && !b->data_owner)
        for (uint32_t i = 0; i < b->columns; i++) {
            ud_column *v = b->column + i;
            if (v->views)
                for (uint32_t r = 0; r < b->capacity; r++)
                    ud_free(v->views[r]);
            ud_free(v->views);
            ud_batch_destroy(v->nested);
            ud_free(v->validity);
            ud_free(v->absence);
            ud_free(v->values);
            ud_free(v->offsets);
            ud_free(v->strings);
        }
    ud_free(b->append_values);
    ud_free(b->row_info);
    ud_free(b->column);
    ud_batch_destroy(b->data_owner);
    ud_schema_owner_release(b->schema);
    ud_free(b);
}
ud_status ud_batch_seal(ud_batch *b) {
    if (!b)
        return UD_INVALID;
    if (b->sealed)
        return UD_OK;
    for (uint32_t j = 0; j < b->columns; j++) {
        if (!b->column[j].nested)
            continue;
        UD_TRY(ud_batch_seal(b->column[j].nested));
        for (uint32_t row = 0; row < b->rows; row++) {
            ud_value value;
            UD_TRY(ud_batch_get(b, row, j, &value));
        }
    }
    ud_free(b->append_values);
    b->append_values = NULL;
    b->sealed = 1;
    return UD_OK;
}
ud_status ud_batch_retain(ud_batch *b) {
    if (!b || !b->sealed)
        return UD_INVALID;
    unsigned current = atomic_load_explicit(&b->references, memory_order_relaxed);
    do {
        if (!current || current == UINT_MAX)
            return UD_RESOURCE;
    } while (!atomic_compare_exchange_weak_explicit(&b->references, &current, current + 1,
                                                    memory_order_relaxed, memory_order_relaxed));
    return UD_OK;
}
static int accepts_nullability(const ud_field *target, const ud_field *source) {
    if (source->nullable && !target->nullable)
        return 0;
    for (uint32_t j = 0; j < source->child_count; j++)
        if (!accepts_nullability(target->children + j, source->children + j))
            return 0;
    return 1;
}
ud_status ud_batch_project(ud_context *ctx, const ud_batch *input, const uint32_t *indices,
                           const ud_field *fields, uint32_t columns, ud_batch **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!ctx || !input || input->ctx != ctx || !input->sealed || !indices || !fields || !columns ||
        columns > 65536)
        return UD_INVALID;
    for (uint32_t j = 0; j < columns; j++) {
        if (indices[j] >= input->columns)
            return UD_SCHEMA;
        const ud_field *source = &input->column[indices[j]].field;
        if (!ud_field_matches(source, fields + j, UD_FIELD_VALUE_TYPE) ||
            !accepts_nullability(fields + j, source))
            return UD_SCHEMA;
    }
    ud_schema_owner *schema = NULL;
    UD_TRY(ud_schema_owner_create(ctx, fields, columns, (ud_string){0}, &schema));
    ud_batch *b = ud_alloc(ctx, sizeof(*b));
    if (!b) {
        ud_schema_owner_release(schema);
        return UD_RESOURCE;
    }
    atomic_init(&b->references, 1);
    b->ctx = ctx;
    b->schema = schema;
    b->columns = columns;
    b->rows = b->capacity = input->rows;
    b->row_offset = input->row_offset;
    b->sealed = 1;
    b->column = ud_alloc(ctx, columns * sizeof(*b->column));
    ud_status s = b->column ? ud_batch_retain((ud_batch *)input) : UD_RESOURCE;
    if (!s) {
        b->data_owner = (ud_batch *)input;
        for (uint32_t j = 0; j < columns; j++) {
            b->column[j] = input->column[indices[j]];
            b->column[j].field = schema->fields[j];
        }
    }
    if (s) {
        ud_batch_destroy(b);
        return s;
    }
    *out = b;
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
    row += b->row_offset;
    const ud_column *v = b->column + col;
    memset(out, 0, sizeof(*out));
    out->type = v->field.type;
    out->precision = v->field.precision;
    out->scale = v->field.scale;
    out->is_absent = v->absence && (v->absence[row / 8] & (1u << (row % 8))) ? 1 : 0;
    out->is_null = !(v->validity[row / 8] & (1u << (row % 8)));
    if (out->is_null)
        return UD_OK;
    if (out->type >= UD_STRUCT) {
        uint32_t start = out->type == UD_STRUCT ? row : (uint32_t)v->offsets[row];
        uint32_t count = out->type == UD_STRUCT ? v->field.child_count
                                                : (uint32_t)(v->offsets[row + 1] - v->offsets[row]);
        for (uint32_t j = 0; !b->sealed && j < count; j++) {
            ud_status s = ud_batch_get(v->nested, out->type == UD_STRUCT ? row : start + j,
                                       out->type == UD_STRUCT ? j : 0, v->views[row] + j);
            if (s)
                return s;
        }
        out->children = v->views[row];
        out->child_count = count;
        return UD_OK;
    }
    switch (out->type) {
    case UD_BOOL:
        out->integer = !!(((uint8_t *)v->values)[row / 8] & (1u << (row % 8)));
        break;
    case UD_UINT64:
        out->unsigned_integer = ((uint64_t *)v->values)[row];
        break;
    case UD_INTERVAL:
        out->interval = ((ud_interval *)v->values)[row];
        break;
    case UD_INT64:
    case UD_DURATION:
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
static int32_t value_has_absent(const ud_value *value) {
    if (value->is_absent)
        return 1;
    if (!value->is_null)
        for (uint32_t child = 0; child < value->child_count; child++)
            if (value_has_absent(value->children + child))
                return 1;
    return 0;
}
int32_t ud_batch_has_absent(const ud_batch *batch) {
    if (!batch)
        return 0;
    for (uint32_t j = 0; j < batch->columns; j++) {
        const ud_column *column = batch->column + j;
        if (column->absence)
            for (uint32_t row = batch->row_offset; row < batch->row_offset + batch->rows; row++)
                if (column->absence[row / 8] & (1u << (row % 8)))
                    return 1;
        if (column->nested)
            for (uint32_t row = 0; row < batch->rows; row++) {
                ud_value value;
                if (ud_batch_get(batch, row, j, &value) || value_has_absent(&value))
                    return 1;
            }
    }
    return 0;
}
ud_status ud_batch_cell_present(const ud_batch *batch, uint32_t row, uint32_t column,
                                int32_t *present) {
    if (!present)
        return UD_INVALID;
    ud_value value;
    UD_TRY(ud_batch_get(batch, row, column, &value));
    *present = !value.is_absent;
    return UD_OK;
}
static void truncate_batch(ud_batch *b, uint32_t rows) {
    for (uint32_t i = 0; i < b->columns; i++) {
        ud_column *c = b->column + i;
        if (c->nested)
            truncate_batch(c->nested,
                           c->field.type == UD_STRUCT ? rows : (uint32_t)c->offsets[rows]);
        if (variable(c->field.type))
            c->string_bytes = (size_t)c->offsets[rows];
        if (c->views)
            for (uint32_t j = rows; j < b->capacity; j++) {
                ud_free(c->views[j]);
                c->views[j] = NULL;
            }
    }
    b->rows = rows;
}
static ud_status grow_nested(ud_column *c, uint32_t need) {
    if (need <= c->nested->capacity)
        return UD_OK;
    uint32_t cap = c->nested->capacity ? c->nested->capacity : 8;
    while (cap < need) {
        if (cap > INT32_MAX / 2) {
            cap = need;
            break;
        }
        cap *= 2;
    }
    ud_batch *replacement = NULL;
    UD_TRY(create_depth(c->nested->ctx, c->nested->schema, c->field.children, c->field.child_count,
                        cap, &replacement, 0));
    ud_value *v = ud_alloc(c->nested->ctx, c->field.child_count * sizeof(*v));
    if (!v) {
        ud_batch_destroy(replacement);
        return UD_RESOURCE;
    }
    ud_status s = UD_OK;
    for (uint32_t r = 0; !s && r < c->nested->rows; r++) {
        for (uint32_t j = 0; j < c->field.child_count; j++)
            s = ud_batch_get(c->nested, r, j, v + j);
        if (!s)
            s = ud_batch_append(replacement, v, c->field.child_count);
    }
    ud_free(v);
    if (s) {
        ud_batch_destroy(replacement);
        return s;
    }
    ud_batch_destroy(c->nested);
    c->nested = replacement;
    return UD_OK;
}
static ud_status validate_value(const ud_field *f, const ud_value *v, uint32_t depth, int masked) {
    if (depth > 32 || f->type != v->type || v->is_null < 0 || v->is_null > 1 || v->is_absent < 0 ||
        v->is_absent > 1 || (v->is_absent && !v->is_null) ||
        (v->is_null && !f->nullable && !masked && !v->is_absent) ||
        (f->type < UD_STRUCT && v->child_count))
        return UD_SCHEMA;
    if (v->is_null)
        return UD_OK;
    if (f->dictionary_kind)
        UD_TRY(ud_dictionary_validate(f, v));
    if (f->type >= UD_STRUCT) {
        if ((v->child_count && !v->children) ||
            (f->type == UD_STRUCT && v->child_count != f->child_count))
            return UD_SCHEMA;
        for (uint32_t j = 0; j < v->child_count; j++)
            UD_TRY(validate_value(f->children + (f->type == UD_STRUCT ? j : 0), v->children + j,
                                  depth + 1, 0));
    }
    return UD_OK;
}
static ud_status append_core(ud_batch *b, const ud_value *v, uint32_t n, int masked) {
    if (!b || !v || n != b->columns)
        return UD_INVALID;
    if (b->rows == b->capacity)
        return ud_fail(b->ctx, UD_RESOURCE, "batch capacity reached");
    /* Validate and reserve all columns before mutating row/offset/validity. */
    for (uint32_t i = 0; i < n; i++) {
        ud_column *c = b->column + i;
        ud_status validated = validate_value(&c->field, v + i, 0, masked);
        if (validated)
            return ud_fail(b->ctx, validated, "column %u type or nullability mismatch", i);
        if (v[i].is_absent && !c->absence) {
            c->absence = ud_alloc(b->ctx, ((size_t)b->capacity + 7) / 8 + 1);
            if (!c->absence)
                return UD_RESOURCE;
        }
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
            ud_status s = ud_decimal_rescale(v[i].decimal, v[i].scale, v[i].precision, v[i].scale,
                                             UD_ROUND_EXACT, &checked);
            if (s)
                return s;
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
        if (!c->nested)
            continue;
        uint32_t count = v[i].is_null ? 0 : v[i].child_count;
        ud_status s = UD_OK;
        if (count) {
            if ((size_t)count > SIZE_MAX / sizeof(ud_value))
                s = UD_RESOURCE;
            else {
                c->views[r] = ud_alloc(b->ctx, (size_t)count * sizeof(ud_value));
                if (!c->views[r])
                    s = UD_RESOURCE;
            }
        }
        if (!s && c->field.type == UD_STRUCT) {
            if (!v[i].is_null)
                s = append_core(c->nested, v[i].children, c->field.child_count, 0);
            else {
                ud_value *nulls = ud_alloc(b->ctx, c->field.child_count * sizeof(*nulls));
                if (!nulls)
                    s = UD_RESOURCE;
                else {
                    for (uint32_t j = 0; j < c->field.child_count; j++) {
                        nulls[j].type = c->field.children[j].type;
                        nulls[j].is_null = 1;
                    }
                    s = append_core(c->nested, nulls, c->field.child_count, 1);
                    ud_free(nulls);
                }
            }
        } else if (!s) {
            if (count > (uint32_t)INT32_MAX - c->nested->rows)
                s = UD_OVERFLOW;
            if (!s)
                s = grow_nested(c, c->nested->rows + count);
            for (uint32_t j = 0; !s && j < count; j++)
                s = append_core(c->nested, v[i].children + j, 1, 0);
            if (!s)
                c->offsets[r + 1] = (int32_t)c->nested->rows;
        }
        if (s) {
            truncate_batch(b, r);
            return s;
        }
    }
    for (uint32_t i = 0; i < n; i++) {
        ud_column *c = b->column + i;
        c->validity[r / 8] &= (uint8_t)~(1u << (r % 8));
        if (c->absence) {
            c->absence[r / 8] &= (uint8_t)~(1u << (r % 8));
            if (v[i].is_absent)
                c->absence[r / 8] |= (uint8_t)(1u << (r % 8));
        }
        if (!v[i].is_null) {
            c->validity[r / 8] |= (uint8_t)(1u << (r % 8));
            switch (v[i].type) {
            case UD_BOOL:
                ((uint8_t *)c->values)[r / 8] &= (uint8_t)~(1u << (r % 8));
                if (v[i].integer)
                    ((uint8_t *)c->values)[r / 8] |= (uint8_t)(1u << (r % 8));
                break;
            case UD_UINT64:
                ((uint64_t *)c->values)[r] = v[i].unsigned_integer;
                break;
            case UD_INTERVAL:
                ((ud_interval *)c->values)[r] = v[i].interval;
                break;
            case UD_INT64:
            case UD_DURATION:
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
    if (!v)
        return;
    if (!v->is_null && variable(v->type))
        ud_free((void *)v->text);
    if (v->type >= UD_STRUCT) {
        for (uint32_t j = 0; j < v->child_count; j++)
            ud_clear_value((ud_value *)v->children + j);
        ud_free((void *)v->children);
    }
    memset(v, 0, sizeof(*v));
}
ud_status ud_copy_value(ud_context *ctx, const ud_value *v, ud_value *out, uint32_t depth) {
    if (!v || !out || depth > 32)
        return UD_SCHEMA;
    *out = *v;
    if (variable(v->type)) {
        out->text = NULL;
        if (!v->is_null) {
            if (!v->text && v->text_length)
                return UD_INVALID;
            out->text = ud_strdup_n(ctx, v->text ? v->text : "", v->text_length);
            if (!out->text)
                return UD_RESOURCE;
        }
    } else if (v->type >= UD_STRUCT) {
        out->children = NULL;
        out->child_count = 0;
        if (!v->is_null && v->child_count) {
            if (!v->children || (size_t)v->child_count > SIZE_MAX / sizeof(ud_value))
                return UD_INVALID;
            ud_value *children = ud_alloc(ctx, (size_t)v->child_count * sizeof(*children));
            if (!children)
                return UD_RESOURCE;
            out->children = children;
            out->child_count = v->child_count;
            for (uint32_t j = 0; j < v->child_count; j++) {
                ud_status s = ud_copy_value(ctx, v->children + j, children + j, depth + 1);
                if (s)
                    return s;
            }
        }
    }
    return UD_OK;
}
ud_status ud_batch_append(ud_batch *b, const ud_value *v, uint32_t n) {
    if (!b || !v || n != b->columns)
        return UD_INVALID;
    if (b->sealed)
        return UD_BUSY;
    /* Deep-copy before reserves: nested / same-batch views can alias any child buffer. */
    ud_value *copy = ud_alloc(b->ctx, (size_t)n * sizeof(*copy));
    if (!copy)
        return UD_RESOURCE;
    ud_status s = UD_OK;
    for (uint32_t i = 0; !s && i < n; i++)
        s = ud_copy_value(b->ctx, v + i, copy + i, 0);
    if (!s)
        s = append_core(b, copy, n, 0);
    for (uint32_t i = 0; i < n; i++)
        ud_clear_value(copy + i);
    ud_free(copy);
    return s;
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
        if (!s)
            s = ud_batch_copy_row_info(*out, (*out)->rows - 1, b, r);
    }
    ud_free(fields);
    ud_free(v);
    if (s && *out) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}
ud_status ud_batch_slice(ud_context *ctx, const ud_batch *input, uint32_t first, uint32_t rows,
                         ud_batch **out) {
    if (!input || first > input->rows || rows > input->rows - first || !input->sealed)
        return UD_INVALID;
    uint32_t *columns = ud_alloc(ctx, input->columns * sizeof(*columns));
    if (!columns)
        return UD_RESOURCE;
    for (uint32_t i = 0; i < input->columns; i++)
        columns[i] = i;
    ud_status status =
        ud_batch_project(ctx, input, columns, input->schema->fields, input->columns, out);
    ud_free(columns);
    if (!status) {
        (*out)->row_offset = input->row_offset + first;
        (*out)->owner_first = first;
        (*out)->rows = (*out)->capacity = rows;
    }
    return status;
}
