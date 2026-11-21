#include "../internal.h"
#include "ud/operator.h"
ud_status ud_compute_join_schema(ud_context *ctx, const ud_batch *left, const ud_batch *right,
                                 ud_batch **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!ctx || !left || !right || left->columns > 65536 - right->columns)
        return UD_INVALID;
    uint32_t count = left->columns + right->columns;
    ud_field *fields = ud_alloc(ctx, count * sizeof(*fields));
    if (!fields)
        return UD_RESOURCE;
    ud_status s = UD_OK;
    for (uint32_t j = 0; !s && j < count; j++) {
        const ud_field *source =
            j < left->columns ? &left->column[j].field : &right->column[j - left->columns].field;
        char id[33], scope[48];
        ud_id_format(source->identity, id);
        snprintf(scope, sizeof(scope), "join.%s.%s", j < left->columns ? "left" : "right", id);
        ud_string name = {source->name, source->name_length};
        ud_bytes renamed = {.ctx = ctx};
        if (j >= left->columns) {
            int collision = 0;
            for (uint32_t k = 0; k < j; k++)
                if (fields[k].name_length == name.length &&
                    !memcmp(fields[k].name, name.data, name.length))
                    collision = 1;
            if (collision) {
                char suffix[48];
                int n = snprintf(suffix, sizeof(suffix), "right.%u.", j - left->columns);
                s = ud_bytes_put(&renamed, suffix, (size_t)n);
                if (!s)
                    s = ud_bytes_put(&renamed, name.data, name.length);
                name = (ud_string){(char *)renamed.data, (uint32_t)renamed.size};
            }
        }
        int duplicate = 0;
        for (uint32_t k = 0; k < j; k++)
            duplicate |= !ud_id_compare(source->identity, fields[k].identity);
        if (!s && (duplicate || renamed.size))
            s = ud_field_derive(ctx, source, (ud_string){scope, (uint32_t)strlen(scope)}, name,
                                fields + j);
        else if (!s)
            s = ud_field_copy(ctx, source, (ud_id){0}, fields + j);
        if (!s)
            fields[j].key_order = 0;
        ud_bytes_clear(&renamed);
    }
    if (!s)
        s = ud_batch_create(ctx, fields, count, 0, out);
    for (uint32_t j = 0; j < count; j++)
        ud_field_clear(fields + j);
    ud_free(fields);
    return s;
}
static ud_status union_field(ud_context *ctx, const ud_field *left, const ud_field *right,
                             ud_string scope, ud_field *out, uint32_t depth) {
    if (depth > 32 || !ud_field_matches(left, right, UD_FIELD_VALUE_TYPE) ||
        left->name_length != right->name_length ||
        memcmp(left->name, right->name, left->name_length))
        return UD_SCHEMA;
    ud_field parents[2] = {*left, *right}, spec = *left;
    ud_id *origins = NULL;
    uint32_t count = 0;
    UD_TRY(ud_lineage_merge(ctx, parents, 2, &origins, &count));
    spec.lineage = origins;
    spec.lineage_count = count;
    spec.nullable = left->nullable || right->nullable;
    ud_status s =
        ud_field_derive(ctx, &spec, scope, (ud_string){left->name, left->name_length}, out);
    ud_free(origins);
    char nested[33];
    if (!s)
        ud_id_format(out->identity, nested);
    for (uint32_t j = 0; !s && j < left->child_count; j++) {
        ud_field child = {0};
        s = union_field(ctx, left->children + j, right->children + j, (ud_string){nested, 32},
                        &child, depth + 1);
        if (!s) {
            ud_field_clear((ud_field *)out->children + j);
            ((ud_field *)out->children)[j] = child;
        } else
            ud_field_clear(&child);
    }
    return s;
}
ud_status ud_compute_union_schema(ud_context *ctx, const ud_batch *left, const ud_batch *right,
                                  ud_batch **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!ctx || !left || !right || left->columns != right->columns)
        return UD_SCHEMA;
    ud_field *fields = ud_alloc(ctx, left->columns * sizeof(*fields));
    if (!fields)
        return UD_RESOURCE;
    ud_status s = UD_OK;
    for (uint32_t j = 0; !s && j < left->columns; j++) {
        char a[33], b[33], scope[80];
        ud_id_format(left->column[j].field.identity, a);
        ud_id_format(right->column[j].field.identity, b);
        snprintf(scope, sizeof(scope), "union.%s.%s", a, b);
        s = union_field(ctx, &left->column[j].field, &right->column[j].field,
                        (ud_string){scope, (uint32_t)strlen(scope)}, fields + j, 0);
    }
    if (!s)
        s = ud_batch_create(ctx, fields, left->columns, 0, out);
    for (uint32_t j = 0; j < left->columns; j++)
        ud_field_clear(fields + j);
    ud_free(fields);
    return s;
}
static ud_status fields_of(ud_context *c, const ud_batch *b, ud_field **out) {
    uint32_t n = ud_batch_columns(b);
    *out = ud_alloc(c, n * sizeof(**out));
    if (!*out)
        return UD_RESOURCE;
    for (uint32_t i = 0; i < n; i++)
        ud_batch_field(b, i, *out + i);
    return UD_OK;
}
static ud_status append_row(ud_batch *out, const ud_batch *in, uint32_t row, ud_value *v) {
    uint32_t n = ud_batch_columns(in);
    for (uint32_t i = 0; i < n; i++)
        UD_TRY(ud_batch_get(in, row, i, v + i));
    UD_TRY(ud_batch_append(out, v, n));
    return ud_batch_copy_row_info(out, out->rows - 1, in, row);
}
ud_status ud_compute_slice(ud_context *c, const ud_batch *b, uint64_t offset, uint64_t limit,
                           ud_batch **out) {
    if (!b || !out)
        return UD_INVALID;
    *out = NULL;
    uint32_t rows = ud_batch_rows(b), n = ud_batch_columns(b);
    uint64_t count = offset < rows ? rows - offset : 0;
    if (count > limit)
        count = limit;
    ud_field *f = NULL;
    UD_TRY(fields_of(c, b, &f));
    ud_status s = ud_batch_create(c, f, n, (uint32_t)count, out);
    ud_free(f);
    ud_value *v = ud_alloc(c, n * sizeof(*v));
    if (!v && !s)
        s = UD_RESOURCE;
    for (uint32_t r = 0; !s && r < count; r++) {
        if (ud_is_cancelled(c)) {
            s = UD_CANCELLED;
            break;
        }
        s = append_row(*out, b, (uint32_t)offset + r, v);
    }
    ud_free(v);
    if (s && *out) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}
int ud_compute_stage_order(const ud_batch *left, uint32_t left_row, const ud_batch *right, uint32_t right_row) {
    ud_row_info a, b;
    ud_batch_row_info(left, left_row, &a);
    ud_batch_row_info(right, right_row, &b);
    int x = !!(a.flags & UD_ROW_STAGE_ORDERED), y = !!(b.flags & UD_ROW_STAGE_ORDERED);
    if (x != y)
        return y - x;
    return !x ? 0 : (a.source_position > b.source_position) - (a.source_position < b.source_position);
}

int ud_compute_compare_keys(const ud_batch *left, uint32_t left_row, const ud_batch *right,
                            uint32_t right_row, const uint32_t *keys, uint32_t count,
                            int descending, uint32_t directions, int stage_ordered) {
    for (uint32_t i = 0; i < count; i++) {
        uint32_t key = keys[i];
        ud_value x, y;
        ud_batch_get(left, left_row, key, &x);
        ud_batch_get(right, right_row, key, &y);
        const ud_field *field = &left->column[key].field;
        int order = ud_field_value_compare(field, &x, field, &y);
        if (order)
            return ud_key_descending(descending, directions, i) ? -order : order;
    }
    return stage_ordered ? ud_compute_stage_order(left, left_row, right, right_row) : 0;
}

ud_status ud_compute_sort_keys(ud_context *c, const ud_batch *b, const uint32_t *keys,
                               uint32_t count, int descending, uint32_t directions,
                               int stage_ordered, ud_batch **out) {
    *out = NULL;
    uint32_t rows = ud_batch_rows(b), n = ud_batch_columns(b);
    uint32_t *order = ud_alloc(c, (size_t)rows * sizeof(*order) + 1),
             *scratch = ud_alloc(c, (size_t)rows * sizeof(*scratch) + 1);
    if (!order || !scratch) {
        ud_free(order);
        ud_free(scratch);
        return UD_RESOURCE;
    }
    for (uint32_t i = 0; i < rows; i++)
        order[i] = i;
    for (uint64_t width = 1; width < rows; width *= 2) {
        for (uint64_t lo = 0; lo < rows; lo += 2 * width) {
            uint64_t mid = lo + width < rows ? lo + width : rows,
                     hi = lo + 2 * width < rows ? lo + 2 * width : rows, a = lo, z = mid, k = lo;
            while (a < mid && z < hi) {
                int cmp = ud_compute_compare_keys(b, order[a], b, order[z], keys, count,
                                                  descending, directions, stage_ordered);
                scratch[k++] = cmp <= 0 ? order[a++] : order[z++];
            }
            while (a < mid)
                scratch[k++] = order[a++];
            while (z < hi)
                scratch[k++] = order[z++];
        }
        uint32_t *swap = order;
        order = scratch;
        scratch = swap;
        if (ud_is_cancelled(c)) {
            ud_free(order);
            ud_free(scratch);
            return UD_CANCELLED;
        }
    }
    ud_field *f = NULL;
    ud_status s = fields_of(c, b, &f);
    if (!s)
        s = ud_batch_create(c, f, n, rows, out);
    ud_free(f);
    for (uint32_t i = 0; !s && i < rows; i++)
        s = ud_batch_append_from(*out, b, order[i]);
    ud_free(order);
    ud_free(scratch);
    if (s && *out) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}

ud_status ud_compute_sort(ud_context *c, const ud_batch *b, uint32_t key, int32_t descending,
                          ud_batch **out) {
    if (!b || !out || key >= ud_batch_columns(b) || (descending != 0 && descending != 1))
        return UD_INVALID;
    return ud_compute_sort_keys(c, b, &key, 1, descending, 0, 0, out);
}
