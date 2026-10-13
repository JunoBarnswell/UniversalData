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
        if (!s)
            s = ud_field_derive(ctx, source, (ud_string){scope, (uint32_t)strlen(scope)}, name,
                                fields + j);
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
ud_status ud_compute_sort(ud_context *c, const ud_batch *b, uint32_t key, int32_t descending,
                          ud_batch **out) {
    if (!b || !out || key >= ud_batch_columns(b) || (descending != 0 && descending != 1))
        return UD_INVALID;
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
                ud_value x, y;
                ud_batch_get(b, order[a], key, &x);
                ud_batch_get(b, order[z], key, &y);
                int cmp = ud_value_compare(&x, &y);
                if (descending)
                    cmp = -cmp;
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
    ud_value *v = ud_alloc(c, n * sizeof(*v));
    if (!v && !s)
        s = UD_RESOURCE;
    for (uint32_t i = 0; !s && i < rows; i++)
        s = append_row(*out, b, order[i], v);
    ud_free(v);
    ud_free(order);
    ud_free(scratch);
    if (s && *out) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}
ud_status ud_compute_inner_join(ud_context *ctx, const ud_batch *left, uint32_t lk,
                                const ud_batch *right, uint32_t rk, ud_batch **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!ctx || !left || !right)
        return UD_INVALID;
    const ud_batch *schemas[] = {left, right};
    ud_buffer_manager *buffers = NULL;
    ud_operator *operator= NULL;
    ud_buffer_options budget = {.struct_size = sizeof(budget),
                                .abi_version = UD_ABI_VERSION,
                                .max_blocks = 16384,
                                .resident_limit = UINT64_C(67108864),
                                .working_limit = UINT64_C(67108864)};
    ud_status s = ud_buffer_manager_create(ctx, &budget, &buffers);
    ud_operator_options options = {.struct_size = sizeof(options),
                                   .abi_version = UD_ABI_VERSION,
                                   .kind = UD_OPERATOR_JOIN,
                                   .max_batch_rows = 4096,
                                   .keys = &lk,
                                   .key_count = 1,
                                   .right_keys = &rk,
                                   .buffers = buffers};
    if (!s)
        s = ud_operator_create(ctx, &options, schemas, &operator);
    ud_batch *schema = NULL;
    if (!s)
        s = ud_operator_schema(operator, 0, & schema);
    for (uint32_t port = 0; !s && port < 2; port++) {
        const ud_batch *input = schemas[port];
        for (uint32_t row = 0; !s && row < input->rows; row += 4096) {
            ud_batch *batch = NULL;
            s = ud_compute_slice(ctx, input, row, 4096, &batch);
            if (!s)
                s = ud_batch_seal(batch);
            if (!s)
                s = ud_operator_push(operator, port, batch);
            ud_batch_destroy(batch);
        }
        if (!s)
            s = ud_operator_finish_input(operator, port);
    }
    ud_batch **parts = NULL;
    uint32_t count = 0;
    uint64_t rows = 0;
    while (!s) {
        ud_batch *batch = NULL;
        s = ud_operator_pull(operator, &batch, NULL);
        if (s == UD_END) {
            s = UD_OK;
            break;
        }
        if (s)
            break;
        if (rows > INT32_MAX - batch->rows) {
            ud_batch_destroy(batch);
            s = UD_RESOURCE;
            break;
        }
        ud_batch **next = ud_alloc(ctx, (size_t)(count + 1) * sizeof(*next));
        if (!next) {
            ud_batch_destroy(batch);
            s = UD_RESOURCE;
            break;
        }
        if (count)
            memcpy(next, parts, count * sizeof(*next));
        ud_free(parts);
        parts = next;
        parts[count++] = batch;
        rows += batch->rows;
    }
    ud_field *fields = NULL;
    ud_value *values = NULL;
    if (!s)
        s = fields_of(ctx, schema, &fields);
    if (!s)
        s = ud_batch_create(ctx, fields, schema->columns, (uint32_t)rows, out);
    if (!s) {
        values = ud_alloc(ctx, schema->columns * sizeof(*values));
        if (!values)
            s = UD_RESOURCE;
    }
    for (uint32_t i = 0; !s && i < count; i++)
        for (uint32_t row = 0; !s && row < parts[i]->rows; row++)
            s = append_row(*out, parts[i], row, values);
    for (uint32_t i = 0; i < count; i++)
        ud_batch_destroy(parts[i]);
    ud_free(parts);
    ud_free(fields);
    ud_free(values);
    ud_batch_destroy(schema);
    ud_status cleanup = ud_operator_destroy(operator);
    if (!s && cleanup)
        s = cleanup;
    cleanup = ud_buffer_manager_destroy(buffers);
    if (!s && cleanup)
        s = cleanup;
    if (s) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}
