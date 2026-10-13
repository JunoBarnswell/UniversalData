#include "relational_internal.h"
#include "ud/connector.h"
ud_status ud_diff_bind(ud_relational *r) {
    r->ports = 2;
    r->outputs = 5;
    r->algorithm = UD_PHYSICAL_MERGE;
    if (r->inputs[0]->columns != r->inputs[1]->columns)
        return UD_SCHEMA;
    for (uint32_t i = 0; i < r->inputs[0]->columns; i++)
        if (!ud_field_matches(&r->inputs[0]->column[i].field, &r->inputs[1]->column[i].field,
                              UD_FIELD_LAYOUT))
            return UD_SCHEMA;
    UD_TRY(ud_rel_keys(r, 0, ud_rel_member(r->config, "leftKeys")));
    UD_TRY(ud_rel_keys(r, 1, ud_rel_member(r->config, "rightKeys")));
    for (uint32_t p = 0; p < 2; p++) {
        ud_operator_options sort = r->options;
        sort.kind = UD_OPERATOR_SORT;
        sort.configuration = (ud_string){0};
        sort.keys = r->keys[p];
        sort.key_count = r->key_count;
        sort.partition_count = 1;
        const ud_batch *in[] = {r->inputs[p]};
        UD_TRY(ud_operator_create(r->ctx, &sort, in, &r->input_sort[p]));
        r->order[p] = (ud_sort_contract){.count = r->key_count, .stable = 1};
        for (uint32_t k = 0; k < r->key_count; k++)
            r->order[p].keys[k] =
                (ud_sort_key){r->inputs[p]->column[r->keys[p][k]].field.identity, 0, 1};
    }
    ud_batch *union_schema = NULL;
    UD_TRY(ud_compute_union_schema(r->ctx, r->inputs[0], r->inputs[1], &union_schema));
    uint32_t n = union_schema->columns;
    ud_field *fields = ud_alloc(r->ctx, (n + 1) * sizeof(*fields));
    ud_status status = fields ? UD_OK : UD_RESOURCE;
    for (uint32_t i = 0; !status && i < n; i++)
        fields[i] = union_schema->column[i].field;
    if (!status) {
        ud_field before = {.type = UD_STRUCT,
                           .nullable = 1,
                           .children = r->inputs[0]->schema->fields,
                           .child_count = n};
        status = ud_rel_derive(r, &before, r->inputs[0]->schema->fields, n,
                               (ud_string){"before", 6}, fields + n);
    }
    for (uint32_t p = 0; !status && p < 4; p++)
        status = ud_batch_create(r->ctx, fields, n + 1, 0, r->schema + p);
    if (fields)
        ud_field_clear(fields + n);
    ud_free(fields);
    ud_batch_destroy(union_schema);
    if (!status)
        status = ud_error_schema(&r->options.errors, r->inputs[0],
                                 ud_rel_named(ud_rel_member(r->config, "includeInput"), "true"),
                                 r->schema + 4);
    return status;
}
static ud_status unique_take(ud_relational *r, uint32_t p) {
    const ud_batch *b = r->in[p].batch;
    uint32_t row = r->in[p].row;
    if (r->in[p].have_last) {
        int same = 1;
        for (uint32_t k = 0; k < r->key_count; k++) {
            ud_value v;
            UD_TRY(ud_batch_get(b, row, r->keys[p][k], &v));
            same &= !ud_value_compare(&v, r->in[p].last + k);
        }
        if (same)
            return UD_QUALITY;
    }
    for (uint32_t k = 0; k < r->key_count; k++) {
        ud_value v;
        UD_TRY(ud_batch_get(b, row, r->keys[p][k], &v));
        if (v.is_null)
            return UD_QUALITY;
    }
    return ud_rel_take(r, p);
}
static ud_status emit(ud_relational *r, ud_batch *out, const ud_batch *before, uint32_t br,
                      const ud_batch *after, uint32_t ar, uint32_t port) {
    uint32_t n = r->inputs[0]->columns;
    ud_value *values = ud_alloc(r->ctx, (n + 1) * sizeof(*values)),
             *old = before ? ud_alloc(r->ctx, n * sizeof(*old)) : NULL;
    if (!values || (before && !old)) {
        ud_free(values);
        ud_free(old);
        return UD_RESOURCE;
    }
    ud_status s = UD_OK;
    const ud_batch *row = after ? after : before;
    uint32_t index = after ? ar : br;
    for (uint32_t i = 0; !s && i < n; i++) {
        s = ud_batch_get(row, index, i, values + i);
        if (!s && before)
            s = ud_batch_get(before, br, i, old + i);
    }
    values[n] = (ud_value){
        .type = UD_STRUCT, .is_null = !before, .children = old, .child_count = before ? n : 0};
    if (!s)
        s = ud_batch_append(out, values, n + 1);
    if (!s) {
        ud_row_info info;
        s = ud_batch_row_info(row, index, &info);
        if (!s) {
            info.operation = port == 0   ? UD_CHANGE_INSERT
                             : port == 1 ? UD_CHANGE_UPDATE
                             : port == 2 ? UD_CHANGE_DELETE
                                         : 0;
            s = ud_batch_set_row_info(out, out->rows - 1, &info);
        }
    }
    ud_free(values);
    ud_free(old);
    return s;
}
ud_status ud_diff_pull(ud_relational *r, ud_batch **out, uint32_t *output) {
    *out = NULL;
    uint32_t port = 0;
    ud_status s = UD_OK;
    while (!s) {
        s = ud_rel_control(r);
        if (s)
            break;
        const ud_batch *a = NULL, *b = NULL;
        uint32_t ar = 0, br = 0;
        ud_status sa = ud_rel_peek(r, 0, &a, &ar), sb = ud_rel_peek(r, 1, &b, &br);
        if (sa == UD_BUSY || sb == UD_BUSY) {
            s = UD_BUSY;
            break;
        }
        if ((sa && sa != UD_END) || (sb && sb != UD_END)) {
            s = sa && sa != UD_END ? sa : sb;
            break;
        }
        if (sa == UD_END && sb == UD_END) {
            s = UD_END;
            break;
        }
        int null = 0, c = sa == UD_END   ? 1
                          : sb == UD_END ? -1
                                         : ud_rel_compare_rows(r, a, ar, b, br, &null);
        if (null) {
            s = UD_QUALITY;
            break;
        }
        uint32_t selected = c < 0 ? 2 : c > 0 ? 0 : 3;
        if (!c) {
            for (uint32_t i = 0; i < a->columns; i++) {
                ud_value left, right;
                s = ud_batch_get(a, ar, i, &left);
                if (!s)
                    s = ud_batch_get(b, br, i, &right);
                if (s)
                    break;
                if (ud_value_compare(&left, &right))
                    selected = 1;
            }
        }
        if (s)
            break;
        if (*out && selected != port)
            break;
        if (!*out) {
            port = selected;
            s = ud_rel_clone(r->ctx, r->schema[port], r->options.max_batch_rows, out);
            if (s)
                break;
        }
        /* Validate uniqueness before publishing this classification. */
        if (c <= 0)
            s = unique_take(r, 0);
        if (!s && c >= 0)
            s = unique_take(r, 1);
        if (!s)
            s = emit(r, *out, c > 0 ? NULL : a, ar, c < 0 ? NULL : b, br, port);
        if (s || (*out)->rows == r->options.max_batch_rows)
            break;
    }
    if ((s == UD_BUSY || s == UD_END) && *out && (*out)->rows)
        s = UD_OK;
    if (s || !*out || !(*out)->rows) {
        ud_batch_destroy(*out);
        *out = NULL;
        return s ? s : UD_BUSY;
    }
    if (output)
        *output = port;
    return ud_batch_seal(*out);
}
