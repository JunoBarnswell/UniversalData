#include "relational_internal.h"
ud_status ud_rel_unpivot_bind(ud_relational *r) {
    const ud_json *cols = ud_rel_member(r->config, "columns");
    r->cell_count = ud_json_size(cols);
    if (ud_json_kind(cols) != UD_JSON_ARRAY || !r->cell_count ||
        r->cell_count > r->inputs[0]->columns)
        return UD_SCHEMA;
    r->unpivot_columns = ud_alloc(r->ctx, r->cell_count * sizeof(uint32_t));
    r->pivot_values = ud_alloc(r->ctx, r->cell_count * sizeof(ud_value));
    if (!r->unpivot_columns || !r->pivot_values)
        return UD_RESOURCE;
    UD_TRY(ud_rel_selectors(r, r->inputs[0], ud_rel_member(r->config, "retain"), &r->retain,
                            &r->retain_count));
    ud_field *deps = ud_alloc(r->ctx, r->cell_count * sizeof(ud_field));
    if (!deps)
        return UD_RESOURCE;
    ud_status s = UD_OK;
    for (uint32_t i = 0; !s && i < r->cell_count; i++) {
        const ud_json *col = ud_json_at(cols, i);
        static const char *a[] = {"column", "key"};
        s = ud_json_keys(col, a, 2);
        if (!s)
            s = ud_rel_selector(r->inputs[0], ud_rel_member(col, "column"), r->unpivot_columns + i);
        ud_string key;
        if (!s)
            s = ud_rel_text(ud_rel_member(col, "key"), &key);
        if (!s) {
            ud_value v = {.type = UD_UTF8, .text = key.data, .text_length = key.length};
            s = ud_copy_value(r->ctx, &v, r->pivot_values + i, 0);
            deps[i] = r->inputs[0]->column[r->unpivot_columns[i]].field;
        }
        if (!s && i && !ud_field_matches(deps, deps + i, UD_FIELD_VALUE_TYPE))
            s = UD_SCHEMA;
        for (uint32_t k = 0; !s && k < i; k++)
            if (r->unpivot_columns[k] == r->unpivot_columns[i])
                s = UD_SCHEMA;
    }
    ud_field *f = ud_alloc(r->ctx, (r->retain_count + 2) * sizeof(ud_field));
    if (!f)
        s = UD_RESOURCE;
    for (uint32_t i = 0; !s && i < r->retain_count; i++)
        f[i] = r->inputs[0]->column[r->retain[i]].field;
    ud_string key = {"key", 3}, value = {"value", 5};
    if (!s && ud_rel_member(r->config, "keyName"))
        s = ud_rel_text(ud_rel_member(r->config, "keyName"), &key);
    if (!s && ud_rel_member(r->config, "valueName"))
        s = ud_rel_text(ud_rel_member(r->config, "valueName"), &value);
    ud_field spec = {.type = UD_UTF8};
    if (!s)
        s = ud_rel_derive(r, &spec, deps, r->cell_count, key, f + r->retain_count);
    if (!s) {
        spec = deps[0];
        spec.nullable = 1;
        s = ud_rel_derive(r, &spec, deps, r->cell_count, value, f + r->retain_count + 1);
    }
    if (!s)
        s = ud_batch_create(r->ctx, f, r->retain_count + 2, 0, &r->schema[0]);
    if (f) {
        ud_field_clear(f + r->retain_count);
        ud_field_clear(f + r->retain_count + 1);
    }
    ud_free(f);
    ud_free(deps);
    const ud_json *n = ud_rel_member(r->config, "nulls");
    if (n && !ud_rel_named(n, "skip") && !ud_rel_named(n, "include"))
        return UD_SCHEMA;
    r->nulls = ud_rel_named(n, "include");
    return s;
}
ud_status ud_rel_unpivot_pull(ud_relational *r, ud_batch **out) {
    UD_TRY(ud_rel_clone(r->ctx, r->schema[0], r->options.max_batch_rows, out));
    ud_value *v = ud_alloc(r->ctx, (*out)->columns * sizeof(*v));
    if (!v) {
        ud_batch_destroy(*out);
        *out = NULL;
        return UD_RESOURCE;
    }
    ud_status s = UD_OK;
    while (!s && (*out)->rows < r->options.max_batch_rows) {
        s = ud_rel_control(r);
        if (s)
            break;
        const ud_batch *b;
        uint32_t row;
        s = ud_rel_peek(r, 0, &b, &row);
        if (s)
            break;
        uint32_t i = r->unpivot_at++;
        for (uint32_t j = 0; !s && j < r->retain_count; j++)
            s = ud_batch_get(b, row, r->retain[j], v + j);
        v[r->retain_count] = r->pivot_values[i];
        if (!s)
            s = ud_batch_get(b, row, r->unpivot_columns[i], v + r->retain_count + 1);
        if (!s && (r->nulls || !v[r->retain_count + 1].is_null)) {
            s = ud_batch_append(*out, v, (*out)->columns);
            ud_row_info info;
            if (!s)
                s = ud_batch_row_info(b, row, &info);
            char parent[33], field[33];
            if (!s) {
                ud_id_format(info.id, parent);
                ud_id_format(b->column[r->unpivot_columns[i]].field.identity, field);
                s = ud_id_make(r->ctx, (ud_string){parent, 32}, (ud_string){field, 32}, &info.id);
            }
            if (!s)
                s = ud_batch_set_row_info(*out, (*out)->rows - 1, &info);
        }
        if (!s && r->unpivot_at == r->cell_count) {
            r->unpivot_at = 0;
            s = ud_rel_take(r, 0);
        }
    }
    ud_free(v);
    if ((s == UD_BUSY || s == UD_END) && (*out)->rows)
        s = UD_OK;
    if (s || !(*out)->rows) {
        ud_batch_destroy(*out);
        *out = NULL;
        return s ? s : UD_BUSY;
    }
    return ud_batch_seal(*out);
}
