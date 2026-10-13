#include "relational_internal.h"
ud_status ud_rel_pivot_bind(ud_relational *r) {
    UD_TRY(ud_rel_keys(r, 0, ud_rel_member(r->config, "setKeys")));
    UD_TRY(ud_rel_require_order(r, 0, 0));
    UD_TRY(ud_rel_selector(r->inputs[0], ud_rel_member(r->config, "pivotKey"), &r->pivot_key));
    UD_TRY(ud_rel_selector(r->inputs[0], ud_rel_member(r->config, "valueColumn"), &r->value_column));
    const ud_json *vals = ud_rel_member(r->config, "values");
    r->cell_count = ud_json_size(vals);
    if (ud_json_kind(vals) != UD_JSON_ARRAY || !r->cell_count || r->cell_count > 4096)
        return UD_SCHEMA;
    const ud_json *dup = ud_rel_member(r->config, "duplicate");
    r->duplicate = !dup || ud_rel_named(dup, "reject") ? 0
                   : ud_rel_named(dup, "sum")          ? 1
                   : ud_rel_named(dup, "count")        ? 2
                   : ud_rel_named(dup, "min")          ? 3
                   : ud_rel_named(dup, "max")          ? 4
                   : ud_rel_named(dup, "first")        ? 5
                   : ud_rel_named(dup, "last")         ? 6
                                                : -1;
    if (r->duplicate < 0)
        return UD_SCHEMA;
    if (r->duplicate >= 5)
        UD_TRY(ud_rel_require_order(r, 0, 1));
    r->pivot_values = ud_alloc(r->ctx, r->cell_count * sizeof(ud_value));
    r->cells = ud_alloc(r->ctx, r->cell_count * sizeof(ud_value));
    r->seen = ud_alloc(r->ctx, r->cell_count);
    r->acc = ud_alloc(r->ctx, r->cell_count * sizeof(*r->acc));
    if (!r->pivot_values || !r->cells || !r->seen || !r->acc)
        return UD_RESOURCE;
    ud_field *f = ud_alloc(r->ctx, (r->key_count + r->cell_count) * sizeof(*f));
    if (!f)
        return UD_RESOURCE;
    ud_status s = UD_OK;
    for (uint32_t k = 0; k < r->key_count; k++)
        f[k] = r->inputs[0]->column[r->keys[0][k]].field;
    ud_field spec = r->inputs[0]->column[r->value_column].field;
    spec.nullable = 1;
    if (r->duplicate == 2) {
        spec.type = UD_INT64;
        spec.precision = spec.scale = 0;
    }
    ud_field deps[2] = {r->inputs[0]->column[r->pivot_key].field,
                        r->inputs[0]->column[r->value_column].field};
    for (uint32_t i = 0; !s && i < r->cell_count; i++) {
        const ud_json *v = ud_json_at(vals, i);
        static const char *a[] = {"value", "name"};
        s = ud_json_keys(v, a, 2);
        ud_string name = {0};
        if (!s)
            s = ud_rel_text(ud_rel_member(v, "name"), &name);
        if (!s)
            s = ud_json_value_decode(r->ctx, &r->inputs[0]->column[r->pivot_key].field,
                                     ud_rel_member(v, "value"), UD_ROUND_EXACT, r->pivot_values + i);
        for (uint32_t k = 0; !s && k < i; k++)
            if (!ud_value_compare(r->pivot_values + i, r->pivot_values + k))
                s = UD_SCHEMA;
        if (!s)
            s = ud_rel_derive(r, &spec, deps, 2, name, f + r->key_count + i);
    }
    if (!s)
        s = ud_batch_create(r->ctx, f, r->key_count + r->cell_count, 0, &r->schema[0]);
    for (uint32_t i = 0; i < r->cell_count; i++)
        ud_field_clear(f + r->key_count + i);
    ud_free(f);
    if (!s && r->duplicate >= 1 && r->duplicate <= 4) {
        const char *fn[] = {"", "sum", "count", "min", "max"};
        ud_bytes j = {.ctx = r->ctx};
        s = ud_bytes_put(&j, "{\"version\":" UD_PROTOCOL_VERSION_TEXT ",\"aggregates\":[{\"function\":",
                         sizeof("{\"version\":" UD_PROTOCOL_VERSION_TEXT ",\"aggregates\":[{\"function\":") - 1);
        if (!s)
            s = ud_json_quote(&j, fn[r->duplicate], (uint32_t)strlen(fn[r->duplicate]));
        if (!s)
            s = ud_bytes_put(&j, ",\"column\":", sizeof(",\"column\":") - 1);
        const ud_field *v = &r->inputs[0]->column[r->value_column].field;
        if (!s)
            s = ud_json_quote(&j, v->name, v->name_length);
        if (!s)
            s = ud_bytes_put(&j, ",\"name\":\"value\"}]}", sizeof(",\"name\":\"value\"}]}") - 1);
        if (!s)
            s = ud_plan_create_fields(
                r->ctx, UD_PLAN_VERSION, r->inputs[0]->schema->fields, r->inputs[0]->columns,
                (ud_string){(char *)j.data, (uint32_t)j.size}, NULL, &r->pivot_plan);
        ud_bytes_clear(&j);
    }
    r->outputs = 2;
    if (!s)
        s = ud_rel_clone(r->ctx, r->inputs[0], 0, &r->schema[1]);
    r->output_order = r->order[0];
    r->output_order.count = r->key_count;
    return s;
}
static void pivot_reset(ud_relational *r) {
    for (uint32_t i = 0; i < r->cell_count; i++) {
        ud_clear_value(r->cells + i);
        ud_accumulator_destroy(r->acc[i]);
        r->acc[i] = NULL;
        r->seen[i] = 0;
    }
    r->active = 0;
}
static ud_status pivot_result(ud_relational *r, ud_batch *out) {
    ud_value *v = ud_alloc(r->ctx, out->columns * sizeof(*v));
    if (!v)
        return UD_RESOURCE;
    ud_status s = UD_OK;
    for (uint32_t k = 0; k < r->key_count; k++)
        v[k] = r->group_key[k];
    for (uint32_t i = 0; !s && i < r->cell_count; i++) {
        v[r->key_count + i] =
            (ud_value){.type = out->column[r->key_count + i].field.type, .is_null = 1};
        if (r->seen[i]) {
            if (r->acc[i]) {
                ud_batch *b = NULL;
                s = ud_accumulator_snapshot(r->acc[i], &b);
                ud_value x;
                if (!s)
                    s = ud_batch_get(b, 0, 0, &x);
                if (!s)
                    s = ud_copy_value(r->ctx, &x, r->cells + i, 0);
                ud_batch_destroy(b);
            }
            v[r->key_count + i] = r->cells[i];
        }
    }
    if (!s)
        s = ud_batch_append(out, v, out->columns);
    if (!s) {
        ud_batch *key_row = NULL, *identified = NULL;
        ud_status identity = ud_rel_clone(r->ctx, r->schema[0], 1, &key_row);
        if (!identity)
            identity = ud_batch_append(key_row, v, key_row->columns);
        if (!identity)
            identity = ud_batch_seal(key_row);
        ud_id scope = {0};
        if (!identity)
            identity = ud_id_make(r->ctx, (ud_string){"native.pivot", 12}, (ud_string){"group", 5},
                                  &scope);
        uint32_t k[16];
        for (uint32_t i = 0; i < r->key_count; i++)
            k[i] = i;
        if (!identity)
            identity = ud_batch_identify_keys(r->ctx, key_row, scope, k, r->key_count, &identified);
        if (!identity)
            identity = ud_batch_copy_row_info(out, out->rows - 1, identified, 0);
        ud_batch_destroy(identified);
        ud_batch_destroy(key_row);
        s = identity;
    }
    ud_free(v);
    if (!s) {
        r->groups++;
        pivot_reset(r);
    }
    return s;
}
ud_status ud_rel_pivot_pull(ud_relational *r, ud_batch **out, uint32_t *port) {
    ud_batch *result = NULL;
    UD_TRY(ud_rel_clone(r->ctx, r->schema[0], r->options.max_batch_rows, &result));
    ud_status status = UD_OK;
    while (!status && result->rows < r->options.max_batch_rows) {
        status = ud_rel_control(r);
        if (status)
            break;
        const ud_batch *b = NULL;
        uint32_t row = 0;
        ud_status next = ud_rel_peek(r, 0, &b, &row);
        if (next == UD_BUSY) {
            status = next;
            break;
        }
        if (next == UD_END) {
            if (r->active)
                status = pivot_result(r, result);
            else
                status = UD_END;
            break;
        }
        if (next) {
            status = next;
            break;
        }
        if (r->active && !ud_rel_same_group(r, 0, b, row)) {
            status = pivot_result(r, result);
            continue;
        }
        if (!r->active) {
            status = ud_rel_set_group(r, b, row);
            r->active = !status;
        }
        ud_value key;
        if (!status)
            status = ud_batch_get(b, row, r->pivot_key, &key);
        uint32_t i = 0;
        for (; !status && i < r->cell_count; i++)
            if (!ud_value_compare(&key, r->pivot_values + i))
                break;
        if (!status && (i == r->cell_count || (r->seen[i] && !r->duplicate))) {
            if (result->rows)
                break;
            status = ud_rel_reject_row(r, b, row, out, port);
            if (*out)
                break;
            continue;
        }
        if (!status) {
            if (r->seen[i])
                r->duplicates++;
            if (r->duplicate >= 1 && r->duplicate <= 4) {
                if (!r->acc[i])
                    status = ud_accumulator_create(r->pivot_plan, r->acc + i);
                if (!status)
                    status = ud_accumulator_add_accepted(r->acc[i], b, row);
            } else if (!r->seen[i] || r->duplicate == 6) {
                ud_value v;
                status = ud_batch_get(b, row, r->value_column, &v);
                ud_clear_value(r->cells + i);
                if (!status)
                    status = ud_copy_value(r->ctx, &v, r->cells + i, 0);
            }
            r->seen[i] = 1;
            if (!status)
                status = ud_rel_take(r, 0);
        }
    }
    if (*out) {
        ud_batch_destroy(result);
        return status;
    }
    if ((status == UD_BUSY || status == UD_END) && result->rows)
        status = UD_OK;
    if (status || !result->rows) {
        ud_batch_destroy(result);
        return status ? status : UD_BUSY;
    }
    *out = result;
    return ud_batch_seal(result);
}
