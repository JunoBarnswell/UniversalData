#include "relational_internal.h"
const ud_json *ud_rel_member(const ud_json *j, const char *key) {
    return ud_json_find(j, key, (uint32_t)strlen(key));
}
int ud_rel_named(const ud_json *j, const char *text) {
    const char *p = NULL;
    uint32_t n = 0;
    return j && !ud_json_text(j, &p, &n) && n == strlen(text) && !memcmp(p, text, n);
}
ud_status ud_rel_number(const ud_json *j, uint64_t fallback, uint64_t *out) {
    if (!j) {
        *out = fallback;
        return UD_OK;
    }
    const char *p;
    uint32_t n;
    UD_TRY(ud_json_text(j, &p, &n));
    if (ud_json_kind(j) != UD_JSON_NUMBER || !n)
        return UD_SCHEMA;
    uint64_t v = 0;
    for (uint32_t i = 0; i < n; i++) {
        if (p[i] < '0' || p[i] > '9')
            return UD_SCHEMA;
        uint32_t digit = (uint32_t)(p[i] - '0');
        if (v > (UINT64_MAX - digit) / 10)
            return UD_OVERFLOW;
        v = v * 10 + digit;
    }
    *out = v;
    return UD_OK;
}
ud_status ud_rel_text(const ud_json *j, ud_string *out) {
    if (ud_json_kind(j) != UD_JSON_STRING)
        return UD_SCHEMA;
    return ud_json_text(j, &out->data, &out->length);
}
ud_status ud_rel_clone(ud_context *ctx, const ud_batch *b, uint32_t capacity, ud_batch **out) {
    return ud_batch_create(ctx, b->schema->fields, b->columns, capacity, out);
}
ud_status ud_rel_control(ud_relational *r) {
    if (ud_is_cancelled(r->ctx))
        return UD_CANCELLED;
    return r->options.check ? r->options.check(r->options.user) : UD_OK;
}
ud_status ud_rel_selector(const ud_batch *b, const ud_json *j, uint32_t *out) {
    if (ud_json_kind(j) == UD_JSON_NUMBER) {
        uint64_t n;
        UD_TRY(ud_rel_number(j, 0, &n));
        if (n >= b->columns)
            return UD_SCHEMA;
        *out = (uint32_t)n;
        return UD_OK;
    }
    ud_string name;
    UD_TRY(ud_rel_text(j, &name));
    ud_id id = {0};
    int is_id = name.length == 32 && !ud_id_parse(name, &id);
    for (uint32_t i = 0; i < b->columns; i++) {
        const ud_field *f = &b->column[i].field;
        if ((is_id && !ud_id_compare(id, f->identity)) ||
            (name.length == f->name_length && !memcmp(name.data, f->name, name.length))) {
            *out = i;
            return UD_OK;
        }
    }
    return UD_SCHEMA;
}
ud_status ud_rel_selectors(ud_relational *r, const ud_batch *b, const ud_json *j, uint32_t **out,
                           uint32_t *n) {
    if (ud_json_kind(j) != UD_JSON_ARRAY)
        return UD_SCHEMA;
    *n = ud_json_size(j);
    if (*n > b->columns)
        return UD_SCHEMA;
    *out = ud_alloc(r->ctx, ((size_t)*n + 1) * sizeof(**out));
    if (!*out)
        return UD_RESOURCE;
    for (uint32_t i = 0; i < *n; i++) {
        UD_TRY(ud_rel_selector(b, ud_json_at(j, i), *out + i));
        for (uint32_t k = 0; k < i; k++)
            if ((*out)[k] == (*out)[i])
                return UD_SCHEMA;
    }
    return UD_OK;
}
ud_status ud_rel_keys(ud_relational *r, uint32_t port, const ud_json *j) {
    uint32_t *k = NULL, n = 0;
    ud_status s = ud_rel_selectors(r, r->inputs[port], j, &k, &n);
    if (!s && (!n || n > 16 || (port && n != r->key_count)))
        s = UD_SCHEMA;
    if (!s) {
        memcpy(r->keys[port], k, n * sizeof(*k));
        r->key_count = n;
    }
    ud_free(k);
    return s;
}
ud_status ud_rel_require_order(ud_relational *r, uint32_t port, int stable) {
    if (!r->options.input_order || r->order[port].count < r->key_count ||
        (stable && !r->order[port].stable))
        return ud_fail(r->ctx, UD_SCHEMA,
                       "operator requires a bound stable field-ID sort contract");
    for (uint32_t i = 0; i < r->key_count; i++)
        if (ud_id_compare(r->order[port].keys[i].field,
                          r->inputs[port]->column[r->keys[port][i]].field.identity))
            return UD_SCHEMA;
    return UD_OK;
}
static int compare_value(ud_relational *r, uint32_t k, const ud_value *a, const ud_value *b) {
    if (a->is_null != b->is_null)
        return a->is_null ? (r->order[0].keys[k].nulls_first ? -1 : 1)
                          : (r->order[0].keys[k].nulls_first ? 1 : -1);
    int c = ud_value_compare(a, b);
    return r->order[0].keys[k].descending ? -c : c;
}
ud_status ud_rel_peek(ud_relational *r, uint32_t p, const ud_batch **b, uint32_t *row) {
    stream *s = r->in + p;
    if (s->batch && s->row >= s->batch->rows) {
        ud_buffer_view_release(s->view);
        s->view = NULL;
        s->batch = NULL;
        UD_TRY(ud_buffer_release(s->block));
        s->block = NULL;
        s->row = 0;
    }
    if (!s->block && r->input_sort[p] && !r->sort_done[p]) {
        ud_batch *next = NULL;
        ud_status status = ud_operator_pull(r->input_sort[p], &next, NULL);
        if (status == UD_END)
            r->sort_done[p] = 1;
        else if (status)
            return status;
        else {
            status = ud_buffer_store(r->options.buffers, next, &s->block);
            ud_batch_destroy(next);
            if (status)
                return status;
        }
    }
    if (!s->block)
        return s->ended ? UD_END : UD_BUSY;
    if (!s->view) {
        UD_TRY(ud_buffer_read(s->block, &s->view));
        s->batch = ud_buffer_view_batch(s->view);
    }
    if (!s->batch->rows) {
        s->row = s->batch->rows;
        return ud_rel_peek(r, p, b, row);
    }
    *b = s->batch;
    *row = s->row;
    return UD_OK;
}
ud_status ud_rel_take(ud_relational *r, uint32_t p) {
    stream *s = r->in + p;
    if (r->key_count) {
        for (uint32_t k = 0; k < r->key_count; k++) {
            ud_value v;
            UD_TRY(ud_batch_get(s->batch, s->row, r->keys[p][k], &v));
            if (s->have_last) {
                int c = compare_value(r, k, s->last + k, &v);
                if (c > 0)
                    return ud_fail(r->ctx, UD_SCHEMA, "input violates its declared sort order");
                if (c < 0)
                    break;
            }
        }
        for (uint32_t k = 0; k < r->key_count; k++) {
            ud_value v;
            UD_TRY(ud_batch_get(s->batch, s->row, r->keys[p][k], &v));
            ud_clear_value(s->last + k);
            UD_TRY(ud_copy_value(r->ctx, &v, s->last + k, 0));
        }
        s->have_last = 1;
    }
    s->row++;
    r->processed[p]++;
    return UD_OK;
}
int ud_rel_compare_rows(ud_relational *r, const ud_batch *a, uint32_t ar, const ud_batch *b,
                        uint32_t br, int *null) {
    *null = 0;
    for (uint32_t k = 0; k < r->key_count; k++) {
        ud_value x, y;
        ud_batch_get(a, ar, r->keys[0][k], &x);
        ud_batch_get(b, br, r->keys[1][k], &y);
        *null |= x.is_null || y.is_null;
        int c = compare_value(r, k, &x, &y);
        if (c)
            return c;
    }
    return 0;
}
int ud_rel_same_group(ud_relational *r, uint32_t p, const ud_batch *b, uint32_t row) {
    for (uint32_t k = 0; k < r->key_count; k++) {
        ud_value v;
        ud_batch_get(b, row, r->keys[p][k], &v);
        if (compare_value(r, k, r->group_key + k, &v))
            return 0;
    }
    return 1;
}
ud_status ud_rel_set_group(ud_relational *r, const ud_batch *b, uint32_t row) {
    for (uint32_t k = 0; k < r->key_count; k++) {
        ud_value v;
        UD_TRY(ud_batch_get(b, row, r->keys[0][k], &v));
        ud_clear_value(r->group_key + k);
        UD_TRY(ud_copy_value(r->ctx, &v, r->group_key + k, 0));
    }
    return UD_OK;
}
ud_status ud_rel_scan_next(scan *s) {
    while (!s->batch || s->row >= s->batch->rows) {
        ud_buffer_view_release(s->view);
        s->view = NULL;
        s->batch = NULL;
        s->row = 0;
        if (!s->at)
            return UD_END;
        page *p = s->at;
        s->at = p->next;
        UD_TRY(ud_buffer_read(p->block, &s->view));
        s->batch = ud_buffer_view_batch(s->view);
    }
    return UD_OK;
}
ud_status ud_rel_flush_group(ud_relational *r, uint32_t p) {
    if (!r->chunk[p] || !r->chunk[p]->rows)
        return UD_OK;
    page *x = ud_alloc(r->ctx, sizeof(*x));
    if (!x)
        return UD_RESOURCE;
    ud_status s = ud_batch_seal(r->chunk[p]);
    if (!s)
        s = ud_buffer_store(r->options.buffers, r->chunk[p], &x->block);
    if (s) {
        ud_free(x);
        return s;
    }
    if (r->tail[p])
        r->tail[p]->next = x;
    else
        r->group[p] = x;
    r->tail[p] = x;
    ud_batch_destroy(r->chunk[p]);
    r->chunk[p] = NULL;
    return UD_OK;
}
ud_status ud_rel_clear_group(ud_relational *r) {
    for (uint32_t p = 0; p < 2; p++) {
        ud_buffer_view_release(r->scans[p].view);
        memset(r->scans + p, 0, sizeof(scan));
        ud_batch_destroy(r->chunk[p]);
        r->chunk[p] = NULL;
        while (r->group[p]) {
            page *x = r->group[p];
            UD_TRY(ud_buffer_release(x->block));
            r->group[p] = x->next;
            ud_free(x);
        }
        r->tail[p] = NULL;
        r->group_closed[p] = 0;
    }
    r->phase = 0;
    return UD_OK;
}
ud_status ud_rel_derive(ud_relational *r, const ud_field *spec, const ud_field *deps, uint32_t n,
                        ud_string name, ud_field *out) {
    ud_field f = *spec;
    ud_id *lineage = NULL;
    uint32_t count = 0;
    UD_TRY(ud_lineage_merge(r->ctx, deps, n, &lineage, &count));
    f.lineage = lineage;
    f.lineage_count = count;
    f.key_order = 0;
    ud_status s = ud_field_derive(r->ctx, &f, (ud_string){"native.relational.8", 19}, name, out);
    ud_free(lineage);
    return s;
}
ud_status ud_rel_reject_row(ud_relational *r, const ud_batch *b, uint32_t row, ud_batch **out,
                            uint32_t *port) {
    r->rejected++;
    if (r->policy == UD_ERROR_FAIL)
        return UD_SCHEMA;
    if (r->policy == UD_ERROR_IGNORE) {
        r->ignored++;
        return ud_rel_take(r, 0);
    }
    UD_TRY(ud_rel_clone(r->ctx, b, 1, out));
    UD_TRY(ud_batch_append_from(*out, b, row));
    UD_TRY(ud_batch_seal(*out));
    *port = 1;
    return ud_rel_take(r, 0);
}
ud_status ud_relational_create(ud_context *ctx, const ud_operator_options *o,
                               const ud_batch *const *in, ud_relational **out) {
    *out = NULL;
    ud_relational *r = ud_alloc(ctx, sizeof(*r));
    if (!r)
        return UD_RESOURCE;
    r->ctx = ctx;
    r->options = *o;
    if (!r->options.errors.memory) {
        r->options.errors = (ud_error_context){
            .struct_size = sizeof(ud_error_context), .abi_version = UD_ABI_VERSION, .memory = ctx};
        ud_id_make(ctx, (ud_string){"native.compute", 14}, (ud_string){"errors", 6},
                   &r->options.errors.field_scope);
    }
    r->ports = o->kind == UD_OPERATOR_JOIN || o->kind == UD_OPERATOR_MERGE ||
                       o->kind == UD_OPERATOR_MERGE_JOIN || o->kind == UD_OPERATOR_ASOF_JOIN ||
                       o->kind == UD_OPERATOR_TEMPORAL_JOIN || o->kind == UD_OPERATOR_DIFF_COMPARE
                   ? 2
                   : 1;
    r->outputs = 1;
    ud_bytes definition = {.ctx = ctx};
    ud_status s = UD_OK;
    if (o->kind == UD_OPERATOR_JOIN && !o->configuration.length) {
        const char *prefix = "{\"leftKeys\":[";
        s = ud_bytes_put(&definition, prefix, strlen(prefix));
        for (uint32_t k = 0; !s && k < o->key_count; k++) {
            char n[32];
            int count = snprintf(n, sizeof(n), "%s%u", k ? "," : "", o->keys[k]);
            s = ud_bytes_put(&definition, n, (size_t)count);
        }
        if (!s)
            s = ud_bytes_put(&definition, "],\"rightKeys\":[", 15);
        for (uint32_t k = 0; !s && k < o->key_count; k++) {
            char n[32];
            int count = snprintf(n, sizeof(n), "%s%u", k ? "," : "", o->right_keys[k]);
            s = ud_bytes_put(&definition, n, (size_t)count);
        }
        if (!s)
            s = ud_bytes_put(&definition, "]}", 2);
        if (!s)
            s = ud_json_parse(ctx, (char *)definition.data, (uint32_t)definition.size, &r->config,
                              NULL);
    } else
        s = ud_json_parse(ctx, o->configuration.data, o->configuration.length, &r->config, NULL);
    ud_bytes_clear(&definition);
    ud_string declared = {0};
    ud_json *schema = NULL, *bound = NULL;
    if (!s)
        s = ud_operator_configuration_schema(o->kind, &declared);
    if (!s)
        s = ud_json_parse(ctx, declared.data, declared.length, &schema, NULL);
    if (!s)
        s = ud_json_bind_schema(ctx, schema, r->config, &bound);
    ud_json_destroy(schema);
    if (!s) {
        ud_json_destroy(r->config);
        r->config = bound;
    } else
        ud_json_destroy(bound);

    for (uint32_t p = 0; !s && p < r->ports; p++) {
        if (!in[p])
            s = UD_SCHEMA;
        else
            s = ud_rel_clone(ctx, in[p], 0, r->inputs + p);
        if (o->input_order)
            r->order[p] = o->input_order[p];
    }
    if (!s && o->kind == UD_OPERATOR_SCD)
        s = in[1] ? ud_rel_clone(ctx, in[1], 0, r->inputs + 1) : UD_SCHEMA;
    const ud_json *policy = ud_rel_member(r->config, "errorPolicy");
    r->policy = !policy || ud_rel_named(policy, "fail") ? UD_ERROR_FAIL
                : ud_rel_named(policy, "redirect")      ? UD_ERROR_REDIRECT
                : ud_rel_named(policy, "ignore")        ? UD_ERROR_IGNORE
                                                        : -1;
    if (r->policy < 0)
        s = UD_SCHEMA;
    if (!s && (o->kind == UD_OPERATOR_JOIN || o->kind == UD_OPERATOR_MERGE ||
               o->kind == UD_OPERATOR_MERGE_JOIN)) {
        if (!s)
            s = ud_rel_keys(
                r, 0, ud_rel_member(r->config, o->kind == UD_OPERATOR_MERGE ? "keys" : "leftKeys"));
        if (!s)
            s = ud_rel_keys(
                r, 1,
                ud_rel_member(r->config, o->kind == UD_OPERATOR_MERGE ? "keys" : "rightKeys"));
        r->algorithm = UD_PHYSICAL_MERGE;
        if (!s && o->kind == UD_OPERATOR_JOIN) {
            int ordered = 1;
            for (uint32_t p = 0; p < 2; p++) {
                ordered &= r->order[p].count >= r->key_count && r->order[p].stable;
                for (uint32_t k = 0; ordered && k < r->key_count; k++)
                    ordered &= !ud_id_compare(r->order[p].keys[k].field,
                                              r->inputs[p]->column[r->keys[p][k]].field.identity) &&
                               !r->order[p].keys[k].descending && r->order[p].keys[k].nulls_first;
            }
            const ud_json *strategy = ud_rel_member(r->config, "strategy");
            uint64_t budget = o->memory_budget ? o->memory_budget : 16u * 1024u * 1024u;
            uint64_t rows = o->estimated_rows[1];
            r->algorithm = ud_rel_named(strategy, "merge")       ? UD_PHYSICAL_MERGE
                           : ud_rel_named(strategy, "hash")      ? UD_PHYSICAL_HASH
                           : ud_rel_named(strategy, "graceHash") ? UD_PHYSICAL_GRACE_HASH
                           : ordered                             ? UD_PHYSICAL_MERGE
                           : rows && rows <= budget / 64         ? UD_PHYSICAL_HASH
                                                                 : UD_PHYSICAL_GRACE_HASH;
        }
        for (uint32_t p = 0; !s && p < 2; p++) {
            if (r->algorithm != UD_PHYSICAL_MERGE) {
                r->order[p] = (ud_sort_contract){.count = r->key_count, .stable = 0};
                for (uint32_t k = 0; k < r->key_count; k++)
                    r->order[p].keys[k] =
                        (ud_sort_key){r->inputs[p]->column[r->keys[p][k]].field.identity, 0, 1};
                continue;
            }
            int sorted = r->order[p].count >= r->key_count && r->order[p].stable;
            for (uint32_t k = 0; k < r->key_count && sorted; k++)
                sorted = !ud_id_compare(r->order[p].keys[k].field,
                                        r->inputs[p]->column[r->keys[p][k]].field.identity) &&
                         !r->order[p].keys[k].descending && r->order[p].keys[k].nulls_first;
            if (o->kind == UD_OPERATOR_JOIN && !sorted) {
                ud_operator_options sort = *o;
                sort.kind = UD_OPERATOR_SORT;
                sort.configuration = (ud_string){0};
                sort.keys = r->keys[p];
                sort.key_count = r->key_count;
                sort.right_keys = NULL;
                sort.partition_count = 1;
                sort.configuration = (ud_string){0};
                sort.input_order = NULL;
                sort.reference = NULL;
                const ud_batch *input[] = {in[p]};
                s = ud_operator_create(ctx, &sort, input, r->input_sort + p);
                r->order[p].count = r->key_count;
                r->order[p].stable = 1;
                for (uint32_t k = 0; k < r->key_count; k++)
                    r->order[p].keys[k] =
                        (ud_sort_key){in[p]->column[r->keys[p][k]].field.identity, 0, 1};
            } else
                s = ud_rel_require_order(r, p, 1);
        }
        for (uint32_t k = 0; !s && k < r->key_count; k++)
            if (r->order[0].keys[k].descending != r->order[1].keys[k].descending ||
                r->order[0].keys[k].nulls_first != r->order[1].keys[k].nulls_first ||
                !ud_field_matches(&in[0]->column[r->keys[0][k]].field,
                                  &in[1]->column[r->keys[1][k]].field, UD_FIELD_VALUE_TYPE))
                s = UD_SCHEMA;
        if (!s && o->kind == UD_OPERATOR_MERGE) {
            if (in[0]->columns != in[1]->columns)
                s = UD_SCHEMA;
            for (uint32_t i = 0; !s && i < in[0]->columns; i++)
                if (!ud_field_matches(&in[0]->column[i].field, &in[1]->column[i].field,
                                      UD_FIELD_LAYOUT))
                    s = UD_SCHEMA;
            if (!s)
                s = ud_compute_union_schema(ctx, in[0], in[1], &r->schema[0]);
        } else if (!s) {
            const ud_json *type = ud_rel_member(r->config, "joinType");
            r->join_type = !type || ud_rel_named(type, "inner") ? 0
                           : ud_rel_named(type, "left")         ? 1
                           : ud_rel_named(type, "full")         ? 2
                           : ud_rel_named(type, "right")        ? 3
                           : ud_rel_named(type, "semi")         ? 4
                           : ud_rel_named(type, "anti")         ? 5
                                                                : -1;
            if (r->join_type < 0)
                s = UD_SCHEMA;
            if (!s)
                s = ud_rel_join_schema(r);
        }
        if (!s) {
            r->output_order = r->order[0];
            r->output_order.count = r->key_count;
            for (uint32_t k = 0; k < r->key_count; k++)
                r->output_order.keys[k].field = r->schema[0]->column[r->keys[0][k]].field.identity;
            if (r->join_type == 2 || r->join_type == 3)
                memset(&r->output_order, 0, sizeof(r->output_order));
        }
    } else if (!s && (o->kind == UD_OPERATOR_ASOF_JOIN || o->kind == UD_OPERATOR_TEMPORAL_JOIN))
        s = ud_temporal_bind(r);
    else if (!s && o->kind == UD_OPERATOR_DIFF_COMPARE)
        s = ud_diff_bind(r);
    else if (!s && o->kind == UD_OPERATOR_PIVOT)
        s = ud_rel_pivot_bind(r);
    else if (!s && o->kind == UD_OPERATOR_UNPIVOT)
        s = ud_rel_unpivot_bind(r);
    else if (!s &&
             (o->kind == UD_OPERATOR_ROW_SAMPLING || o->kind == UD_OPERATOR_PERCENTAGE_SAMPLING))
        s = ud_rel_sampling_bind(r);
    else if (!s && o->kind == UD_OPERATOR_SCD)
        s = ud_rel_scd_bind(r);
    if (!s && o->kind == UD_OPERATOR_JOIN && r->algorithm != UD_PHYSICAL_MERGE) {
        memset(&r->output_order, 0, sizeof(r->output_order));
        s = ud_hash_join_create(r);
    }
    if (s) {
        ud_relational_destroy(r);
        return s;
    }
    *out = r;
    return UD_OK;
}
ud_status ud_relational_schema(const ud_relational *r, uint32_t p, ud_batch **out) {
    return p < r->outputs ? ud_rel_clone(r->ctx, r->schema[p], 0, out) : UD_INVALID;
}
ud_status ud_relational_order(const ud_relational *r, ud_sort_contract *out) {
    *out = r->output_order;
    return UD_OK;
}
uint64_t ud_relational_demand(const ud_relational *r) {
    if (r->hash_join)
        return (r->in[0].ended ? 0 : 1) | (r->in[1].ended ? 0 : 2);
    uint64_t mask = 0;
    if (r->options.kind == UD_OPERATOR_ROW_SAMPLING)
        return r->in[0].ended ? 0 : 1;
    if (r->input_sort[0] || r->input_sort[1]) {
        uint64_t mask = 0;
        for (uint32_t p = 0; p < r->ports; p++)
            if (!r->in[p].ended && (r->input_sort[p] || !r->in[p].block))
                mask |= UINT64_C(1) << p;
        return mask;
    }
    if (r->pending)
        return 0;
    for (uint32_t p = 0; p < r->ports; p++)
        if (!r->in[p].ended && !r->in[p].block)
            mask |= UINT64_C(1) << p;
    return mask;
}
ud_status ud_relational_push(ud_relational *r, uint32_t p, const ud_batch *b) {
    if (p >= r->ports || r->in[p].ended || !r->options.buffers)
        return UD_INVALID;
    if (r->hash_join)
        return ud_hash_join_push(r, p, b);
    if (r->input_sort[p])
        return ud_operator_push(r->input_sort[p], 0, b);
    if (r->in[p].block)
        return UD_INVALID;
    UD_TRY(ud_rel_control(r));
    if (r->options.kind == UD_OPERATOR_ROW_SAMPLING)
        return ud_rel_sample_push(r, b);
    return ud_buffer_store(r->options.buffers, b, &r->in[p].block);
}
ud_status ud_relational_end(ud_relational *r, uint32_t p) {
    if (r->hash_join)
        return ud_hash_join_end(r, p);
    if (p >= r->ports || r->in[p].ended)
        return UD_INVALID;
    r->in[p].ended = 1;
    if (r->input_sort[p])
        return ud_operator_finish_input(r->input_sort[p], 0);
    return r->rank_sort ? ud_operator_finish_input(r->rank_sort, 0) : UD_OK;
}
ud_status ud_relational_pull(ud_relational *r, ud_batch **out, uint32_t *port) {
    *out = NULL;
    uint32_t p = 0;
    ud_status s = ud_rel_control(r);
    if (!s) {
        switch (r->options.kind) {
        case UD_OPERATOR_ASOF_JOIN:
        case UD_OPERATOR_TEMPORAL_JOIN:
            s = ud_temporal_pull(r, out, &p);
            break;
        case UD_OPERATOR_DIFF_COMPARE:
            s = ud_diff_pull(r, out, &p);
            break;
        case UD_OPERATOR_JOIN:
            s = r->hash_join ? ud_hash_join_pull(r, out) : ud_rel_merge_pull(r, out);
            break;
        case UD_OPERATOR_MERGE:
        case UD_OPERATOR_MERGE_JOIN:
            s = ud_rel_merge_pull(r, out);
            break;
        case UD_OPERATOR_PIVOT:
            s = ud_rel_pivot_pull(r, out, &p);
            break;
        case UD_OPERATOR_UNPIVOT:
            s = ud_rel_unpivot_pull(r, out);
            break;
        case UD_OPERATOR_ROW_SAMPLING:
        case UD_OPERATOR_PERCENTAGE_SAMPLING:
            s = ud_rel_sample_pull(r, out, &p);
            break;
        case UD_OPERATOR_SCD:
            s = ud_rel_scd_pull(r, out, &p);
            break;
        default:
            s = UD_INVALID;
        }
    }
    if (!s)
        r->emitted[p] += (*out)->rows;
    if (port)
        *port = p;
    return s;
}
ud_status ud_relational_diagnostics(const ud_relational *r, char **out, uint32_t *n) {
    char b[1024];
    int count =
        snprintf(b, sizeof(b),
                 "{\"processed\":%llu,\"rightProcessed\":%llu,\"groups\":%llu,\"duplicateCells\":%"
                 "llu,\"rejected\":%llu,\"ignored\":%llu,\"seed\":%llu,\"sampled\":%llu,"
                 "\"complement\":%llu,\"mode\":\"%s\",\"recoverability\":\"stable-row-id and "
                 "source snapshot replay\",\"outputs\":[%llu,%llu,%llu,%llu,%llu,%llu]}",
                 (unsigned long long)r->processed[0], (unsigned long long)r->processed[1],
                 (unsigned long long)r->groups, (unsigned long long)r->duplicates,
                 (unsigned long long)r->rejected, (unsigned long long)r->ignored,
                 (unsigned long long)r->seed, (unsigned long long)r->sampled,
                 (unsigned long long)r->emitted[1],
                 r->options.kind == UD_OPERATOR_ROW_SAMPLING          ? "exact-global-priority"
                 : r->options.kind == UD_OPERATOR_PERCENTAGE_SAMPLING ? "bernoulli"
                                                                      : "relational",
                 (unsigned long long)r->emitted[0], (unsigned long long)r->emitted[1],
                 (unsigned long long)r->emitted[2], (unsigned long long)r->emitted[3],
                 (unsigned long long)r->emitted[4], (unsigned long long)r->emitted[5]);
    if (count < 0 || (size_t)count >= sizeof(b))
        return UD_OVERFLOW;
    *out = ud_strdup_n(r->ctx, b, (size_t)count);
    if (!*out)
        return UD_RESOURCE;
    *n = (uint32_t)count;
    return UD_OK;
}
void ud_relational_destroy(ud_relational *r) {
    if (!r)
        return;
    ud_hash_join_destroy(r);
    ud_batch_destroy(r->pending_error);
    for (uint32_t p = 0; p < 2; p++) {
        ud_buffer_view_release(r->in[p].view);
        ud_buffer_release(r->in[p].block);
        for (uint32_t k = 0; k < 16; k++)
            ud_clear_value(r->in[p].last + k);
        ud_batch_destroy(r->inputs[p]);
        ud_operator_destroy(r->input_sort[p]);
    }
    ud_rel_clear_group(r);
    for (uint32_t k = 0; k < 16; k++)
        ud_clear_value(r->group_key + k);
    for (uint32_t i = 0; i < r->cell_count; i++) {
        if (r->pivot_values)
            ud_clear_value(r->pivot_values + i);
        if (r->cells)
            ud_clear_value(r->cells + i);
        if (r->acc)
            ud_accumulator_destroy(r->acc[i]);
    }
    ud_free(r->pivot_values);
    ud_free(r->cells);
    ud_free(r->seen);
    ud_free(r->acc);
    ud_free(r->unpivot_columns);
    ud_free(r->retain);
    ud_free(r->mapping);
    ud_free(r->attributes);
    ud_free(r->attribute_kind);
    ud_plan_destroy(r->pivot_plan);
    ud_operator_destroy(r->rank_sort);
    ud_batch_destroy(r->rank_schema);
    ud_batch_destroy(r->pending);
    ud_batch_destroy(r->current_row);
    ud_cache_reader_destroy(r->reader);
    ud_cache_release(r->reference);
    for (uint32_t i = 0; i < 6; i++)
        ud_batch_destroy(r->schema[i]);
    ud_json_destroy(r->config);
    ud_free(r);
}

ud_status ud_compute_field_index(const ud_batch *schema, const ud_json *field, uint32_t *out) {
    return ud_rel_selector(schema, field, out);
}

uint32_t ud_relational_algorithm(const ud_relational *r) {
    return r->algorithm ? r->algorithm : r->rank_sort ? UD_PHYSICAL_SORT : UD_PHYSICAL_STREAM;
}

uint32_t ud_relational_output_count(const ud_relational *r) {
    return r->outputs;
}
