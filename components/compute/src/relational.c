#include "relational.h"
#include "../internal.h"
#include "ud/cache.h"
#include <math.h>
/* These algorithms use the same operator controller and Buffer pages as Sort,
 * Aggregate and Cache. Only an equal-key join group may accumulate input. */
typedef struct page {
    ud_buffer_block *block;
    struct page *next;
} page;
typedef struct stream {
    ud_buffer_block *block;
    ud_buffer_view *view;
    const ud_batch *batch;
    uint32_t row;
    int ended, have_last;
    ud_value last[16];
} stream;
typedef struct scan {
    page *at;
    ud_buffer_view *view;
    const ud_batch *batch;
    uint32_t row;
} scan;
struct ud_relational {
    ud_context *ctx;
    ud_operator_options options;
    ud_json *config;
    ud_batch *inputs[2], *schema[6];
    uint32_t ports, outputs;
    stream in[2];
    uint32_t keys[2][16], key_count;
    ud_sort_contract order[2], output_order;
    uint64_t processed[2], emitted[6], rejected, ignored, groups, duplicates, sampled;
    int join_type, phase, group_closed[2], active, duplicate, policy, nulls;
    page *group[2], *tail[2];
    ud_batch *chunk[2];
    scan scans[2];
    ud_value group_key[16];
    uint32_t pivot_key, value_column, cell_count, *unpivot_columns, *retain, retain_count,
        unpivot_at;
    ud_value *pivot_values, *cells;
    uint8_t *seen;
    ud_plan *pivot_plan;
    ud_accumulator **acc;
    uint64_t seed, count, total, position;
    double percentage;
    ud_operator *rank_sort;
    ud_operator *input_sort[2];
    int sort_done[2];
    ud_batch *rank_schema, *pending;
    uint32_t pending_row;
    ud_cache *reference;
    ud_cache_reader *reader;
    uint32_t *mapping, *attributes;
    int32_t *attribute_kind;
    uint32_t attribute_count;
    uint32_t current, valid_from, valid_to, surrogate, version, effective;
    ud_batch *current_row;
    ud_id current_business;
    int scd_ready;
};
static const ud_json *member(const ud_json *j, const char *key) {
    return ud_json_find(j, key, (uint32_t)strlen(key));
}
static int named(const ud_json *j, const char *text) {
    const char *p = NULL;
    uint32_t n = 0;
    return j && !ud_json_text(j, &p, &n) && n == strlen(text) && !memcmp(p, text, n);
}
static ud_status number(const ud_json *j, uint64_t fallback, uint64_t *out) {
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
static ud_status text(const ud_json *j, ud_string *out) {
    if (ud_json_kind(j) != UD_JSON_STRING)
        return UD_SCHEMA;
    return ud_json_text(j, &out->data, &out->length);
}
static ud_status clone(ud_context *ctx, const ud_batch *b, uint32_t capacity, ud_batch **out) {
    return ud_batch_create(ctx, b->schema->fields, b->columns, capacity, out);
}
static ud_status control(ud_relational *r) {
    if (ud_is_cancelled(r->ctx))
        return UD_CANCELLED;
    return r->options.check ? r->options.check(r->options.user) : UD_OK;
}
static ud_status selector(const ud_batch *b, const ud_json *j, uint32_t *out) {
    if (ud_json_kind(j) == UD_JSON_NUMBER) {
        uint64_t n;
        UD_TRY(number(j, 0, &n));
        if (n >= b->columns)
            return UD_SCHEMA;
        *out = (uint32_t)n;
        return UD_OK;
    }
    ud_string name;
    UD_TRY(text(j, &name));
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
static ud_status selectors(ud_relational *r, const ud_batch *b, const ud_json *j, uint32_t **out,
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
        UD_TRY(selector(b, ud_json_at(j, i), *out + i));
        for (uint32_t k = 0; k < i; k++)
            if ((*out)[k] == (*out)[i])
                return UD_SCHEMA;
    }
    return UD_OK;
}
static ud_status keys(ud_relational *r, uint32_t port, const ud_json *j) {
    uint32_t *k = NULL, n = 0;
    ud_status s = selectors(r, r->inputs[port], j, &k, &n);
    if (!s && (!n || n > 16 || (port && n != r->key_count)))
        s = UD_SCHEMA;
    if (!s) {
        memcpy(r->keys[port], k, n * sizeof(*k));
        r->key_count = n;
    }
    ud_free(k);
    return s;
}
static ud_status require_order(ud_relational *r, uint32_t port, int stable) {
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
static ud_status peek(ud_relational *r, uint32_t p, const ud_batch **b, uint32_t *row) {
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
        return peek(r, p, b, row);
    }
    *b = s->batch;
    *row = s->row;
    return UD_OK;
}
static ud_status take(ud_relational *r, uint32_t p) {
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
static int compare_rows(ud_relational *r, const ud_batch *a, uint32_t ar, const ud_batch *b,
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
static int same_group(ud_relational *r, uint32_t p, const ud_batch *b, uint32_t row) {
    for (uint32_t k = 0; k < r->key_count; k++) {
        ud_value v;
        ud_batch_get(b, row, r->keys[p][k], &v);
        if (compare_value(r, k, r->group_key + k, &v))
            return 0;
    }
    return 1;
}
static ud_status set_group(ud_relational *r, const ud_batch *b, uint32_t row) {
    for (uint32_t k = 0; k < r->key_count; k++) {
        ud_value v;
        UD_TRY(ud_batch_get(b, row, r->keys[0][k], &v));
        ud_clear_value(r->group_key + k);
        UD_TRY(ud_copy_value(r->ctx, &v, r->group_key + k, 0));
    }
    return UD_OK;
}
static ud_status scan_next(scan *s) {
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
static ud_status flush_group(ud_relational *r, uint32_t p) {
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
static ud_status clear_group(ud_relational *r) {
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
static ud_status join_row(ud_relational *r, ud_batch *out, const ud_batch *a, uint32_t ar,
                          const ud_batch *b, uint32_t br) {
    ud_value *v = ud_alloc(r->ctx, (size_t)out->columns * sizeof(*v));
    if (!v)
        return UD_RESOURCE;
    ud_status s = UD_OK;
    for (uint32_t i = 0; !s && i < r->inputs[0]->columns; i++)
        if (a)
            s = ud_batch_get(a, ar, i, v + i);
        else
            v[i] = (ud_value){.type = r->inputs[0]->column[i].field.type, .is_null = 1};
    for (uint32_t i = 0; !s && i < r->inputs[1]->columns; i++)
        if (b)
            s = ud_batch_get(b, br, i, v + r->inputs[0]->columns + i);
        else
            v[r->inputs[0]->columns + i] =
                (ud_value){.type = r->inputs[1]->column[i].field.type, .is_null = 1};
    if (!s)
        s = ud_batch_append(out, v, out->columns);
    if (!s) {
        if (a && b)
            s = ud_batch_join_row_info(out, out->rows - 1, a, ar, b, br);
        else
            s = ud_batch_copy_row_info(out, out->rows - 1, a ? a : b, a ? ar : br);
    }
    ud_free(v);
    return s;
}
static ud_status merge_pull(ud_relational *r, ud_batch **out) {
    UD_TRY(clone(r->ctx, r->schema[0], r->options.max_batch_rows, out));
    ud_status status = UD_OK;
    while (!status && (*out)->rows < r->options.max_batch_rows) {
        UD_TRY(control(r));
        if (r->phase == 1) {
            for (uint32_t p = 0; p < 2 && !status; p++)
                while (!r->group_closed[p]) {
                    const ud_batch *b = NULL;
                    uint32_t row = 0;
                    ud_status s = peek(r, p, &b, &row);
                    if (s == UD_BUSY) {
                        status = s;
                        break;
                    }
                    if (s == UD_END || (!s && !same_group(r, p, b, row))) {
                        r->group_closed[p] = 1;
                        status = flush_group(r, p);
                        break;
                    }
                    if (s) {
                        status = s;
                        break;
                    }
                    if (p == 1 && r->join_type >= 4) {
                        status = take(r, p);
                        continue;
                    }
                    if (!r->chunk[p])
                        status =
                            clone(r->ctx, r->inputs[p], r->options.max_batch_rows, r->chunk + p);
                    if (!status)
                        status = ud_batch_append_from(r->chunk[p], b, row);
                    if (!status)
                        status = take(r, p);
                    if (!status && r->chunk[p]->rows == r->options.max_batch_rows)
                        status = flush_group(r, p);
                }
            if (status)
                break;
            r->phase = 2;
            r->scans[0].at = r->group[0];
            r->scans[1].at = r->group[1];
            r->groups++;
        }
        if (r->phase == 2 && r->join_type == 5) {
            status = clear_group(r);
            continue;
        }
        if (r->phase == 2 && r->join_type == 4) {
            ud_status next = scan_next(r->scans);
            if (next == UD_END) {
                status = clear_group(r);
                continue;
            }
            if (next) {
                status = next;
                break;
            }
            status = ud_batch_append_from(*out, r->scans[0].batch, r->scans[0].row++);
            continue;
        }
        if (r->phase == 2) {
            ud_status a = scan_next(r->scans), b = scan_next(r->scans + 1);
            if (a == UD_END) {
                status = clear_group(r);
                continue;
            }
            if (a) {
                status = a;
                break;
            }
            if (b == UD_END) {
                r->scans[0].row++;
                ud_buffer_view_release(r->scans[1].view);
                memset(r->scans + 1, 0, sizeof(scan));
                r->scans[1].at = r->group[1];
                continue;
            }
            if (b) {
                status = b;
                break;
            }
            status = join_row(r, *out, r->scans[0].batch, r->scans[0].row, r->scans[1].batch,
                              r->scans[1].row++);
            continue;
        }
        const ud_batch *a = NULL, *b = NULL;
        uint32_t ar = 0, br = 0;
        ud_status sa = peek(r, 0, &a, &ar), sb = peek(r, 1, &b, &br);
        if (sa == UD_BUSY || sb == UD_BUSY) {
            status = UD_BUSY;
            break;
        }
        if ((sa && sa != UD_END) || (sb && sb != UD_END)) {
            status = sa && sa != UD_END ? sa : sb;
            break;
        }
        if (sa == UD_END && sb == UD_END) {
            status = UD_END;
            break;
        }
        int null = 0, c = sa == UD_END   ? 1
                          : sb == UD_END ? -1
                                         : compare_rows(r, a, ar, b, br, &null);
        if (r->options.kind == UD_OPERATOR_MERGE) {
            uint32_t p = c <= 0 ? 0 : 1;
            status = ud_batch_append_from(*out, p ? b : a, p ? br : ar);
            if (!status)
                status = take(r, p);
        } else if (!c && !null) {
            status = set_group(r, a, ar);
            if (!status)
                r->phase = 1;
        } else {
            uint32_t p = c <= 0 ? 0 : 1;
            if (!p && r->join_type == 5)
                status = ud_batch_append_from(*out, a, ar);
            else if ((!p && (r->join_type == 1 || r->join_type == 2)) ||
                     (p && (r->join_type == 2 || r->join_type == 3)))
                status = join_row(r, *out, p ? NULL : a, ar, p ? b : NULL, br);
            if (!status)
                status = take(r, p);
        }
    }
    if (status == UD_BUSY || status == UD_END) {
        if ((*out)->rows)
            status = UD_OK;
    }
    if (status || !(*out)->rows) {
        ud_batch_destroy(*out);
        *out = NULL;
        return status ? status : UD_BUSY;
    }
    return ud_batch_seal(*out);
}
static ud_status derive(ud_relational *r, const ud_field *spec, const ud_field *deps, uint32_t n,
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
static ud_status pivot_bind(ud_relational *r) {
    static const char *allow[] = {"setKeys",   "pivotKey",    "valueColumn", "values",
                                  "duplicate", "errorPolicy", "includeInput"};
    UD_TRY(ud_json_keys(r->config, allow, 7));
    UD_TRY(keys(r, 0, member(r->config, "setKeys")));
    UD_TRY(require_order(r, 0, 0));
    UD_TRY(selector(r->inputs[0], member(r->config, "pivotKey"), &r->pivot_key));
    UD_TRY(selector(r->inputs[0], member(r->config, "valueColumn"), &r->value_column));
    const ud_json *vals = member(r->config, "values");
    r->cell_count = ud_json_size(vals);
    if (ud_json_kind(vals) != UD_JSON_ARRAY || !r->cell_count || r->cell_count > 4096)
        return UD_SCHEMA;
    const ud_json *dup = member(r->config, "duplicate");
    r->duplicate = !dup || named(dup, "reject") ? 0
                   : named(dup, "sum")          ? 1
                   : named(dup, "count")        ? 2
                   : named(dup, "min")          ? 3
                   : named(dup, "max")          ? 4
                   : named(dup, "first")        ? 5
                   : named(dup, "last")         ? 6
                                                : -1;
    if (r->duplicate < 0)
        return UD_SCHEMA;
    if (r->duplicate >= 5)
        UD_TRY(require_order(r, 0, 1));
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
            s = text(member(v, "name"), &name);
        if (!s)
            s = ud_json_value_decode(r->ctx, &r->inputs[0]->column[r->pivot_key].field,
                                     member(v, "value"), UD_ROUND_EXACT, r->pivot_values + i);
        for (uint32_t k = 0; !s && k < i; k++)
            if (!ud_value_compare(r->pivot_values + i, r->pivot_values + k))
                s = UD_SCHEMA;
        if (!s)
            s = derive(r, &spec, deps, 2, name, f + r->key_count + i);
    }
    if (!s)
        s = ud_batch_create(r->ctx, f, r->key_count + r->cell_count, 0, &r->schema[0]);
    for (uint32_t i = 0; i < r->cell_count; i++)
        ud_field_clear(f + r->key_count + i);
    ud_free(f);
    if (!s && r->duplicate >= 1 && r->duplicate <= 4) {
        const char *fn[] = {"", "sum", "count", "min", "max"};
        ud_bytes j = {.ctx = r->ctx};
        s = ud_bytes_put(&j, "{\"version\":8,\"aggregates\":[{\"function\":",
                         sizeof("{\"version\":8,\"aggregates\":[{\"function\":") - 1);
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
        s = clone(r->ctx, r->inputs[0], 0, &r->schema[1]);
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
        ud_status identity = clone(r->ctx, r->schema[0], 1, &key_row);
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
static ud_status reject_row(ud_relational *r, const ud_batch *b, uint32_t row, ud_batch **out,
                            uint32_t *port) {
    r->rejected++;
    if (r->policy == UD_ERROR_FAIL)
        return UD_SCHEMA;
    if (r->policy == UD_ERROR_IGNORE) {
        r->ignored++;
        return take(r, 0);
    }
    UD_TRY(clone(r->ctx, b, 1, out));
    UD_TRY(ud_batch_append_from(*out, b, row));
    UD_TRY(ud_batch_seal(*out));
    *port = 1;
    return take(r, 0);
}
static ud_status pivot_pull(ud_relational *r, ud_batch **out, uint32_t *port) {
    ud_batch *result = NULL;
    UD_TRY(clone(r->ctx, r->schema[0], r->options.max_batch_rows, &result));
    ud_status status = UD_OK;
    while (!status && result->rows < r->options.max_batch_rows) {
        status = control(r);
        if (status)
            break;
        const ud_batch *b = NULL;
        uint32_t row = 0;
        ud_status next = peek(r, 0, &b, &row);
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
        if (r->active && !same_group(r, 0, b, row)) {
            status = pivot_result(r, result);
            continue;
        }
        if (!r->active) {
            status = set_group(r, b, row);
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
            status = reject_row(r, b, row, out, port);
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
                status = take(r, 0);
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
static ud_status unpivot_bind(ud_relational *r) {
    static const char *allow[] = {"columns", "retain", "keyName", "valueName", "nulls"};
    UD_TRY(ud_json_keys(r->config, allow, 5));
    const ud_json *cols = member(r->config, "columns");
    r->cell_count = ud_json_size(cols);
    if (ud_json_kind(cols) != UD_JSON_ARRAY || !r->cell_count ||
        r->cell_count > r->inputs[0]->columns)
        return UD_SCHEMA;
    r->unpivot_columns = ud_alloc(r->ctx, r->cell_count * sizeof(uint32_t));
    r->pivot_values = ud_alloc(r->ctx, r->cell_count * sizeof(ud_value));
    if (!r->unpivot_columns || !r->pivot_values)
        return UD_RESOURCE;
    UD_TRY(selectors(r, r->inputs[0], member(r->config, "retain"), &r->retain, &r->retain_count));
    ud_field *deps = ud_alloc(r->ctx, r->cell_count * sizeof(ud_field));
    if (!deps)
        return UD_RESOURCE;
    ud_status s = UD_OK;
    for (uint32_t i = 0; !s && i < r->cell_count; i++) {
        const ud_json *col = ud_json_at(cols, i);
        static const char *a[] = {"column", "key"};
        s = ud_json_keys(col, a, 2);
        if (!s)
            s = selector(r->inputs[0], member(col, "column"), r->unpivot_columns + i);
        ud_string key;
        if (!s)
            s = text(member(col, "key"), &key);
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
    if (!s && member(r->config, "keyName"))
        s = text(member(r->config, "keyName"), &key);
    if (!s && member(r->config, "valueName"))
        s = text(member(r->config, "valueName"), &value);
    ud_field spec = {.type = UD_UTF8};
    if (!s)
        s = derive(r, &spec, deps, r->cell_count, key, f + r->retain_count);
    if (!s) {
        spec = deps[0];
        spec.nullable = 1;
        s = derive(r, &spec, deps, r->cell_count, value, f + r->retain_count + 1);
    }
    if (!s)
        s = ud_batch_create(r->ctx, f, r->retain_count + 2, 0, &r->schema[0]);
    if (f) {
        ud_field_clear(f + r->retain_count);
        ud_field_clear(f + r->retain_count + 1);
    }
    ud_free(f);
    ud_free(deps);
    const ud_json *n = member(r->config, "nulls");
    if (n && !named(n, "skip") && !named(n, "include"))
        return UD_SCHEMA;
    r->nulls = named(n, "include");
    return s;
}
static ud_status unpivot_pull(ud_relational *r, ud_batch **out) {
    UD_TRY(clone(r->ctx, r->schema[0], r->options.max_batch_rows, out));
    ud_value *v = ud_alloc(r->ctx, (*out)->columns * sizeof(*v));
    if (!v) {
        ud_batch_destroy(*out);
        *out = NULL;
        return UD_RESOURCE;
    }
    ud_status s = UD_OK;
    while (!s && (*out)->rows < r->options.max_batch_rows) {
        s = control(r);
        if (s)
            break;
        const ud_batch *b;
        uint32_t row;
        s = peek(r, 0, &b, &row);
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
            s = take(r, 0);
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
static ud_status sampling_bind(ud_relational *r) {
    static const char *allow[] = {"seed", "count", "percentage", "whenShort"};
    UD_TRY(ud_json_keys(r->config, allow, 4));
    UD_TRY(number(member(r->config, "seed"), 0, &r->seed));
    r->outputs = 2;
    UD_TRY(clone(r->ctx, r->inputs[0], 0, &r->schema[0]));
    UD_TRY(clone(r->ctx, r->inputs[0], 0, &r->schema[1]));
    if (r->options.kind == UD_OPERATOR_PERCENTAGE_SAMPLING) {
        const char *p;
        uint32_t n;
        UD_TRY(ud_json_text(member(r->config, "percentage"), &p, &n));
        ud_value v;
        UD_TRY(ud_value_parse(r->ctx, UD_FLOAT64, p, n, &v));
        if (!isfinite(v.real) || v.real < 0 || v.real > 100)
            return UD_SCHEMA;
        r->percentage = v.real;
        return UD_OK;
    }
    UD_TRY(number(member(r->config, "count"), UINT64_MAX, &r->count));
    if (r->count == UINT64_MAX)
        return UD_SCHEMA;
    const ud_json *shorter = member(r->config, "whenShort");
    if (shorter && !named(shorter, "all") && !named(shorter, "fail"))
        return UD_SCHEMA;
    r->nulls = named(shorter, "fail");
    ud_field *f = ud_alloc(r->ctx, (r->inputs[0]->columns + 2) * sizeof(*f));
    if (!f)
        return UD_RESOURCE;
    f[0] = (ud_field){.name = "_samplePriority", .name_length = 15, .type = UD_UTF8};
    f[1] = (ud_field){.name = "_sampleOrdinal", .name_length = 14, .type = UD_INT64};
    ud_status s = ud_id_make(r->ctx, (ud_string){"native.sampling", 15}, (ud_string){"priority", 8},
                             &f[0].identity);
    if (!s)
        s = ud_id_make(r->ctx, (ud_string){"native.sampling", 15}, (ud_string){"ordinal", 7},
                       &f[1].identity);
    for (uint32_t i = 0; i < r->inputs[0]->columns; i++)
        f[i + 2] = r->inputs[0]->column[i].field;
    if (!s)
        s = ud_batch_create(r->ctx, f, r->inputs[0]->columns + 2, 0, &r->rank_schema);
    ud_free(f);
    uint32_t k[] = {0, 1};
    ud_operator_options o = r->options;
    o.kind = UD_OPERATOR_SORT;
    o.key_count = 2;
    o.keys = k;
    o.partition_count = 1;
    o.configuration = (ud_string){0};
    o.input_order = NULL;
    o.reference = NULL;
    const ud_batch *in[] = {r->rank_schema};
    if (!s)
        s = ud_operator_create(r->ctx, &o, in, &r->rank_sort);
    return s;
}
static ud_status priority(ud_relational *r, const ud_batch *b, uint32_t row, char hex[65],
                          uint64_t *value) {
    ud_row_info info;
    UD_TRY(ud_batch_row_info(b, row, &info));
    if (!info.id.high && !info.id.low)
        return ud_fail(r->ctx, UD_SCHEMA, "sampling requires stable source row identities");
    ud_bytes bytes = {.ctx = r->ctx};
    UD_TRY(ud_put_u64(&bytes, r->seed));
    UD_TRY(ud_put_u64(&bytes, info.id.high));
    UD_TRY(ud_put_u64(&bytes, info.id.low));
    uint8_t h[32];
    ud_hash(bytes.data, bytes.size, h);
    ud_bytes_clear(&bytes);
    static const char digits[] = "0123456789abcdef";
    *value = 0;
    for (uint32_t i = 0; i < 32; i++) {
        hex[i * 2] = digits[h[i] >> 4];
        hex[i * 2 + 1] = digits[h[i] & 15];
        if (i < 8)
            *value = (*value << 8) | h[i];
    }
    hex[64] = 0;
    return UD_OK;
}
static ud_status sample_push(ud_relational *r, const ud_batch *b) {
    ud_batch *rank = NULL;
    UD_TRY(clone(r->ctx, r->rank_schema, b->rows, &rank));
    ud_value *v = ud_alloc(r->ctx, rank->columns * sizeof(*v));
    ud_status s = v ? UD_OK : UD_RESOURCE;
    for (uint32_t row = 0; !s && row < b->rows; row++) {
        s = control(r);
        char hex[65];
        uint64_t h;
        if (!s)
            s = priority(r, b, row, hex, &h);
        v[0] = (ud_value){.type = UD_UTF8, .text = hex, .text_length = 64};
        if (r->total > INT64_MAX)
            s = UD_OVERFLOW;
        v[1] = (ud_value){.type = UD_INT64, .integer = (int64_t)r->total++};
        for (uint32_t i = 0; !s && i < b->columns; i++)
            s = ud_batch_get(b, row, i, v + i + 2);
        if (!s)
            s = ud_batch_append(rank, v, rank->columns);
        if (!s)
            s = ud_batch_copy_row_info(rank, row, b, row);
    }
    if (!s)
        s = ud_batch_seal(rank);
    if (!s)
        s = ud_operator_push(r->rank_sort, 0, rank);
    ud_free(v);
    ud_batch_destroy(rank);
    if (!s)
        r->processed[0] += b->rows;
    return s;
}
static ud_status sample_pull(ud_relational *r, ud_batch **out, uint32_t *port) {
    if (r->options.kind == UD_OPERATOR_PERCENTAGE_SAMPLING) {
        const ud_batch *b;
        uint32_t row;
        UD_TRY(peek(r, 0, &b, &row));
        char hex[65];
        uint64_t value;
        UD_TRY(priority(r, b, row, hex, &value));
        long double threshold = (long double)r->percentage / 100.0L * 18446744073709551616.0L;
        *port = (long double)value < threshold ? 0 : 1;
        UD_TRY(clone(r->ctx, r->schema[*port], r->options.max_batch_rows, out));
        while ((*out)->rows < r->options.max_batch_rows) {
            ud_status s = peek(r, 0, &b, &row);
            if (s == UD_BUSY || s == UD_END)
                break;
            UD_TRY(s);
            UD_TRY(priority(r, b, row, hex, &value));
            if (((long double)value < threshold ? 0u : 1u) != *port)
                break;
            UD_TRY(ud_batch_append_from(*out, b, row));
            UD_TRY(take(r, 0));
        }
        if (!*port)
            r->sampled += (*out)->rows;
        return ud_batch_seal(*out);
    }
    if (!r->in[0].ended)
        return UD_BUSY;
    if (r->nulls && r->total < r->count)
        return UD_SCHEMA;
    if (!r->pending) {
        UD_TRY(ud_operator_pull(r->rank_sort, &r->pending, NULL));
        r->pending_row = 0;
    }
    *port = r->position < r->count ? 0 : 1;
    UD_TRY(clone(r->ctx, r->schema[*port], r->options.max_batch_rows, out));
    ud_value *v = ud_alloc(r->ctx, (*out)->columns * sizeof(*v));
    if (!v)
        return UD_RESOURCE;
    ud_status s = UD_OK;
    while (!s && r->pending_row < r->pending->rows && (*out)->rows < r->options.max_batch_rows &&
           (r->position < r->count ? 0u : 1u) == *port) {
        for (uint32_t i = 0; !s && i < (*out)->columns; i++)
            s = ud_batch_get(r->pending, r->pending_row, i + 2, v + i);
        if (!s)
            s = ud_batch_append(*out, v, (*out)->columns);
        if (!s)
            s = ud_batch_copy_row_info(*out, (*out)->rows - 1, r->pending, r->pending_row);
        if (!s) {
            r->pending_row++;
            r->position++;
        }
    }
    ud_free(v);
    if (r->pending_row == r->pending->rows) {
        ud_batch_destroy(r->pending);
        r->pending = NULL;
    }
    if (!*port)
        r->sampled += (*out)->rows;
    return s ? s : ud_batch_seal(*out);
}
static ud_status scd_bind(ud_relational *r) {
    static const char *allow[] = {"cache",       "businessKeys",  "dimensionKeys",   "mapping",
                                  "current",     "validFrom",     "validTo",         "surrogate",
                                  "version",     "effectiveTime", "surrogateSource", "errorPolicy",
                                  "includeInput"};
    UD_TRY(ud_json_keys(r->config, allow, 13));
    if (!r->inputs[1])
        return UD_SCHEMA;
    UD_TRY(keys(r, 0, member(r->config, "businessKeys")));
    UD_TRY(keys(r, 1, member(r->config, "dimensionKeys")));
    UD_TRY(require_order(r, 0, 1));
    UD_TRY(selector(r->inputs[0], member(r->config, "effectiveTime"), &r->effective));
    if (r->inputs[0]->column[r->effective].field.type != UD_TIMESTAMP)
        return UD_SCHEMA;
    if (r->order[0].count <= r->key_count ||
        ud_id_compare(r->order[0].keys[r->key_count].field,
                      r->inputs[0]->column[r->effective].field.identity) ||
        r->order[0].keys[r->key_count].descending)
        return UD_SCHEMA;
    for (uint32_t k = 0; k < r->key_count; k++)
        if (r->order[0].keys[k].descending ||
            !ud_field_matches(&r->inputs[0]->column[r->keys[0][k]].field,
                              &r->inputs[1]->column[r->keys[1][k]].field, UD_FIELD_VALUE_TYPE))
            return UD_SCHEMA;
    UD_TRY(selector(r->inputs[1], member(r->config, "current"), &r->current));
    UD_TRY(selector(r->inputs[1], member(r->config, "validFrom"), &r->valid_from));
    UD_TRY(selector(r->inputs[1], member(r->config, "validTo"), &r->valid_to));
    UD_TRY(selector(r->inputs[1], member(r->config, "surrogate"), &r->surrogate));
    r->version = UINT32_MAX;
    if (member(r->config, "version"))
        UD_TRY(selector(r->inputs[1], member(r->config, "version"), &r->version));
    if (r->inputs[1]->column[r->current].field.type != UD_BOOL ||
        r->inputs[1]->column[r->valid_from].field.type != UD_TIMESTAMP ||
        r->inputs[1]->column[r->valid_to].field.type != UD_TIMESTAMP ||
        !r->inputs[1]->column[r->valid_to].field.nullable ||
        (r->version != UINT32_MAX && r->inputs[1]->column[r->version].field.type != UD_INT64))
        return UD_SCHEMA;
    r->value_column = UINT32_MAX;
    if (member(r->config, "surrogateSource"))
        UD_TRY(selector(r->inputs[0], member(r->config, "surrogateSource"), &r->value_column));
    if (r->value_column == UINT32_MAX && r->inputs[1]->column[r->surrogate].field.type != UD_UTF8)
        return UD_SCHEMA;
    if (r->value_column != UINT32_MAX &&
        !ud_field_matches(&r->inputs[0]->column[r->value_column].field,
                          &r->inputs[1]->column[r->surrogate].field, UD_FIELD_VALUE_TYPE))
        return UD_SCHEMA;
    const ud_json *mapping = member(r->config, "mapping");
    r->attribute_count = ud_json_size(mapping);
    if (ud_json_kind(mapping) != UD_JSON_ARRAY || !r->attribute_count ||
        r->attribute_count > r->inputs[1]->columns)
        return UD_SCHEMA;
    r->mapping = ud_alloc(r->ctx, r->attribute_count * sizeof(uint32_t));
    r->attributes = ud_alloc(r->ctx, r->attribute_count * sizeof(uint32_t));
    r->attribute_kind = ud_alloc(r->ctx, r->attribute_count * sizeof(int32_t));
    if (!r->mapping || !r->attributes || !r->attribute_kind)
        return UD_RESOURCE;
    uint8_t *covered = ud_alloc(r->ctx, r->inputs[1]->columns);
    if (!covered)
        return UD_RESOURCE;
    ud_status s = UD_OK;
    covered[r->current] = covered[r->valid_from] = covered[r->valid_to] = covered[r->surrogate] = 1;
    if (r->version != UINT32_MAX)
        covered[r->version] = 1;
    for (uint32_t i = 0; !s && i < r->attribute_count; i++) {
        const ud_json *m = ud_json_at(mapping, i);
        static const char *a[] = {"source", "target", "change"};
        s = ud_json_keys(m, a, 3);
        if (!s)
            s = selector(r->inputs[0], member(m, "source"), r->mapping + i);
        if (!s)
            s = selector(r->inputs[1], member(m, "target"), r->attributes + i);
        r->attribute_kind[i] = named(member(m, "change"), "key")     ? 0
                               : named(member(m, "change"), "type1") ? 1
                               : named(member(m, "change"), "type2") ? 2
                               : named(member(m, "change"), "fixed") ? 3
                                                                     : -1;
        if (!s && (r->attribute_kind[i] < 0 || covered[r->attributes[i]]))
            s = UD_SCHEMA;
        if (!s &&
            !ud_field_matches(&r->inputs[0]->column[r->mapping[i]].field,
                              &r->inputs[1]->column[r->attributes[i]].field, UD_FIELD_VALUE_TYPE))
            s = UD_SCHEMA;
        if (!s)
            covered[r->attributes[i]] = 1;
    }
    for (uint32_t i = 0; !s && i < r->inputs[1]->columns; i++)
        if (!covered[i])
            s = UD_SCHEMA;
    for (uint32_t k = 0; !s && k < r->key_count; k++) {
        int found = 0;
        for (uint32_t i = 0; i < r->attribute_count; i++)
            if (r->mapping[i] == r->keys[0][k] && r->attributes[i] == r->keys[1][k] &&
                r->attribute_kind[i] == 0)
                found++;
        if (found != 1)
            s = UD_SCHEMA;
    }
    ud_free(covered);
    if (s)
        return s;
    uint32_t n = r->inputs[1]->columns;
    ud_field *f = ud_alloc(r->ctx, (n + 4) * sizeof(*f));
    if (!f)
        return UD_RESOURCE;
    for (uint32_t i = 0; i < n; i++)
        f[i] = r->inputs[1]->column[i].field;
    f[n] = (ud_field){.name = "_before",
                      .name_length = 7,
                      .type = UD_STRUCT,
                      .nullable = 1,
                      .children = r->inputs[1]->schema->fields,
                      .child_count = n};
    f[n + 1] = (ud_field){.name = "_eventId", .name_length = 8, .type = UD_UTF8};
    f[n + 2] = (ud_field){.name = "_effectiveTime",
                          .name_length = 14,
                          .type = UD_TIMESTAMP,
                          .timezone = "UTC",
                          .timezone_length = 3};
    f[n + 3] = (ud_field){.name = "_action", .name_length = 7, .type = UD_INT64};
    for (uint32_t i = n; !s && i < n + 4; i++)
        s = ud_id_make(r->ctx, (ud_string){"native.scd", 10},
                       (ud_string){f[i].name, f[i].name_length}, &f[i].identity);
    if (!s)
        s = ud_batch_create(r->ctx, f, n + 4, 0, &r->schema[0]);
    ud_free(f);
    r->outputs = 6;
    for (uint32_t i = 1; !s && i < 5; i++)
        s = clone(r->ctx, r->schema[0], 0, r->schema + i);
    if (!s)
        s = clone(r->ctx, r->inputs[0], 0, &r->schema[5]);
    if (!s && r->options.reference) {
        s = ud_cache_retain(r->options.reference);
        if (!s) {
            r->reference = r->options.reference;
            s = ud_cache_reader_create(r->reference, r->inputs[0], r->keys[0], r->key_count,
                                       &r->reader);
        }
    }
    return s;
}
static ud_status scd_envelope(ud_relational *r, const ud_value *values, const ud_value *before,
                              ud_id id, int64_t time, int64_t action, uint32_t port,
                              ud_batch **out) {
    uint32_t n = r->inputs[1]->columns;
    ud_value *v = ud_alloc(r->ctx, (n + 4) * sizeof(*v));
    if (!v)
        return UD_RESOURCE;
    memcpy(v, values, n * sizeof(*v));
    char event[33];
    ud_id_format(id, event);
    v[n] = (ud_value){
        .type = UD_STRUCT, .is_null = !before, .children = before, .child_count = before ? n : 0};
    v[n + 1] = (ud_value){.type = UD_UTF8, .text = event, .text_length = 32};
    v[n + 2] = (ud_value){.type = UD_TIMESTAMP, .integer = time};
    v[n + 3] = (ud_value){.type = UD_INT64, .integer = action};
    ud_status s = clone(r->ctx, r->schema[port], 1, out);
    if (!s)
        s = ud_batch_append(*out, v, n + 4);
    if (!s) {
        ud_row_info info = {.id = id};
        s = ud_batch_set_row_info(*out, 0, &info);
    }
    if (!s)
        s = ud_batch_seal(*out);
    ud_free(v);
    return s;
}
static ud_status scd_pull(ud_relational *r, ud_batch **out, uint32_t *port) {
    if (r->pending) {
        *out = r->pending;
        r->pending = NULL;
        *port = 4;
        return UD_OK;
    }
    const ud_batch *b;
    uint32_t row;
    UD_TRY(peek(r, 0, &b, &row));
    if (!r->reader)
        return UD_INVALID;
    UD_TRY(control(r));
    ud_value time;
    UD_TRY(ud_batch_get(b, row, r->effective, &time));
    if (time.is_null)
        return UD_SCHEMA;
    for (uint32_t k = 0; k < r->key_count; k++) {
        ud_value v;
        UD_TRY(ud_batch_get(b, row, r->keys[0][k], &v));
        if (v.is_null)
            return UD_SCHEMA;
    }
    if (!r->active || !same_group(r, 0, b, row)) {
        UD_TRY(set_group(r, b, row));
        r->active = 1;
        ud_batch_destroy(r->current_row);
        r->current_row = NULL;
        UD_TRY(ud_cache_probe(r->reader, b, row));
        const ud_batch *dim = NULL;
        uint32_t dr = 0;
        ud_status next = ud_cache_next(r->reader, &dim, &dr);
        if (!next) {
            UD_TRY(clone(r->ctx, r->inputs[1], 1, &r->current_row));
            UD_TRY(ud_batch_append_from(r->current_row, dim, dr));
            UD_TRY(ud_batch_seal(r->current_row));
            next = ud_cache_next(r->reader, &dim, &dr);
            if (next != UD_END)
                return next ? next : UD_SCHEMA;
        } else if (next != UD_END)
            return next;
        r->groups++;
    }
    uint32_t n = r->inputs[1]->columns;
    ud_value *old = ud_alloc(r->ctx, n * sizeof(*old)), *now = ud_alloc(r->ctx, n * sizeof(*now));
    if (!old || !now) {
        ud_free(old);
        ud_free(now);
        return UD_RESOURCE;
    }
    ud_status s = UD_OK;
    int type1 = 0, type2 = 0, fixed = 0;
    for (uint32_t i = 0; !s && i < n; i++) {
        if (r->current_row)
            s = ud_batch_get(r->current_row, 0, i, old + i);
        else
            old[i] = (ud_value){.type = r->inputs[1]->column[i].field.type, .is_null = 1};
        now[i] = old[i];
    }
    if (!s && r->current_row &&
        (old[r->current].is_null || !old[r->current].integer || !old[r->valid_to].is_null ||
         old[r->valid_from].is_null || time.integer < old[r->valid_from].integer))
        s = UD_SCHEMA;
    for (uint32_t i = 0; !s && i < r->attribute_count; i++) {
        ud_value v;
        s = ud_batch_get(b, row, r->mapping[i], &v);
        if (!s) {
            uint32_t t = r->attributes[i];
            int changed = ud_value_compare(old + t, &v) != 0;
            if (r->current_row && changed) {
                type1 |= r->attribute_kind[i] == 1;
                type2 |= r->attribute_kind[i] == 2;
                fixed |= r->attribute_kind[i] == 3;
            }
            now[t] = v;
        }
    }
    if (!s && fixed) {
        ud_free(old);
        ud_free(now);
        r->rejected++;
        if (r->policy == UD_ERROR_FAIL)
            return UD_SCHEMA;
        if (r->policy == UD_ERROR_IGNORE) {
            r->ignored++;
            UD_TRY(take(r, 0));
            return scd_pull(r, out, port);
        }
        UD_TRY(clone(r->ctx, b, 1, out));
        UD_TRY(ud_batch_append_from(*out, b, row));
        UD_TRY(ud_batch_seal(*out));
        *port = 5;
        return take(r, 0);
    }
    ud_row_info input_info;
    ud_id event = {0};
    char source[33], symbol[80];
    if (!s)
        s = ud_batch_row_info(b, row, &input_info);
    if (!s && (!input_info.id.high && !input_info.id.low))
        s = UD_SCHEMA;
    if (!s) {
        ud_id_format(input_info.id, source);
        snprintf(symbol, sizeof(symbol), "%lld", (long long)time.integer);
        s = ud_id_make(r->ctx, (ud_string){source, 32},
                       (ud_string){symbol, (uint32_t)strlen(symbol)}, &event);
    }
    if (!s && r->current_row && !type1 && !type2) {
        *port = 0;
        s = scd_envelope(r, old, NULL, event, time.integer, 0, 0, out);
    } else if (!s) {
        int fresh = !r->current_row;
        *port = fresh ? 1 : type2 ? 3 : 2;
        if (type2 && time.integer <= old[r->valid_from].integer)
            s = UD_SCHEMA;
        if (!s && (fresh || type2)) {
            now[r->current] = (ud_value){.type = UD_BOOL, .integer = 1};
            now[r->valid_from] = (ud_value){.type = UD_TIMESTAMP, .integer = time.integer};
            now[r->valid_to] = (ud_value){.type = UD_TIMESTAMP, .is_null = 1};
            if (r->version != UINT32_MAX) {
                if (!fresh && old[r->version].integer == INT64_MAX)
                    s = UD_OVERFLOW;
                now[r->version] = (ud_value){.type = UD_INT64,
                                             .integer = fresh ? 1 : old[r->version].integer + 1};
            }
            if (r->value_column != UINT32_MAX)
                s = ud_batch_get(b, row, r->value_column, now + r->surrogate);
            else {
                ud_id_format(event, source);
                now[r->surrogate] = (ud_value){.type = UD_UTF8, .text = source, .text_length = 32};
            }
            if (!s && !fresh && !ud_value_compare(now + r->surrogate, old + r->surrogate))
                s = UD_SCHEMA;
        }
        if (!s && type2) {
            ud_value *expired = ud_alloc(r->ctx, n * sizeof(*expired));
            if (!expired)
                s = UD_RESOURCE;
            else {
                memcpy(expired, old, n * sizeof(*expired));
                expired[r->current] = (ud_value){.type = UD_BOOL, .integer = 0};
                expired[r->valid_to] = (ud_value){.type = UD_TIMESTAMP, .integer = time.integer};
                s = scd_envelope(r, expired, old, event, time.integer, 3, 3, out);
                ud_free(expired);
            }
            if (!s)
                s = scd_envelope(r, now, NULL, event, time.integer, 4, 4, &r->pending);
        } else if (!s)
            s = scd_envelope(r, now, fresh ? NULL : old, event, time.integer, fresh ? 1 : 2, *port,
                             out);
        if (!s) {
            ud_batch *replacement = NULL;
            s = clone(r->ctx, r->inputs[1], 1, &replacement);
            if (!s)
                s = ud_batch_append(replacement, now, n);
            if (!s)
                s = ud_batch_seal(replacement);
            if (!s) {
                ud_batch_destroy(r->current_row);
                r->current_row = replacement;
            } else
                ud_batch_destroy(replacement);
        }
    }
    ud_free(old);
    ud_free(now);
    if (!s)
        s = take(r, 0);
    return s;
}
ud_status ud_relational_create(ud_context *ctx, const ud_operator_options *o,
                               const ud_batch *const *in, ud_relational **out) {
    *out = NULL;
    ud_relational *r = ud_alloc(ctx, sizeof(*r));
    if (!r)
        return UD_RESOURCE;
    r->ctx = ctx;
    r->options = *o;
    r->ports = o->kind == UD_OPERATOR_JOIN || o->kind == UD_OPERATOR_MERGE ||
                       o->kind == UD_OPERATOR_MERGE_JOIN
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
    for (uint32_t p = 0; !s && p < r->ports; p++) {
        if (!in[p])
            s = UD_SCHEMA;
        else
            s = clone(ctx, in[p], 0, r->inputs + p);
        if (o->input_order)
            r->order[p] = o->input_order[p];
    }
    if (!s && o->kind == UD_OPERATOR_SCD)
        s = in[1] ? clone(ctx, in[1], 0, r->inputs + 1) : UD_SCHEMA;
    const ud_json *policy = member(r->config, "errorPolicy");
    r->policy = !policy || named(policy, "fail") ? UD_ERROR_FAIL
                : named(policy, "redirect")      ? UD_ERROR_REDIRECT
                : named(policy, "ignore")        ? UD_ERROR_IGNORE
                                                 : -1;
    if (r->policy < 0)
        s = UD_SCHEMA;
    if (!s && (o->kind == UD_OPERATOR_JOIN || o->kind == UD_OPERATOR_MERGE ||
               o->kind == UD_OPERATOR_MERGE_JOIN)) {
        static const char *allow[] = {"keys",         "leftKeys",   "rightKeys", "joinType",
                                      "rightAliases", "partitions", "strategy"};
        s = ud_json_keys(r->config, allow, 7);
        if (!s)
            s = keys(r, 0, member(r->config, o->kind == UD_OPERATOR_MERGE ? "keys" : "leftKeys"));
        if (!s)
            s = keys(r, 1, member(r->config, o->kind == UD_OPERATOR_MERGE ? "keys" : "rightKeys"));
        for (uint32_t p = 0; !s && p < 2; p++) {
            int sorted = r->order[p].count >= r->key_count && r->order[p].stable;
            for (uint32_t k = 0; k < r->key_count && sorted; k++)
                sorted = !ud_id_compare(r->order[p].keys[k].field,
                                        r->inputs[p]->column[r->keys[p][k]].field.identity) &&
                         !r->order[p].keys[k].descending && r->order[p].keys[k].nulls_first;
            if (o->kind == UD_OPERATOR_JOIN && !sorted) {
                ud_operator_options sort = *o;
                sort.kind = UD_OPERATOR_SORT;
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
                s = require_order(r, p, 1);
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
            const ud_json *type = member(r->config, "joinType");
            r->join_type = !type || named(type, "inner") ? 0
                           : named(type, "left")         ? 1
                           : named(type, "full")         ? 2
                           : named(type, "right")        ? 3
                           : named(type, "semi")         ? 4
                           : named(type, "anti")         ? 5
                                                         : -1;
            if (r->join_type < 0)
                s = UD_SCHEMA;
            const ud_json *aliases = member(r->config, "rightAliases");
            ud_field *right = ud_alloc(ctx, in[1]->columns * sizeof(*right));
            ud_batch *aliased = NULL;
            if (!right)
                s = UD_RESOURCE;
            for (uint32_t i = 0; !s && i < in[1]->columns; i++) {
                right[i] = in[1]->column[i].field;
                const ud_json *alias = ud_json_find(aliases, right[i].name, right[i].name_length);
                if (alias) {
                    ud_string n;
                    s = text(alias, &n);
                    right[i].name = n.data;
                    right[i].name_length = n.length;
                }
            }
            if (!s)
                s = ud_batch_create(ctx, right, in[1]->columns, 0, &aliased);
            if (!s)
                s = r->join_type >= 4 ? clone(ctx, in[0], 0, &r->schema[0])
                                      : ud_compute_join_schema(ctx, in[0], aliased, &r->schema[0]);
            ud_free(right);
            ud_batch_destroy(aliased);
            if (!s && r->join_type >= 1 && r->join_type <= 3) {
                ud_field *f = ud_alloc(ctx, r->schema[0]->columns * sizeof(*f));
                if (!f)
                    s = UD_RESOURCE;
                for (uint32_t i = 0; !s && i < r->schema[0]->columns; i++) {
                    f[i] = r->schema[0]->column[i].field;
                    if ((i >= in[0]->columns && (r->join_type == 1 || r->join_type == 2)) ||
                        (i < in[0]->columns && (r->join_type == 2 || r->join_type == 3)))
                        f[i].nullable = 1;
                }
                ud_batch *nullable = NULL;
                if (!s)
                    s = ud_batch_create(ctx, f, r->schema[0]->columns, 0, &nullable);
                ud_free(f);
                if (!s) {
                    ud_batch_destroy(r->schema[0]);
                    r->schema[0] = nullable;
                }
            }
        }
        if (!s) {
            r->output_order = r->order[0];
            r->output_order.count = r->key_count;
            for (uint32_t k = 0; k < r->key_count; k++)
                r->output_order.keys[k].field = r->schema[0]->column[r->keys[0][k]].field.identity;
            if (r->join_type == 2 || r->join_type == 3)
                memset(&r->output_order, 0, sizeof(r->output_order));
        }
    } else if (!s && o->kind == UD_OPERATOR_PIVOT)
        s = pivot_bind(r);
    else if (!s && o->kind == UD_OPERATOR_UNPIVOT)
        s = unpivot_bind(r);
    else if (!s &&
             (o->kind == UD_OPERATOR_ROW_SAMPLING || o->kind == UD_OPERATOR_PERCENTAGE_SAMPLING))
        s = sampling_bind(r);
    else if (!s && o->kind == UD_OPERATOR_SCD)
        s = scd_bind(r);
    if (s) {
        ud_relational_destroy(r);
        return s;
    }
    *out = r;
    return UD_OK;
}
ud_status ud_relational_schema(const ud_relational *r, uint32_t p, ud_batch **out) {
    return p < r->outputs ? clone(r->ctx, r->schema[p], 0, out) : UD_INVALID;
}
ud_status ud_relational_order(const ud_relational *r, ud_sort_contract *out) {
    *out = r->output_order;
    return UD_OK;
}
uint64_t ud_relational_demand(const ud_relational *r) {
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
    if (r->input_sort[p])
        return ud_operator_push(r->input_sort[p], 0, b);
    if (r->in[p].block)
        return UD_INVALID;
    UD_TRY(control(r));
    if (r->options.kind == UD_OPERATOR_ROW_SAMPLING)
        return sample_push(r, b);
    return ud_buffer_store(r->options.buffers, b, &r->in[p].block);
}
ud_status ud_relational_end(ud_relational *r, uint32_t p) {
    if (p >= r->ports || r->in[p].ended || (!r->input_sort[p] && r->in[p].block))
        return UD_INVALID;
    r->in[p].ended = 1;
    if (r->input_sort[p])
        return ud_operator_finish_input(r->input_sort[p], 0);
    return r->rank_sort ? ud_operator_finish_input(r->rank_sort, 0) : UD_OK;
}
ud_status ud_relational_pull(ud_relational *r, ud_batch **out, uint32_t *port) {
    *out = NULL;
    uint32_t p = 0;
    ud_status s = control(r);
    if (!s) {
        switch (r->options.kind) {
        case UD_OPERATOR_JOIN:
        case UD_OPERATOR_MERGE:
        case UD_OPERATOR_MERGE_JOIN:
            s = merge_pull(r, out);
            break;
        case UD_OPERATOR_PIVOT:
            s = pivot_pull(r, out, &p);
            break;
        case UD_OPERATOR_UNPIVOT:
            s = unpivot_pull(r, out);
            break;
        case UD_OPERATOR_ROW_SAMPLING:
        case UD_OPERATOR_PERCENTAGE_SAMPLING:
            s = sample_pull(r, out, &p);
            break;
        case UD_OPERATOR_SCD:
            s = scd_pull(r, out, &p);
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
    for (uint32_t p = 0; p < 2; p++) {
        ud_buffer_view_release(r->in[p].view);
        ud_buffer_release(r->in[p].block);
        for (uint32_t k = 0; k < 16; k++)
            ud_clear_value(r->in[p].last + k);
        ud_batch_destroy(r->inputs[p]);
        ud_operator_destroy(r->input_sort[p]);
    }
    clear_group(r);
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
