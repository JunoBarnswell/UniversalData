#include "../internal.h"
#include <math.h>
#include <ctype.h>
typedef struct group {
    ud_value *values;
    uint64_t hash;
    struct group *next, *bucket_next;
} group;
struct ud_accumulator {
    const ud_plan *plan;
    group *head, *tail;
    group **buckets;
    uint32_t groups, bucket_count;
    int failed;
};
ud_status ud_plan_schema(const ud_plan *p, const ud_field **f, uint32_t *n) {
    if (!p || !f || !n)
        return UD_INVALID;
    *f = p->fields;
    *n = p->n;
    return UD_OK;
}
void ud_plan_fingerprint(const ud_plan *p, uint8_t out[32]) {
    if (p && out)
        memcpy(out, p->fingerprint, 32);
}
ud_status ud_plan_check_batch(const ud_plan *p, const ud_batch *b) {
    if (!p || !b || ud_batch_columns(b) != p->n)
        return UD_SCHEMA;
    for (uint32_t i = 0; i < p->n; i++) {
        ud_field f;
        ud_batch_field(b, i, &f);
        if (!ud_field_matches(&f, p->fields + i, 1))
            return ud_fail(p->ctx, UD_SCHEMA, "input schema mismatch at column %u", i);
    }
    return UD_OK;
}
static ud_status own_value(ud_context *ctx, ud_value *target, const ud_value *source) {
    ud_status s = ud_copy_value(ctx, source, target, 0);
    if (s) ud_clear_value(target);
    return s;
}
static void clear_values(ud_value *v, uint32_t n) {
    if (!v)
        return;
    for (uint32_t i = 0; i < n; i++) ud_clear_value(v + i);
    ud_free(v);
}
static ud_status index_group(ud_accumulator *a, group *g) {
    if (!a->plan->key_n)
        return UD_OK;
    if (!a->bucket_count || (a->groups >= a->bucket_count && a->bucket_count < 65536)) {
        uint32_t count = a->bucket_count ? a->bucket_count * 2 : 64;
        group **buckets = ud_alloc(a->plan->ctx, (size_t)count * sizeof(*buckets));
        if (!buckets)
            return UD_RESOURCE;
        for (group *prior = a->head; prior; prior = prior->next) {
            uint32_t slot = (uint32_t)(prior->hash & (count - 1));
            prior->bucket_next = buckets[slot];
            buckets[slot] = prior;
        }
        ud_free(a->buckets);
        a->buckets = buckets;
        a->bucket_count = count;
    }
    uint32_t slot = (uint32_t)(g->hash & (a->bucket_count - 1));
    g->bucket_next = a->buckets[slot];
    a->buckets[slot] = g;
    return UD_OK;
}
static ud_status new_group(ud_accumulator *a, const ud_batch *b, uint32_t row, uint64_t hash,
                           group **out) {
    const ud_plan *p = a->plan;
    uint32_t n = p->key_n + p->agg_n;
    if (a->groups == INT32_MAX)
        return UD_RESOURCE;
    group *g = ud_alloc(p->ctx, sizeof(*g));
    if (!g)
        return UD_RESOURCE;
    g->values = ud_alloc(p->ctx, n * sizeof(ud_value));
    if (!g->values) {
        ud_free(g);
        return UD_RESOURCE;
    }
    for (uint32_t i = 0; i < p->key_n; i++) {
        ud_value v;
        ud_batch_get(b, row, p->keys[i], &v);
        ud_status s = own_value(p->ctx, g->values + i, &v);
        if (s) {
            clear_values(g->values, n);
            ud_free(g);
            return s;
        }
    }
    for (uint32_t i = 0; i < p->agg_n; i++) {
        ud_value *v = g->values + p->key_n + i;
        v->type = p->aggs[i].kind == 0 ? UD_INT64 : p->fields[p->aggs[i].column].type;
        v->is_null = p->aggs[i].kind != 0;
        if (p->aggs[i].kind != 0) {
            v->precision = p->fields[p->aggs[i].column].precision;
            v->scale = p->fields[p->aggs[i].column].scale;
        }
    }
    g->hash = hash;
    ud_status indexed = index_group(a, g);
    if (indexed) {
        clear_values(g->values, n);
        ud_free(g);
        return indexed;
    }
    if (a->tail)
        a->tail->next = g;
    else
        a->head = g;
    a->tail = g;
    a->groups++;
    *out = g;
    return UD_OK;
}
ud_status ud_accumulator_create(const ud_plan *p, ud_accumulator **out) {
    if (!p || !out || !p->agg_n)
        return UD_INVALID;
    *out = ud_alloc(p->ctx, sizeof(**out));
    if (!*out)
        return UD_RESOURCE;
    (*out)->plan = p;
    ud_status retained = ud_plan_retain((ud_plan *)p);
    if (retained) { ud_free(*out); *out = NULL; return retained; }
    if (!p->key_n) {
        group *g;
        ud_status s = new_group(*out, NULL, 0, UD_VALUE_HASH_SEED, &g);
        if (s) {
            ud_accumulator_destroy(*out);
            *out = NULL;
            return s;
        }
    }
    return UD_OK;
}
void ud_accumulator_destroy(ud_accumulator *a) {
    if (!a)
        return;
    group *g = a->head;
    while (g) {
        group *next = g->next;
        clear_values(g->values, a->plan->key_n + a->plan->agg_n);
        ud_free(g);
        g = next;
    }
    ud_free(a->buckets);
    ud_plan_destroy((ud_plan *)a->plan);
    ud_free(a);
}
static ud_status add_rows(ud_accumulator *a, const ud_batch *b, uint32_t begin, uint32_t end, int filtered) {
    const ud_plan *p = a->plan;
    UD_TRY(ud_plan_check_batch(p, b));
    for (uint32_t r = begin; r < end; r++) {
        int accept = 1;
        if (filtered) UD_TRY(ud_plan_accept(p, b, r, &accept));
        if (!accept)
            continue;
        uint64_t hash = UD_VALUE_HASH_SEED;
        for (uint32_t k = 0; k < p->key_n; k++) {
            ud_value v;
            UD_TRY(ud_batch_get(b, r, p->keys[k], &v));
            hash = ud_value_hash(&v, hash);
        }
        group *g = !p->key_n         ? a->head
                   : a->bucket_count ? a->buckets[hash & (a->bucket_count - 1)]
                                     : NULL;
        for (; g; g = g->bucket_next) {
            if (g->hash != hash)
                continue;
            uint32_t k = 0;
            for (; k < p->key_n; k++) {
                ud_value v;
                ud_batch_get(b, r, p->keys[k], &v);
                if (ud_value_compare(&v, g->values + k))
                    break;
            }
            if (k == p->key_n)
                break;
        }
        if (!g)
            UD_TRY(new_group(a, b, r, hash, &g));
        for (uint32_t i = 0; i < p->agg_n; i++) {
            const ud_aggregate_def *def = p->aggs + i;
            ud_value input = {.type = UD_INT64}, *v = g->values + p->key_n + i;
            if (def->column != UINT32_MAX)
                ud_batch_get(b, r, def->column, &input);
            if (input.is_null)
                continue;
            if (def->kind == 0) {
                if (v->integer == INT64_MAX)
                    return UD_OVERFLOW;
                v->integer++;
            } else if (v->is_null) {
                ud_clear_value(v);
                UD_TRY(own_value(p->ctx, v, &input));
            } else if (def->kind == 1) {
                UD_TRY(ud_value_numeric(UD_NUMERIC_ADD,v,&input,p->fields+def->column,v));
            } else {
                int cmp = ud_value_compare(&input, v);
                if ((def->kind == 2 && cmp < 0) || (def->kind == 3 && cmp > 0)) {
                    ud_value replacement;
                    UD_TRY(own_value(p->ctx, &replacement, &input));
                    ud_clear_value(v);
                    *v = replacement;
                }
            }
        }
    }
    return UD_OK;
}
ud_status ud_accumulator_add(ud_accumulator *a, const ud_batch *b) {
    if (!a || a->failed)
        return UD_INVALID;
    ud_status s = add_rows(a, b, 0, ud_batch_rows(b), 1);
    if (s)
        a->failed = 1;
    return s;
}
ud_status ud_accumulator_add_accepted(ud_accumulator *a, const ud_batch *b, uint32_t row) {
    if (!a || a->failed || !b || row >= b->rows) return UD_INVALID;
    ud_status s = add_rows(a, b, row, row + 1, 0);
    if (s) a->failed = 1;
    return s;
}
static ud_status output_fields(const ud_plan *p, ud_field **out, uint32_t *n) {
    *n = p->output_schema->columns;
    *out = ud_alloc(p->ctx, *n * sizeof(**out));
    if (!*out) return UD_RESOURCE;
    for (uint32_t i = 0; i < *n; i++) (*out)[i] = p->output_schema->column[i].field;
    return UD_OK;
}
ud_status ud_accumulator_snapshot(const ud_accumulator *a, ud_batch **out) {
    if (!a || !out || a->failed)
        return UD_INVALID;
    *out = NULL;
    ud_field *fields;
    uint32_t n;
    UD_TRY(output_fields(a->plan, &fields, &n));
    ud_status s = ud_batch_create(a->plan->ctx, fields, n, a->groups, out);
    ud_free(fields);
    for (group *g = a->head; g && !s; g = g->next)
        s = ud_batch_append(*out, g->values, n);
    if (s && *out) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}
ud_status ud_accumulator_restore(ud_accumulator *a, const ud_batch *b) {
    if (!a || a->failed || a->groups > (a->plan->key_n ? 0u : 1u))
        return UD_INVALID;
    ud_field *fields;
    uint32_t n;
    UD_TRY(output_fields(a->plan, &fields, &n));
    ud_status s = UD_OK;
    if (ud_batch_columns(b) != n || (!a->plan->key_n && ud_batch_rows(b) != 1))
        s = UD_CORRUPT;
    for (uint32_t i = 0; !s && i < n; i++) {
        ud_field f;
        ud_batch_field(b, i, &f);
        if (!ud_field_matches(&f, fields + i, 1))
            s = UD_CORRUPT;
    }
    ud_free(fields);
    if (s)
        return s;
    if (a->head) {
        clear_values(a->head->values, n);
        ud_free(a->head);
        a->head = a->tail = NULL;
        a->groups = 0;
    }
    ud_free(a->buckets);
    a->buckets = NULL;
    a->bucket_count = 0;
    for (uint32_t r = 0; r < ud_batch_rows(b); r++) {
        group *g = ud_alloc(a->plan->ctx, sizeof(*g));
        if (!g) {
            a->failed = 1;
            return UD_RESOURCE;
        }
        g->values = ud_alloc(a->plan->ctx, n * sizeof(ud_value));
        if (!g->values) {
            ud_free(g);
            a->failed = 1;
            return UD_RESOURCE;
        }
        for (uint32_t i = 0; i < n; i++) {
            ud_value v;
            ud_batch_get(b, r, i, &v);
            s = own_value(a->plan->ctx, g->values + i, &v);
            if (s)
                break;
        }
        g->hash = UD_VALUE_HASH_SEED;
        for (uint32_t k = 0; !s && k < a->plan->key_n; k++)
            g->hash = ud_value_hash(g->values + k, g->hash);
        if (!s && a->bucket_count) {
            for (group *prior = a->buckets[g->hash & (a->bucket_count - 1)]; prior;
                 prior = prior->bucket_next) {
                if (prior->hash != g->hash)
                    continue;
                uint32_t k = 0;
                for (; k < a->plan->key_n; k++)
                    if (ud_value_compare(prior->values + k, g->values + k))
                        break;
                if (k == a->plan->key_n) {
                    s = UD_CORRUPT;
                    break;
                }
            }
        }
        if (!s)
            s = index_group(a, g);
        if (s) {
            clear_values(g->values, n);
            ud_free(g);
            a->failed = 1;
            return s;
        }
        if (a->tail)
            a->tail->next = g;
        else
            a->head = g;
        a->tail = g;
        a->groups++;
    }
    return UD_OK;
}
ud_status ud_compute_apply(const ud_plan *p, const ud_batch *b, ud_batch **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    UD_TRY(ud_plan_check_batch(p, b));
    if (p->agg_n) {
        ud_accumulator *a;
        UD_TRY(ud_accumulator_create(p, &a));
        ud_status s = ud_accumulator_add(a, b);
        if (!s)
            s = ud_accumulator_snapshot(a, out);
        ud_accumulator_destroy(a);
        return s;
    }
    uint32_t n = p->project_n ? p->project_n : p->n;
    ud_field *f = ud_alloc(p->ctx, n * sizeof(*f));
    ud_value *v = ud_alloc(p->ctx, n * sizeof(*v));
    if (!f || !v) {
        ud_free(f);
        ud_free(v);
        return UD_RESOURCE;
    }
    for (uint32_t i = 0; i < n; i++)
        f[i] = p->fields[p->project_n ? p->projection[i] : i];
    ud_status s = ud_batch_create(p->ctx, f, n, ud_batch_rows(b), out);
    for (uint32_t r = 0; !s && r < ud_batch_rows(b); r++) {
        int accept;
        s = ud_plan_accept(p, b, r, &accept);
        if (s || !accept)
            continue;
        for (uint32_t i = 0; i < n; i++)
            ud_batch_get(b, r, p->project_n ? p->projection[i] : i, v + i);
        s = ud_batch_append(*out, v, n);
    }
    ud_free(f);
    ud_free(v);
    if (s && *out) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}
