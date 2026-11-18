#include "../internal.h"
#include "stat.h"
#include <math.h>
#include <ctype.h>
typedef struct group {
    ud_value *values;
    uint64_t hash;
    ud_aggregate_state *states;
    uint32_t flags;
    ud_id rule;
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
    if (s)
        ud_clear_value(target);
    return s;
}
static void clear_values(ud_value *v, uint32_t n) {
    if (!v)
        return;
    for (uint32_t i = 0; i < n; i++)
        ud_clear_value(v + i);
    ud_free(v);
}
static ud_field input_field(const ud_plan *p, uint32_t i) {
    return p->aggs[i].column == UINT32_MAX
               ? (ud_field){.name = "value", .name_length = 5, .type = UD_INT64}
               : p->fields[p->aggs[i].column];
}
static void state_field(const ud_plan *p, uint32_t column, ud_field *field) {
    *field = p->output_schema->column[column].field;
    if (column >= p->key_n) {
        field->type = UD_BINARY;
        field->scalar=(ud_scalar_metadata){0};field->timezone=NULL;field->timezone_length=0;field->children=NULL;field->child_count=0;field->dictionary=NULL;field->dictionary_count=0;field->dictionary_kind=0;
        field->precision = field->scale = 0;
        field->nullable = 0;
    }
}
ud_status ud_accumulator_state_schema(const ud_plan *p, ud_batch **out) {
    uint32_t n = p->output_schema->columns;
    ud_field *fields = ud_alloc(p->ctx, (size_t)n * sizeof(*fields));
    if (!fields)
        return UD_RESOURCE;
    for (uint32_t i = 0; i < n; i++)
        state_field(p, i, fields + i);
    ud_status status = ud_batch_create(p->ctx, fields, n, 0, out);
    ud_free(fields);
    return status;
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
    g->states = ud_alloc(p->ctx, (size_t)p->agg_n * sizeof(*g->states));
    if (!g->values || !g->states) {
        ud_free(g->values);
        for (uint32_t k = 0; g->states && k < p->agg_n; k++)
            ud_stat_clear(g->states + k);
        ud_free(g->states);
        ud_free(g);
        return UD_RESOURCE;
    }
    for (uint32_t i = 0; b && i < p->key_n; i++) {
        ud_value v;
        ud_batch_get(b, row, p->keys[i], &v);
        ud_status s = own_value(p->ctx, g->values + i, &v);
        if (s) {
            clear_values(g->values, n);
            for (uint32_t k = 0; g->states && k < p->agg_n; k++)
                ud_stat_clear(g->states + k);
            ud_free(g->states);
            ud_free(g);
            return s;
        }
    }
    for (uint32_t i = 0; i < p->agg_n; i++) {
        g->states[i].ctx = p->ctx;
        g->values[p->key_n + i] =
            (ud_value){.type = p->output_schema->column[p->key_n + i].field.type,
                       .is_null = p->aggs[i].kind != 0 && p->aggs[i].kind != 5};
    }
    g->hash = hash;
    ud_status indexed = index_group(a, g);
    if (indexed) {
        clear_values(g->values, n);
        for (uint32_t k = 0; g->states && k < p->agg_n; k++)
            ud_stat_clear(g->states + k);
        ud_free(g->states);
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
    if (retained) {
        ud_free(*out);
        *out = NULL;
        return retained;
    }
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
        for (uint32_t k = 0; g->states && k < a->plan->agg_n; k++)
            ud_stat_clear(g->states + k);
        ud_free(g->states);
        ud_free(g);
        g = next;
    }
    ud_free(a->buckets);
    ud_plan_destroy((ud_plan *)a->plan);
    ud_free(a);
}
static ud_status add_rows(ud_accumulator *a, const ud_batch *b, uint32_t begin, uint32_t end,
                          int filtered) {
    const ud_plan *p = a->plan;
    UD_TRY(ud_plan_check_batch(p, b));
    for (uint32_t r = begin; r < end; r++) {
        int accept = 1;
        if (filtered)
            UD_TRY(ud_plan_accept(p, b, r, &accept));
        if (!accept)
            continue;
        uint64_t hash = UD_VALUE_HASH_SEED;
        for (uint32_t k = 0; k < p->key_n; k++) {
            ud_value v;
            UD_TRY(ud_batch_get(b, r, p->keys[k], &v));
            hash = ud_field_value_hash(&p->output_schema->column[k].field, &v, hash);
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
                const ud_field *field = &p->output_schema->column[k].field;
                if (ud_field_value_compare(field, &v, field, g->values + k))
                    break;
            }
            if (k == p->key_n)
                break;
        }
        if (!g)
            UD_TRY(new_group(a, b, r, hash, &g));
        ud_row_info provenance;
        UD_TRY(ud_batch_row_info(b, r, &provenance));
        g->flags |= provenance.flags;
        if (provenance.flags)
            g->rule = provenance.rule;
        for (uint32_t i = 0; i < p->agg_n; i++) {
            const ud_aggregate_def *def = p->aggs + i;
            if (def->condition) {
                ud_value condition;
                UD_TRY(ud_expression_evaluate(def->condition, b, r, &condition));
                if (condition.is_null || !condition.integer)
                    continue;
            }
            ud_value input = {.type = UD_INT64};
            if (def->column != UINT32_MAX)
                UD_TRY(ud_batch_get(b, r, def->column, &input));
            UD_TRY(ud_stat_add(g->states + i, def, &input));
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
    if (!a || a->failed)
        return UD_INVALID;
    ud_status status = add_rows(a, b, row, row + 1, 0);
    if (status)
        a->failed = 1;
    return status;
}
ud_status ud_accumulator_merge_row(ud_accumulator *a, const ud_batch *b, uint32_t row) {
    if (!a || !b || a->failed || row >= b->rows)
        return UD_INVALID;
    const ud_plan *p = a->plan;
    if (b->columns != p->output_schema->columns)
        return UD_SCHEMA;
    for (uint32_t i = 0; i < b->columns; i++) {
        ud_field expected;
        state_field(p, i, &expected);
        if (!ud_field_matches(&b->column[i].field, &expected, UD_FIELD_PORT))
            return UD_SCHEMA;
    }
    uint64_t hash = UD_VALUE_HASH_SEED;
    for (uint32_t k = 0; k < p->key_n; k++) {
        ud_value key;
        UD_TRY(ud_batch_get(b, row, k, &key));
        hash = ud_field_value_hash(&p->output_schema->column[k].field, &key, hash);
    }
    group *g = !p->key_n         ? a->head
               : a->bucket_count ? a->buckets[hash & (a->bucket_count - 1)]
                                 : NULL;
    for (; g && p->key_n; g = g->bucket_next) {
        if (g->hash != hash)
            continue;
        uint32_t k = 0;
        for (; k < p->key_n; k++) {
            ud_value key;
            UD_TRY(ud_batch_get(b, row, k, &key));
            const ud_field *field = &p->output_schema->column[k].field;
            if (ud_field_value_compare(field, &key, field, g->values + k))
                break;
        }
        if (k == p->key_n)
            break;
    }
    if (!g) {
        UD_TRY(new_group(a, NULL, 0, hash, &g));
        for (uint32_t k = 0; k < p->key_n; k++) {
            ud_value key;
            UD_TRY(ud_batch_get(b, row, k, &key));
            UD_TRY(own_value(p->ctx, g->values + k, &key));
        }
    }
    ud_row_info provenance;
    UD_TRY(ud_batch_row_info(b, row, &provenance));
    g->flags |= provenance.flags;
    if (provenance.flags)
        g->rule = provenance.rule;
    for (uint32_t i = 0; i < p->agg_n; i++) {
        ud_value input;
        UD_TRY(ud_batch_get(b, row, p->key_n + i, &input));
        ud_field field = input_field(p, i);
        UD_TRY(ud_stat_merge(g->states + i, p->aggs + i, &field, &input));
    }
    return UD_OK;
}
ud_status ud_accumulator_merge(ud_accumulator *a, const ud_batch *snapshot) {
    if (!a || !snapshot)
        return UD_INVALID;
    for (uint32_t row = 0; row < snapshot->rows; row++) {
        ud_status status = ud_accumulator_merge_row(a, snapshot, row);
        if (status) {
            a->failed = 1;
            return status;
        }
    }
    return UD_OK;
}
static ud_status accumulator_snapshot(const ud_accumulator *a, ud_batch **out, int partial) {
    if (!a || !out || a->failed)
        return UD_INVALID;
    *out = NULL;
    const ud_plan *p = a->plan;
    uint32_t n = p->key_n + p->agg_n;
    ud_field *fields = ud_alloc(p->ctx, (size_t)n * sizeof(*fields));
    ud_value *values = ud_alloc(p->ctx, (size_t)n * sizeof(*values));
    ud_value *encoded = ud_alloc(p->ctx, (size_t)p->agg_n * sizeof(*encoded));
    ud_status status = !fields || !values || !encoded ? UD_RESOURCE : UD_OK;
    for (uint32_t i = 0; !status && i < n; i++) {
        if (partial)
            state_field(p, i, fields + i);
        else
            fields[i] = p->output_schema->column[i].field;
    }
    if (!status)
        status = ud_batch_create(p->ctx, fields, n, a->groups, out);
    for (group *g = a->head; !status && g; g = g->next) {
        memcpy(values, g->values, (size_t)n * sizeof(*values));
        for (uint32_t i = 0; !status && i < p->agg_n; i++) {
            ud_field field = input_field(p, i);
            if (partial) {
                status = ud_stat_encode(g->states + i, p->aggs + i, &field, encoded + i);
                values[p->key_n + i] = encoded[i];
            } else
                status = ud_stat_result(g->states + i, p->aggs + i,
                                        &p->output_schema->column[p->key_n + i].field,
                                        values + p->key_n + i);
        }
        if (!status)
            status = ud_batch_append(*out, values, n);
        for (uint32_t i = 0; i < p->agg_n; i++)
            ud_clear_value(encoded + i);
    }
    if (!status) {
        uint8_t fingerprint[32];
        status = ud_schema_fingerprint(p->output_schema, fingerprint);
        char scope[65];
        static const char hex[] = "0123456789abcdef";
        for (uint32_t j = 0; j < 32; j++) {
            scope[j * 2] = hex[fingerprint[j] >> 4];
            scope[j * 2 + 1] = hex[fingerprint[j] & 15];
        }
        scope[64] = 0;
        ud_id identity = {0};
        if (!status)
            status =
                ud_id_make(p->ctx, (ud_string){"aggregate", 9}, (ud_string){scope, 64}, &identity);
        uint32_t *keys = ud_alloc(p->ctx, (size_t)(p->key_n + 1) * sizeof(*keys));
        if (!status && !keys)
            status = UD_RESOURCE;
        for (uint32_t j = 0; !status && j < p->key_n; j++)
            keys[j] = j;
        if (!status)
            status = ud_batch_seal(*out);
        ud_batch *identified = NULL;
        if (!status)
            status = ud_batch_identify_keys(p->ctx, *out, identity, keys, p->key_n, &identified);
        ud_free(keys);
        if (!status) {
            uint32_t row = 0;
            for (group *g = a->head; g; g = g->next, row++) {
                identified->row_info[row].flags = g->flags;
                identified->row_info[row].rule = g->rule;
            }
            ud_batch_destroy(*out);
            *out = identified;
        }
    }
    ud_free(fields);
    ud_free(values);
    ud_free(encoded);
    if (status) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return status;
}
ud_status ud_accumulator_snapshot(const ud_accumulator *a, ud_batch **out) {
    return accumulator_snapshot(a, out, 0);
}
ud_status ud_accumulator_partial_snapshot(const ud_accumulator *a, ud_batch **out) {
    return accumulator_snapshot(a, out, 1);
}
ud_status ud_accumulator_restore(ud_accumulator *a, const ud_batch *b) {
    if (!a || !b || a->failed || a->groups > (a->plan->key_n ? 0u : 1u))
        return UD_INVALID;
    const ud_plan *p = a->plan;
    uint32_t n = p->key_n + p->agg_n;
    if (b->columns != n)
        return UD_CORRUPT;
    int partial = 1;
    for (uint32_t i = 0; i < n; i++) {
        ud_field expected;
        state_field(p, i, &expected);
        if (!ud_field_matches(&b->column[i].field, &expected, UD_FIELD_PORT))
            partial = 0;
    }
    if (!partial) {
        for (uint32_t i = 0; i < p->agg_n; i++)
            if (p->aggs[i].kind > 3 || p->aggs[i].distinct)
                return UD_UNSUPPORTED;
        for (uint32_t i = 0; i < n; i++)
            if (!ud_field_matches(&b->column[i].field, &p->output_schema->column[i].field,
                                  UD_FIELD_PORT))
                return UD_CORRUPT;
    }
    if (a->head) {
        clear_values(a->head->values, n);
        for (uint32_t i = 0; i < p->agg_n; i++)
            ud_stat_clear(a->head->states + i);
        ud_free(a->head->states);
        ud_free(a->head);
        a->head = a->tail = NULL;
        a->groups = 0;
    }
    ud_free(a->buckets);
    a->buckets = NULL;
    a->bucket_count = 0;
    if (partial)
        return ud_accumulator_merge(a, b);
    for (uint32_t row = 0; row < b->rows; row++) {
        group *g = NULL;
        uint64_t hash = UD_VALUE_HASH_SEED;
        for (uint32_t k = 0; k < p->key_n; k++) {
            ud_value v;
            UD_TRY(ud_batch_get(b, row, k, &v));
            hash = ud_field_value_hash(&p->output_schema->column[k].field, &v, hash);
        }
        for (group *prior = a->head; prior; prior = prior->next) {
            int same = prior->hash == hash;
            for (uint32_t k = 0; same && k < p->key_n; k++) {
                ud_value v;
                UD_TRY(ud_batch_get(b, row, k, &v));
                const ud_field *field = &p->output_schema->column[k].field;
                same = !ud_field_value_compare(field, &v, field, prior->values + k);
            }
            if (same) {
                a->failed = 1;
                return UD_CORRUPT;
            }
        }
        UD_TRY(new_group(a, NULL, 0, hash, &g));
        for (uint32_t k = 0; k < p->key_n; k++) {
            ud_value v;
            UD_TRY(ud_batch_get(b, row, k, &v));
            UD_TRY(own_value(p->ctx, g->values + k, &v));
        }
        for (uint32_t i = 0; i < p->agg_n; i++) {
            ud_value v;
            UD_TRY(ud_batch_get(b, row, p->key_n + i, &v));
            if (p->aggs[i].kind == 0) {
                if (v.is_null || v.integer < 0)
                    return UD_CORRUPT;
                g->states[i].sum.count = (uint64_t)v.integer;
            } else
                UD_TRY(ud_stat_add(g->states + i, p->aggs + i, &v));
        }
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
    ud_batch *mask = NULL;
    if (!s && p->predicate)
        s = ud_expression_evaluate_batch(p->predicate, b, (ud_string){"accepted", 8}, &mask);
    for (uint32_t r = 0; !s && r < ud_batch_rows(b); r++) {
        int accept = 1;
        if (ud_is_cancelled(p->ctx))
            s = UD_CANCELLED;
        if (!s && mask) {
            ud_value flag;
            s = ud_batch_get(mask, r, 0, &flag);
            accept = !flag.is_null && flag.integer;
        }

        if (s || !accept)
            continue;
        for (uint32_t i = 0; i < n; i++)
            ud_batch_get(b, r, p->project_n ? p->projection[i] : i, v + i);
        s = ud_batch_append(*out, v, n);
        if (!s)
            s = ud_batch_copy_row_info(*out, (*out)->rows - 1, b, r);
    }
    ud_free(f);
    ud_free(v);
    ud_batch_destroy(mask);
    if (s && *out) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}