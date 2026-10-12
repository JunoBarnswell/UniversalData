#include "../internal.h"
#include <math.h>
#include <ctype.h>
typedef struct group {
    ud_value *values;
    uint64_t hash;
    uint64_t *sums;
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
static int wide_sum(const ud_plan *p, uint32_t i) {
    return p->aggs[i].kind == 1 && (p->fields[p->aggs[i].column].type == UD_INT64 ||
                                    p->fields[p->aggs[i].column].type == UD_DECIMAL128);
}
static ud_status wide_add(uint64_t target[4], const uint64_t input[4]) {
    uint64_t sign_a = target[3] >> 63, sign_b = input[3] >> 63, carry = 0;
    for (uint32_t i = 0; i < 4; i++) {
        uint64_t first = target[i] + input[i], c1 = first < target[i];
        uint64_t second = first + carry, c2 = second < first;
        target[i] = second;
        carry = c1 | c2;
    }
    return sign_a == sign_b && (target[3] >> 63) != sign_a ? UD_OVERFLOW : UD_OK;
}
static void wide_value(const ud_value *v, uint64_t words[4]) {
    if (v->type == UD_INT64) {
        words[0] = (uint64_t)v->integer;
        words[1] = words[2] = words[3] = v->integer < 0 ? UINT64_MAX : 0;
    } else {
        words[0] = v->decimal.low;
        words[1] = (uint64_t)v->decimal.high;
        words[2] = words[3] = v->decimal.high < 0 ? UINT64_MAX : 0;
    }
}
static void state_field(const ud_plan *p, uint32_t column, ud_field *field) {
    *field = p->output_schema->column[column].field;
    if (column >= p->key_n && wide_sum(p, column - p->key_n)) {
        field->type = UD_BINARY;
        field->precision = field->scale = 0;
        field->nullable = 1;
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
static ud_status final_sum(const ud_plan *p, uint32_t i, const uint64_t words[4], ud_value *out) {
    const ud_field *f = &p->output_schema->column[p->key_n + i].field;
    if (f->type == UD_INT64) {
        uint64_t sign = words[0] >> 63 ? UINT64_MAX : 0;
        if (words[1] != sign || words[2] != sign || words[3] != sign)
            return UD_OVERFLOW;
        out->integer = (int64_t)words[0];
        return UD_OK;
    }
    uint64_t sign = words[1] >> 63 ? UINT64_MAX : 0;
    if (words[2] != sign || words[3] != sign)
        return UD_OVERFLOW;
    ud_decimal128 value = {words[0], (int64_t)words[1]};
    return ud_decimal_rescale(value, f->scale, f->precision, f->scale, UD_ROUND_EXACT,
                              &out->decimal);
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
    g->sums = ud_alloc(p->ctx, (size_t)p->agg_n * 4 * sizeof(uint64_t));
    if (!g->values || !g->sums) {
        ud_free(g->values);
        ud_free(g->sums);
        ud_free(g);
        return UD_RESOURCE;
    }
    for (uint32_t i = 0; b && i < p->key_n; i++) {
        ud_value v;
        ud_batch_get(b, row, p->keys[i], &v);
        ud_status s = own_value(p->ctx, g->values + i, &v);
        if (s) {
            clear_values(g->values, n);
            ud_free(g->sums);
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
        ud_free(g->sums);
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
        ud_free(g->sums);
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
        ud_row_info provenance;
        UD_TRY(ud_batch_row_info(b, r, &provenance));
        g->flags |= provenance.flags;
        if (provenance.flags)
            g->rule = provenance.rule;
        for (uint32_t i = 0; i < p->agg_n; i++) {
            const ud_aggregate_def *def = p->aggs + i;
            ud_value input = {.type = UD_INT64}, *v = g->values + p->key_n + i;
            if (def->column != UINT32_MAX)
                ud_batch_get(b, r, def->column, &input);
            if (input.is_null)
                continue;
            if (wide_sum(p, i)) {
                uint64_t words[4];
                wide_value(&input, words);
                UD_TRY(wide_add(g->sums + (size_t)i * 4, words));
                v->is_null = 0;
            } else if (def->kind == 0) {
                if (v->integer == INT64_MAX)
                    return UD_OVERFLOW;
                v->integer++;
            } else if (v->is_null) {
                ud_clear_value(v);
                UD_TRY(own_value(p->ctx, v, &input));
            } else if (def->kind == 1) {
                UD_TRY(ud_value_numeric(UD_NUMERIC_ADD, v, &input, p->fields + def->column, v));
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

static ud_status output_fields(const ud_plan *p, ud_field **out, uint32_t *n) {
    *n = p->output_schema->columns;
    *out = ud_alloc(p->ctx, *n * sizeof(**out));
    if (!*out)
        return UD_RESOURCE;
    for (uint32_t i = 0; i < *n; i++)
        (*out)[i] = p->output_schema->column[i].field;
    return UD_OK;
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
        hash = ud_value_hash(&key, hash);
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
            if (ud_value_compare(&key, g->values + k))
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
        ud_value *value = g->values + p->key_n + i;
        if (input.is_null)
            continue;
        if (wide_sum(p, i)) {
            if (input.type != UD_BINARY || input.text_length != 32 || !input.text)
                return UD_CORRUPT;
            uint64_t words[4] = {0};
            for (uint32_t k = 0; k < 4; k++)
                for (uint32_t j = 0; j < 8; j++)
                    words[k] |= (uint64_t)(uint8_t)input.text[k * 8 + j] << (j * 8);
            UD_TRY(wide_add(g->sums + (size_t)i * 4, words));
            value->is_null = 0;
        } else if (value->is_null) {
            ud_clear_value(value);
            UD_TRY(own_value(p->ctx, value, &input));
        } else if (p->aggs[i].kind <= 1)
            UD_TRY(ud_value_numeric(UD_NUMERIC_ADD, value, &input,
                                    &p->output_schema->column[p->key_n + i].field, value));
        else {
            int order = ud_value_compare(&input, value);
            if ((p->aggs[i].kind == 2 && order < 0) || (p->aggs[i].kind == 3 && order > 0)) {
                ud_value replacement = {0};
                UD_TRY(own_value(p->ctx, &replacement, &input));
                ud_clear_value(value);
                *value = replacement;
            }
        }
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
    uint8_t *encoded = ud_alloc(p->ctx, (size_t)p->agg_n * 32);
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
        for (uint32_t i = 0; !status && i < p->agg_n; i++)
            if (wide_sum(p, i)) {
                ud_value *value = values + p->key_n + i;
                if (partial) {
                    value->type = UD_BINARY;
                    value->precision = value->scale = 0;
                    value->text = (const char *)(encoded + (size_t)i * 32);
                    value->text_length = value->is_null ? 0 : 32;
                    for (uint32_t k = 0; k < 4; k++)
                        for (uint32_t j = 0; j < 8; j++)
                            encoded[(size_t)i * 32 + k * 8 + j] =
                                (uint8_t)(g->sums[(size_t)i * 4 + k] >> (j * 8));
                } else if (!value->is_null)
                    status = final_sum(p, i, g->sums + (size_t)i * 4, value);
            }
        if (!status)
            status = ud_batch_append(*out, values, n);
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
        ud_free(a->head->sums);
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
        g->sums = ud_alloc(a->plan->ctx, (size_t)a->plan->agg_n * 4 * sizeof(uint64_t));
        if (!g->values || !g->sums) {
            ud_free(g->values);
            ud_free(g->sums);
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
        for (uint32_t i = 0; !s && i < a->plan->agg_n; i++)
            if (wide_sum(a->plan, i) && !g->values[a->plan->key_n + i].is_null)
                wide_value(g->values + a->plan->key_n + i, g->sums + (size_t)i * 4);
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
            ud_free(g->sums);
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
        if (!s)
            s = ud_batch_copy_row_info(*out, (*out)->rows - 1, b, r);
    }
    ud_free(f);
    ud_free(v);
    if (s && *out) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}