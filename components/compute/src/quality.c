#include "ud/quality.h"
#include "ud/regex.h"
#include "ud/cache.h"
#include "snapshot.h"
#include "../internal.h"
typedef struct seen_key {
    ud_value *values;
    uint64_t hash;
    struct seen_key *next, *bucket_next;
} seen_key;
typedef struct quality_rule {
    ud_quality_rule config;
    ud_cache *reference;
    ud_cache_reader *reader;
    ud_quality_rule_metrics metrics;
    seen_key *seen;
    seen_key **buckets;
    uint32_t bucket_count;
    uint64_t key_count;
    ud_regex *regex;
} quality_rule;
struct ud_quality {
    ud_context *ctx;
    ud_batch *schema, *accepted_schema;
    quality_rule *rules;
    uint32_t count;
    ud_quality_options options;
    int reference_ready;
    int failed, finished;
    ud_quality_metrics metrics;
};
static ud_status key_hash(const ud_batch *batch, uint32_t row, const uint32_t *columns,
                          uint32_t count, uint64_t *out) {
    *out = UD_VALUE_HASH_SEED;
    for (uint32_t j = 0; j < count; j++) {
        ud_value v;
        UD_TRY(ud_batch_get(batch, row, columns[j], &v));
        *out = ud_value_hash(&v, *out);
    }
    return UD_OK;
}
void ud_quality_destroy(ud_quality *q) {
    if (!q)
        return;
    for (uint32_t j = 0; j < q->count; j++) {
        quality_rule *r = q->rules + j;
        ud_free((void *)r->config.id);
        ud_free((void *)r->config.columns);
        ud_free((void *)r->config.reference_columns);
        ud_free((void *)r->config.pattern);
        ud_clear_value(&r->config.minimum);
        ud_clear_value(&r->config.maximum);
        ud_cache_reader_destroy(r->reader);
        ud_cache_release(r->reference);
        ud_regex_destroy(r->regex);
        ud_free(r->buckets);
        seen_key *key = r->seen;
        while (key) {
            seen_key *next = key->next;
            for (uint32_t k = 0; k < r->config.column_count; k++)
                ud_clear_value(key->values + k);
            ud_free(key->values);
            ud_free(key);
            key = next;
        }
    }
    ud_free(q->rules);
    ud_batch_destroy(q->schema);
    ud_batch_destroy(q->accepted_schema);
    ud_free(q);
}
ud_status ud_quality_create(ud_context *ctx, const ud_field *fields, uint32_t columns,
                            const ud_quality_rule *rules, uint32_t count,
                            const ud_quality_options *options, ud_quality **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!ctx || !rules || !count || count > 65536 || !options ||
        options->struct_size != sizeof(*options) || options->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!options->max_batch_rows || options->policy < 0 || options->policy > 2)
        return UD_INVALID;
    ud_quality *q = ud_alloc(ctx, sizeof(*q));
    if (!q)
        return UD_RESOURCE;
    q->ctx = ctx;
    q->options = *options;
    q->reference_ready = 1;
    ud_status s = ud_batch_create(ctx, fields, columns, 0, &q->schema);
    if (!s) {
        q->rules = ud_alloc(ctx, count * sizeof(*q->rules));
        if (!q->rules)
            s = UD_RESOURCE;
    }
    for (uint32_t j = 0; !s && j < count; j++) {
        const ud_quality_rule *input = rules + j;
        quality_rule *r = q->rules + j;
        if (input->struct_size != sizeof(*input) || input->abi_version != UD_ABI_VERSION) {
            s = UD_INCOMPATIBLE;
            break;
        }
        if (!input->rule_version || input->policy < -1 || input->policy > 2 || !input->id ||
            !input->id_length || memchr(input->id, 0, input->id_length) ||
            !ud_utf8_valid(input->id, input->id_length) || input->kind < UD_RULE_NOT_NULL ||
            input->kind > UD_RULE_ROW_COUNT ||
            (input->kind != UD_RULE_ROW_COUNT &&
             (!input->columns || !input->column_count || input->column_count > columns)) ||
            ((input->kind == UD_RULE_RANGE || input->kind == UD_RULE_REGEX) &&
             input->column_count != 1)) {
            s = UD_SCHEMA;
            break;
        }
        for (uint32_t k = 0; k < j; k++)
            if (input->id_length == rules[k].id_length &&
                !memcmp(input->id, rules[k].id, input->id_length))
                s = UD_SCHEMA;
        if (s)
            break;
        r->config = *input;
        if (r->config.policy == UD_ERROR_INHERIT)
            r->config.policy = options->policy;
        if (r->config.kind == UD_RULE_ROW_COUNT && r->config.policy != UD_ERROR_FAIL) {
            s = UD_UNSUPPORTED;
            break;
        }
        r->config.id = NULL;
        r->config.columns = NULL;
        r->config.reference_columns = NULL;
        r->config.pattern = NULL;
        r->config.minimum = (ud_value){0};
        r->config.maximum = (ud_value){0};
        r->config.reference = NULL;
        q->count = j + 1;
        r->config.id = ud_strdup_n(ctx, input->id, input->id_length);
        if (!r->config.id) {
            s = UD_RESOURCE;
            break;
        }
        if (input->kind != UD_RULE_ROW_COUNT) {
            uint32_t *indices = ud_alloc(ctx, input->column_count * sizeof(*indices));
            r->config.columns = indices;
            if (!indices) {
                s = UD_RESOURCE;
                break;
            }
            for (uint32_t k = 0; !s && k < input->column_count; k++) {
                indices[k] = input->columns[k];
                if (indices[k] >= columns)
                    s = UD_SCHEMA;
                for (uint32_t earlier = 0; earlier < k; earlier++)
                    if (indices[earlier] == indices[k])
                        s = UD_SCHEMA;
            }
        } else {
            r->config.column_count = 0;
        }
        if (s)
            break;
        if (input->kind == UD_RULE_RANGE) {
            s = ud_copy_value(ctx, &input->minimum, &r->config.minimum, 0);
            if (!s)
                s = ud_copy_value(ctx, &input->maximum, &r->config.maximum, 0);
            ud_field field = fields[input->columns[0]];
            field.nullable = 1;
            ud_batch *check = NULL;
            if (!s)
                s = ud_batch_create(ctx, &field, 1, 2, &check);
            if (!s && !input->minimum.is_null)
                s = ud_batch_append(check, &input->minimum, 1);
            if (!s && !input->maximum.is_null)
                s = ud_batch_append(check, &input->maximum, 1);
            if (!s && !input->minimum.is_null && !input->maximum.is_null &&
                ud_value_compare(&input->minimum, &input->maximum) > 0)
                s = UD_SCHEMA;
            ud_batch_destroy(check);
        } else if (input->kind == UD_RULE_FOREIGN_KEY) {
            if (!input->reference || !input->reference_columns) {
                s = UD_SCHEMA;
                break;
            }
            uint32_t *indices = ud_alloc(ctx, input->column_count * sizeof(*indices));
            r->config.reference_columns = indices;
            if (!indices) {
                s = UD_RESOURCE;
                break;
            }
            for (uint32_t k = 0; !s && k < input->column_count; k++) {
                indices[k] = input->reference_columns[k];
                ud_field reference, field = fields[input->columns[k]];
                s = ud_batch_field(input->reference, indices[k], &reference);
                if (!s)
                    reference.nullable = field.nullable;
                if (!s && !ud_field_matches(&field, &reference, 0))
                    s = UD_SCHEMA;
            }
            ud_cache_options cache = {.struct_size = sizeof(cache),
                                      .abi_version = UD_ABI_VERSION,
                                      .max_batch_rows = options->max_batch_rows,
                                      .keys = indices,
                                      .key_count = input->column_count,
                                      .duplicates = UD_CACHE_FIRST,
                                      .buffers = options->buffers,
                                      .check = options->check,
                                      .user = options->user,
                                      .record_state = options->record_state};
            if (!s && options->buffers)
                s = ud_cache_create(ctx, input->reference, &cache, &r->reference);
            q->reference_ready = 0;
            if (!s && input->reference->rows && !options->buffers)
                s = UD_INVALID;
            if (!s && input->reference->rows) {
                ud_batch *owned = NULL;
                s = ud_batch_clone(input->reference, &owned);
                if (!s)
                    s = ud_batch_seal(owned);
                if (!s)
                    s = ud_cache_append(r->reference, owned);
                ud_batch_destroy(owned);
            }
        } else if (input->kind == UD_RULE_REGEX) {
            if (fields[input->columns[0]].type != UD_UTF8 ||
                (!input->pattern && input->pattern_length) ||
                !ud_utf8_valid(input->pattern, input->pattern_length)) {
                s = UD_SCHEMA;
                break;
            }
            r->config.pattern =
                ud_strdup_n(ctx, input->pattern ? input->pattern : "", input->pattern_length);
            if (!r->config.pattern)
                s = UD_RESOURCE;
            if (!s && !input->match)
                s = ud_regex_compile(ctx, r->config.pattern, r->config.pattern_length,
                                     &(ud_regex_options){.struct_size = sizeof(ud_regex_options),
                                                         .abi_version = UD_ABI_VERSION,
                                                         .max_steps = 1000000,
                                                         .max_depth = 64,
                                                         .heap_bytes = 1048576,
                                                         .check = options->check,
                                                         .user = options->user},
                                     &r->regex);
        }
    }
    if (!s) {
        ud_field *accepted = ud_alloc(ctx, (size_t)columns * sizeof(*accepted));
        if (!accepted)
            s = UD_RESOURCE;
        for (uint32_t i = 0; !s && i < columns; i++)
            accepted[i] = q->schema->column[i].field;
        for (uint32_t i = 0; !s && i < q->count; i++) {
            const ud_quality_rule *rule = &q->rules[i].config;
            if ((rule->kind == UD_RULE_NOT_NULL || rule->kind == UD_RULE_PRIMARY_KEY) &&
                rule->policy != UD_ERROR_IGNORE)
                for (uint32_t j = 0; j < rule->column_count; j++)
                    accepted[rule->columns[j]].nullable = 0;
        }
        if (!s)
            s = ud_batch_create(ctx, accepted, columns, 0, &q->accepted_schema);
        ud_free(accepted);
    }
    if (s) {
        ud_quality_destroy(q);
        return s;
    }
    *out = q;
    return UD_OK;
}
static ud_status check_rule(ud_quality *q, quality_rule *r, const ud_batch *input, uint32_t row,
                            int *valid) {
    const ud_quality_rule *rule = &r->config;
    *valid = 1;
    if (rule->kind == UD_RULE_ROW_COUNT)
        return UD_OK;
    int null = 0;
    for (uint32_t j = 0; j < rule->column_count; j++) {
        ud_value v;
        UD_TRY(ud_batch_get(input, row, rule->columns[j], &v));
        null |= v.is_null;
    }
    if (null) {
        if (rule->kind == UD_RULE_NOT_NULL || rule->kind == UD_RULE_PRIMARY_KEY)
            *valid = 0;
        return UD_OK;
    }
    if (rule->kind == UD_RULE_NOT_NULL)
        return UD_OK;
    if (rule->kind == UD_RULE_RANGE) {
        ud_value value;
        UD_TRY(ud_batch_get(input, row, rule->columns[0], &value));
        *valid = (rule->minimum.is_null || ud_value_compare(&value, &rule->minimum) >= 0) &&
                 (rule->maximum.is_null || ud_value_compare(&value, &rule->maximum) <= 0);
    } else if (rule->kind == UD_RULE_REGEX) {
        ud_value value;
        UD_TRY(ud_batch_get(input, row, rule->columns[0], &value));
        int32_t matched = 0;
        UD_TRY(rule->match ? rule->match(rule->match_user, rule->pattern, rule->pattern_length,
                                         value.text, value.text_length, &matched)
                           : ud_regex_test(r->regex, value.text, value.text_length, &matched));
        if (matched != 0 && matched != 1)
            return UD_INVALID;
        *valid = matched;
    } else if (rule->kind == UD_RULE_FOREIGN_KEY) {
        *valid = 0;
        if (!q->reference_ready || !r->reader)
            return UD_BUSY;
        UD_TRY(ud_cache_probe(r->reader, input, row));
        const ud_batch *reference = NULL;
        uint32_t at = 0;
        ud_status found = ud_cache_next(r->reader, &reference, &at);
        if (found != UD_END && found)
            return found;
        *valid = found == UD_OK;
    } else {
        uint64_t hash;
        UD_TRY(key_hash(input, row, rule->columns, rule->column_count, &hash));
        for (seen_key *key = r->buckets ? r->buckets[hash & (r->bucket_count - 1)] : NULL; key;
             key = key->bucket_next) {
            if (ud_is_cancelled(q->ctx))
                return UD_CANCELLED;
            if (key->hash != hash)
                continue;
            uint32_t j = 0;
            for (; j < rule->column_count; j++) {
                ud_value value;
                UD_TRY(ud_batch_get(input, row, rule->columns[j], &value));
                if (ud_value_compare(&value, key->values + j))
                    break;
            }
            if (j == rule->column_count) {
                *valid = 0;
                break;
            }
        }
    }
    return UD_OK;
}
static ud_status record_one_key(ud_quality *q, quality_rule *rule, const ud_batch *input,
                                uint32_t row) {
    int null = 0;
    for (uint32_t j = 0; j < rule->config.column_count; j++) {
        ud_value value;
        UD_TRY(ud_batch_get(input, row, rule->config.columns[j], &value));
        null |= value.is_null;
    }
    if (null)
        return UD_OK;
    if (!q->options.buffers)
        return UD_SCHEMA;
    int absent = 0;
    UD_TRY(check_rule(q, rule, input, row, &absent));
    if (!absent)
        return UD_OK;
    if (!rule->buckets || rule->key_count >= rule->bucket_count / 2) {
        uint32_t capacity = rule->bucket_count ? rule->bucket_count * 2 : 64;
        if (capacity < rule->bucket_count)
            return UD_RESOURCE;
        seen_key **buckets = NULL;
        UD_TRY(ud_buffer_working_allocate(q->options.buffers, (size_t)capacity * sizeof(*buckets),
                                          (void **)&buckets));
        for (seen_key *key = rule->seen; key; key = key->next) {
            uint32_t at = (uint32_t)key->hash & (capacity - 1);
            key->bucket_next = buckets[at];
            buckets[at] = key;
        }
        ud_free(rule->buckets);
        rule->buckets = buckets;
        rule->bucket_count = capacity;
    }
    seen_key *key = NULL;
    UD_TRY(ud_buffer_working_allocate(q->options.buffers, sizeof(*key), (void **)&key));
    ud_status allocation = ud_buffer_working_allocate(
        q->options.buffers, rule->config.column_count * sizeof(*key->values),
        (void **)&key->values);
    if (allocation) {
        ud_free(key);
        return allocation;
    }
    key->next = rule->seen;
    rule->seen = key;
    for (uint32_t j = 0; j < rule->config.column_count; j++) {
        ud_value value;
        UD_TRY(ud_batch_get(input, row, rule->config.columns[j], &value));
        UD_TRY(ud_buffer_working_copy_value(q->options.buffers, &value, key->values + j));
    }
    UD_TRY(key_hash(input, row, rule->config.columns, rule->config.column_count, &key->hash));
    key->bucket_next = rule->buckets[key->hash & (rule->bucket_count - 1)];
    rule->buckets[key->hash & (rule->bucket_count - 1)] = key;
    rule->key_count++;
    return UD_OK;
}
static ud_status record_keys(ud_quality *q, const ud_batch *input, uint32_t row) {
    for (uint32_t r = 0; r < q->count; r++) {
        quality_rule *rule = q->rules + r;
        if (rule->config.kind == UD_RULE_UNIQUE || rule->config.kind == UD_RULE_PRIMARY_KEY)
            UD_TRY(record_one_key(q, rule, input, row));
    }
    return UD_OK;
}
static ud_status quality_check(ud_quality *q) {
    if (ud_is_cancelled(q->ctx))
        return UD_CANCELLED;
    return q->options.check ? q->options.check(q->options.user) : UD_OK;
}
ud_status ud_quality_reference_push(ud_quality *q, const ud_batch *reference) {
    if (!q || !reference || q->reference_ready || q->failed)
        return UD_INVALID;
    UD_TRY(quality_check(q));
    for (uint32_t j = 0; j < q->count; j++)
        if (q->rules[j].reference)
            UD_TRY(ud_cache_append(q->rules[j].reference, reference));
    return UD_OK;
}
ud_status ud_quality_reference_end(ud_quality *q) {
    if (!q || q->failed)
        return UD_INVALID;
    if (q->reference_ready)
        return UD_OK;
    UD_TRY(quality_check(q));
    for (uint32_t j = 0; j < q->count; j++) {
        if (q->rules[j].config.kind == UD_RULE_FOREIGN_KEY && !q->rules[j].reference)
            return UD_INVALID;
        if (!q->rules[j].reference)
            continue;
        UD_TRY(ud_cache_seal(q->rules[j].reference));
        UD_TRY(ud_cache_reader_create(q->rules[j].reference, q->schema, q->rules[j].config.columns,
                                      q->rules[j].config.column_count, &q->rules[j].reader));
    }
    q->reference_ready = 1;
    return UD_OK;
}
ud_status ud_quality_rule_statistics(const ud_quality *q, uint32_t rule,
                                     ud_quality_rule_metrics *out) {
    if (!q || !out || rule >= q->count)
        return UD_INVALID;
    *out = q->rules[rule].metrics;
    if (q->rules[rule].reference) {
        ud_cache_metrics m;
        ud_cache_get_metrics(q->rules[rule].reference, &m);
        out->reference_loaded = m.loaded;
        out->reference_hits = m.hits;
        out->reference_misses = m.misses;
        out->reference_duplicates = m.duplicates;
        out->reference_pages = m.pages;
    }
    return UD_OK;
}
ud_status ud_quality_schema(const ud_quality *q, ud_batch **out) {
    if (!q || !out)
        return UD_INVALID;
    return ud_batch_create(q->ctx, q->accepted_schema->schema->fields, q->accepted_schema->columns,
                           0, out);
}
ud_status ud_quality_apply(ud_quality *q, const ud_batch *input, ud_batch **out,
                           ud_batch **rejected_out, ud_quality_violation **violations_out,
                           uint32_t *count_out) {
    if (!out || (!!rejected_out != !!violations_out) || (!!rejected_out != !!count_out))
        return UD_INVALID;
    *out = NULL;
    if (rejected_out) {
        *rejected_out = NULL;
        *violations_out = NULL;
        *count_out = 0;
    }
    if (!q || q->failed || q->finished || !input || input->columns != q->schema->columns ||
        !q->reference_ready)
        return UD_INVALID;
    for (uint32_t j = 0; j < input->columns; j++)
        if (!ud_field_matches(&input->column[j].field, &q->schema->column[j].field, UD_FIELD_PORT))
            return UD_SCHEMA;
    ud_field *fields = ud_alloc(q->ctx, (size_t)input->columns * sizeof(*fields));
    ud_status status = fields ? UD_OK : UD_RESOURCE;
    for (uint32_t j = 0; !status && j < input->columns; j++)
        fields[j] = q->schema->column[j].field;
    ud_batch *bad = NULL;
    if (!status)
        status = ud_batch_create(q->ctx, q->accepted_schema->schema->fields, input->columns,
                                 input->rows, out);
    if (!status && rejected_out)
        status = ud_batch_create(q->ctx, fields, input->columns, input->rows, &bad);
    ud_quality_violation *violations = NULL;
    uint32_t count = 0, capacity = 0, rejected = 0, ignored = 0;
    for (uint32_t row = 0; !status && row < input->rows; row++) {
        status = quality_check(q);
        int reject = 0, ignore = 0, fail = 0;
        uint32_t rejected_row = rejected;
        for (uint32_t rule = 0; !status && rule < q->count; rule++) {
            quality_rule *r = q->rules + rule;
            if (r->config.kind == UD_RULE_ROW_COUNT)
                continue;
            int valid = 0;
            status = check_rule(q, r, input, row, &valid);
            if (status)
                break;
            r->metrics.evaluated++;
            if (valid)
                continue;
            r->metrics.failed++;
            fail |= r->config.policy == UD_ERROR_FAIL;
            reject |= r->config.policy == UD_ERROR_REDIRECT;
            ignore |= r->config.policy == UD_ERROR_IGNORE;
            if (r->config.policy == UD_ERROR_REDIRECT)
                r->metrics.rejected++;
            if (r->config.policy == UD_ERROR_IGNORE)
                r->metrics.ignored++;
            if (count == capacity) {
                uint32_t next = capacity ? capacity * 2 : 16;
                if (next < capacity ||
                    (q->options.max_violations && count >= q->options.max_violations)) {
                    status = UD_RESOURCE;
                    break;
                }
                if (q->options.max_violations && next > q->options.max_violations)
                    next = q->options.max_violations;
                ud_quality_violation *items = ud_alloc(q->ctx, (size_t)next * sizeof(*items));
                if (!items) {
                    status = UD_RESOURCE;
                    break;
                }
                if (count)
                    memcpy(items, violations, (size_t)count * sizeof(*items));
                ud_free(violations);
                violations = items;
                capacity = next;
            }
            uint32_t column = r->config.columns[0];
            if (r->config.kind == UD_RULE_NOT_NULL || r->config.kind == UD_RULE_PRIMARY_KEY)
                for (uint32_t j = 0; j < r->config.column_count; j++) {
                    ud_value value;
                    status = ud_batch_get(input, row, r->config.columns[j], &value);
                    if (!status && value.is_null) {
                        column = r->config.columns[j];
                        break;
                    }
                }
            violations[count++] = (ud_quality_violation){row, rejected_row, rule, r->config.policy,
                                                         input->column[column].field.identity};
        }
        if (!status && fail)
            status = UD_QUALITY;
        if (!status && reject) {
            if (bad)
                status = ud_batch_append_from(bad, input, row);
            rejected++;
        } else if (!status) {
            status = ud_batch_append_from(*out, input, row);
        }
        if (!status)
            status = record_keys(q, input, row);
        if (ignore)
            ignored++;
    }
    if (!status) {
        q->metrics.processed += input->rows;
        q->metrics.accepted += (*out)->rows;
        q->metrics.rejected += rejected;
        q->metrics.violations += count;
        q->metrics.ignored += ignored;
        if (rejected_out) {
            *rejected_out = bad;
            bad = NULL;
            *violations_out = violations;
            violations = NULL;
            *count_out = count;
        }
    }
    ud_free(fields);
    ud_batch_destroy(bad);
    ud_free(violations);
    if (status) {
        q->failed = 1;
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return status;
}
ud_status ud_quality_finish(ud_quality *q) {
    if (!q || q->failed)
        return UD_INVALID;
    if (q->finished)
        return UD_OK;
    for (uint32_t j = 0; j < q->count; j++)
        if (q->rules[j].config.kind == UD_RULE_ROW_COUNT &&
            q->metrics.accepted != q->rules[j].config.expected_rows) {
            q->failed = 1;
            return ud_fail(q->ctx, UD_QUALITY,
                           "accepted row count differs from reconciliation rule %u", j);
        }
    q->finished = 1;
    return UD_OK;
}
void ud_quality_get_metrics(const ud_quality *q, ud_quality_metrics *out) {
    if (out)
        *out = q ? q->metrics : (ud_quality_metrics){0};
}
static ud_status quality_contract(ud_quality *q, uint8_t digest[32]) {
    ud_bytes bytes = {.ctx = q->ctx};
    uint8_t schema[32];
    ud_status status = ud_schema_fingerprint(q->schema, schema);
    if (!status)
        status = ud_bytes_put(&bytes, schema, 32);
    if (!status)
        status = ud_put_u64(&bytes, q->count);
    for (uint32_t i = 0; !status && i < q->count; i++) {
        ud_quality_rule *r = &q->rules[i].config;
        if (r->match) {
            status = UD_UNSUPPORTED;
            break;
        }
        status = ud_put_string(&bytes, r->id, r->id_length);
        uint64_t values[] = {r->kind, r->rule_version, r->policy, r->column_count,
                             r->expected_rows};
        for (uint32_t k = 0; !status && k < 5; k++)
            status = ud_put_u64(&bytes, values[k]);
        for (uint32_t k = 0; !status && k < r->column_count; k++)
            status = ud_put_u64(&bytes, r->columns[k]);
        if (!status && r->kind == UD_RULE_RANGE) {
            ud_field field = q->schema->column[r->columns[0]].field;
            field.nullable = 1;
            ud_batch *bounds = NULL;
            status = ud_batch_create(q->ctx, &field, 1, 2, &bounds);
            if (!status)
                status = ud_batch_append(bounds, &r->minimum, 1);
            if (!status)
                status = ud_batch_append(bounds, &r->maximum, 1);
            if (!status)
                status = ud_batch_seal(bounds);
            if (!status)
                status = ud_encode_batch(&bytes, bounds);
            ud_batch_destroy(bounds);
        }
        if (!status)
            status = ud_put_string(&bytes, r->pattern, r->pattern_length);
        if (!status && r->kind == UD_RULE_FOREIGN_KEY) {
            ud_batch *reference = NULL;
            status = ud_cache_schema(q->rules[i].reference, &reference);
            if (!status)
                status = ud_schema_fingerprint(reference, schema);
            if (!status)
                status = ud_bytes_put(&bytes, schema, 32);
            for (uint32_t k = 0; !status && k < r->column_count; k++)
                status = ud_put_u64(&bytes, r->reference_columns[k]);
            ud_batch_destroy(reference);
        }
    }
    if (!status)
        ud_hash(bytes.data, bytes.size, digest);
    ud_bytes_clear(&bytes);
    return status;
}
static ud_status rule_metrics_write(ud_bytes *b, const ud_quality_rule_metrics *m) {
    uint64_t values[] = {
        m->evaluated,        m->failed,         m->rejected,         m->ignored,
        m->reference_loaded, m->reference_hits, m->reference_misses, m->reference_duplicates,
        m->reference_pages};
    for (uint32_t i = 0; i < 9; i++)
        UD_TRY(ud_put_u64(b, values[i]));
    return UD_OK;
}
static ud_status rule_metrics_read(ud_bytes *b, ud_quality_rule_metrics *m) {
    uint64_t values[9];
    for (uint32_t i = 0; i < 9; i++)
        UD_TRY(ud_get_u64(b, values + i));
    *m = (ud_quality_rule_metrics){values[0], values[1], values[2], values[3], values[4],
                                   values[5], values[6], values[7], values[8]};
    return UD_OK;
}
ud_status ud_quality_checkpoint(ud_quality *q, ud_batch **out) {
    if (!q || !out || q->failed)
        return UD_INVALID;
    *out = NULL;
    UD_TRY(quality_check(q));
    ud_bytes bytes = {.ctx = q->ctx};
    uint8_t contract[32];
    ud_status status = quality_contract(q, contract);
    if (!status)
        status = ud_bytes_put(&bytes, "UDQUALITY9", 10);
    if (!status)
        status = ud_bytes_put(&bytes, contract, 32);
    uint64_t values[] = {q->reference_ready,  q->finished,         q->metrics.processed,
                         q->metrics.accepted, q->metrics.rejected, q->metrics.violations,
                         q->metrics.ignored};
    for (uint32_t i = 0; !status && i < 7; i++)
        status = ud_put_u64(&bytes, values[i]);
    for (uint32_t i = 0; !status && i < q->count; i++) {
        quality_rule *r = q->rules + i;
        status = rule_metrics_write(&bytes, &r->metrics);
        if (!status)
            status = ud_put_u64(&bytes, r->key_count);
        if (!status && r->key_count) {
            ud_field *fields = ud_alloc(q->ctx, r->config.column_count * sizeof(*fields));
            if (!fields)
                status = UD_RESOURCE;
            for (uint32_t k = 0; fields && k < r->config.column_count; k++)
                fields[k] = q->schema->column[r->config.columns[k]].field;
            ud_batch *chunk = NULL;
            if (!status)
                status = ud_batch_create(q->ctx, fields, r->config.column_count,
                                         q->options.max_batch_rows, &chunk);
            for (seen_key *key = r->seen; !status && key; key = key->next) {
                status = ud_batch_append(chunk, key->values, r->config.column_count);
                if (!status && (chunk->rows == q->options.max_batch_rows || !key->next)) {
                    status = ud_batch_seal(chunk);
                    if (!status)
                        status = ud_encode_batch(&bytes, chunk);
                    ud_batch_destroy(chunk);
                    chunk = NULL;
                    if (!status && key->next)
                        status = ud_batch_create(q->ctx, fields, r->config.column_count,
                                                 q->options.max_batch_rows, &chunk);
                }
            }
            ud_batch_destroy(chunk);
            ud_free(fields);
        }
        if (!status && r->reference) {
            ud_batch *cache = NULL;
            status = ud_cache_checkpoint(r->reference, &cache);
            if (!status)
                status = ud_encode_batch(&bytes, cache);
            ud_batch_destroy(cache);
        }
    }
    if (!status)
        status = ud_snapshot_batch(q->ctx, &bytes, out);
    ud_bytes_clear(&bytes);
    return status;
}
ud_status ud_quality_restore(ud_quality **owner, const ud_batch *state) {
    if (!owner || !*owner)
        return UD_INVALID;
    ud_quality *q = *owner;
    uint32_t rule_count = q->count;
    UD_TRY(quality_check(q));
    ud_bytes bytes;
    UD_TRY(ud_snapshot_bytes(q->ctx, state, &bytes));
    uint8_t magic[10], contract[32], expected[32];
    UD_TRY(ud_bytes_get(&bytes, magic, 10));
    UD_TRY(ud_bytes_get(&bytes, contract, 32));
    UD_TRY(quality_contract(q, expected));
    if (memcmp(magic, "UDQUALITY9", 10) || memcmp(contract, expected, 32))
        return UD_FENCED;
    ud_quality_rule *rules = ud_alloc(q->ctx, q->count * sizeof(*rules));
    ud_batch **references = ud_alloc(q->ctx, q->count * sizeof(*references));
    ud_status status = rules && references ? UD_OK : UD_RESOURCE;
    for (uint32_t i = 0; !status && i < q->count; i++) {
        rules[i] = q->rules[i].config;
        if (q->rules[i].reference) {
            status = ud_cache_schema(q->rules[i].reference, references + i);
            rules[i].reference = references[i];
        }
    }
    ud_quality *fresh = NULL;
    if (!status)
        status = ud_quality_create(q->ctx, q->schema->schema->fields, q->schema->columns, rules,
                                   q->count, &q->options, &fresh);
    uint64_t values[7] = {0};
    for (uint32_t i = 0; !status && i < 7; i++)
        status = ud_get_u64(&bytes, values + i);
    if (!status && (values[0] > 1 || values[1] > 1 || values[3] > values[2] ||
                    values[4] > values[2] || values[3] != values[2] - values[4]))
        status = UD_CORRUPT;
    for (uint32_t i = 0; !status && i < q->count; i++) {
        quality_rule *r = fresh->rules + i;
        status = rule_metrics_read(&bytes, &r->metrics);
        uint64_t count = 0;
        if (!status)
            status = ud_get_u64(&bytes, &count);
        if (!status && count && r->config.kind != UD_RULE_UNIQUE &&
            r->config.kind != UD_RULE_PRIMARY_KEY)
            status = UD_CORRUPT;
        uint32_t *columns = ud_alloc(q->ctx, r->config.column_count * sizeof(*columns));
        if (r->config.column_count && !columns && !status)
            status = UD_RESOURCE;
        for (uint32_t k = 0; columns && k < r->config.column_count; k++)
            columns[k] = k;
        while (!status && r->key_count < count) {
            ud_batch *chunk = NULL;
            status = ud_decode_batch(&bytes, &chunk);
            if (!status && (!chunk->rows || chunk->columns != r->config.column_count ||
                            chunk->rows > count - r->key_count))
                status = UD_CORRUPT;
            for (uint32_t k = 0; !status && k < chunk->columns; k++)
                if (!ud_field_matches(&chunk->column[k].field,
                                      &fresh->schema->column[r->config.columns[k]].field,
                                      UD_FIELD_PORT))
                    status = UD_CORRUPT;
            const uint32_t *original = r->config.columns;
            r->config.columns = columns;
            for (uint32_t row = 0; !status && row < chunk->rows; row++) {
                uint64_t before = r->key_count;
                status = record_one_key(fresh, r, chunk, row);
                if (!status && r->key_count != before + 1)
                    status = UD_CORRUPT;
            }
            r->config.columns = original;
            ud_batch_destroy(chunk);
        }
        ud_free(columns);
        if (!status && r->reference) {
            ud_batch *cache = NULL;
            status = ud_decode_batch(&bytes, &cache);
            if (!status)
                status = ud_cache_restore(r->reference, cache);
            ud_batch_destroy(cache);
            if (!status && values[0])
                status = ud_cache_reader_create(r->reference, fresh->schema, r->config.columns,
                                                r->config.column_count, &r->reader);
        }
    }
    if (!status && bytes.pos != bytes.size)
        status = UD_CORRUPT;
    if (!status) {
        fresh->reference_ready = (int)values[0];
        fresh->finished = (int)values[1];
        fresh->metrics =
            (ud_quality_metrics){values[2], values[3], values[4], values[5], values[6]};
        *owner = fresh;
        fresh = NULL;
        ud_quality_destroy(q);
    }
    ud_quality_destroy(fresh);
    for (uint32_t i = 0; references && i < rule_count; i++)
        ud_batch_destroy(references[i]);
    ud_free(references);
    ud_free(rules);
    return status;
}

int32_t ud_quality_reference_ready(const ud_quality *q) {
    return q ? q->reference_ready : 0;
}
int32_t ud_quality_is_finished(const ud_quality *q) {
    return q ? q->finished : 0;
}
