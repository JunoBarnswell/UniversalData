#include "ud/profile.h"
#include "../internal.h"
#include "sketch.h"
#include <math.h>
typedef struct top_entry {
    ud_value value;
    uint64_t count, error;
} top_entry;
typedef struct column_profile {
    uint64_t nulls, count, lengths[32], histogram[132], length_sum, length_min, length_max;
    ud_value_set distinct;
    uint8_t cardinality[UD_HLL_REGISTERS];
    ud_value minimum, maximum;
    top_entry top[32];
    uint32_t top_count;
    ud_id anomalies[8];
    uint32_t anomaly_count;
    double mean, m2;
    uint64_t numeric_count;
} column_profile;
struct ud_profile {
    ud_context *ctx;
    ud_batch *schema;
    ud_profile_statistic *statistics;
    ud_value *values;
    column_profile *columns;
    uint32_t column_count;
    uint32_t count;
    uint64_t rows, maximum;
    ud_profile_check_fn check;
    void *user;
    int failed;
};
void ud_profile_destroy(ud_profile *p) {
    if (!p)
        return;
    for (uint32_t i = 0; i < p->count; i++)
        ud_clear_value(p->values + i);
    for (uint32_t i = 0; p->columns && i < p->column_count; i++) {
        column_profile *c = p->columns + i;
        ud_value_set_clear(&c->distinct);
        ud_clear_value(&c->minimum);
        ud_clear_value(&c->maximum);
        for (uint32_t k = 0; k < c->top_count; k++)
            ud_clear_value(&c->top[k].value);
    }
    ud_free(p->columns);
    ud_free(p->values);
    ud_free(p->statistics);
    ud_batch_destroy(p->schema);
    ud_free(p);
}
ud_status ud_profile_create(ud_context *ctx, const ud_batch *schema,
                            const ud_profile_statistic *statistics, uint32_t count,
                            uint64_t maximum, ud_profile_check_fn check, void *user,
                            ud_profile **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!ctx || !schema || !statistics || !count || count > 256 || !maximum || maximum > INT64_MAX)
        return UD_INVALID;
    ud_profile *p = ud_alloc(ctx, sizeof(*p));
    if (!p)
        return UD_RESOURCE;
    p->ctx = ctx;
    p->maximum = maximum;
    p->check = check;
    p->user = user;
    p->statistics = ud_alloc(ctx, count * sizeof(*p->statistics));
    p->values = ud_alloc(ctx, count * sizeof(*p->values));
    uint32_t columns = ud_batch_columns(schema);
    p->column_count = columns;
    p->columns = ud_alloc(ctx, (size_t)columns * sizeof(*p->columns));
    ud_field *fields = ud_alloc(ctx, (columns + 1) * sizeof(*fields));
    ud_status s = !p->statistics || !p->values || !fields || !p->columns ? UD_RESOURCE : UD_OK;
    for (uint32_t i = 0; !s && i < columns; i++)
        s = ud_batch_field(schema, i, fields + i);
    if (!s)
        s = ud_batch_create(ctx, fields, columns, 0, &p->schema);
    for (uint32_t i = 0; !s && i < count; i++) {
        const ud_profile_statistic *stat = statistics + i;
        if (stat->struct_size != sizeof(*stat) || stat->abi_version != UD_ABI_VERSION)
            s = UD_INCOMPATIBLE;
        else if (stat->kind < UD_PROFILE_ROWS || stat->kind > UD_PROFILE_REPORT ||
                 (stat->kind != UD_PROFILE_ROWS && stat->column >= columns))
            s = UD_SCHEMA;
        if (s)
            break;
        p->statistics[i] = *stat;
        p->values[i].type = UD_INT64;
        if (stat->kind == UD_PROFILE_MINIMUM || stat->kind == UD_PROFILE_MAXIMUM) {
            ud_type type = fields[stat->column].type;
            if (type < UD_BOOL || type > UD_MAP) {
                s = UD_UNSUPPORTED;
                break;
            }
            p->values[i].type = type;
            p->values[i].is_null = 1;
        }
        if (stat->kind >= UD_PROFILE_TOPK) p->values[i].type = UD_UTF8;
        p->count++;
    }
    ud_free(fields);
    if (s) {
        ud_profile_destroy(p);
        return s;
    }
    *out = p;
    return UD_OK;
}
static uint32_t length_bucket(uint64_t n) {
    uint32_t b = 0;
    while (n > 1 && b < 31) {
        n >>= 1;
        b++;
    }
    return b;
}
static ud_status profile_value(ud_profile *p, const ud_batch *batch, uint32_t row,
                               uint32_t column) {
    column_profile *c = p->columns + column;
    ud_value v;
    UD_TRY(ud_batch_get(batch, row, column, &v));
    if (v.is_null) {
        c->nulls++;
        return UD_OK;
    }
    c->count++;
    ud_hll_add(c->cardinality,&v);
    c->distinct.ctx = p->ctx;
    c->distinct.maximum = p->maximum > UINT32_MAX ? UINT32_MAX : (uint32_t)p->maximum;
    int added;
    UD_TRY(ud_value_set_add(&c->distinct, &v, &added));
    if (c->count == 1 || ud_value_compare(&v, &c->minimum) < 0) {
        ud_clear_value(&c->minimum);
        UD_TRY(ud_copy_value(p->ctx, &v, &c->minimum, 0));
    }
    if (c->count == 1 || ud_value_compare(&v, &c->maximum) > 0) {
        ud_clear_value(&c->maximum);
        UD_TRY(ud_copy_value(p->ctx, &v, &c->maximum, 0));
    }
    uint64_t length = v.type == UD_UTF8 || v.type == UD_BINARY ? v.text_length : v.child_count;
    if (v.type == UD_UTF8) {
        length = 0;
        for (uint32_t i = 0; i < v.text_length; i++)
            length += ((unsigned char)v.text[i] & 192) != 128;
    }
    if (v.type == UD_UTF8 || v.type == UD_BINARY || v.type >= UD_STRUCT) {
        if (UINT64_MAX - c->length_sum < length)
            return UD_OVERFLOW;
        c->length_sum += length;
        if (c->count == 1 || length < c->length_min)
            c->length_min = length;
        if (length > c->length_max)
            c->length_max = length;
        c->lengths[length_bucket(length)]++;
    }
    uint32_t at = 0;
    for (; at < c->top_count; at++)
        if (!ud_value_compare(&v, &c->top[at].value))
            break;
    if (at < c->top_count)
        c->top[at].count++;
    else {
        uint64_t error = 0;
        if (c->top_count < 32)
            at = c->top_count++;
        else {
            at = 0;
            for (uint32_t i = 1; i < 32; i++)
                if (c->top[i].count < c->top[at].count ||
                    (c->top[i].count == c->top[at].count &&
                     ud_value_compare(&c->top[i].value, &c->top[at].value) > 0))
                    at = i;
            error = c->top[at].count;
            ud_clear_value(&c->top[at].value);
        }
        UD_TRY(ud_copy_value(p->ctx, &v, &c->top[at].value, 0));
        c->top[at].error = error;
        c->top[at].count = error + 1;
    }
    int numeric = (v.type >= UD_INT64 && v.type <= UD_TIMESTAMP && v.type != UD_UTF8) ||
                  v.type == UD_UINT64 || v.type == UD_DURATION;
    if (batch->column[column].field.dictionary_kind)
        numeric = 0;
    if (numeric) {
        double x = v.type == UD_FLOAT64  ? v.real
                   : v.type == UD_UINT64 ? (double)v.unsigned_integer
                                         : (double)v.integer;
        if (v.type == UD_DECIMAL128) {
            char text[160];
            uint32_t length;
            ud_value approximated;
            UD_TRY(ud_value_format(&v, text, sizeof(text), &length));
            UD_TRY(ud_value_parse(p->ctx, UD_FLOAT64, text, length - 1, &approximated));
            x = approximated.real;
        }
        if (v.type == UD_DECIMAL128) {
            char text[128];
            uint32_t n;
            UD_TRY(ud_value_format(&v, text, sizeof(text), &n));
            x = strtod(text, NULL);
        }
        int anomalous = !isfinite(x);
        uint32_t bin = 0;
        if (isfinite(x)) {
            int exponent = 0;
            frexp(fabs(x), &exponent);
            if (exponent < -32)
                exponent = -32;
            if (exponent > 32)
                exponent = 32;
            bin = x == 0 ? 0 : 1 + (uint32_t)(exponent + 32) + (x < 0 ? 65 : 0);
            if (c->numeric_count > 31 && c->m2 > 0)
                anomalous = fabs(x - c->mean) > 6 * sqrt(c->m2 / (c->numeric_count - 1));
            c->numeric_count++;
            double d = x - c->mean;
            c->mean += d / c->numeric_count;
            c->m2 += d * (x - c->mean);
        } else
            bin = 131;
        c->histogram[bin]++;
        if (anomalous) {
            ud_row_info info;
            UD_TRY(ud_batch_row_info(batch, row, &info));
            if (info.id.high || info.id.low) {
                char id[33], prior[33];
                ud_id_format(info.id, id);
                uint32_t i = 0;
                for (; i < c->anomaly_count; i++) {
                    ud_id_format(c->anomalies[i], prior);
                    if (strcmp(id, prior) <= 0)
                        break;
                }
                if (i < 8) {
                    uint32_t n = c->anomaly_count < 8 ? c->anomaly_count++ : 7;
                    while (n > i) {
                        c->anomalies[n] = c->anomalies[n - 1];
                        n--;
                    }
                    c->anomalies[i] = info.id;
                }
            }
        }
    }
    return UD_OK;
}
ud_status ud_profile_add(ud_profile *p, const ud_batch *batch) {
    if (!p || !batch || p->failed)
        return UD_INVALID;
    if (ud_batch_has_absent(batch))
        return UD_INCOMPATIBLE;
    for (uint32_t at = 0; at < p->count; at++)
        if (p->statistics[at].kind >= UD_PROFILE_TOPK) { ud_clear_value(p->values + at); p->values[at].type = UD_UTF8; }
    ud_status s = UD_OK;
    uint32_t columns = ud_batch_columns(batch), rows = ud_batch_rows(batch);
    if (columns != ud_batch_columns(p->schema))
        s = UD_SCHEMA;
    for (uint32_t i = 0; !s && i < columns; i++) {
        ud_field expected, actual;
        s = ud_batch_field(p->schema, i, &expected);
        if (!s)
            s = ud_batch_field(batch, i, &actual);
        if (!s && !ud_field_matches(&expected, &actual, 1))
            s = UD_SCHEMA;
    }
    if (!s && rows > p->maximum - p->rows)
        s = UD_RESOURCE;
    for (uint32_t row = 0; !s && row < rows; row++) {
        if (ud_is_cancelled(p->ctx))
            s = UD_CANCELLED;
        if (!s && p->check)
            s = p->check(p->user);
        for (uint32_t col = 0; !s && col < columns; col++)
            s = profile_value(p, batch, row, col);
        for (uint32_t i = 0; !s && i < p->count; i++) {
            const ud_profile_statistic *stat = p->statistics + i;
            ud_value value = {0}, *result = p->values + i;
            if (stat->kind == UD_PROFILE_ROWS) {
                result->integer++;
                continue;
            }
            if (stat->kind >= UD_PROFILE_TOPK) continue;
            s = ud_batch_get(batch, row, stat->column, &value);
            if (s)
                break;
            if (stat->kind == UD_PROFILE_NULLS) {
                result->integer += !!value.is_null;
                continue;
            }
            if (stat->kind == UD_PROFILE_DISTINCT) {
                result->integer = (int64_t)p->columns[stat->column].distinct.count;
                continue;
            }
            if (value.is_null)
                continue;
            int comparison = result->is_null ? 0 : ud_value_compare(&value, result);
            if (result->is_null || (stat->kind == UD_PROFILE_MINIMUM && comparison < 0) ||
                (stat->kind == UD_PROFILE_MAXIMUM && comparison > 0)) {
                ud_value copy = {0};
                s = ud_copy_value(p->ctx, &value, &copy, 0);
                if (!s) {
                    ud_clear_value(result);
                    *result = copy;
                }
            }
        }
    }
    if (s)
        p->failed = 1;
    else
        p->rows += rows;
    return s;
}
ud_status ud_profile_get(ud_profile *p, uint32_t statistic, ud_value *out) {
    if (!p || !out || p->failed || statistic >= p->count) return UD_INVALID;
    if (ud_is_cancelled(p->ctx)) return UD_CANCELLED;
    if (p->check) UD_TRY(p->check(p->user));
    const ud_profile_statistic *stat = p->statistics + statistic;
    ud_value *value = p->values + statistic;
    if (stat->kind >= UD_PROFILE_TOPK && !value->text) {
        char *json = NULL; uint32_t length = 0;
        UD_TRY(ud_profile_report(p,stat->column,&json,&length));
        if (stat->kind != UD_PROFILE_REPORT) {
            const char *key = stat->kind == UD_PROFILE_TOPK ? "topK" : stat->kind == UD_PROFILE_HISTOGRAM ? "histogram" :
                stat->kind == UD_PROFILE_LENGTH_DISTRIBUTION ? "lengths" : stat->kind == UD_PROFILE_APPROXIMATE_CARDINALITY ? "approximateCardinality" : "anomalies";
            ud_json *document = NULL;
            ud_status status = ud_json_parse(p->ctx,json,length,&document,NULL);
            ud_free(json); json = NULL;
            if (!status) status = ud_json_serialize(ud_json_find(document,key,(uint32_t)strlen(key)),&json,&length);
            ud_json_destroy(document);
            if (status) { ud_free(json); return status; }
        }
        *value = (ud_value){.type = UD_UTF8,.text = json,.text_length = length};
    }
    *out = *value; return UD_OK;
}
static ud_status json_number(ud_bytes *b, uint64_t n) {
    char text[32];
    int length = snprintf(text, sizeof(text), "%llu", (unsigned long long)n);
    return ud_bytes_put(b, text, (size_t)length);
}
static ud_status report_value(ud_bytes *b, const ud_field *f, const ud_value *v, int empty) {
    ud_value value = empty ? (ud_value){.type = f->type, .is_null = 1} : *v;
    return ud_json_value_encode(b, f, &value);
}
ud_status ud_profile_report(const ud_profile *p, uint32_t column, char **out, uint32_t *length) {
    if (!p || !out || !length || p->failed || column >= p->schema->columns)
        return UD_INVALID;
    *out = NULL;
    *length = 0;
    const column_profile *c = p->columns + column;
    const ud_field *f = &p->schema->column[column].field;
    ud_bytes b = {.ctx = p->ctx, .limit = 16u * 1024u * 1024u};
    char header[400];
    int n = snprintf(header, sizeof(header),
                     "{\"rows\":%llu,\"nulls\":%llu,\"distinct\":{\"count\":%llu,\"method\":"
                     "\"ExactNativeValueSet\"},\"minimum\":",
                     (unsigned long long)p->rows, (unsigned long long)c->nulls,
                     (unsigned long long)c->distinct.count);
    ud_status s = ud_bytes_put(&b, header, (size_t)n);
    if (!s) s = report_value(&b, f, &c->minimum, !c->count);
    if (!s)
        s = ud_bytes_put(&b, ",\"maximum\":", 11);
    if (!s)
        s = report_value(&b, f, &c->maximum, !c->count);
    char approximation[192];
    int approximation_size = snprintf(approximation,sizeof(approximation),
        ",\"approximateCardinality\":{\"count\":%llu,\"method\":\"HLL1024\",\"relativeStandardError\":0.0325}",
        (unsigned long long)ud_hll_count(c->cardinality));
    if (!s) s = ud_bytes_put(&b,approximation,(size_t)approximation_size);
    const char *h = ",\"histogram\":{\"method\":\"signedLog2Approximate\",\"buckets\":[";
    if (!s)
        s = ud_bytes_put(&b, h, strlen(h));
    for (uint32_t i = 0; !s && i < 132; i++) {
        if (i)
            s = ud_bytes_put(&b, ",", 1);
        if (!s)
            s = json_number(&b, c->histogram[i]);
    }
    h = "]},\"lengths\":{\"unit\":\"UnicodeCodepointsOrBinaryBytesOrChildren\",\"sum\":";
    if (!s)
        s = ud_bytes_put(&b, h, strlen(h));
    if (!s)
        s = json_number(&b, c->length_sum);
    if (!s)
        s = ud_bytes_put(&b, ",\"minimum\":", 11);
    if (!s)
        s = json_number(&b, c->length_min);
    if (!s)
        s = ud_bytes_put(&b, ",\"maximum\":", 11);
    if (!s)
        s = json_number(&b, c->length_max);
    if (!s)
        s = ud_bytes_put(&b, ",\"log2Buckets\":[", 16);
    for (uint32_t i = 0; !s && i < 32; i++) {
        if (i)
            s = ud_bytes_put(&b, ",", 1);
        if (!s)
            s = json_number(&b, c->lengths[i]);
    }
    h = "]},\"topK\":{\"method\":\"SpaceSaving32\",\"values\":[";
    if (!s)
        s = ud_bytes_put(&b, h, strlen(h));
    uint32_t order[32];
    for (uint32_t i = 0; i < c->top_count; i++) {
        uint32_t j = i;
        while (j && (c->top[order[j - 1]].count < c->top[i].count ||
                     (c->top[order[j - 1]].count == c->top[i].count &&
                      ud_value_compare(&c->top[order[j - 1]].value, &c->top[i].value) > 0))) {
            order[j] = order[j - 1];
            j--;
        }
        order[j] = i;
    }
    for (uint32_t k = 0; !s && k < c->top_count; k++) {
        uint32_t i = order[k];
        if (k)
            s = ud_bytes_put(&b, ",", 1);
        if (!s)
            s = ud_bytes_put(&b, "{\"value\":", 9);
        if (!s)
            s = ud_json_value_encode(&b, f, &c->top[i].value);
        if (!s)
            s = ud_bytes_put(&b, ",\"count\":", 9);
        if (!s)
            s = json_number(&b, c->top[i].count);
        if (!s)
            s = ud_bytes_put(&b, ",\"error\":", 9);
        if (!s)
            s = json_number(&b, c->top[i].error);
        if (!s)
            s = ud_bytes_put(&b, "}", 1);
    }
    h = "]},\"anomalies\":{\"method\":\"nonfiniteOrOnlineSixSigma\",\"sampleRowIds\":[";
    if (!s)
        s = ud_bytes_put(&b, h, strlen(h));
    for (uint32_t i = 0; !s && i < c->anomaly_count; i++) {
        char id[33];
        ud_id_format(c->anomalies[i], id);
        if (i)
            s = ud_bytes_put(&b, ",", 1);
        if (!s)
            s = ud_json_quote(&b, id, 32);
    }
    if (!s)
        s = ud_bytes_put(&b, "]}}", 3);
    if (!s) {
        *length = (uint32_t)b.size;
        s = ud_bytes_put(&b, "", 1);
    }
    if (!s) {
        *out = (char *)b.data;
        b.data = NULL;
    }
    ud_bytes_clear(&b);
    return s;
}
