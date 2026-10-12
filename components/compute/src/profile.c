#include "ud/profile.h"
#include "../internal.h"
struct ud_profile {
    ud_context *ctx;
    ud_batch *schema;
    ud_profile_statistic *statistics;
    ud_value *values;
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
    ud_field *fields = ud_alloc(ctx, (columns + 1) * sizeof(*fields));
    ud_status s = !p->statistics || !p->values || !fields ? UD_RESOURCE : UD_OK;
    for (uint32_t i = 0; !s && i < columns; i++)
        s = ud_batch_field(schema, i, fields + i);
    if (!s)
        s = ud_batch_create(ctx, fields, columns, 0, &p->schema);
    for (uint32_t i = 0; !s && i < count; i++) {
        const ud_profile_statistic *stat = statistics + i;
        if (stat->struct_size < sizeof(*stat) || stat->abi_version != UD_ABI_VERSION)
            s = UD_INCOMPATIBLE;
        else if (stat->kind < UD_PROFILE_ROWS || stat->kind > UD_PROFILE_MAXIMUM ||
                 (stat->kind != UD_PROFILE_ROWS && stat->column >= columns))
            s = UD_SCHEMA;
        if (s)
            break;
        p->statistics[i] = *stat;
        p->values[i].type = UD_INT64;
        if (stat->kind >= UD_PROFILE_MINIMUM) {
            ud_type type = fields[stat->column].type;
            if (type < UD_BOOL || type > UD_UTF8) {
                s = UD_UNSUPPORTED;
                break;
            }
            p->values[i].type = type;
            p->values[i].is_null = 1;
        }
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
ud_status ud_profile_add(ud_profile *p, const ud_batch *batch) {
    if (!p || !batch || p->failed)
        return UD_INVALID;
    ud_status s = UD_OK;
    uint32_t columns = ud_batch_columns(batch), rows = ud_batch_rows(batch);
    if (columns != ud_batch_columns(p->schema))
        s = UD_SCHEMA;
    for (uint32_t i = 0; !s && i < columns; i++) {
        ud_field expected, actual;
        s = ud_batch_field(p->schema, i, &expected);
        if (!s)
            s = ud_batch_field(batch, i, &actual);
        if (!s && !ud_field_equal(&expected, &actual, 1))
            s = UD_SCHEMA;
    }
    if (!s && rows > p->maximum - p->rows)
        s = UD_RESOURCE;
    for (uint32_t row = 0; !s && row < rows; row++) {
        if (ud_is_cancelled(p->ctx))
            s = UD_CANCELLED;
        if (!s && p->check)
            s = p->check(p->user);
        for (uint32_t i = 0; !s && i < p->count; i++) {
            const ud_profile_statistic *stat = p->statistics + i;
            ud_value value = {0}, *result = p->values + i;
            if (stat->kind == UD_PROFILE_ROWS) {
                result->integer++;
                continue;
            }
            s = ud_batch_get(batch, row, stat->column, &value);
            if (s)
                break;
            if (stat->kind == UD_PROFILE_NULLS) {
                result->integer += !!value.is_null;
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
ud_status ud_profile_get(const ud_profile *p, uint32_t statistic, ud_value *out) {
    if (!p || !out || p->failed || statistic >= p->count)
        return UD_INVALID;
    *out = p->values[statistic];
    return UD_OK;
}
