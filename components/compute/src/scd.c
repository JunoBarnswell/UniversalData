#include "relational_internal.h"
ud_status ud_rel_scd_bind(ud_relational *r) {
    if (!r->inputs[1])
        return UD_SCHEMA;
    UD_TRY(ud_rel_keys(r, 0, ud_rel_member(r->config, "businessKeys")));
    UD_TRY(ud_rel_keys(r, 1, ud_rel_member(r->config, "dimensionKeys")));
    UD_TRY(ud_rel_require_order(r, 0, 1));
    UD_TRY(ud_rel_selector(r->inputs[0], ud_rel_member(r->config, "effectiveTime"), &r->effective));
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
    UD_TRY(ud_rel_selector(r->inputs[1], ud_rel_member(r->config, "current"), &r->current));
    UD_TRY(ud_rel_selector(r->inputs[1], ud_rel_member(r->config, "validFrom"), &r->valid_from));
    UD_TRY(ud_rel_selector(r->inputs[1], ud_rel_member(r->config, "validTo"), &r->valid_to));
    UD_TRY(ud_rel_selector(r->inputs[1], ud_rel_member(r->config, "surrogate"), &r->surrogate));
    r->version = UINT32_MAX;
    if (ud_rel_member(r->config, "version"))
        UD_TRY(ud_rel_selector(r->inputs[1], ud_rel_member(r->config, "version"), &r->version));
    if (r->inputs[1]->column[r->current].field.type != UD_BOOL ||
        r->inputs[1]->column[r->valid_from].field.type != UD_TIMESTAMP ||
        r->inputs[1]->column[r->valid_to].field.type != UD_TIMESTAMP ||
        !r->inputs[1]->column[r->valid_to].field.nullable ||
        (r->version != UINT32_MAX && r->inputs[1]->column[r->version].field.type != UD_INT64))
        return UD_SCHEMA;
    r->value_column = UINT32_MAX;
    if (ud_rel_member(r->config, "surrogateSource"))
        UD_TRY(ud_rel_selector(r->inputs[0], ud_rel_member(r->config, "surrogateSource"),
                               &r->value_column));
    if (r->value_column == UINT32_MAX && r->inputs[1]->column[r->surrogate].field.type != UD_UTF8)
        return UD_SCHEMA;
    if (r->value_column != UINT32_MAX &&
        !ud_field_matches(&r->inputs[0]->column[r->value_column].field,
                          &r->inputs[1]->column[r->surrogate].field, UD_FIELD_VALUE_TYPE))
        return UD_SCHEMA;
    const ud_json *mapping = ud_rel_member(r->config, "mapping");
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
            s = ud_rel_selector(r->inputs[0], ud_rel_member(m, "source"), r->mapping + i);
        if (!s)
            s = ud_rel_selector(r->inputs[1], ud_rel_member(m, "target"), r->attributes + i);
        r->attribute_kind[i] = ud_rel_named(ud_rel_member(m, "change"), "key")     ? 0
                               : ud_rel_named(ud_rel_member(m, "change"), "type1") ? 1
                               : ud_rel_named(ud_rel_member(m, "change"), "type2") ? 2
                               : ud_rel_named(ud_rel_member(m, "change"), "fixed") ? 3
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
        s = ud_rel_clone(r->ctx, r->schema[0], 0, r->schema + i);
    if (!s)
        s = ud_rel_clone(r->ctx, r->inputs[0], 0, &r->schema[5]);
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
    ud_status s = ud_rel_clone(r->ctx, r->schema[port], 1, out);
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
ud_status ud_rel_scd_pull(ud_relational *r, ud_batch **out, uint32_t *port) {
    if (r->pending) {
        *out = r->pending;
        r->pending = NULL;
        *port = 4;
        return UD_OK;
    }
    const ud_batch *b;
    uint32_t row;
    UD_TRY(ud_rel_peek(r, 0, &b, &row));
    if (!r->reader)
        return UD_INVALID;
    UD_TRY(ud_rel_control(r));
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
    if (!r->active || !ud_rel_same_group(r, 0, b, row)) {
        UD_TRY(ud_rel_set_group(r, b, row));
        r->active = 1;
        ud_batch_destroy(r->current_row);
        r->current_row = NULL;
        UD_TRY(ud_cache_probe(r->reader, b, row));
        const ud_batch *dim = NULL;
        uint32_t dr = 0;
        ud_status next = ud_cache_next(r->reader, &dim, &dr);
        if (!next) {
            UD_TRY(ud_rel_clone(r->ctx, r->inputs[1], 1, &r->current_row));
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
            UD_TRY(ud_rel_take(r, 0));
            return ud_rel_scd_pull(r, out, port);
        }
        UD_TRY(ud_rel_clone(r->ctx, b, 1, out));
        UD_TRY(ud_batch_append_from(*out, b, row));
        UD_TRY(ud_batch_seal(*out));
        *port = 5;
        return ud_rel_take(r, 0);
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
            s = ud_rel_clone(r->ctx, r->inputs[1], 1, &replacement);
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
        s = ud_rel_take(r, 0);
    return s;
}
