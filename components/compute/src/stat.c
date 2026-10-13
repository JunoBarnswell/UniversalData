#include "stat.h"
static void put64(uint8_t *p, uint64_t n) {
    for (uint32_t i = 0; i < 8; i++)
        p[i] = (uint8_t)(n >> (i * 8));
}
static uint64_t get64(const uint8_t *p) {
    uint64_t n = 0;
    for (uint32_t i = 0; i < 8; i++)
        n |= (uint64_t)p[i] << (i * 8);
    return n;
}
ud_status ud_stat_add(ud_aggregate_state *s, const ud_aggregate_def *d, const ud_value *v) {
    if (v->is_null)
        return UD_OK;
    if (d->distinct) {
        s->distinct.ctx = s->ctx;
        s->distinct.maximum = d->maximum_distinct;
        int added;
        UD_TRY(ud_value_set_add(&s->distinct, v, &added));
        if (!added)
            return UD_OK;
    }
    if (d->kind == 5) {
        if (!s->hll) {
            s->hll = ud_alloc(s->ctx, UD_HLL_REGISTERS);
            if (!s->hll)
                return UD_RESOURCE;
        }
        ud_hll_add(s->hll, v);
        return UD_OK;
    }
    if (d->kind == 1 || d->kind == 4)
        return ud_sum_update(&s->sum, v, 1);
    if (s->sum.count == INT64_MAX)
        return UD_OVERFLOW;
    s->sum.count++;
    if (d->kind == 2 || d->kind == 3) {
        if (s->sum.count == 1 || (d->kind == 2 ? ud_value_compare(v, &s->extremum) < 0
                                               : ud_value_compare(v, &s->extremum) > 0)) {
            ud_clear_value(&s->extremum);
            return ud_copy_value(s->ctx, v, &s->extremum, 0);
        }
    }
    return UD_OK;
}
ud_status ud_stat_result(const ud_aggregate_state *s, const ud_aggregate_def *d, const ud_field *f,
                         ud_value *out) {
    *out = (ud_value){.type = f->type, .precision = f->precision, .scale = f->scale};
    if (d->kind == 0) {
        out->integer = (int64_t)s->sum.count;
        return UD_OK;
    }
    if (d->kind == 5) {
        out->integer = s->hll ? (int64_t)ud_hll_count(s->hll) : 0;
        return UD_OK;
    }
    if (d->kind == 1 || d->kind == 4)
        return ud_sum_result(&s->sum, f, d->kind == 4, d->rounding, out);
    if (!s->sum.count) {
        out->is_null = 1;
        return UD_OK;
    }
    *out = s->extremum;
    return UD_OK;
}
ud_status ud_stat_encode(const ud_aggregate_state *s, const ud_aggregate_def *d, const ud_field *f,
                         ud_value *out) {
    ud_field field = *f;
    field.nullable = 1;
    field.key_order = 0;
    uint32_t count = d->distinct                                      ? s->distinct.count
                     : (d->kind == 2 || d->kind == 3) && s->sum.count ? 1
                                                                      : 0;
    ud_batch *payload = NULL;
    UD_TRY(ud_batch_create(s->ctx, &field, 1, count, &payload));
    ud_status status = UD_OK;
    for (uint32_t i = 0; !status && i < count; i++)
        status =
            ud_batch_append(payload, d->distinct ? &s->distinct.entries[i].value : &s->extremum, 1);
    if (!status)
        status = ud_batch_seal(payload);
    uint64_t wire = 0;
    uint8_t *encoded = NULL;
    if (!status)
        status = ud_batch_serialize(payload, &encoded, &wire);
    if (!status && wire > UINT32_MAX - 64 - UD_HLL_REGISTERS)
        status = UD_RESOURCE;
    size_t offset = 64 + (d->kind == 5 ? UD_HLL_REGISTERS : 0);
    uint8_t *bytes = !status ? ud_alloc(s->ctx, offset + wire) : NULL;
    if (!status && !bytes)
        status = UD_RESOURCE;
    if (!status) {
        memcpy(bytes, "UDAGG9\0", 8);
        bytes[8] = (uint8_t)d->kind;
        bytes[9] = (uint8_t)d->distinct;
        put64(bytes + 16, s->sum.count);
        for (uint32_t i = 0; i < 4; i++)
            put64(bytes + 24 + i * 8, s->sum.words[i]);
        uint64_t real;
        memcpy(&real, &s->sum.real, 8);
        put64(bytes + 56, real);
        if (s->hll)
            memcpy(bytes + 64, s->hll, UD_HLL_REGISTERS);
        memcpy(bytes + offset, encoded, (size_t)wire);
    }
    ud_batch_destroy(payload);
    ud_free(encoded);
    if (status) {
        ud_free(bytes);
        return status;
    }
    *out = (ud_value){
        .type = UD_BINARY, .text = (const char *)bytes, .text_length = (uint32_t)(offset + wire)};
    return UD_OK;
}
ud_status ud_stat_merge(ud_aggregate_state *s, const ud_aggregate_def *d, const ud_field *f,
                        const ud_value *v) {
    size_t offset = 64 + (d->kind == 5 ? UD_HLL_REGISTERS : 0);
    if (v->type != UD_BINARY || v->is_null || v->text_length <= offset)
        return UD_CORRUPT;
    const uint8_t *b = (const uint8_t *)v->text;
    if (memcmp(b, "UDAGG9\0", 8) || b[8] != d->kind || b[9] != d->distinct)
        return UD_CORRUPT;
    for (uint32_t i = 10; i < 16; i++)
        if (b[i])
            return UD_CORRUPT;
    ud_batch *payload = NULL;
    UD_TRY(ud_batch_deserialize(s->ctx, b + offset, v->text_length - offset, &payload));
    ud_status status = payload->columns != 1 ||
                               !ud_field_matches(&payload->column[0].field, f, UD_FIELD_VALUE_TYPE)
                           ? UD_CORRUPT
                           : UD_OK;
    if (d->distinct) {
        for (uint32_t i = 0; !status && i < payload->rows; i++) {
            ud_value value;
            status = ud_batch_get(payload, i, 0, &value);
            if (!status)
                status = ud_stat_add(s, d, &value);
        }
    } else if (d->kind == 5) {
        if (!s->hll) {
            s->hll = ud_alloc(s->ctx, UD_HLL_REGISTERS);
            if (!s->hll)
                status = UD_RESOURCE;
        }
        for (uint32_t i = 0; !status && i < UD_HLL_REGISTERS; i++)
            if (b[64 + i] > 55)
                status = UD_CORRUPT;
        if (!status)
            ud_hll_merge(s->hll, b + 64);
    } else if (!status) {
        uint64_t count = get64(b + 16);
        if (count > INT64_MAX - s->sum.count)
            status = UD_OVERFLOW;
        if (!status && (d->kind == 2 || d->kind == 3)) {
            if (payload->rows != (count ? 1u : 0u))
                status = UD_CORRUPT;
            else if (count) {
                ud_value value;
                status = ud_batch_get(payload, 0, 0, &value);
                if (!status)
                    status = ud_stat_add(s, d, &value);
                if (!status)
                    s->sum.count += count - 1;
            }
        } else if (!status) {
            uint64_t words[4];
            for (uint32_t i = 0; i < 4; i++)
                words[i] = get64(b + 24 + i * 8);
            status = ud_sum_wide_add(s->sum.words, words);
            if (!status) {
                uint64_t real = get64(b + 56);
                double value;
                memcpy(&value, &real, 8);
                s->sum.real += value;
                s->sum.count += count;
                s->sum.scale = f->scale;
            }
        }
    }
    ud_batch_destroy(payload);
    return status;
}
void ud_stat_clear(ud_aggregate_state *s) {
    ud_clear_value(&s->extremum);
    ud_value_set_clear(&s->distinct);
    ud_free(s->hll);
    memset(s, 0, sizeof(*s));
}
