#include "advanced_internal.h"
ud_status ud_cache_lookup_bind(ud_advanced *a) {
    if (!a->inputs[1] || !a->options.reference_keys || !a->options.reference_key_count)
        return UD_SCHEMA;
    UD_TRY(ud_advanced_select(a, ud_advanced_member(a->config, "keys"), &a->keys, &a->key_count));
    if (a->key_count != a->options.reference_key_count)
        return UD_SCHEMA;
    /* cacheLookup/dbLookup expose a dedicated no-match output when requested;
     * port 1 remains the error port. */
    a->mode = ud_advanced_named(ud_advanced_member(a->config, "noMatch"), "output");
    a->outputs = 3;
    for (uint32_t k = 0; k < a->key_count; k++) {
        if (a->options.reference_keys[k] >= a->inputs[1]->columns)
            return UD_SCHEMA;
        const ud_field *probe = &a->inputs[0]->column[a->keys[k]].field;
        const ud_field *reference = &a->inputs[1]->column[a->options.reference_keys[k]].field;
        if (!ud_field_matches(probe, reference, UD_FIELD_VALUE_TYPE) ||
            (a->options.reference &&
             (probe->scalar.locale_id != reference->scalar.locale_id ||
              probe->scalar.comparison_flags != reference->scalar.comparison_flags)))
            return UD_SCHEMA;
    }
    UD_TRY(ud_compute_join_schema(a->ctx, a->inputs[0], a->inputs[1], &a->schema[0]));
    if (a->options.reference) {
        UD_TRY(ud_cache_retain(a->options.reference));
        a->reference = a->options.reference;
        UD_TRY(ud_cache_reader_create(a->reference, a->inputs[0], a->keys, a->key_count,
                                      &a->cache_reader));
    }
    return UD_OK;
}
ud_status ud_advanced_lookup_observe(ud_advanced *a, const ud_batch *batch) {
    if (!batch || !batch->sealed || batch->ctx != a->ctx || !a->pending ||
        a->lookup_observations || a->reference || a->row)
        return UD_INVALID;
    uint32_t columns = a->inputs[1]->columns;
    if (batch->rows != a->pending->rows || batch->columns != columns + 1 ||
        batch->column[columns].field.type != UD_BOOL ||
        batch->column[columns].field.nullable)
        return UD_SCHEMA;
    for (uint32_t i = 0; i < columns; i++)
        if (!ud_field_matches(&batch->column[i].field, &a->inputs[1]->column[i].field,
                              UD_FIELD_VALUE_TYPE))
            return UD_SCHEMA;
    UD_TRY(ud_batch_retain((ud_batch *)batch));
    a->lookup_observations = (ud_batch *)batch;
    return UD_OK;
}

ud_status ud_cache_lookup_row(ud_advanced *a, ud_value *v, uint32_t *port, int *advance) {
    *port = 0;
    const ud_batch *reference = NULL;
    uint32_t row = 0, columns = a->inputs[1]->columns;
    ud_status status;
    if (a->lookup_observations) {
        reference = a->lookup_observations;
        row = a->row;
        ud_value matched;
        UD_TRY(ud_batch_get(reference, row, columns, &matched));
        status = matched.integer ? UD_OK : UD_END;
        *advance = 1;
    } else {
        if (!a->cache_reader)
            return UD_INVALID;
        if (!a->active) {
            UD_TRY(ud_cache_probe(a->cache_reader, a->pending, a->row));
            a->active = 1;
        }
        status = ud_cache_next(a->cache_reader, &reference, &row);
        *advance = 0;
    }
    if (status == UD_END) {
        if (!a->element && a->mode) {
            *port = 2;
            *advance = 1;
            for (uint32_t i = 0; i < a->pending->columns; i++)
                UD_TRY(ud_batch_get(a->pending, a->row, i, v + i));
            return UD_OK;
        }
        *advance = 2;
        if (!a->element && a->policy != UD_ERROR_IGNORE)
            return UD_QUALITY;
        return UD_OK;
    }
    if (status)
        return status;
    for (uint32_t i = 0; i < a->pending->columns; i++)
        UD_TRY(ud_batch_get(a->pending, a->row, i, v + i));
    for (uint32_t i = 0; i < columns; i++)
        UD_TRY(ud_batch_get(reference, row, i, v + a->pending->columns + i));
    ud_row_info left, right;
    UD_TRY(ud_batch_row_info(a->pending, a->row, &left));
    UD_TRY(ud_batch_row_info(reference, row, &right));
    UD_TRY(ud_row_info_join(a->ctx, &left, &right, &a->next_info));
    a->override_info = 1;
    return UD_OK;
}
