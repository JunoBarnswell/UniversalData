#include "advanced_internal.h"
ud_status ud_cache_lookup_bind(ud_advanced *a) {
    if (!a->inputs[1] || !a->options.reference_keys || !a->options.reference_key_count)
        return UD_SCHEMA;
    UD_TRY(ud_advanced_select(a, ud_advanced_member(a->config, "keys"), &a->keys, &a->key_count));
    if (a->key_count != a->options.reference_key_count)
        return UD_SCHEMA;
    for (uint32_t k = 0; k < a->key_count; k++)
        if (a->options.reference_keys[k] >= a->inputs[1]->columns ||
            !ud_field_matches(&a->inputs[0]->column[a->keys[k]].field,
                              &a->inputs[1]->column[a->options.reference_keys[k]].field,
                              UD_FIELD_VALUE_TYPE))
            return UD_SCHEMA;
    UD_TRY(ud_compute_join_schema(a->ctx, a->inputs[0], a->inputs[1], &a->schema[0]));
    if (a->options.reference) {
        UD_TRY(ud_cache_retain(a->options.reference));
        a->reference = a->options.reference;
        UD_TRY(ud_cache_reader_create(a->reference, a->inputs[0], a->keys, a->key_count,
                                      &a->cache_reader));
    }
    return UD_OK;
}
ud_status ud_cache_lookup_row(ud_advanced *a, ud_value *v, uint32_t *port, int *advance) {
    if (!a->reference || !a->cache_reader)
        return UD_INVALID;
    *port = 0;
    *advance = 0;
    if (!a->active) {
        UD_TRY(ud_cache_probe(a->cache_reader, a->pending, a->row));
        a->active = 1;
        a->element = 0;
    }
    const ud_batch *reference = NULL;
    uint32_t row = 0;
    ud_status status = ud_cache_next(a->cache_reader, &reference, &row);
    if (status == UD_END) {
        *advance = 2;
        if (!a->element && a->policy != UD_ERROR_IGNORE)
            return UD_QUALITY;
        return UD_OK;
    }
    if (status)
        return status;
    for (uint32_t i = 0; i < a->pending->columns; i++)
        UD_TRY(ud_batch_get(a->pending, a->row, i, v + i));
    for (uint32_t i = 0; i < reference->columns; i++)
        UD_TRY(ud_batch_get(reference, row, i, v + a->pending->columns + i));
    ud_row_info left, right;
    UD_TRY(ud_batch_row_info(a->pending, a->row, &left));
    UD_TRY(ud_batch_row_info(reference, row, &right));
    UD_TRY(ud_row_info_join(a->ctx, &left, &right, &a->next_info));
    a->override_info = 1;
    return UD_OK;
}
