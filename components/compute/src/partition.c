#include "ud/partition.h"
#include "../internal.h"
ud_status ud_partition_hash_row(const ud_batch *input, uint32_t row, const uint32_t *keys,
                                uint32_t key_count, uint32_t partitions, uint32_t *partition) {
    uint64_t hash = UD_VALUE_HASH_SEED;
    for (uint32_t i = 0; i < key_count; i++) {
        ud_value value;
        UD_TRY(ud_batch_get(input, row, keys[i], &value));
        hash = ud_field_value_hash(input->schema->fields + keys[i], &value, hash);
    }
    *partition = (uint32_t)(hash % partitions);
    return UD_OK;
}

static ud_status select_partition(const ud_batch *input, const ud_partition_options *o,
                                  uint32_t row, uint32_t *out) {
    if (o->strategy == UD_PARTITION_ROUND_ROBIN) {
        *out = (uint32_t)((o->row_offset + row) % o->count);
        return UD_OK;
    }
    if (o->strategy == UD_PARTITION_HASH)
        return ud_partition_hash_row(input, row, o->keys, o->key_count, o->count, out);
    ud_value value = {0};
    UD_TRY(ud_batch_get(input, row, o->keys[0], &value));
    const ud_field *field = &input->column[o->keys[0]].field;
    *out = 0;
    if (!value.is_null)
        while (*out + 1 < o->count &&
               ud_field_value_compare(field, &value, field, o->boundaries + *out) >= 0)
            (*out)++;
    return UD_OK;
}
ud_status ud_batch_partition(ud_context *ctx, const ud_batch *input, const ud_partition_options *o,
                             ud_batch ***out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!ctx || !input || input->ctx != ctx || !input->sealed || !o)
        return UD_INVALID;
    if (o->struct_size != sizeof(*o) || o->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!o->count || o->count > 64 || o->strategy < UD_PARTITION_HASH ||
        o->strategy > UD_PARTITION_BROADCAST || o->key_count > input->columns ||
        (o->key_count && !o->keys))
        return UD_INVALID;
    if (o->row_offset > UINT64_MAX - input->rows)
        return UD_OVERFLOW;
    for (uint32_t i = 0; i < o->key_count; i++)
        if (o->keys[i] >= input->columns)
            return UD_SCHEMA;
    if (o->strategy == UD_PARTITION_HASH && !o->key_count)
        return UD_SCHEMA;
    if (o->strategy == UD_PARTITION_RANGE) {
        if (o->key_count != 1 || (o->count > 1 && !o->boundaries))
            return UD_SCHEMA;
        const ud_field *field = &input->column[o->keys[0]].field;
        for (uint32_t i = 0; i + 1 < o->count; i++) {
            if (o->boundaries[i].is_null || o->boundaries[i].type != field->type ||
                (field->type == UD_DECIMAL128 && (o->boundaries[i].precision != field->precision ||
                                                  o->boundaries[i].scale != field->scale)) ||
                (i && ud_field_value_compare(field, o->boundaries + i - 1,
                                             field, o->boundaries + i) >= 0))
                return UD_SCHEMA;
        }
    }
    ud_batch **outputs = ud_alloc(ctx, (size_t)o->count * sizeof(*outputs));
    if (!outputs)
        return UD_RESOURCE;
    ud_status status = UD_OK;
    if (o->strategy == UD_PARTITION_BROADCAST) {
        for (uint32_t i = 0; !status && i < o->count; i++) {
            status = ud_batch_retain((ud_batch *)input);
            if (!status)
                outputs[i] = (ud_batch *)input;
        }
    } else {
        /* Batch capacity is fixed. Plan exact per-partition row capacity before
         * creating outputs; memory is O(rows + partitions), never rows*width. */
        uint32_t *routes = ud_alloc(ctx, ((size_t)input->rows + 1) * sizeof(*routes));
        uint32_t *counts = ud_alloc(ctx, (size_t)o->count * sizeof(*counts));
        ud_field *fields = ud_alloc(ctx, (size_t)input->columns * sizeof(*fields));
        ud_value *values = ud_alloc(ctx, (size_t)input->columns * sizeof(*values));
        if (!routes || !counts || !fields || !values)
            status = UD_RESOURCE;
        for (uint32_t row = 0; !status && row < input->rows; row++) {
            if (ud_is_cancelled(ctx)) {
                status = UD_CANCELLED;
                break;
            }
            status = select_partition(input, o, row, routes + row);
            if (!status)
                counts[routes[row]]++;
        }
        for (uint32_t i = 0; !status && i < input->columns; i++)
            fields[i] = input->column[i].field;
        for (uint32_t i = 0; !status && i < o->count; i++)
            status = ud_batch_create(ctx, fields, input->columns, counts[i], outputs + i);
        for (uint32_t row = 0; !status && row < input->rows; row++) {
            for (uint32_t i = 0; !status && i < input->columns; i++)
                status = ud_batch_get(input, row, i, values + i);
            if (!status)
                status = ud_batch_append(outputs[routes[row]], values, input->columns);
            if (!status)
                status = ud_batch_copy_row_info(outputs[routes[row]],
                                                outputs[routes[row]]->rows - 1, input, row);
        }
        for (uint32_t i = 0; !status && i < o->count; i++)
            status = ud_batch_seal(outputs[i]);
        ud_free(routes);
        ud_free(counts);
        ud_free(fields);
        ud_free(values);
    }
    if (status) {
        for (uint32_t i = 0; i < o->count; i++)
            ud_batch_destroy(outputs[i]);
        ud_free(outputs);
        return status;
    }
    *out = outputs;
    return UD_OK;
}
