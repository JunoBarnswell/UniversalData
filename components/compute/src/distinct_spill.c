#include "stat.h"
#include <math.h>
#include <stdio.h>

#define UD_DISTINCT_PARTITIONS 64
#define UD_DISTINCT_PENDING_ROWS 256
#define UD_DISTINCT_MAX_DEPTH 8

struct ud_distinct_spill {
    ud_context *ctx;
    ud_buffer_manager *buffers;
    const ud_field *field;
    uint32_t maximum;
    uint32_t partition_depth;
    uint64_t memory_limit, reserved_bytes, rows;
    ud_value_set values;
    FILE *files[UD_DISTINCT_PARTITIONS];
    uint64_t final_count;
    ud_sum_state final_sum;
    int finalized;
};

static uint32_t partition(uint64_t hash, uint32_t depth) {
    hash ^= hash >> 30;
    hash *= UINT64_C(0xbf58476d1ce4e5b9);
    hash ^= hash >> 27;
    hash *= UINT64_C(0x94d049bb133111eb);
    hash ^= hash >> 31;
    uint32_t shift = depth * 6;
    return (uint32_t)((hash >> shift) & (UD_DISTINCT_PARTITIONS - 1));
}

static void put64(uint8_t *bytes, uint64_t value) {
    for (uint32_t i = 0; i < 8; i++)
        bytes[i] = (uint8_t)(value >> (i * 8));
}

static uint64_t get64(const uint8_t *bytes) {
    uint64_t value = 0;
    for (uint32_t i = 0; i < 8; i++)
        value |= (uint64_t)bytes[i] << (i * 8);
    return value;
}

static int seek_start(FILE *file) {
#ifdef _WIN32
    return _fseeki64(file, 0, SEEK_SET);
#else
    return fseeko(file, 0, SEEK_SET);
#endif
}

static ud_status open_partition(ud_distinct_spill *spill, uint32_t index) {
    if (spill->files[index])
        return UD_OK;
    spill->files[index] = tmpfile();
    return spill->files[index] ? UD_OK : UD_IO;
}

static ud_status write_batch(ud_distinct_spill *spill, uint32_t index, ud_batch *batch) {
    if (!batch || !batch->rows)
        return UD_OK;
    uint8_t *wire = NULL;
    uint64_t length = 0;
    UD_TRY(ud_batch_serialize_spill(batch, &wire, &length));
    if (length > UINT64_MAX - sizeof(length)) {
        ud_free(wire);
        return UD_OVERFLOW;
    }
    uint64_t reserved = length + sizeof(length);
    ud_status status = spill->buffers ? ud_buffer_reserve_external(spill->buffers, reserved) : UD_OK;
    if (!status)
        status = open_partition(spill, index);
    uint8_t header[sizeof(length)];
    put64(header, length);
    if (!status && (fwrite(header, sizeof(header), 1, spill->files[index]) != 1 ||
                    length && fwrite(wire, (size_t)length, 1, spill->files[index]) != 1))
        status = UD_IO;
    if (status && spill->buffers)
        ud_buffer_release_external(spill->buffers, reserved);
    if (!status) {
        spill->reserved_bytes += reserved;
        if (spill->rows > UINT64_MAX - batch->rows)
            status = UD_OVERFLOW;
        else
            spill->rows += batch->rows;
    }
    ud_free(wire);
    return status;
}

static void clear_values(ud_value_set *values, ud_context *ctx, uint32_t maximum) {
    ud_value_set_clear(values);
    values->ctx = ctx;
    values->maximum = maximum;
}

static ud_status flush_values(ud_distinct_spill *spill) {
    if (!spill->values.count)
        return UD_OK;
    ud_batch *pending[UD_DISTINCT_PARTITIONS] = {0};
    ud_status status = UD_OK;
    for (uint32_t i = 0; !status && i < spill->values.count; i++) {
        ud_set_entry *entry = spill->values.entries + i;
        uint32_t index = partition(entry->hash, spill->partition_depth);
        if (!pending[index])
            status = ud_batch_create(spill->ctx, spill->field, 1, UD_DISTINCT_PENDING_ROWS,
                                     pending + index);
        if (!status)
            status = ud_batch_append(pending[index], &entry->value, 1);
        if (!status && pending[index]->rows == UD_DISTINCT_PENDING_ROWS) {
            status = ud_batch_seal(pending[index]);
            if (!status)
                status = write_batch(spill, index, pending[index]);
            ud_batch_destroy(pending[index]);
            pending[index] = NULL;
        }
    }
    for (uint32_t i = 0; i < UD_DISTINCT_PARTITIONS; i++) {
        if (!pending[i])
            continue;
        if (!status)
            status = ud_batch_seal(pending[i]);
        if (!status)
            status = write_batch(spill, i, pending[i]);
        ud_batch_destroy(pending[i]);
    }
    if (!status)
        clear_values(&spill->values, spill->ctx, UINT32_MAX);
    return status;
}

static ud_status read_batch(ud_distinct_spill *spill, FILE *file, ud_batch **out, int *end) {
    uint8_t header[sizeof(uint64_t)];
    size_t read = fread(header, 1, sizeof(header), file);
    if (!read && feof(file)) {
        *end = 1;
        *out = NULL;
        return UD_OK;
    }
    if (read != sizeof(header))
        return UD_CORRUPT;
    uint64_t length = get64(header);
    if (!length || length > SIZE_MAX)
        return UD_CORRUPT;
    uint8_t *wire = ud_alloc(spill->ctx, (size_t)length);
    if (!wire)
        return UD_RESOURCE;
    ud_status status = fread(wire, (size_t)length, 1, file) == 1
        ? ud_batch_deserialize_spill(spill->ctx, wire, length, out) : UD_CORRUPT;
    ud_free(wire);
    if (status)
        return status;
    if (!*out)
        return UD_CORRUPT;
    if ((*out)->columns != 1) {
        ud_batch_destroy(*out);
        *out = NULL;
        return UD_CORRUPT;
    }
    *end = 0;
    return UD_OK;
}

static ud_status add_to_child(ud_distinct_spill *child, const ud_value_set *values) {
    for (uint32_t i = 0; i < values->count; i++) {
        int added;
        UD_TRY(ud_distinct_spill_add(child, &values->entries[i].value, &added));
    }
    return UD_OK;
}

static ud_status merge_sum(ud_sum_state *target, const ud_sum_state *source) {
    if (!source->count)
        return UD_OK;
    if (source->count > UINT64_MAX - target->count)
        return UD_OVERFLOW;
    uint64_t words[4];
    memcpy(words, source->words, sizeof(words));
    UD_TRY(ud_sum_wide_add(target->words, words));
    for (uint32_t lane = 0; lane < 2; lane++) {
        memcpy(words, source->calendar[lane], sizeof(words));
        UD_TRY(ud_sum_wide_add(target->calendar[lane], words));
    }
    double real = target->real + source->real;
    if (isfinite(target->real) && isfinite(source->real) && !isfinite(real))
        return UD_OVERFLOW;
    if (!target->count)
        target->scale = source->scale;
    target->real = real;
    target->count += source->count;
    return UD_OK;
}

static int sumable(const ud_field *field) {
    return field->type == UD_INT64 || field->type == UD_UINT64 ||
           field->type == UD_DURATION || field->type == UD_INTERVAL ||
           field->type == UD_FLOAT64 || field->type == UD_DECIMAL128;
}

static ud_status finalize_at_depth(ud_distinct_spill *spill, ud_sum_state *sum,
                                   uint64_t *count, uint32_t depth);

static ud_status count_file(ud_distinct_spill *spill, FILE *file, uint32_t depth,
                            ud_sum_state *sum, uint64_t *out) {
    if (!file) {
        *out = 0;
        return UD_OK;
    }
    if (seek_start(file))
        return UD_IO;
    ud_value_set values = {.ctx = spill->ctx, .maximum = UINT32_MAX};
    ud_distinct_spill *child = NULL;
    ud_sum_state local = {0};
    uint64_t count = 0;
    ud_status status = UD_OK;
    for (;;) {
        ud_batch *batch = NULL;
        int end = 0;
        status = read_batch(spill, file, &batch, &end);
        if (status || end)
            break;
        for (uint32_t row = 0; !status && row < batch->rows; row++) {
            ud_value value;
            status = ud_batch_get(batch, row, 0, &value);
            if (status)
                break;
            if (child) {
                int added;
                status = ud_distinct_spill_add(child, &value, &added);
                continue;
            }
            int added;
            status = ud_value_set_add(&values, &value, &added);
            if (status)
                break;
            if (values.bytes >= spill->memory_limit) {
                if (depth >= UD_DISTINCT_MAX_DEPTH) {
                    status = UD_RESOURCE;
                    break;
                }
                status = ud_distinct_spill_create(spill->ctx, spill->buffers, spill->field,
                                                  spill->maximum, &child);
                if (!status)
                    child->partition_depth = depth + 1;
                if (!status)
                    status = add_to_child(child, &values);
                clear_values(&values, spill->ctx, UINT32_MAX);
            }
        }
        ud_batch_destroy(batch);
        if (status)
            break;
    }
    if (!status && child) {
        status = depth >= UD_DISTINCT_MAX_DEPTH
            ? UD_RESOURCE
            : finalize_at_depth(child, &local, &count, depth + 1);
        ud_distinct_spill_destroy(child);
        child = NULL;
    } else if (!status) {
        count = values.count;
        if (sumable(spill->field))
            for (uint32_t i = 0; !status && i < values.count; i++)
                status = ud_sum_update(&local, &values.entries[i].value, 1);
    }
    ud_value_set_clear(&values);
    if (child)
        ud_distinct_spill_destroy(child);
    if (!status)
        status = merge_sum(sum, &local);
    if (!status && count > spill->maximum)
        status = UD_RESOURCE;
    *out = count;
    return status;
}

ud_status ud_distinct_spill_create(ud_context *ctx, ud_buffer_manager *buffers, const ud_field *field,
                                   uint32_t maximum, ud_distinct_spill **out) {
    if (!ctx || !buffers || !field || !out || !maximum)
        return UD_INVALID;
    *out = ud_alloc(ctx, sizeof(**out));
    if (!*out)
        return UD_RESOURCE;
    (*out)->ctx = ctx;
    (*out)->buffers = buffers;
    (*out)->field = field;
    (*out)->maximum = maximum;
    (*out)->values.ctx = ctx;
    (*out)->values.maximum = UINT32_MAX;
    ud_buffer_options limits = {0};
    ud_status status = ud_buffer_manager_limits(buffers, &limits);
    uint64_t limit = !status ? limits.working_limit / 16 : 0;
    if (limit < 64 * 1024)
        limit = 64 * 1024;
    if (limit > 8 * 1024 * 1024)
        limit = 8 * 1024 * 1024;
    (*out)->memory_limit = limit;
    if (status) {
        ud_free(*out);
        *out = NULL;
    }
    return status;
}

ud_status ud_distinct_spill_add(ud_distinct_spill *spill, const ud_value *value, int *added) {
    if (!spill || !value || !added || spill->finalized)
        return UD_INVALID;
    if (spill->values.bytes >= spill->memory_limit)
        UD_TRY(flush_values(spill));
    UD_TRY(ud_value_set_add(&spill->values, value, added));
    if (*added && spill->values.bytes >= spill->memory_limit)
        return flush_values(spill);
    return UD_OK;
}

static ud_status finalize_at_depth(ud_distinct_spill *spill, ud_sum_state *sum,
                                   uint64_t *count, uint32_t depth) {
    if (!spill || !sum || !count)
        return UD_INVALID;
    if (spill->finalized) {
        *sum = spill->final_sum;
        *count = spill->final_count;
        return UD_OK;
    }
    UD_TRY(flush_values(spill));
    *sum = (ud_sum_state){0};
    uint64_t total = 0;
    for (uint32_t i = 0; i < UD_DISTINCT_PARTITIONS; i++) {
        uint64_t partition_count = 0;
        UD_TRY(count_file(spill, spill->files[i], depth, sum, &partition_count));
        if (partition_count > UINT64_MAX - total)
            return UD_OVERFLOW;
        total += partition_count;
    }
    if (total > spill->maximum)
        return UD_RESOURCE;
    spill->final_count = total;
    spill->final_sum = *sum;
    spill->finalized = 1;
    *count = total;
    return UD_OK;
}

ud_status ud_distinct_spill_finalize(ud_distinct_spill *spill, ud_sum_state *sum, uint64_t *count) {
    return finalize_at_depth(spill, sum, count, 0);
}

static ud_status visit_file(ud_distinct_spill *spill, FILE *file, ud_distinct_visit visit, void *user) {
    if (!file)
        return UD_OK;
    if (seek_start(file))
        return UD_IO;
    for (;;) {
        ud_batch *batch = NULL;
        int end = 0;
        UD_TRY(read_batch(spill, file, &batch, &end));
        if (end)
            return UD_OK;
        for (uint32_t row = 0; row < batch->rows; row++) {
            ud_value value;
            ud_status status = ud_batch_get(batch, row, 0, &value);
            if (!status)
                status = visit(user, &value);
            if (status) {
                ud_batch_destroy(batch);
                return status;
            }
        }
        ud_batch_destroy(batch);
    }
}

ud_status ud_distinct_spill_visit(ud_distinct_spill *spill, ud_distinct_visit visit, void *user) {
    if (!spill || !visit)
        return UD_INVALID;
    UD_TRY(flush_values(spill));
    for (uint32_t i = 0; i < UD_DISTINCT_PARTITIONS; i++)
        UD_TRY(visit_file(spill, spill->files[i], visit, user));
    return UD_OK;
}

uint64_t ud_distinct_spill_rows(const ud_distinct_spill *spill) {
    if (!spill || spill->rows > UINT64_MAX - spill->values.count)
        return spill ? UINT64_MAX : 0;
    return spill->rows + spill->values.count;
}

void ud_distinct_spill_destroy(ud_distinct_spill *spill) {
    if (!spill)
        return;
    for (uint32_t i = 0; i < UD_DISTINCT_PARTITIONS; i++)
        if (spill->files[i])
            fclose(spill->files[i]);
    if (spill->buffers && spill->reserved_bytes)
        ud_buffer_release_external(spill->buffers, spill->reserved_bytes);
    ud_value_set_clear(&spill->values);
    ud_free(spill);
}
