#include "../internal.h"
static ud_status metadata(ud_batch *batch) {
    if (batch->row_info)
        return UD_OK;
    if ((uint64_t)batch->capacity > SIZE_MAX / sizeof(*batch->row_info))
        return UD_OVERFLOW;
    batch->row_info = ud_alloc(batch->ctx, (size_t)batch->capacity * sizeof(*batch->row_info) + 1);
    return batch->row_info ? UD_OK : UD_RESOURCE;
}
ud_status ud_batch_row_info(const ud_batch *batch, uint32_t row, ud_row_info *out) {
    if (!batch || !out || row >= batch->rows)
        return UD_INVALID;
    if (batch->row_info)
        *out = batch->row_info[row];
    else if (batch->data_owner)
        return ud_batch_row_info(batch->data_owner, row + batch->owner_first, out);
    else
        *out = (ud_row_info){0};
    return UD_OK;
}
ud_status ud_batch_set_row_info(ud_batch *batch, uint32_t row, const ud_row_info *info) {
    if (!batch || !info || row >= batch->rows || batch->sealed || batch->data_owner ||
        (info->flags & ~(UD_ROW_ERROR | UD_ROW_JOINED | UD_ROW_STAGE_ORDERED)) || info->operation > 4 ||
        info->has_source_position > 1 || (!info->has_source_position && info->source_position) ||
        ((info->flags & UD_ROW_STAGE_ORDERED) && !info->has_source_position) ||
        ((!info->id.high && !info->id.low) && (info->flags || info->operation)))
        return UD_INVALID;
    UD_TRY(metadata(batch));
    batch->row_info[row] = *info;
    return UD_OK;
}
ud_status ud_batch_copy_row_info(ud_batch *out, uint32_t target, const ud_batch *input,
                                 uint32_t source) {
    ud_row_info info;
    UD_TRY(ud_batch_row_info(input, source, &info));
    if (!info.id.high && !info.id.low)
        return UD_OK;
    return ud_batch_set_row_info(out, target, &info);
}
ud_status ud_batch_copy_row_info_range(ud_batch *destination, const ud_batch *source) {
    if (!destination || !source || destination == source || destination->ctx != source->ctx ||
        !source->sealed || destination->sealed || destination->data_owner ||
        destination->rows != source->rows)
        return UD_INVALID;
    UD_TRY(metadata(destination));
    if (source->row_info) {
        memcpy(destination->row_info, source->row_info,
               (size_t)source->rows * sizeof(*source->row_info));
        return UD_OK;
    }
    if (!source->data_owner) {
        memset(destination->row_info, 0,
               (size_t)destination->rows * sizeof(*destination->row_info));
        return UD_OK;
    }
    for (uint32_t row = 0; row < source->rows; row++) {
        UD_TRY(ud_batch_row_info(source, row, destination->row_info + row));
    }
    return UD_OK;
}
ud_status ud_batch_append_projected(ud_batch *out, const ud_batch *input, uint32_t row,
                                    const uint32_t *columns) {
    if (!out || !input || row >= input->rows || (!columns && out->columns > input->columns) ||
        out->sealed)
        return UD_INVALID;
    ud_row_info info;
    UD_TRY(ud_batch_row_info(input, row, &info));
    if (info.id.high || info.id.low)
        UD_TRY(metadata(out));
    if (!out->append_values)
        out->append_values = ud_alloc(out->ctx, (size_t)out->columns * sizeof(*out->append_values));
    ud_value *values = out->append_values;
    if (!values)
        return UD_RESOURCE;
    ud_status status = UD_OK;
    for (uint32_t col = 0; !status && col < out->columns; col++)
        status = ud_batch_get(input, row, columns ? columns[col] : col, values + col);
    if (!status)
        status = ud_batch_append(out, values, out->columns);
    if (!status && out->row_info)
        out->row_info[out->rows - 1] = info;
    return status;
}
ud_status ud_batch_append_from(ud_batch *out, const ud_batch *input, uint32_t row) {
    if (!out || !input || out->columns != input->columns)
        return UD_INVALID;
    return ud_batch_append_projected(out, input, row, NULL);
}
static ud_id record_id(ud_id scope, uint64_t position) {
    uint8_t bytes[24], digest[32];
    uint64_t values[] = {scope.high, scope.low, position};
    for (uint32_t n = 0; n < 3; n++)
        for (uint32_t j = 0; j < 8; j++)
            bytes[n * 8 + j] = (uint8_t)(values[n] >> (j * 8));
    ud_hash(bytes, sizeof(bytes), digest);
    ud_id id = {0};
    for (uint32_t j = 0; j < 8; j++) {
        id.high = (id.high << 8) | digest[j];
        id.low = (id.low << 8) | digest[j + 8];
    }
    return id;
}
ud_status ud_row_info_source(ud_id scope, uint64_t position, ud_row_info *out) {
    if (!out || (!scope.high && !scope.low))
        return UD_INVALID;
    *out = (ud_row_info){
        .id = record_id(scope, position), .source_position = position, .has_source_position = 1};
    return out->id.high || out->id.low ? UD_OK : UD_CORRUPT;
}
ud_status ud_batch_identify(ud_context *ctx, const ud_batch *input, ud_id scope, uint64_t begin,
                            ud_batch **out) {
    if (!ctx || !input || !input->sealed || !out || (!scope.high && !scope.low) ||
        input->rows > UINT64_MAX - begin)
        return UD_INVALID;
    *out = NULL;
    uint32_t *indices = ud_alloc(ctx, (size_t)input->columns * sizeof(*indices));
    ud_field *fields = ud_alloc(ctx, (size_t)input->columns * sizeof(*fields));
    ud_status status = !indices || !fields ? UD_RESOURCE : UD_OK;
    for (uint32_t j = 0; !status && j < input->columns; j++) {
        indices[j] = j;
        fields[j] = input->column[j].field;
    }
    if (!status)
        status = ud_batch_project(ctx, input, indices, fields, input->columns, out);
    if (!status)
        status = metadata(*out);
    for (uint32_t row = 0; !status && row < input->rows; row++) {
        ud_row_info info = {0};
        ud_batch_row_info(input, row, &info);
        info.id = record_id(scope, begin + row);
        info.source_position = begin + row;
        info.has_source_position = 1;
        info.origins = (ud_id){0};
        info.flags &= ~(UD_ROW_JOINED | UD_ROW_STAGE_ORDERED);
        (*out)->row_info[row] = info;
    }
    ud_free(indices);
    ud_free(fields);
    if (status) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return status;
}
ud_status ud_row_info_join(ud_context *ctx, const ud_row_info *a, const ud_row_info *b,
                           ud_row_info *out) {
    if (!ctx || !a || !b || !out)
        return UD_INVALID;
    if (!a->id.high && !a->id.low) {
        *out = *b;
        return UD_OK;
    }
    if (!b->id.high && !b->id.low) {
        *out = *a;
        return UD_OK;
    }
    if (a->operation && b->operation && a->operation != b->operation)
        return UD_UNSUPPORTED;
    if ((a->flags & UD_ROW_ERROR) && (b->flags & UD_ROW_ERROR) && ud_id_compare(a->rule, b->rule))
        return UD_UNSUPPORTED;
    ud_id x = a->flags & UD_ROW_JOINED ? a->origins : a->id,
          y = b->flags & UD_ROW_JOINED ? b->origins : b->id;
    ud_id origins = {.low = x.low + y.low};
    origins.high = x.high + y.high + (origins.low < x.low);
    uint8_t bytes[16], digest[32];
    uint64_t parts[] = {origins.high, origins.low};
    for (uint32_t n = 0; n < 2; n++)
        for (uint32_t j = 0; j < 8; j++)
            bytes[n * 8 + j] = (uint8_t)(parts[n] >> (j * 8));
    ud_sha256 hash;
    ud_sha256_init(&hash);
    ud_sha256_update(&hash, "native.join.origins.9", 21);
    ud_sha256_update(&hash, bytes, sizeof(bytes));
    ud_sha256_finish(&hash, digest);
    ud_row_info info = {.flags = ((a->flags | b->flags) & ~UD_ROW_STAGE_ORDERED) | UD_ROW_JOINED,
                        .rule = a->flags & UD_ROW_ERROR ? a->rule : b->rule,
                        .operation = a->operation ? a->operation : b->operation,
                        .origins = origins};
    for (uint32_t j = 0; j < 8; j++) {
        info.id.high = (info.id.high << 8) | digest[j];
        info.id.low = (info.id.low << 8) | digest[j + 8];
    }
    *out = info;
    return UD_OK;
}
static ud_status join_origin(ud_context *ctx, ud_row_info *info, ud_id scope) {
    if (!info->id.high && !info->id.low)
        return UD_OK;
    uint64_t parts[] = {scope.high, scope.low, info->id.high, info->id.low};
    uint8_t bytes[32], digest[32];
    for (uint32_t i = 0; i < 4; i++)
        for (uint32_t j = 0; j < 8; j++)
            bytes[i * 8 + j] = (uint8_t)(parts[i] >> (8 * j));
    ud_hash(bytes, sizeof(bytes), digest);
    info->origins = (ud_id){0};
    for (uint32_t j = 0; j < 8; j++) {
        info->origins.high = (info->origins.high << 8) | digest[j];
        info->origins.low = (info->origins.low << 8) | digest[j + 8];
    }
    info->flags |= UD_ROW_JOINED;
    (void)ctx;
    return UD_OK;
}
ud_status ud_batch_join_origin(ud_context *ctx, const ud_batch *input, ud_id scope,
                               ud_batch **out) {
    if (!ctx || !input || !out || (!scope.high && !scope.low))
        return UD_INVALID;
    UD_TRY(ud_batch_slice(ctx, input, 0, input->rows, out));
    ud_status s = metadata(*out);
    for (uint32_t row = 0; !s && row < input->rows; row++) {
        s = ud_batch_row_info(input, row, (*out)->row_info + row);
        if (!s)
            s = join_origin(ctx, (*out)->row_info + row, scope);
    }
    if (s) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}
ud_status ud_batch_join_row_info(ud_batch *out, uint32_t target, const ud_batch *left,
                                 uint32_t lrow, const ud_batch *right, uint32_t rrow) {
    ud_row_info a, b, info;
    UD_TRY(ud_batch_row_info(left, lrow, &a));
    UD_TRY(ud_batch_row_info(right, rrow, &b));
    /* Ordinary binary joins distinguish logical left/right roles, including
     * self joins. Multiway leaves arrive with their frozen input-name roles. */
    if (!(a.flags & UD_ROW_JOINED)) {
        ud_id scope;
        UD_TRY(
            ud_id_make(out->ctx, (ud_string){"binary.join", 11}, (ud_string){"left", 4}, &scope));
        UD_TRY(join_origin(out->ctx, &a, scope));
    }
    if (!(b.flags & UD_ROW_JOINED)) {
        ud_id scope;
        UD_TRY(
            ud_id_make(out->ctx, (ud_string){"binary.join", 11}, (ud_string){"right", 5}, &scope));
        UD_TRY(join_origin(out->ctx, &b, scope));
    }
    UD_TRY(ud_row_info_join(out->ctx, &a, &b, &info));
    return info.id.high || info.id.low ? ud_batch_set_row_info(out, target, &info) : UD_OK;
}
uint64_t ud_batch_error_rows(const ud_batch *batch) {
    uint64_t count = 0;
    if (batch)
        for (uint32_t row = 0; row < batch->rows; row++) {
            ud_row_info info;
            if (!ud_batch_row_info(batch, row, &info) && (info.flags & UD_ROW_ERROR))
                count++;
        }
    return count;
}

static ud_status digest_bytes(void *user, const void *p, size_t n) {
    ud_sha256_update(user, p, n);
    return UD_OK;
}
static ud_status hash_rows(const ud_batch *batch, ud_sha256 *hash, const uint8_t *tag) {
    if (!batch || !hash)
        return UD_INVALID;
    for (uint32_t row = 0; row < batch->rows; row++) {
        if (tag)
            ud_sha256_update(hash, tag, 1);
        for (uint32_t col = 0; col < batch->columns; col++) {
            ud_value value;
            UD_TRY(ud_batch_get(batch, row, col, &value));
            UD_TRY(ud_value_canonical(&value, digest_bytes, hash, 0));
        }
        ud_row_info info;
        UD_TRY(ud_batch_row_info(batch, row, &info));
        uint64_t parts[] = {info.id.high,        info.id.low,      info.rule.high,
                            info.rule.low,       info.flags,       info.operation,
                            info.origins.high,   info.origins.low, info.has_source_position,
                            info.source_position};
        uint8_t bytes[sizeof(parts)];
        for (uint32_t n = 0; n < sizeof(parts) / sizeof(parts[0]); n++)
            for (uint32_t j = 0; j < 8; j++)
                bytes[n * 8 + j] = (uint8_t)(parts[n] >> (8 * j));
        ud_sha256_update(hash, bytes, sizeof(bytes));
    }
    return UD_OK;
}
ud_status ud_batch_hash_rows(const ud_batch *batch, ud_sha256 *hash) {
    return hash_rows(batch, hash, NULL);
}
ud_status ud_batch_hash_tagged_rows(const ud_batch *batch, ud_sha256 *hash, uint8_t port) {
    return hash_rows(batch, hash, &port);
}
ud_status ud_batch_fingerprint(const ud_batch *batch, uint8_t out[32]) {
    if (!batch || !out)
        return UD_INVALID;
    ud_sha256 hash;
    ud_sha256_init(&hash);
    uint8_t schema[32];
    UD_TRY(ud_schema_fingerprint(batch, schema));
    ud_sha256_update(&hash, schema, 32);
    UD_TRY(ud_batch_hash_rows(batch, &hash));
    ud_sha256_finish(&hash, out);
    return UD_OK;
}

ud_status ud_batch_row_info_json(const ud_batch *batch, uint32_t row, char **out,
                                 uint32_t *length) {
    if (!batch || !out || !length)
        return UD_INVALID;
    *out = NULL;
    *length = 0;
    ud_row_info info;
    UD_TRY(ud_batch_row_info(batch, row, &info));
    char id[33], rule[33], origins[33], json[320];
    ud_id_format(info.id, id);
    ud_id_format(info.rule, rule);
    ud_id_format(info.origins, origins);
    int n = snprintf(
        json, sizeof(json),
        "{\"id\":\"%s\",\"rule\":\"%s\",\"origins\":\"%s\",\"flags\":%u,\"operation\":%u,"
        "\"hasSourcePosition\":%s,\"sourcePosition\":\"%llu\"}",
        id, rule, origins, info.flags, info.operation, info.has_source_position ? "true" : "false",
        (unsigned long long)info.source_position);
    *out = ud_strdup_n(batch->ctx, json, (size_t)n);
    if (!*out)
        return UD_RESOURCE;
    *length = (uint32_t)n;
    return UD_OK;
}
static void canonical_key(ud_value *value) {
    if (value->type == UD_FLOAT64 && value->real == 0)
        value->real = 0;
    for (uint32_t i = 0; i < value->child_count; i++)
        canonical_key((ud_value *)value->children + i);
}
ud_status ud_batch_identify_keys(ud_context *ctx, const ud_batch *input, ud_id scope,
                                 const uint32_t *keys, uint32_t count, ud_batch **out) {
    if (!ctx || !input || !input->sealed || !out || (count && !keys))
        return UD_INVALID;
    UD_TRY(ud_batch_identify(ctx, input, scope, 0, out));
    ud_status status = UD_OK;
    for (uint32_t row = 0; !status && row < input->rows; row++) {
        ud_sha256 hash;
        ud_sha256_init(&hash);
        uint8_t prefix[16];
        uint64_t ids[] = {scope.high, scope.low};
        for (uint32_t n = 0; n < 2; n++)
            for (uint32_t j = 0; j < 8; j++)
                prefix[8 * n + j] = (uint8_t)(ids[n] >> (8 * j));
        ud_sha256_update(&hash, prefix, 16);
        ud_bytes bytes = {.ctx = ctx};
        for (uint32_t i = 0; !status && i < count; i++) {
            if (keys[i] >= input->columns) {
                status = UD_SCHEMA;
                break;
            }
            ud_value value, owned = {0};
            status = ud_batch_get(input, row, keys[i], &value);
            if (!status && value.type >= UD_STRUCT) {
                status = ud_copy_value(ctx, &value, &owned, 0);
                if (!status) {
                    canonical_key(&owned);
                    value = owned;
                }
            } else if (!status && value.type == UD_FLOAT64 && value.real == 0)
                value.real = 0;
            bytes.size = 0;
            if (!status)
                status = ud_json_value_encode(&bytes, &input->column[keys[i]].field, &value);
            if (!status) {
                uint8_t length[8];
                for (uint32_t j = 0; j < 8; j++)
                    length[j] = (uint8_t)(bytes.size >> (8 * j));
                ud_sha256_update(&hash, length, 8);
                ud_sha256_update(&hash, bytes.data, bytes.size);
            }
            ud_clear_value(&owned);
        }
        if (!status) {
            uint8_t digest[32];
            ud_sha256_finish(&hash, digest);
            ud_row_info info = {0};
            for (uint32_t j = 0; j < 8; j++) {
                info.id.high = (info.id.high << 8) | digest[j];
                info.id.low = (info.id.low << 8) | digest[j + 8];
            }
            (*out)->row_info[row] = info;
        }
        ud_bytes_clear(&bytes);
    }
    if (status) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return status;
}
