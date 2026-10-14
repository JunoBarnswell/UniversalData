#include "../internal.h"
/* Outside the valid legacy column-count domain, so old/new payloads cannot
 * accidentally parse as each other despite retaining protocol family 10. */
#define WIRE_LAYOUT (UINT64_C(0x5544574952450000) | UD_DATA_LAYOUT_REVISION)
static ud_status put_field(ud_bytes *b, const ud_field *f, uint32_t depth) {
    if (depth > 32)
        return UD_SCHEMA;
    UD_TRY(ud_put_string(b, f->name, f->name_length));
    UD_TRY(ud_put_u64(b, (uint64_t)f->type));
    UD_TRY(ud_put_u64(b, (uint64_t)f->nullable));
    UD_TRY(ud_put_u64(b, (uint64_t)(int64_t)f->precision));
    UD_TRY(ud_put_u64(b, (uint64_t)(int64_t)f->scale));
    UD_TRY(ud_put_string(b, f->timezone, f->timezone_length));
    UD_TRY(ud_put_u64(b, f->identity.high));
    UD_TRY(ud_put_u64(b, f->identity.low));
    UD_TRY(ud_put_u64(b,f->physical_identity.high));
    UD_TRY(ud_put_u64(b,f->physical_identity.low));
    UD_TRY(ud_put_u64(b, f->key_order));
    UD_TRY(ud_put_u64(b, f->lineage_count));
    for (uint32_t i = 0; i < f->lineage_count; i++) {
        UD_TRY(ud_put_u64(b, f->lineage[i].high));
        UD_TRY(ud_put_u64(b, f->lineage[i].low));
    }
    UD_TRY(ud_put_u64(b, f->dictionary_kind));
    UD_TRY(ud_put_u64(b, f->dictionary_count));
    for (uint32_t i = 0; i < f->dictionary_count; i++)
        UD_TRY(ud_put_string(b, f->dictionary[i].data, f->dictionary[i].length));
    UD_TRY(ud_put_u64(b, f->child_count));
    for (uint32_t i = 0; i < f->child_count; i++)
        UD_TRY(put_field(b, f->children + i, depth + 1));
    return UD_OK;
}
static void clear_field(ud_field *f) {
    ud_field_clear(f);
}
static ud_status get_field(ud_bytes *b, ud_field *f, uint32_t depth) {
    if (depth > 32)
        return UD_SCHEMA;
    char *name = NULL, *timezone = NULL;
    size_t start = b->pos;
    ud_status s = ud_get_string(b, &name);
    f->name = name;
    if (s)
        return s;
    f->name_length = (uint32_t)strlen(name);
    if (b->pos - start - 8 != f->name_length)
        return UD_CORRUPT;
    uint64_t type, nullable, precision, scale, children;
    UD_TRY(ud_get_u64(b, &type));
    UD_TRY(ud_get_u64(b, &nullable));
    UD_TRY(ud_get_u64(b, &precision));
    UD_TRY(ud_get_u64(b, &scale));
    int64_t signed_scale;
    memcpy(&signed_scale, &scale, 8);
    if (type < UD_BOOL || type > UD_MAP || nullable > 1 || precision > 38 || signed_scale < -38 ||
        signed_scale > 38)
        return UD_CORRUPT;
    f->type = (ud_type)type;
    f->nullable = (int32_t)nullable;
    f->precision = (int32_t)precision;
    f->scale = (int32_t)signed_scale;
    start = b->pos;
    s = ud_get_string(b, &timezone);
    f->timezone = timezone;
    if (s)
        return s;
    f->timezone_length = (uint32_t)strlen(timezone);
    if (b->pos - start - 8 != f->timezone_length)
        return UD_CORRUPT;
    UD_TRY(ud_get_u64(b, &f->identity.high));
    UD_TRY(ud_get_u64(b, &f->identity.low));
    UD_TRY(ud_get_u64(b,&f->physical_identity.high));
    UD_TRY(ud_get_u64(b,&f->physical_identity.low));
    if (!f->identity.high && !f->identity.low)
        return UD_CORRUPT;
    uint64_t lineage, key_order;
    UD_TRY(ud_get_u64(b, &key_order));
    if (key_order > 65536)
        return UD_CORRUPT;
    f->key_order = (uint32_t)key_order;
    UD_TRY(ud_get_u64(b, &lineage));
    if (lineage > 4096)
        return UD_CORRUPT;
    if (lineage) {
        ud_id *ids = ud_alloc(b->ctx, (size_t)lineage * sizeof(*ids));
        if (!ids)
            return UD_RESOURCE;
        f->lineage = ids;
        f->lineage_count = (uint32_t)lineage;
        for (uint32_t j = 0; j < lineage; j++) {
            UD_TRY(ud_get_u64(b, &ids[j].high));
            UD_TRY(ud_get_u64(b, &ids[j].low));
            if ((!ids[j].high && !ids[j].low) || (j && ud_id_compare(ids[j - 1], ids[j]) >= 0))
                return UD_CORRUPT;
        }
    }
    uint64_t dictionary_kind, dictionary_count;
    UD_TRY(ud_get_u64(b, &dictionary_kind));
    UD_TRY(ud_get_u64(b, &dictionary_count));
    if (dictionary_kind > UD_DICTIONARY_SET || dictionary_count > 65535 ||
        (!dictionary_kind && dictionary_count))
        return UD_CORRUPT;
    f->dictionary_kind = (int32_t)dictionary_kind;
    if (dictionary_count) {
        ud_string *labels = ud_alloc(b->ctx, (size_t)dictionary_count * sizeof(*labels));
        if (!labels)
            return UD_RESOURCE;
        f->dictionary = labels;
        f->dictionary_count = (uint32_t)dictionary_count;
        for (uint32_t j = 0; j < dictionary_count; j++) {
            uint64_t length;
            UD_TRY(ud_get_u64(b, &length));
            if (length > UINT32_MAX || length > b->size - b->pos)
                return UD_CORRUPT;
            labels[j] =
                (ud_string){ud_strdup_n(b->ctx, (const char *)b->data + b->pos, (size_t)length),
                            (uint32_t)length};
            if (!labels[j].data)
                return UD_RESOURCE;
            b->pos += (size_t)length;
        }
    }
    UD_TRY(ud_get_u64(b, &children));
    if (children > 65536 || (f->type < UD_STRUCT && children) ||
        (f->type == UD_STRUCT && !children) || (f->type >= UD_LIST && children != 1))
        return UD_CORRUPT;
    if (children) {
        f->children = ud_alloc(b->ctx, (size_t)children * sizeof(ud_field));
        if (!f->children)
            return UD_RESOURCE;
        f->child_count = (uint32_t)children;
        for (uint32_t j = 0; j < children; j++)
            UD_TRY(get_field(b, (ud_field *)f->children + j, depth + 1));
    }
    return UD_OK;
}
static ud_status put_value(ud_bytes *b, const ud_value *v, uint32_t depth) {
    if (depth > 32)
        return UD_SCHEMA;
    UD_TRY(ud_put_u64(b, (uint64_t)v->is_absent));
    UD_TRY(ud_put_u64(b, (uint64_t)v->is_null));
    if (v->is_null)
        return UD_OK;
    if (v->type == UD_UTF8 || v->type == UD_BINARY)
        return ud_put_string(b, v->text, v->text_length);
    if (v->type == UD_DECIMAL128) {
        UD_TRY(ud_put_u64(b, v->decimal.low));
        return ud_put_u64(b, (uint64_t)v->decimal.high);
    }
    if (v->type == UD_INTERVAL) {
        UD_TRY(ud_put_u64(b, (uint64_t)(int64_t)v->interval.months));
        UD_TRY(ud_put_u64(b, (uint64_t)(int64_t)v->interval.days));
        return ud_put_u64(b, (uint64_t)v->interval.microseconds);
    }
    if (v->type == UD_UINT64)
        return ud_put_u64(b, v->unsigned_integer);
    if (v->type >= UD_STRUCT) {
        UD_TRY(ud_put_u64(b, v->child_count));
        for (uint32_t j = 0; j < v->child_count; j++)
            UD_TRY(put_value(b, v->children + j, depth + 1));
        return UD_OK;
    }
    uint64_t bits;
    if (v->type == UD_FLOAT64)
        memcpy(&bits, &v->real, 8);
    else
        bits = (uint64_t)v->integer;
    return ud_put_u64(b, bits);
}
static ud_status get_value(ud_bytes *b, const ud_field *f, ud_value *v, uint32_t depth) {
    if (depth > 32)
        return UD_SCHEMA;
    v->type = f->type;
    v->precision = f->precision;
    v->scale = f->scale;
    uint64_t null, absent;
    UD_TRY(ud_get_u64(b, &absent));
    if (absent > 1)
        return UD_CORRUPT;
    v->is_absent = (int32_t)absent;
    UD_TRY(ud_get_u64(b, &null));
    if (null > 1 || (absent && !null))
        return UD_CORRUPT;
    v->is_null = (int32_t)null;
    if (null)
        return UD_OK;
    if (f->type == UD_UTF8 || f->type == UD_BINARY) {
        uint64_t size;
        UD_TRY(ud_get_u64(b, &size));
        if (size > UINT32_MAX || size > b->size - b->pos)
            return UD_CORRUPT;
        v->text = ud_strdup_n(b->ctx, (const char *)b->data + b->pos, (size_t)size);
        if (!v->text)
            return UD_RESOURCE;
        v->text_length = (uint32_t)size;
        b->pos += (size_t)size;
    } else if (f->type == UD_DECIMAL128) {
        uint64_t hi;
        UD_TRY(ud_get_u64(b, &v->decimal.low));
        UD_TRY(ud_get_u64(b, &hi));
        memcpy(&v->decimal.high, &hi, 8);
    } else if (f->type == UD_INTERVAL) {
        uint64_t months, days, micros;
        UD_TRY(ud_get_u64(b, &months));
        UD_TRY(ud_get_u64(b, &days));
        UD_TRY(ud_get_u64(b, &micros));
        int64_t m, d;
        memcpy(&m, &months, 8);
        memcpy(&d, &days, 8);
        if (m < INT32_MIN || m > INT32_MAX || d < INT32_MIN || d > INT32_MAX)
            return UD_CORRUPT;
        v->interval.months = (int32_t)m;
        v->interval.days = (int32_t)d;
        memcpy(&v->interval.microseconds, &micros, 8);
    } else if (f->type == UD_UINT64) {
        UD_TRY(ud_get_u64(b, &v->unsigned_integer));
    } else if (f->type >= UD_STRUCT) {
        uint64_t count;
        UD_TRY(ud_get_u64(b, &count));
        if (count > INT32_MAX || count > SIZE_MAX / sizeof(ud_value) ||
            (f->type == UD_STRUCT && count != f->child_count))
            return UD_CORRUPT;
        if (count) {
            v->children = ud_alloc(b->ctx, (size_t)count * sizeof(ud_value));
            if (!v->children)
                return UD_RESOURCE;
            v->child_count = (uint32_t)count;
            for (uint32_t j = 0; j < count; j++)
                UD_TRY(get_value(b, f->children + (f->type == UD_STRUCT ? j : 0),
                                 (ud_value *)v->children + j, depth + 1));
        }
    } else {
        uint64_t bits;
        UD_TRY(ud_get_u64(b, &bits));
        if (f->type == UD_FLOAT64)
            memcpy(&v->real, &bits, 8);
        else
            memcpy(&v->integer, &bits, 8);
    }
    return UD_OK;
}
ud_status ud_encode_batch(ud_bytes *b, const ud_batch *table) {
    if (!b || !table)
        return UD_INVALID;
    UD_TRY(ud_put_u64(b, UD_WIRE_VERSION));
    UD_TRY(ud_put_u64(b,WIRE_LAYOUT));
    UD_TRY(ud_put_u64(b, table->columns));
    UD_TRY(ud_put_u64(b, table->rows));
    for (uint32_t j = 0; j < table->columns; j++)
        UD_TRY(put_field(b, &table->column[j].field, 0));
    for (uint32_t r = 0; r < table->rows; r++)
        for (uint32_t j = 0; j < table->columns; j++) {
            ud_value value;
            UD_TRY(ud_batch_get(table, r, j, &value));
            UD_TRY(put_value(b, &value, 0));
        }
    int present = table->row_info != NULL;
    for (const ud_batch *parent = table->data_owner; parent && !present;
         parent = parent->data_owner)
        present = parent->row_info != NULL;
    UD_TRY(ud_put_u64(b, present));
    uint64_t exceptional = 0;
    for (uint32_t row = 0; present && row < table->rows; row++) {
        ud_row_info info;
        UD_TRY(ud_batch_row_info(table, row, &info));
        UD_TRY(ud_put_u64(b, info.id.high));
        UD_TRY(ud_put_u64(b, info.id.low));
        UD_TRY(ud_put_u64(b, info.has_source_position));
        UD_TRY(ud_put_u64(b, info.source_position));
        if (info.flags || info.operation || info.rule.high || info.rule.low)
            exceptional++;
    }
    if (present)
        UD_TRY(ud_put_u64(b, exceptional));
    for (uint32_t row = 0; present && row < table->rows; row++) {
        ud_row_info info;
        UD_TRY(ud_batch_row_info(table, row, &info));
        if (!info.flags && !info.operation && !info.rule.high && !info.rule.low)
            continue;
        UD_TRY(ud_put_u64(b, row));
        UD_TRY(ud_put_u64(b, info.rule.high));
        UD_TRY(ud_put_u64(b, info.rule.low));
        UD_TRY(ud_put_u64(b, info.flags));
        UD_TRY(ud_put_u64(b, info.operation));
        UD_TRY(ud_put_u64(b, info.origins.high));
        UD_TRY(ud_put_u64(b, info.origins.low));
    }
    return UD_OK;
}
ud_status ud_decode_batch(ud_bytes *b, ud_batch **out) {
    if (!b || !out)
        return UD_INVALID;
    *out = NULL;
    uint64_t version, layout, columns, rows;
    UD_TRY(ud_get_u64(b, &version));
    if (version != UD_WIRE_VERSION)
        return UD_INCOMPATIBLE;
    UD_TRY(ud_get_u64(b,&layout));
    if (layout!=WIRE_LAYOUT) return UD_INCOMPATIBLE;
    UD_TRY(ud_get_u64(b, &columns));
    UD_TRY(ud_get_u64(b, &rows));
    if (!columns || columns > 65536 || rows > INT32_MAX)
        return UD_CORRUPT;
    ud_field *fields = ud_alloc(b->ctx, (size_t)columns * sizeof(*fields));
    ud_value *values = ud_alloc(b->ctx, (size_t)columns * sizeof(*values));
    if (!fields || !values) {
        ud_free(fields);
        ud_free(values);
        return UD_RESOURCE;
    }
    ud_status s = UD_OK;
    for (uint32_t j = 0; !s && j < columns; j++)
        s = get_field(b, fields + j, 0);
    if (!s)
        s = ud_batch_create(b->ctx, fields, (uint32_t)columns, (uint32_t)rows, out);
    for (uint32_t r = 0; !s && r < rows; r++) {
        memset(values, 0, (size_t)columns * sizeof(*values));
        for (uint32_t j = 0; !s && j < columns; j++)
            s = get_value(b, fields + j, values + j, 0);
        if (!s)
            s = ud_batch_append(*out, values, (uint32_t)columns);
        for (uint32_t j = 0; j < columns; j++)
            ud_clear_value(values + j);
    }
    uint64_t present = 0;
    if (!s)
        s = ud_get_u64(b, &present);
    if (!s && present > 1)
        s = UD_CORRUPT;
    for (uint32_t row = 0; !s && present && row < rows; row++) {
        ud_row_info info = {0};
        s = ud_get_u64(b, &info.id.high);
        if (!s)
            s = ud_get_u64(b, &info.id.low);
        uint64_t has_position = 0;
        if (!s)
            s = ud_get_u64(b, &has_position);
        if (!s && has_position > 1)
            s = UD_CORRUPT;
        info.has_source_position = (uint32_t)has_position;
        if (!s)
            s = ud_get_u64(b, &info.source_position);
        if (!s)
            s = ud_batch_set_row_info(*out, row, &info);
    }
    uint64_t exceptional = 0, previous = 0;
    if (!s && present)
        s = ud_get_u64(b, &exceptional);
    if (!s && exceptional > rows)
        s = UD_CORRUPT;
    for (uint64_t i = 0; !s && i < exceptional; i++) {
        uint64_t row = 0, flags = 0, operation = 0;
        s = ud_get_u64(b, &row);
        if (!s && (row >= rows || (i && row <= previous)))
            s = UD_CORRUPT;
        ud_row_info info = {0};
        if (!s)
            s = ud_batch_row_info(*out, (uint32_t)row, &info);
        if (!s)
            s = ud_get_u64(b, &info.rule.high);
        if (!s)
            s = ud_get_u64(b, &info.rule.low);
        if (!s)
            s = ud_get_u64(b, &flags);
        if (!s && (flags & ~((uint64_t)(UD_ROW_ERROR | UD_ROW_JOINED))))
            s = UD_CORRUPT;
        if (!s)
            s = ud_get_u64(b, &operation);
        if (!s && operation > 4)
            s = UD_CORRUPT;
        if (!s)
            s = ud_get_u64(b, &info.origins.high);
        if (!s)
            s = ud_get_u64(b, &info.origins.low);
        if (!s && !(flags & UD_ROW_JOINED) && (info.origins.high || info.origins.low))
            s = UD_CORRUPT;
        info.flags = (uint32_t)flags;
        info.operation = (uint32_t)operation;
        if (!s)
            s = ud_batch_set_row_info(*out, (uint32_t)row, &info);
        previous = row;
    }
    for (uint32_t j = 0; j < columns; j++)
        clear_field(fields + j);
    ud_free(fields);
    ud_free(values);
    if (!s)
        s = ud_batch_seal(*out);
    if (s) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}
ud_status ud_batch_serialize(const ud_batch *batch, uint8_t **data, uint64_t *length) {
    if (!batch || !data || !length)
        return UD_INVALID;
    *data = NULL;
    *length = 0;
    ud_bytes wire = {.ctx = batch->ctx};
    ud_status s = ud_encode_batch(&wire, batch);
    if (s) {
        ud_bytes_clear(&wire);
        return s;
    }
    *data = wire.data;
    *length = wire.size;
    return UD_OK;
}
ud_status ud_batch_deserialize(ud_context *ctx, const uint8_t *data, uint64_t length,
                               ud_batch **out) {
    if (!ctx || !data || !out || length > SIZE_MAX)
        return UD_INVALID;
    ud_bytes wire = {.ctx = ctx, .data = (uint8_t *)data, .size = (size_t)length};
    ud_status s = ud_decode_batch(&wire, out);
    if (!s && wire.pos != wire.size) {
        ud_batch_destroy(*out);
        *out = NULL;
        return UD_CORRUPT;
    }
    return s;
}
