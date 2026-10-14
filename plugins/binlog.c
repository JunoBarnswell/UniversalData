#include "binlog.h"
#include <stdio.h>
#include <math.h>
#include <zlib.h>
#define TRY(call)                                                                                  \
    do {                                                                                           \
        ud_status status = (call);                                                                 \
        if (status)                                                                                \
            return status;                                                                         \
    } while (0)
struct ud_binlog {
    ud_host_api host;
    ud_batch *schema;
    char *database, *table, *transaction, *incarnation;
    uint8_t *types, *meta, *unsigned_columns;
    uint8_t *before_map, *after_map;
    uint32_t columns, batch_rows;
    uint64_t table_id, version;
    int maria, begun, checksum;
    uint8_t *pending;
    size_t pending_size, pending_at;
    int pending_action, pending_control;
    ud_string pending_position;
    int64_t pending_watermark;
    char **owned_values;
    uint32_t owned_count, owned_capacity;
};
static ud_status own_value(ud_binlog *decoder, char *value) {
    if (decoder->owned_count == decoder->owned_capacity) {
        uint32_t capacity = decoder->owned_capacity ? decoder->owned_capacity * 2 : 16;
        if (capacity < decoder->owned_capacity || (uint64_t)capacity * sizeof(char *) > SIZE_MAX) {
            decoder->host.deallocate(value);
            return UD_RESOURCE;
        }
        char **values =
            decoder->host.allocate(decoder->host.context, (size_t)capacity * sizeof(*values));
        if (!values) {
            decoder->host.deallocate(value);
            return UD_RESOURCE;
        }
        if (decoder->owned_count)
            memcpy(values, decoder->owned_values, (size_t)decoder->owned_count * sizeof(*values));
        decoder->host.deallocate(decoder->owned_values);
        decoder->owned_values = values;
        decoder->owned_capacity = capacity;
    }
    decoder->owned_values[decoder->owned_count++] = value;
    return UD_OK;
}
static void clear_values(ud_binlog *decoder) {
    for (uint32_t j = 0; j < decoder->owned_count; j++)
        decoder->host.deallocate(decoder->owned_values[j]);
    decoder->owned_count = 0;
}
typedef struct bytes {
    const uint8_t *p;
    size_t n, at;
} bytes;
static int take(bytes *b, size_t n, const uint8_t **out) {
    if (n > b->n - b->at)
        return 0;
    *out = b->p + b->at;
    b->at += n;
    return 1;
}
static uint64_t little(const uint8_t *p, uint32_t n) {
    uint64_t value = 0;
    for (uint32_t i = 0; i < n; i++)
        value |= (uint64_t)p[i] << (i * 8);
    return value;
}
static uint64_t big(const uint8_t *p, uint32_t n) {
    uint64_t value = 0;
    for (uint32_t i = 0; i < n; i++)
        value = (value << 8) | p[i];
    return value;
}
static int packed(bytes *b, uint64_t *out) {
    const uint8_t *p;
    if (!take(b, 1, &p))
        return 0;
    uint32_t n = *p == 252 ? 2 : *p == 253 ? 3 : *p == 254 ? 8 : 0;
    if (*p < 251) {
        *out = *p;
        return 1;
    }
    if (!n || !take(b, n, &p))
        return 0;
    *out = little(p, n);
    return 1;
}
static uint32_t metadata_size(uint8_t type) {
    return type == 246 || type == 15 || type == 253 || type == 254 || type == 16 ? 2
           : type == 4 || type == 5 || type == 17 || type == 18 || type == 19 || type == 249 ||
                   type == 250 || type == 251 || type == 252 || type == 245
               ? 1
               : 0;
}
static ud_status map(ud_binlog *d, bytes *b) {
    const uint8_t *p;
    if (!take(b, 8, &p))
        return UD_CORRUPT;
    uint64_t table_id = little(p, 6);
    if (!take(b, 1, &p))
        return UD_CORRUPT;
    uint32_t dbn = *p;
    const uint8_t *database;
    if (!take(b, dbn + 1, &database) || database[dbn])
        return UD_CORRUPT;
    if (!take(b, 1, &p))
        return UD_CORRUPT;
    uint32_t tn = *p;
    const uint8_t *table;
    if (!take(b, tn + 1, &table) || table[tn])
        return UD_CORRUPT;
    uint64_t count, meta;
    if (!packed(b, &count))
        return UD_CORRUPT;
    const uint8_t *types;
    if (count > 65536 || !take(b, (size_t)count, &types) || !packed(b, &meta))
        return UD_CORRUPT;
    const uint8_t *metadata;
    if (!take(b, (size_t)meta, &metadata))
        return UD_CORRUPT;
    const uint8_t *nulls;
    if (!take(b, ((size_t)count + 7) / 8, &nulls))
        return UD_CORRUPT;
    if ((d->database && (strlen(d->database) != dbn || memcmp(d->database, database, dbn))) ||
        strlen(d->table) != tn || memcmp(d->table, table, tn))
        return UD_BUSY;
    if (count != d->columns)
        return UD_INCOMPATIBLE;
    uint32_t at = 0;
    for (uint32_t i = 0; i < count; i++) {
        uint32_t size = metadata_size(types[i]);
        if (at > meta || size > meta - at)
            return UD_CORRUPT;
        ud_field field;
        TRY(d->host.field(d->schema, i, &field));
        uint8_t type = types[i];
        int compatible =
            field.type == UD_BOOL ? type == 1
            : field.type == UD_INT64
                ? type == 1 || type == 2 || type == 3 || type == 8 || type == 9 || type == 13
            : field.type == UD_FLOAT64    ? type == 4 || type == 5
            : field.type == UD_UINT64     ? (field.dictionary_kind ? type == 254 : type == 8)
            : field.type == UD_DECIMAL128 ? type == 246 || type == 8
            : field.type == UD_UTF8 ? type == 15 || type == 253 || type == 254 || type == 245 ||
                                          (type >= 249 && type <= 252)
            : field.type == UD_BINARY ? type == 16 || type == 15 || type == 253 || type == 254 ||
                                            (type >= 249 && type <= 252)
            : field.type == UD_DATE32                              ? type == 10 || type == 14
            : field.type == UD_TIME64 || field.type == UD_DURATION ? type == 11 || type == 19
            : field.type == UD_TIMESTAMP ? type == 7 || type == 12 || type == 17 || type == 18
                                         : 0;
        if (!compatible)
            return UD_INCOMPATIBLE;
        if (type == 254 && (metadata[at] == 247 || metadata[at] == 248) &&
            field.dictionary_kind != (metadata[at] == 247 ? UD_DICTIONARY_ENUM : UD_DICTIONARY_SET))
            return UD_INCOMPATIBLE;
        if (type == 246 && (metadata[at] != field.precision || metadata[at + 1] != field.scale))
            return UD_INCOMPATIBLE;
        d->types[i] = type;
        d->meta[i * 2] = size ? metadata[at] : 0;
        d->meta[i * 2 + 1] = size == 2 ? metadata[at + 1] : 0;
        at += size;
    }
    if (at != meta)
        return UD_CORRUPT;
    memset(d->unsigned_columns, 0, d->columns);
    while (b->at < b->n) {
        if (!take(b, 1, &p))
            return UD_CORRUPT;
        uint32_t tag = *p;
        uint64_t n;
        if (!packed(b, &n) || n > b->n - b->at || !take(b, (size_t)n, &p))
            return UD_CORRUPT;
        if (tag == 1) {
            uint32_t bit = 0;
            for (uint32_t i = 0; i < d->columns; i++)
                if (d->types[i] == 1 || d->types[i] == 2 || d->types[i] == 3 || d->types[i] == 8 ||
                    d->types[i] == 9 || d->types[i] == 4 || d->types[i] == 5 ||
                    d->types[i] == 246) {
                    if (bit / 8 >= n)
                        return UD_CORRUPT;
                    d->unsigned_columns[i] = !!(p[bit / 8] & (1u << (7 - bit % 8)));
                    bit++;
                }
        }
    }
    d->table_id = table_id;
    return UD_BUSY;
}
static ud_status decimal(ud_binlog *d, bytes *b, uint32_t column, const ud_field *f, ud_value *v) {
    uint32_t precision = d->meta[column * 2], scale = d->meta[column * 2 + 1],
             integers = precision - scale;
    const uint32_t sizes[] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};
    uint32_t integer_groups = integers / 9, integer_rest = integers % 9,
             fraction_groups = scale / 9, fraction_rest = scale % 9,
             n = integer_groups * 4 + sizes[integer_rest] + fraction_groups * 4 +
                 sizes[fraction_rest];
    const uint8_t *input;
    if (!n || n > 19 || !take(b, n, &input))
        return UD_CORRUPT;
    uint8_t data[19];
    int positive = !!(input[0] & 128);
    uint8_t mask = positive ? 0 : 255;
    for (uint32_t i = 0; i < n; i++)
        data[i] = input[i] ^ mask;
    data[0] ^= 128;
    char text[96];
    uint32_t at = 0, length = 0;
    if (!positive)
        text[length++] = '-';
    int leading = 1;
    if (integer_rest) {
        uint32_t value = (uint32_t)big(data, sizes[integer_rest]);
        uint32_t limit = 1;
        for (uint32_t i = 0; i < integer_rest; i++)
            limit *= 10;
        if (value >= limit)
            return UD_CORRUPT;
        length += (uint32_t)snprintf(text + length, sizeof(text) - length, "%u", value);
        leading = 0;
        at += sizes[integer_rest];
    }
    for (uint32_t i = 0; i < integer_groups; i++) {
        uint32_t value = (uint32_t)big(data + at, 4);
        if (value >= 1000000000)
            return UD_CORRUPT;
        length += (uint32_t)snprintf(text + length, sizeof(text) - length, leading ? "%u" : "%09u",
                                     value);
        leading = 0;
        at += 4;
    }
    if (leading)
        text[length++] = '0';
    if (scale)
        text[length++] = '.';
    for (uint32_t i = 0; i < fraction_groups; i++) {
        uint32_t value = (uint32_t)big(data + at, 4);
        if (value >= 1000000000)
            return UD_CORRUPT;
        length += (uint32_t)snprintf(text + length, sizeof(text) - length, "%09u", value);
        at += 4;
    }
    if (fraction_rest) {
        uint32_t value = (uint32_t)big(data + at, sizes[fraction_rest]), limit = 1;
        for (uint32_t i = 0; i < fraction_rest; i++)
            limit *= 10;
        if (value >= limit)
            return UD_CORRUPT;
        length += (uint32_t)snprintf(text + length, sizeof(text) - length, "%0*u",
                                     (int)fraction_rest, value);
    }
    return d->host.parse_field(d->host.context, f, text, length, UD_ROUND_EXACT, v);
}
static ud_status value(ud_binlog *d, bytes *b, uint32_t column, const ud_field *f, ud_value *v) {
    uint8_t type = d->types[column];
    const uint8_t *p;
    uint32_t width = type == 1 || type == 13                               ? 1
                     : type == 2                                           ? 2
                     : type == 3 || type == 7 || type == 4                 ? 4
                     : type == 8 || type == 12 || type == 5                ? 8
                     : type == 9 || type == 10 || type == 14 || type == 11 ? 3
                                                                           : 0;
    *v = (ud_value){.type = f->type, .precision = f->precision, .scale = f->scale};
    if (type == 246)
        return decimal(d, b, column, f, v);
    if (width) {
        if (!take(b, width, &p))
            return UD_CORRUPT;
        uint64_t raw = little(p, width);
        if (type == 4) {
            uint32_t bits = (uint32_t)raw;
            float real;
            memcpy(&real, &bits, 4);
            v->real = real;
            return UD_OK;
        }
        if (type == 5) {
            memcpy(&v->real, &raw, 8);
            return UD_OK;
        }
        if (type == 7) {
            v->integer = (int64_t)raw * 1000000;
            return UD_OK;
        }
        if (type == 10 || type == 14) {
            char date[24];
            uint32_t year = (uint32_t)(raw >> 9), month = (uint32_t)(raw >> 5) & 15,
                     day = (uint32_t)raw & 31;
            int n = snprintf(date, sizeof(date), "%04u-%02u-%02u", year, month, day);
            return d->host.parse_field(d->host.context, f, date, (uint32_t)n, UD_ROUND_EXACT, v);
        }
        if (type == 12) {
            char timestamp[64];
            uint64_t date = raw / 1000000, time = raw % 1000000;
            int n = snprintf(
                timestamp, sizeof(timestamp), "%04llu-%02llu-%02lluT%02llu:%02llu:%02llu.000000Z",
                (unsigned long long)(date / 10000), (unsigned long long)(date / 100 % 100),
                (unsigned long long)(date % 100), (unsigned long long)(time / 10000),
                (unsigned long long)(time / 100 % 100), (unsigned long long)(time % 100));
            return d->host.parse_field(d->host.context, f, timestamp, (uint32_t)n, UD_ROUND_EXACT,
                                       v);
        }
        if (type == 11) {
            int32_t signed_time = (int32_t)raw;
            if (raw & 0x800000)
                signed_time |= (int32_t)0xff000000;
            int negative = signed_time < 0;
            uint32_t magnitude =
                negative ? (uint32_t)(-(int64_t)signed_time) : (uint32_t)signed_time;
            uint32_t hour = magnitude / 10000, minute = magnitude / 100 % 100,
                     second = magnitude % 100;
            if (hour > 838 || minute >= 60 || second >= 60)
                return UD_CORRUPT;
            int64_t micros = ((int64_t)hour * 3600 + minute * 60 + second) * 1000000;
            if (negative)
                micros = -micros;
            if (f->type == UD_TIME64 && (micros < 0 || micros >= 86400000000LL))
                return UD_UNSUPPORTED;
            v->integer = micros;
            return UD_OK;
        }
        if (type == 13) {
            v->integer = raw ? raw + 1900 : 0;
            return UD_OK;
        }
        if (f->type == UD_DECIMAL128) {
            char text[32];
            int64_t signed_value;
            memcpy(&signed_value, &raw, 8);
            int length = d->unsigned_columns[column]
                             ? snprintf(text, sizeof(text), "%llu", (unsigned long long)raw)
                             : snprintf(text, sizeof(text), "%lld", (long long)signed_value);
            return d->host.parse_field(d->host.context, f, text, (uint32_t)length, UD_ROUND_EXACT,
                                       v);
        }
        if (!d->unsigned_columns[column] && width < 8 && (raw & (UINT64_C(1) << (width * 8 - 1))))
            raw |= UINT64_MAX << (width * 8);
        memcpy(&v->integer, &raw, 8);
        if (f->type == UD_BOOL && v->integer != 0 && v->integer != 1)
            return UD_SCHEMA;
        return UD_OK;
    }
    if (type == 17 || type == 18 || type == 19) {
        uint32_t fraction = d->meta[column * 2], bytes = (fraction + 1) / 2;
        if (fraction > 6)
            return UD_CORRUPT;
        uint32_t base = type == 17 ? 4 : type == 18 ? 5 : 3;
        if (!take(b, base, &p))
            return UD_CORRUPT;
        uint64_t raw = big(p, base);
        const uint8_t *decimal;
        if (!take(b, bytes, &decimal))
            return UD_CORRUPT;
        uint32_t raw_fraction = (uint32_t)big(decimal, bytes);
        int64_t fractional = raw_fraction;
        int64_t packed_time = (int64_t)raw - 0x800000;
        if (type == 19 && packed_time < 0 && raw_fraction) {
            packed_time++;
            fractional -= (INT64_C(1) << (bytes * 8));
        }
        if (bytes == 1)
            fractional *= 10000;
        else if (bytes == 2)
            fractional *= 100;
        if (fractional <= -1000000 || fractional >= 1000000)
            return UD_CORRUPT;
        uint32_t micros = (uint32_t)fractional;
        if (type == 17) {
            v->integer = (int64_t)raw * 1000000 + fractional;
            return UD_OK;
        }
        if (type == 19) {
            uint64_t magnitude = packed_time < 0 ? (uint64_t)-packed_time : (uint64_t)packed_time;
            uint32_t hour = (uint32_t)(magnitude >> 12), minute = (uint32_t)(magnitude >> 6) & 63,
                     second = (uint32_t)magnitude & 63;
            if (hour > 838 || minute >= 60 || second >= 60)
                return UD_CORRUPT;
            int64_t seconds = (int64_t)hour * 3600 + minute * 60 + second;
            v->integer = (packed_time < 0 ? -seconds : seconds) * 1000000 + fractional;
            if (f->type == UD_TIME64 && (v->integer < 0 || v->integer >= 86400000000LL))
                return UD_UNSUPPORTED;
            return UD_OK;
        }
        if (raw < UINT64_C(0x8000000000))
            return UD_PARSE;
        raw -= UINT64_C(0x8000000000);
        uint32_t ymd = (uint32_t)(raw >> 17), ym = ymd >> 5;
        char timestamp[80];
        int n = snprintf(timestamp, sizeof(timestamp), "%04u-%02u-%02uT%02u:%02u:%02u.%06uZ",
                         ym / 13, ym % 13, ymd & 31, (uint32_t)(raw >> 12) & 31,
                         (uint32_t)(raw >> 6) & 63, (uint32_t)raw & 63, micros);
        return d->host.parse_field(d->host.context, f, timestamp, (uint32_t)n, UD_ROUND_EXACT, v);
    }
    uint64_t length = 0;
    if (type == 15 || type == 253 || type == 254) {
        uint32_t maximum = (uint32_t)d->meta[column * 2] | ((uint32_t)d->meta[column * 2 + 1] << 8);
        if (type == 254) {
            uint32_t meta = ((uint32_t)d->meta[column * 2] << 8) | d->meta[column * 2 + 1];
            uint32_t real = meta >> 8;
            if (real == 247 || real == 248) {
                width = meta & 255;
                if (!width || width > (real == 247 ? 2u : 8u) || !take(b, width, &p))
                    return UD_CORRUPT;
                uint64_t index = little(p, width);
                if ((real == 247 && index > f->dictionary_count) ||
                    (real == 248 && f->dictionary_count < 64 && (index >> f->dictionary_count)))
                    return UD_CORRUPT;
                v->unsigned_integer = index;
                return UD_OK;
            }
            maximum = ((meta >> 4) & 0x300) ^ (meta & 255);
        }
        width = maximum > 255 ? 2 : 1;
        if (!take(b, width, &p))
            return UD_CORRUPT;
        length = little(p, width);
    } else if (type == 16) {
        uint32_t bits = (uint32_t)d->meta[column * 2] + (uint32_t)d->meta[column * 2 + 1] * 8;
        length = (bits + 7) / 8;
    } else if ((type >= 249 && type <= 252) || type == 245) {
        width = d->meta[column * 2];
        if (width < 1 || width > 4 || !take(b, width, &p))
            return UD_CORRUPT;
        length = little(p, width);
    } else
        return UD_UNSUPPORTED;
    if (length > 16777216 || !take(b, (size_t)length, &p))
        return length > 16777216 ? UD_RESOURCE : UD_CORRUPT;
    if (type == 245) {
        char *json = NULL;
        uint32_t n;
        TRY(d->host.mysql_json_decode(d->host.context, p, (uint32_t)length, &json, &n));
        TRY(own_value(d, json));
        v->text = json;
        v->text_length = n;
        return UD_OK;
    }
    if (f->type == UD_BINARY) {
        v->text = (const char *)p;
        v->text_length = (uint32_t)length;
        return UD_OK;
    }
    return d->host.parse_field(d->host.context, f, (const char *)p, (uint32_t)length,
                               UD_ROUND_EXACT, v);
}
static ud_status tuple(ud_binlog *d, bytes *b, const uint8_t *present, ud_value *values) {
    uint32_t count = 0;
    for (uint32_t i = 0; i < d->columns; i++)
        count += !!(present[i / 8] & (1u << (i % 8)));
    const uint8_t *nulls;
    if (!take(b, (count + 7) / 8, &nulls))
        return UD_CORRUPT;
    uint32_t at = 0;
    for (uint32_t i = 0; i < d->columns; i++) {
        ud_field f;
        TRY(d->host.field(d->schema, i, &f));
        values[i] =
            (ud_value){.type = f.type, .is_null = 1, .precision = f.precision, .scale = f.scale};
        if (!(present[i / 8] & (1u << (i % 8)))) {
            values[i].is_absent = 1;
            continue;
        }
        if (!(nulls[at / 8] & (1u << (at % 8))))
            TRY(value(d, b, i, &f, values + i));
        at++;
    }
    return UD_OK;
}
static ud_status rows_payload(ud_binlog *d, bytes *b, int action, ud_cdc_event *out) {
    ud_field *f = NULL;
    TRY(plugin_fields(&d->host, d->schema, &f));
    uint32_t capacity = d->batch_rows;
    ud_status s = d->host.batch_create(d->host.context, f, d->columns, capacity, &out->after);
    if (!s && action != UD_CHANGE_INSERT)
        s = d->host.batch_create(d->host.context, f, d->columns, capacity, &out->before);
    d->host.deallocate(f);
    ud_value *before = d->host.allocate(d->host.context, d->columns * sizeof(*before)),
             *after = d->host.allocate(d->host.context, d->columns * sizeof(*after));
    out->operations = d->host.allocate(d->host.context, capacity * sizeof(*out->operations));
    if (!before || !after || !out->operations)
        s = UD_RESOURCE;
    while (!s && b->at < b->n) {
        uint32_t row = d->host.rows(out->after);
        if (row == capacity)
            break;
        if (action != UD_CHANGE_INSERT)
            s = tuple(d, b, d->before_map, before);
        if (!s && action != UD_CHANGE_DELETE)
            s = tuple(d, b, d->after_map, after);
        if (!s && action == UD_CHANGE_DELETE)
            memcpy(after, before, d->columns * sizeof(*after));
        for (uint32_t i = 0; !s && action == UD_CHANGE_UPDATE && i < d->columns; i++) {
            ud_field field;
            s = d->host.field(d->schema, i, &field);
            if (!s && field.key_order && after[i].is_absent && !before[i].is_absent)
                after[i] = before[i];
        }
        if (!s)
            s = d->host.append(out->after, after, d->columns);
        if (!s && out->before)
            s = d->host.append(out->before, before, d->columns);
        if (!s)
            out->operations[row] = action;
    }
    d->host.deallocate(before);
    d->host.deallocate(after);
    clear_values(d);
    out->kind = UD_CDC_ROWS;
    return s;
}
static ud_status rows(ud_binlog *d, bytes *b, int kind, ud_cdc_event *out) {
    const uint8_t *p;
    if (!take(b, 8, &p))
        return UD_CORRUPT;
    uint64_t table = little(p, 6);
    if (table != d->table_id)
        return UD_BUSY;
    int v2 = kind >= 30;
    if (v2) {
        if (!take(b, 2, &p))
            return UD_CORRUPT;
        uint32_t extra = (uint32_t)little(p, 2);
        if (extra < 2 || !take(b, extra - 2, &p))
            return UD_CORRUPT;
    }
    uint64_t count;
    if (!packed(b, &count) || count != d->columns)
        return UD_INCOMPATIBLE;
    uint32_t maps = kind == 24 || kind == 31 ? 2 : 1;
    for (uint32_t m = 0; m < maps; m++) {
        if (!take(b, (d->columns + 7) / 8, &p))
            return UD_CORRUPT;
        uint8_t *map = maps == 2                  ? (m ? d->after_map : d->before_map)
                       : kind == 23 || kind == 30 ? d->after_map
                                                  : d->before_map;
        memcpy(map, p, (d->columns + 7) / 8);
    }
    int action = kind == 23 || kind == 30   ? UD_CHANGE_INSERT
                 : kind == 24 || kind == 31 ? UD_CHANGE_UPDATE
                                            : UD_CHANGE_DELETE;
    ud_status status = rows_payload(d, b, action, out);
    if (!status && b->at < b->n) {
        d->pending_size = b->n - b->at;
        d->pending = d->host.allocate(d->host.context, d->pending_size);
        if (!d->pending)
            return UD_RESOURCE;
        memcpy(d->pending, b->p + b->at, d->pending_size);
        d->pending_at = 0;
        d->pending_action = action;
    }
    return status;
}

ud_status ud_binlog_packet(const void *raw, size_t size, const uint8_t **out, size_t *length) {
    if (!raw || !out || !length)
        return UD_INVALID;
    const uint8_t *data = raw;
    /* Client APIs differ only in whether their raw event includes the protocol
     * status byte. Exact event length proves its presence; zero timestamps do
     * not. Every decoder and rotation uses this same framing owner. */
    if (size >= 20 && data[0] == 0 && little(data + 10, 4) == size - 1) {
        data++;
        size--;
    }
    if (size < 19 || little(data + 9, 4) != size)
        return UD_CORRUPT;
    *out = data;
    *length = size;
    return UD_OK;
}
static void sql_space(const char **text, size_t *size) {
    while (*size && (**text == ' ' || **text == '\t' || **text == '\r' || **text == '\n')) {
        (*text)++;
        (*size)--;
    }
}
static int sql_word(const char **text, size_t *size, const char *word) {
    size_t length = strlen(word);
    if (*size < length)
        return 0;
    for (size_t at = 0; at < length; at++) {
        unsigned char value = (unsigned char)(*text)[at];
        if (value >= 'a' && value <= 'z')
            value -= 'a' - 'A';
        if (value != (unsigned char)word[at])
            return 0;
    }
    if (*size > length && (*text)[length] != ' ' && (*text)[length] != '\t' &&
        (*text)[length] != '\n' && (*text)[length] != '\r')
        return 0;
    *text += length;
    *size -= length;
    sql_space(text, size);
    return 1;
}
static int sql_name(const char **text, size_t *size, char output[257]) {
    uint32_t length = 0;
    int quoted = *size && **text == '`';
    if (quoted) {
        (*text)++;
        (*size)--;
    }
    int closed = !quoted;
    while (*size) {
        unsigned char value = (unsigned char)**text;
        if (quoted && value == '`') {
            (*text)++;
            (*size)--;
            if (*size && **text == '`') {
                value = '`';
            } else {
                closed = 1;
                break;
            }
        } else if (!quoted && !(value > 127 || (value >= 'a' && value <= 'z') ||
                                (value >= 'A' && value <= 'Z') || (value >= '0' && value <= '9') ||
                                value == '_' || value == '$'))
            break;
        if (length == 256 || !value)
            return 0;
        output[length++] = (char)value;
        (*text)++;
        (*size)--;
    }
    output[length] = 0;
    sql_space(text, size);
    return length && closed;
}
static int source_truncate(ud_binlog *decoder, const char *query, size_t size, const char *database,
                           size_t database_length) {
    if (!sql_word(&query, &size, "TRUNCATE"))
        return 0;
    (void)sql_word(&query, &size, "TABLE");
    char first[257], second[257];
    if (!sql_name(&query, &size, first))
        return 0;
    const char *table = first, *schema = NULL;
    if (size && *query == '.') {
        query++;
        size--;
        sql_space(&query, &size);
        if (!sql_name(&query, &size, second))
            return 0;
        schema = first;
        table = second;
    }
    if (size && *query == ';') {
        query++;
        size--;
        sql_space(&query, &size);
    }
    if (size || strcmp(table, decoder->table))
        return 0;
    if (decoder->database) {
        if (schema)
            return !strcmp(schema, decoder->database);
        return strlen(decoder->database) == database_length &&
               !memcmp(database, decoder->database, database_length);
    }
    return schema == NULL;
}
ud_status ud_binlog_decode(ud_binlog *d, const void *raw, size_t size, ud_string file,
                           ud_cdc_event *out) {
    memset(out, 0, sizeof(*out));
    out->struct_size = sizeof(*out);
    out->abi_version = UD_ABI_VERSION;
    out->schema_version = d->version;
    const uint8_t *data = NULL;
    TRY(ud_binlog_packet(raw, size, &data, &size));
    uint32_t kind = data[4];
    uint64_t position = little(data + 13, 4);
    bytes body = {data + 19, size - 19, 0};
    out->watermark = (int64_t)little(data, 4) * 1000000;
    if (kind == 15) {
        if (body.n < 58)
            return UD_CORRUPT;
        int checksum = body.p[body.n - 5];
        if (checksum == 1) {
            uint32_t expected = (uint32_t)little(data + size - 4, 4);
            if ((uint32_t)crc32(0, data, (uInt)(size - 4)) != expected)
                return UD_CORRUPT;
            d->checksum = 1;
        } else if (checksum != 0 && checksum != 255)
            return UD_UNSUPPORTED;
        return UD_BUSY;
    }
    if (d->checksum) {
        if (body.n < 4)
            return UD_CORRUPT;
        if ((uint32_t)crc32(0, data, (uInt)(size - 4)) != little(data + size - 4, 4)) {
            return UD_CORRUPT;
        }
        body.n -= 4;
    }
    if (kind == 19) {
        ud_status mapped = map(d, &body);
        return mapped == UD_OK ? UD_BUSY : mapped;
    }
    ud_status s = UD_BUSY;
    if (kind == 33 || kind == 34 || kind == 162) {
        char transaction[160];
        if (kind == 162) {
            if (!d->maria || body.n < 13)
                return UD_CORRUPT;
            uint64_t sequence = little(body.p, 8);
            uint32_t domain = (uint32_t)little(body.p + 8, 4),
                     server = (uint32_t)little(data + 5, 4);
            snprintf(transaction, sizeof(transaction), "%u-%u-%llu", domain, server,
                     (unsigned long long)sequence);
            if (body.p[12] & 32) {
                d->host.deallocate(d->transaction);
                d->transaction =
                    plugin_string(&d->host, transaction, (uint32_t)strlen(transaction));
                return d->transaction ? UD_BUSY : UD_RESOURCE;
            }
        } else {
            if (d->maria || body.n < 25)
                return UD_CORRUPT;
            char sid[33];
            for (uint32_t i = 0; i < 16; i++)
                snprintf(sid + i * 2, 3, "%02x", body.p[1 + i]);
            snprintf(transaction, sizeof(transaction), "%s:%llu", sid,
                     (unsigned long long)little(body.p + 17, 8));
        }
        if (d->begun)
            return UD_CORRUPT;
        d->begun = 1;
        d->host.deallocate(d->transaction);
        d->transaction = plugin_string(&d->host, transaction, (uint32_t)strlen(transaction));
        out->kind = UD_CDC_BEGIN;
        out->transaction_id =
            (ud_string){plugin_string(&d->host, transaction, (uint32_t)strlen(transaction)),
                        (uint32_t)strlen(transaction)};
        s = out->transaction_id.data ? UD_OK : UD_RESOURCE;
    } else if (kind == 16) {
        if (!d->begun || body.n != 8)
            return UD_CORRUPT;
        d->begun = 0;
        out->kind = UD_CDC_COMMIT;
        s = UD_OK;
    } else if (kind == 23 || kind == 24 || kind == 25 || kind == 30 || kind == 31 || kind == 32) {
        if (!d->begun)
            return UD_CORRUPT;
        s = rows(d, &body, (int)kind, out);
    } else if (kind == 2) {
        if (body.n < 13)
            return UD_CORRUPT;
        uint32_t database_length = body.p[8], status = (uint32_t)little(body.p + 11, 2);
        size_t at = 13 + status + database_length + 1;
        if (at > body.n)
            return UD_CORRUPT;
        const char *query = (const char *)body.p + at;
        size_t n = body.n - at;
        /* Query events may be protocol comment placeholders (MariaDB's
         * annotation downgrade), not DDL. Strip complete SQL comments first. */
        for (;;) {
            while (n && (*query == ' ' || *query == '\t' || *query == '\r' || *query == '\n')) {
                query++;
                n--;
            }
            if (n && (*query == '#' || (n > 1 && query[0] == '-' && query[1] == '-'))) {
                while (n && *query != '\n') {
                    query++;
                    n--;
                }
                continue;
            }
            if (n > 1 && query[0] == '/' && query[1] == '*') {
                size_t end = 2;
                while (end + 1 < n && !(query[end] == '*' && query[end + 1] == '/'))
                    end++;
                if (end + 1 == n)
                    return UD_CORRUPT;
                query += end + 2;
                n -= end + 2;
                continue;
            }
            break;
        }
        if (!n)
            return UD_BUSY;
        if (n == 6 && !memcmp(query, "COMMIT", 6)) {
            if (!d->begun)
                return UD_CORRUPT;
            d->begun = 0;
            out->kind = UD_CDC_COMMIT;
            s = UD_OK;
        } else if (n == 5 && !memcmp(query, "BEGIN", 5)) {
            if (d->begun)
                s = UD_BUSY;
            else {
                /* Legacy binlog streams expose BEGIN without a GTID packet.
                 * Its physical start position is the transaction identity. */
                char identity[320];
                int length = snprintf(identity, sizeof(identity), "%.*s:%llu", (int)file.length,
                                      file.data, (unsigned long long)position);
                if (length < 0 || (size_t)length >= sizeof(identity))
                    return UD_RESOURCE;
                d->begun = 1;
                d->host.deallocate(d->transaction);
                d->transaction = plugin_string(&d->host, identity, (uint32_t)length);
                out->transaction_id = (ud_string){
                    plugin_string(&d->host, identity, (uint32_t)length), (uint32_t)length};
                out->kind = UD_CDC_BEGIN;
                s = d->transaction && out->transaction_id.data ? UD_OK : UD_RESOURCE;
            }
        } else if (source_truncate(d, query, n, (const char *)body.p + 13 + status,
                                   database_length)) {
            if (!d->begun) {
                char identity[320];
                int length = snprintf(identity, sizeof(identity), "%.*s:%llu", (int)file.length,
                                      file.data, (unsigned long long)position);
                if (length < 0 || (size_t)length >= sizeof(identity))
                    return UD_RESOURCE;
                d->host.deallocate(d->transaction);
                d->transaction = plugin_string(&d->host, identity, (uint32_t)length);
                out->transaction_id = (ud_string){
                    plugin_string(&d->host, identity, (uint32_t)length), (uint32_t)length};
                if (!d->transaction || !out->transaction_id.data)
                    return UD_RESOURCE;
                d->begun = 1;
                out->kind = UD_CDC_BEGIN;
                d->pending_control = 1;
            } else {
                out->kind = UD_CDC_TRUNCATE;
                d->pending_control = 2;
            }
            s = UD_OK;
        } else {
            out->kind = UD_CDC_SCHEMA_CHANGE;
            s = UD_OK;
        }
    } else if (kind == 27) {
        out->kind = UD_CDC_HEARTBEAT;
        s = UD_OK;
    } else if (kind == 4 || kind == 35 || kind == 163 || kind == 29 || kind == 160)
        s = UD_BUSY;
    else if (kind == 39 || kind == 40 || kind >= 166)
        s = UD_UNSUPPORTED;
    if (!s) {
        if (out->kind == UD_CDC_SCHEMA_CHANGE)
            out->schema_version = ++d->version;
        size_t n = file.length + strlen(d->incarnation) + 256;
        char *text = d->host.allocate(d->host.context, n);
        if (!text)
            return UD_RESOURCE;
        int length = snprintf(text, n,
                              "{\"file\":\"%.*s\",\"position\":%llu,\"gtid\":\"%s\",\"vendor\":\"%"
                              "s\",\"incarnation\":\"%s\"}",
                              (int)file.length, file.data, (unsigned long long)position,
                              d->transaction ? d->transaction : "", d->maria ? "mariadb" : "mysql",
                              d->incarnation);
        if (length < 0 || (size_t)length >= n) {
            d->host.deallocate(text);
            return UD_RESOURCE;
        }
        out->position = (ud_string){text, (uint32_t)length};
        if (d->pending || d->pending_control) {
            d->pending_position =
                (ud_string){plugin_string(&d->host, text, (uint32_t)length), (uint32_t)length};
            d->pending_watermark = out->watermark;
            if (!d->pending_position.data)
                return UD_RESOURCE;
        }
    }
    return s;
}
ud_status ud_binlog_create(const ud_host_api *h, const ud_cdc_options *o, int maria,
                           ud_string incarnation, int checksum, ud_binlog **out) {
    *out = NULL;
    if (!o->schema || !o->table.length)
        return UD_SCHEMA;
    ud_binlog *d = h->allocate(h->context, sizeof(*d));
    if (!d)
        return UD_RESOURCE;
    d->host = *h;
    d->incarnation = plugin_string(h, incarnation.data, incarnation.length);
    d->maria = maria;
    d->checksum = checksum;
    d->version = 1;
    d->columns = h->columns(o->schema);
    d->batch_rows = o->batch_rows;
    const char *dot = memchr(o->table.data, '.', o->table.length);
    if (dot) {
        d->database = plugin_string(h, o->table.data, (uint32_t)(dot - o->table.data));
        d->table = plugin_string(h, dot + 1, o->table.length - (uint32_t)(dot - o->table.data) - 1);
    } else
        d->table = plugin_string(h, o->table.data, o->table.length);
    d->types = h->allocate(h->context, d->columns);
    d->meta = h->allocate(h->context, d->columns * 2);
    d->unsigned_columns = h->allocate(h->context, d->columns);
    d->before_map = h->allocate(h->context, (d->columns + 7) / 8);
    d->after_map = h->allocate(h->context, (d->columns + 7) / 8);
    ud_field *f = NULL;
    ud_status s = d->incarnation && d->table && d->types && d->meta && d->unsigned_columns &&
                          d->before_map && d->after_map
                      ? plugin_fields(h, o->schema, &f)
                      : UD_RESOURCE;
    if (!s)
        s = h->batch_create(h->context, f, d->columns, 0, &d->schema);
    h->deallocate(f);
    if (s) {
        ud_binlog_destroy(d);
        return s;
    }
    *out = d;
    return UD_OK;
}
void ud_binlog_destroy(ud_binlog *d) {
    if (!d)
        return;
    clear_values(d);
    d->host.deallocate(d->owned_values);
    d->host.batch_destroy(d->schema);
    d->host.deallocate(d->database);
    d->host.deallocate(d->table);
    d->host.deallocate(d->transaction);
    d->host.deallocate(d->incarnation);
    d->host.deallocate(d->pending);
    d->host.deallocate(d->before_map);
    d->host.deallocate(d->after_map);
    d->host.deallocate((void *)d->pending_position.data);
    d->host.deallocate(d->types);
    d->host.deallocate(d->meta);
    d->host.deallocate(d->unsigned_columns);
    d->host.deallocate(d);
}

ud_status ud_binlog_rebind(ud_binlog *d, const ud_batch *schema, uint64_t version,
                           ud_string position) {
    if (d->pending || d->pending_control)
        return UD_BUSY;
    ud_field *fields = NULL;
    ud_batch *owned = NULL;
    ud_status status = plugin_fields(&d->host, schema, &fields);
    uint32_t columns = d->host.columns(schema);
    if (!status)
        status = d->host.batch_create(d->host.context, fields, columns, 0, &owned);
    d->host.deallocate(fields);
    uint8_t *types = NULL, *meta = NULL, *unsigned_columns = NULL, *before = NULL, *after = NULL;
    if (!status) {
        types = d->host.allocate(d->host.context, columns);
        meta = d->host.allocate(d->host.context, columns * 2);
        unsigned_columns = d->host.allocate(d->host.context, columns);
        before = d->host.allocate(d->host.context, (columns + 7) / 8);
        after = d->host.allocate(d->host.context, (columns + 7) / 8);
        if (!types || !meta || !unsigned_columns || !before || !after)
            status = UD_RESOURCE;
    }
    if (!status) {
        d->host.batch_destroy(d->schema);
        d->schema = owned;
        owned = NULL;
        d->host.deallocate(d->types);
        d->host.deallocate(d->meta);
        d->host.deallocate(d->unsigned_columns);
        d->host.deallocate(d->before_map);
        d->host.deallocate(d->after_map);
        d->types = types;
        d->meta = meta;
        d->unsigned_columns = unsigned_columns;
        d->before_map = before;
        d->after_map = after;
        d->columns = columns;
        d->table_id = 0;
        d->version = version;
        if (position.length) {
            d->pending_position = (ud_string){
                plugin_string(&d->host, position.data, position.length), position.length};
            if (!d->pending_position.data)
                return UD_RESOURCE;
            d->pending_control = d->begun ? 2 : 3;
        }
    } else {
        d->host.deallocate(types);
        d->host.deallocate(meta);
        d->host.deallocate(unsigned_columns);
        d->host.deallocate(before);
        d->host.deallocate(after);
    }
    d->host.batch_destroy(owned);
    return status;
}
void ud_binlog_unchanged_schema(ud_binlog *d, ud_cdc_event *out) {
    /* Unrelated database DDL still completes its log transaction. It changes
     * no source columns and cannot leave a previously emitted BEGIN open. */
    out->kind = d->begun ? UD_CDC_COMMIT : UD_CDC_HEARTBEAT;
    d->begun = 0;
}
ud_status ud_binlog_drain(ud_binlog *d, ud_cdc_event *out) {
    if (d->pending_control) {
        memset(out, 0, sizeof(*out));
        out->struct_size = sizeof(*out);
        out->abi_version = UD_ABI_VERSION;
        out->schema_version = d->version;
        out->watermark = d->pending_watermark;
        out->position = (ud_string){
            plugin_string(&d->host, d->pending_position.data, d->pending_position.length),
            d->pending_position.length};
        if (!out->position.data)
            return UD_RESOURCE;
        if (d->pending_control == 3) {
            out->kind = UD_CDC_BEGIN;
            d->begun = 1;
            d->pending_control = 2;
            out->transaction_id = (ud_string){
                plugin_string(&d->host, d->pending_position.data, d->pending_position.length),
                d->pending_position.length};
            if (!out->transaction_id.data)
                return UD_RESOURCE;
        } else if (d->pending_control == 1) {
            out->kind = UD_CDC_TRUNCATE;
            d->pending_control = 2;
        } else {
            out->kind = UD_CDC_COMMIT;
            d->pending_control = 0;
            d->begun = 0;
            d->host.deallocate((void *)d->pending_position.data);
            d->pending_position = (ud_string){0};
        }
        return UD_OK;
    }
    if (!d->pending)
        return UD_BUSY;
    memset(out, 0, sizeof(*out));
    out->struct_size = sizeof(*out);
    out->abi_version = UD_ABI_VERSION;
    out->schema_version = d->version;
    out->watermark = d->pending_watermark;
    bytes payload = {d->pending, d->pending_size, d->pending_at};
    ud_status status = rows_payload(d, &payload, d->pending_action, out);
    d->pending_at = payload.at;
    if (!status) {
        out->position = (ud_string){
            plugin_string(&d->host, d->pending_position.data, d->pending_position.length),
            d->pending_position.length};
        if (!out->position.data)
            status = UD_RESOURCE;
    }
    if (!status && d->pending_at == d->pending_size) {
        d->host.deallocate(d->pending);
        d->pending = NULL;
        d->host.deallocate((void *)d->pending_position.data);
        d->pending_position = (ud_string){0};
    }
    return status;
}
ud_status ud_binlog_rotation(ud_binlog *d, const void *raw, size_t size, ud_string *file,
                             uint64_t *offset) {
    const uint8_t *data = NULL;
    TRY(ud_binlog_packet(raw, size, &data, &size));
    if (size < 27 || data[4] != 4)
        return UD_CORRUPT;
    size_t n = size - 27;
    if (d->checksum) {
        if (n < 4)
            return UD_CORRUPT;
        n -= 4;
    }
    if (!n || n > 255)
        return UD_CORRUPT;
    for (size_t i = 0; i < n; i++) {
        unsigned char c = data[27 + i];
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
              c == '.' || c == '_' || c == '-'))
            return UD_CORRUPT;
    }
    *offset = little(data + 19, 8);
    *file = (ud_string){plugin_string(&d->host, (const char *)data + 27, (uint32_t)n), (uint32_t)n};
    return file->data ? UD_OK : UD_RESOURCE;
}
