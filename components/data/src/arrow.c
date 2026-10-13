#include "../internal.h"
_Static_assert(sizeof(ud_interval) == 16, "Arrow Interval storage must retain 16-byte layout");
#include <errno.h>
static void release_schema(struct ArrowSchema *s) {
    if (!s || !s->release)
        return;
    s->release = NULL;
    for (int64_t i = 0; s->children && i < s->n_children; i++) {
        if (s->children[i]) {
            release_schema(s->children[i]);
            ud_free(s->children[i]);
        }
    }
    ud_free(s->children);
    ud_free((void *)s->name);
    ud_free((void *)s->format);
    ud_free((void *)s->metadata);
    s->children = NULL;
}
static void release_array(struct ArrowArray *a) {
    if (!a || !a->release)
        return;
    a->release = NULL;
    for (int64_t i = 0; a->children && i < a->n_children; i++)
        if (a->children[i]) {
            release_array(a->children[i]);
            ud_free(a->children[i]);
        }
    if (a->buffers)
        for (int64_t i = 0; i < a->n_buffers; i++)
            ud_free((void *)a->buffers[i]);
    ud_free(a->children);
    ud_free(a->buffers);
    a->children = NULL;
    a->buffers = NULL;
}
static ud_status metadata_integer(ud_bytes *bytes, int32_t value) {
    return ud_bytes_put(bytes, &value, sizeof(value)); /* Arrow C metadata native int32. */
}
static ud_status metadata_pair(ud_bytes *bytes, const char *key, const char *value,
                               uint32_t length) {
    UD_TRY(metadata_integer(bytes, (int32_t)strlen(key)));
    UD_TRY(ud_bytes_put(bytes, key, strlen(key)));
    UD_TRY(metadata_integer(bytes, (int32_t)length));
    return ud_bytes_put(bytes, value, length);
}
static ud_status export_metadata(ud_context *ctx, const ud_field *field, const char **out) {
    *out = NULL;
    ud_bytes metadata = {.ctx = ctx}, lineage = {.ctx = ctx};
    char id[33];
    ud_status s = metadata_integer(&metadata, 2 + (field->type == UD_INTERVAL) +
                                                  (field->dictionary_kind != 0));
    ud_id_format(field->identity, id);
    if (!s)
        s = metadata_pair(&metadata, "ud.column_id", id, 32);
    for (uint32_t j = 0; !s && j < field->lineage_count; j++) {
        if (j)
            s = ud_bytes_put(&lineage, ",", 1);
        ud_id_format(field->lineage[j], id);
        if (!s)
            s = ud_bytes_put(&lineage, id, 32);
    }
    if (!s)
        s = metadata_pair(&metadata, "ud.lineage", (const char *)lineage.data,
                          (uint32_t)lineage.size);
    if (!s && field->type == UD_INTERVAL)
        s = metadata_pair(&metadata, "ARROW:extension:name", "universaldata.interval.microsecond",
                          sizeof("universaldata.interval.microsecond") - 1);
    if (!s && field->dictionary_kind) {
        ud_bytes dictionary = {.ctx = ctx};
        s = ud_dictionary_json(&dictionary, field);
        if (!s)
            s = metadata_pair(&metadata, "ud.dictionary", (const char *)dictionary.data,
                              (uint32_t)dictionary.size);
        ud_bytes_clear(&dictionary);
    }
    if (!s) {
        *out = (const char *)metadata.data;
        metadata.data = NULL;
    }
    ud_bytes_clear(&metadata);
    ud_bytes_clear(&lineage);
    return s;
}
static ud_status export_column(ud_context *ctx, const ud_column *c, const ud_field *field,
                               uint32_t rows, uint32_t offset, struct ArrowArray *a,
                               struct ArrowSchema *s) {
    a->release = release_array;
    s->release = release_schema;
    UD_TRY(export_metadata(ctx, field, &s->metadata));
    a->length = rows;
    a->offset = offset;
    s->flags = field->nullable ? 2 : 0;
    s->name = ud_strdup_n(ctx, field->name, field->name_length);
    const char *format = field->type == UD_BOOL       ? "b"
                         : field->type == UD_INT64    ? "l"
                         : field->type == UD_UINT64   ? "L"
                         : field->type == UD_DURATION ? "tDu"
                         : field->type == UD_INTERVAL ? "w:16"
                         : field->type == UD_FLOAT64  ? "g"
                         : field->type == UD_UTF8     ? "u"
                         : field->type == UD_BINARY   ? "z"
                         : field->type == UD_DATE32   ? "tdD"
                         : field->type == UD_TIME64   ? "ttu"
                         : field->type == UD_STRUCT   ? "+s"
                         : field->type == UD_LIST     ? "+l"
                         : field->type == UD_MAP      ? "+m"
                                                      : NULL;
    char decimal[32];
    if (field->type == UD_DECIMAL128) {
        snprintf(decimal, sizeof(decimal), "d:%d,%d", field->precision, field->scale);
        format = decimal;
    }
    if (field->type == UD_TIMESTAMP) {
        if (field->timezone_length > SIZE_MAX - 5)
            return UD_OVERFLOW;
        char *timestamp = ud_alloc(ctx, (size_t)field->timezone_length + 5);
        if (!timestamp)
            return UD_RESOURCE;
        memcpy(timestamp, "tsu:", 4);
        memcpy(timestamp + 4, field->timezone, field->timezone_length);
        s->format = timestamp;
    } else if (format)
        s->format = ud_strdup_n(ctx, format, strlen(format));
    else
        return UD_UNSUPPORTED;
    if (!s->name || !s->format)
        return UD_RESOURCE;
    a->n_buffers = field->type == UD_STRUCT ? 1 : field->type >= UD_LIST ? 2 : c->offsets ? 3 : 2;
    a->buffers = ud_alloc(ctx, (size_t)a->n_buffers * sizeof(void *));
    if (!a->buffers)
        return UD_RESOURCE;
    size_t size_rows = (size_t)rows + offset;
    size_t sizes[3] = {(size_rows + 7) / 8,
                       c->offsets                 ? (size_rows + 1) * sizeof(int32_t)
                       : field->type == UD_BOOL   ? (size_rows + 7) / 8
                       : field->type == UD_DATE32 ? size_rows * 4
                       : (field->type == UD_DECIMAL128 || field->type == UD_INTERVAL)
                           ? size_rows * 16
                           : size_rows * 8,
                       c->string_bytes};
    const void *src[3] = {c->validity, c->offsets ? (const void *)c->offsets : c->values,
                          c->strings};
    for (int j = 0; j < a->n_buffers; j++) {
        void *buffer = ud_alloc(ctx, sizes[j] + 1);
        if (!buffer)
            return UD_RESOURCE;
        if (sizes[j])
            memcpy(buffer, src[j], sizes[j]);
        a->buffers[j] = buffer;
    }
    for (uint32_t r = offset; r < rows + offset; r++)
        if (!(c->validity[r / 8] & (1u << (r % 8))))
            a->null_count++;
    if (c->nested) {
        a->n_children = s->n_children = field->child_count;
        a->children = ud_alloc(ctx, field->child_count * sizeof(*a->children));
        s->children = ud_alloc(ctx, field->child_count * sizeof(*s->children));
        if (!a->children || !s->children)
            return UD_RESOURCE;
        for (uint32_t j = 0; j < field->child_count; j++) {
            a->children[j] = ud_alloc(ctx, sizeof(struct ArrowArray));
            s->children[j] = ud_alloc(ctx, sizeof(struct ArrowSchema));
            if (!a->children[j] || !s->children[j])
                return UD_RESOURCE;
            UD_TRY(export_column(ctx, c->nested->column + j, field->children + j, c->nested->rows,
                                 0, a->children[j], s->children[j]));
        }
    }
    return UD_OK;
}
ud_status ud_batch_export_arrow(const ud_batch *b, struct ArrowArray *a, struct ArrowSchema *s) {
    if (!b || !a || !s)
        return UD_INVALID;
    memset(a, 0, sizeof(*a));
    memset(s, 0, sizeof(*s));
    if (ud_batch_has_absent(b))
        return UD_INCOMPATIBLE;
    a->release = release_array;
    s->release = release_schema;
    a->length = b->rows;
    a->n_buffers = 1;
    a->buffers = ud_alloc(b->ctx, sizeof(void *));
    s->format = ud_strdup_n(b->ctx, "+s", 2);
    a->n_children = s->n_children = b->columns;
    a->children = ud_alloc(b->ctx, b->columns * sizeof(*a->children));
    s->children = ud_alloc(b->ctx, b->columns * sizeof(*s->children));
    ud_status code =
        !a->buffers || !s->format || !a->children || !s->children ? UD_RESOURCE : UD_OK;
    for (uint32_t j = 0; !code && j < b->columns; j++) {
        a->children[j] = ud_alloc(b->ctx, sizeof(struct ArrowArray));
        s->children[j] = ud_alloc(b->ctx, sizeof(struct ArrowSchema));
        if (!a->children[j] || !s->children[j])
            code = UD_RESOURCE;
        else
            code = export_column(b->ctx, b->column + j, &b->column[j].field, b->rows, b->row_offset,
                                 a->children[j], s->children[j]);
    }
    if (code) {
        release_array(a);
        release_schema(s);
    }
    return code;
}
typedef struct {
    ud_batch *batch;
    int delivered;
    char error[128];
} stream_state;
static int stream_schema(struct ArrowArrayStream *s, struct ArrowSchema *out) {
    stream_state *p = s->private_data;
    struct ArrowArray a;
    ud_status code = ud_batch_export_arrow(p->batch, &a, out);
    if (!code)
        a.release(&a);
    return code ? ENOMEM : 0;
}
static int stream_next(struct ArrowArrayStream *s, struct ArrowArray *out) {
    stream_state *p = s->private_data;
    memset(out, 0, sizeof(*out));
    if (p->delivered)
        return 0;
    struct ArrowSchema f;
    ud_status code = ud_batch_export_arrow(p->batch, out, &f);
    if (code) {
        snprintf(p->error, sizeof(p->error), "Arrow export failed (%d)", code);
        return ENOMEM;
    }
    f.release(&f);
    p->delivered = 1;
    return 0;
}
static const char *stream_error(struct ArrowArrayStream *s) {
    return ((stream_state *)s->private_data)->error;
}
static void stream_release(struct ArrowArrayStream *s) {
    if (!s || !s->release)
        return;
    stream_state *p = s->private_data;
    ud_batch_destroy(p->batch);
    ud_free(p);
    s->release = NULL;
    s->private_data = NULL;
}
ud_status ud_batch_export_stream(const ud_batch *b, struct ArrowArrayStream *s) {
    if (!b || !s)
        return UD_INVALID;
    memset(s, 0, sizeof(*s));
    stream_state *p = ud_alloc(b->ctx, sizeof(*p));
    if (!p)
        return UD_RESOURCE;
    ud_status code = ud_batch_clone(b, &p->batch);
    if (code) {
        ud_free(p);
        return code;
    }
    s->private_data = p;
    s->get_schema = stream_schema;
    s->get_next = stream_next;
    s->get_last_error = stream_error;
    s->release = stream_release;
    return UD_OK;
}
static void clear_field_tree(ud_field *f) {
    for (uint32_t j = 0; j < f->child_count; j++)
        clear_field_tree((ud_field *)f->children + j);
    ud_free((void *)f->children);
    f->children = NULL;
    f->child_count = 0;
    ud_free((void *)f->lineage);
    f->lineage = NULL;
    f->lineage_count = 0;
    ud_dictionary_clear(f);
}
static ud_status import_metadata(ud_context *ctx, const char *data, ud_field *field) {
    if (!data)
        return UD_OK;
    /* Producer must supply a valid Arrow metadata buffer. Lengths and total
     * traversal are bounded; the C interface carries no enclosing byte length. */
    int32_t pairs;
    memcpy(&pairs, data, 4);
    if (pairs < 0 || pairs > 64)
        return UD_SCHEMA;
    uint32_t at = 4;
    int id_seen = 0, lineage_seen = 0;
    for (int32_t j = 0; j < pairs; j++) {
        int32_t key_length, value_length;
        if (at > 1048576 - 4)
            return UD_SCHEMA;
        memcpy(&key_length, data + at, 4);
        at += 4;
        if (key_length < 0 || key_length > 256 || (uint32_t)key_length > 1048576 - at)
            return UD_SCHEMA;
        const char *key = data + at;
        at += (uint32_t)key_length;
        if (at > 1048576 - 4)
            return UD_SCHEMA;
        memcpy(&value_length, data + at, 4);
        at += 4;
        if (value_length < 0 || (uint32_t)value_length > 1048576 - at)
            return UD_SCHEMA;
        const char *value = data + at;
        at += (uint32_t)value_length;
        if (key_length == 20 && !memcmp(key, "ARROW:extension:name", 20)) {
            const char *name = "universaldata.interval.microsecond";
            if ((uint32_t)value_length == strlen(name) &&
                !memcmp(value, name, (size_t)value_length))
                field->type = UD_INTERVAL;
        } else if (key_length == 13 && !memcmp(key, "ud.dictionary", 13)) {
            if (field->dictionary_kind)
                return UD_SCHEMA;
            ud_json *dictionary = NULL;
            ud_status status = ud_json_parse(ctx, value, (uint32_t)value_length, &dictionary, NULL);
            if (!status)
                status = ud_dictionary_parse(ctx, dictionary, field);
            ud_json_destroy(dictionary);
            UD_TRY(status);
        } else if (key_length == 12 && !memcmp(key, "ud.column_id", 12)) {
            if (id_seen++)
                return UD_SCHEMA;
            UD_TRY(ud_id_parse((ud_string){value, (uint32_t)value_length}, &field->identity));
        } else if (key_length == 10 && !memcmp(key, "ud.lineage", 10)) {
            if (lineage_seen++)
                return UD_SCHEMA;
            if (!value_length)
                continue;
            if (((uint32_t)value_length + 1) % 33)
                return UD_SCHEMA;
            uint32_t count = ((uint32_t)value_length + 1) / 33;
            if (count > 4096)
                return UD_SCHEMA;
            ud_id *ids = ud_alloc(ctx, count * sizeof(*ids));
            if (!ids)
                return UD_RESOURCE;
            field->lineage = ids;
            field->lineage_count = count;
            for (uint32_t k = 0; k < count; k++) {
                if (k && value[k * 33 - 1] != ',')
                    return UD_SCHEMA;
                UD_TRY(ud_id_parse((ud_string){value + k * 33, 32}, ids + k));
                if (k && ud_id_compare(ids[k - 1], ids[k]) >= 0)
                    return UD_SCHEMA;
            }
        }
    }
    return id_seen == lineage_seen ? UD_OK : UD_SCHEMA;
}
static ud_status import_field(ud_context *ctx, const struct ArrowSchema *s, ud_field *f,
                              uint32_t depth) {
    if (depth > 32 || !s || !s->format || !s->name || s->n_children < 0 || s->n_children > 65536)
        return UD_SCHEMA;
    if (s->dictionary) {
        UD_TRY(import_field(ctx, s->dictionary, f, depth + 1));
        f->name = s->name;
        f->name_length = (uint32_t)strlen(s->name);
        f->nullable = !!(s->flags & 2);
        return UD_OK;
    }
    f->name = s->name;
    f->name_length = (uint32_t)strlen(s->name);
    f->nullable = !!(s->flags & 2);
    UD_TRY(import_metadata(ctx, s->metadata, f));
    f->type = !strcmp(s->format, "b")                                       ? UD_BOOL
              : (strlen(s->format) == 1 && strchr("cCsSiIl", s->format[0])) ? UD_INT64
              : !strcmp(s->format, "L")                                     ? UD_UINT64
              : !strncmp(s->format, "tD", 2)                                ? UD_DURATION
              : !strcmp(s->format, "tin") || (f->type == UD_INTERVAL && !strcmp(s->format, "w:16"))
                  ? UD_INTERVAL
              : (!strcmp(s->format, "g") || !strcmp(s->format, "f"))     ? UD_FLOAT64
              : (!strcmp(s->format, "u") || !strcmp(s->format, "U"))     ? UD_UTF8
              : (!strcmp(s->format, "z") || !strcmp(s->format, "Z"))     ? UD_BINARY
              : (!strcmp(s->format, "tdD") || !strcmp(s->format, "tdm")) ? UD_DATE32
              : !strncmp(s->format, "tt", 2)                             ? UD_TIME64
              : !strncmp(s->format, "ts", 2) && strlen(s->format) > 4 && s->format[3] == ':'
                  ? UD_TIMESTAMP
              : !strncmp(s->format, "d:", 2)                           ? UD_DECIMAL128
              : !strcmp(s->format, "+s")                               ? UD_STRUCT
              : (!strcmp(s->format, "+l") || !strcmp(s->format, "+L")) ? UD_LIST
              : !strcmp(s->format, "+m")                               ? UD_MAP
                                                                       : 0;
    if (!f->type)
        return UD_UNSUPPORTED;
    if (f->type == UD_DECIMAL128) {
        const char *comma = strchr(s->format + 2, ',');
        ud_value precision, scale;
        if (!comma)
            return UD_SCHEMA;
        const char *bits = strchr(comma + 1, ',');
        if (bits && strcmp(bits + 1, "128"))
            return UD_UNSUPPORTED;
        UD_TRY(ud_value_parse(ctx, UD_INT64, s->format + 2, (uint32_t)(comma - s->format - 2),
                              &precision));
        UD_TRY(ud_value_parse(ctx, UD_INT64, comma + 1,
                              bits ? (uint32_t)(bits - comma - 1) : (uint32_t)strlen(comma + 1),
                              &scale));
        if (precision.integer < 1 || precision.integer > 38 || scale.integer < -38 ||
            scale.integer > 38)
            return UD_SCHEMA;
        f->precision = (int32_t)precision.integer;
        f->scale = (int32_t)scale.integer;
    } else if (f->type == UD_TIMESTAMP) {
        if (!s->format[4])
            return UD_UNSUPPORTED;
        f->timezone = "UTC";
        f->timezone_length = 3;
    }
    if (f->type < UD_STRUCT)
        return s->n_children ? UD_SCHEMA : UD_OK;
    if (!s->n_children || !s->children || (f->type >= UD_LIST && s->n_children != 1))
        return UD_SCHEMA;
    ud_field *children = ud_alloc(ctx, (size_t)s->n_children * sizeof(*children));
    if (!children)
        return UD_RESOURCE;
    f->children = children;
    f->child_count = (uint32_t)s->n_children;
    for (uint32_t j = 0; j < f->child_count; j++)
        UD_TRY(import_field(ctx, s->children[j], children + j, depth + 1));
    return UD_OK;
}
static void clear_import_value(ud_value *v) {
    for (uint32_t j = 0; j < v->child_count; j++)
        clear_import_value((ud_value *)v->children + j);
    ud_free((void *)v->children);
    v->children = NULL;
    v->child_count = 0;
}
static ud_status import_value(ud_context *ctx, const ud_field *f, const struct ArrowArray *a,
                              const struct ArrowSchema *schema, int64_t index, ud_value *v,
                              uint32_t depth) {
    if (depth > 32 || !a || !schema || !schema->format || a->offset < 0 || index < 0 ||
        index >= a->length || a->offset > INT64_MAX - index || !a->buffers ||
        a->n_children != f->child_count)
        return UD_CORRUPT;
    int64_t k = a->offset + index;
    if (a->dictionary) {
        if (!schema->dictionary || a->n_buffers != 2 || !a->buffers[1])
            return UD_CORRUPT;
        int64_t selected;
        const char *format = schema->format;
        switch (format[0]) {
        case 'c':
            selected = ((const int8_t *)a->buffers[1])[k];
            break;
        case 'C':
            selected = ((const uint8_t *)a->buffers[1])[k];
            break;
        case 's':
            selected = ((const int16_t *)a->buffers[1])[k];
            break;
        case 'S':
            selected = ((const uint16_t *)a->buffers[1])[k];
            break;
        case 'i':
            selected = ((const int32_t *)a->buffers[1])[k];
            break;
        case 'I':
            selected = ((const uint32_t *)a->buffers[1])[k];
            break;
        case 'l':
            selected = ((const int64_t *)a->buffers[1])[k];
            break;
        default:
            return UD_UNSUPPORTED;
        }
        if (a->buffers[0] && !(((const uint8_t *)a->buffers[0])[k / 8] & (1u << (k % 8)))) {
            *v = (ud_value){
                .type = f->type, .is_null = 1, .precision = f->precision, .scale = f->scale};
            return UD_OK;
        }
        return import_value(ctx, f, a->dictionary, schema->dictionary, selected, v, depth + 1);
    }
    v->type = f->type;
    v->precision = f->precision;
    v->scale = f->scale;
    int buffers = f->type == UD_STRUCT                         ? 1
                  : f->type >= UD_LIST                         ? 2
                  : f->type == UD_UTF8 || f->type == UD_BINARY ? 3
                                                               : 2;
    if (a->n_buffers != buffers)
        return UD_SCHEMA;
    v->is_null = a->buffers[0] ? !(((const uint8_t *)a->buffers[0])[k / 8] & (1u << (k % 8))) : 0;
    if (v->is_null)
        return UD_OK;
    if (f->type != UD_STRUCT && !a->buffers[1])
        return UD_CORRUPT;
    if (f->type == UD_BOOL)
        v->integer = !!(((const uint8_t *)a->buffers[1])[k / 8] & (1u << (k % 8)));
    else if (f->type == UD_FLOAT64)
        v->real = schema->format[0] == 'f' ? ((const float *)a->buffers[1])[k]
                                           : ((const double *)a->buffers[1])[k];
    else if (f->type == UD_DATE32) {
        if (!strcmp(schema->format, "tdm")) {
            int64_t value = ((const int64_t *)a->buffers[1])[k];
            if (value % 86400000 || value / 86400000 > INT32_MAX || value / 86400000 < INT32_MIN)
                return UD_SCHEMA;
            v->integer = value / 86400000;
        } else
            v->integer = ((const int32_t *)a->buffers[1])[k];
    } else if (f->type == UD_UINT64) {
        v->unsigned_integer = ((const uint64_t *)a->buffers[1])[k];
    } else if (f->type == UD_INTERVAL) {
        const char *bytes = (const char *)a->buffers[1] + k * 16;
        memcpy(&v->interval, bytes, 16);
        if (!strcmp(schema->format, "tin")) {
            if (v->interval.microseconds % 1000)
                return UD_SCHEMA;
            v->interval.microseconds /= 1000;
        }
    } else if (f->type == UD_DECIMAL128) {
        memcpy(&v->decimal, (const char *)a->buffers[1] + k * 16, 16);
    } else if (f->type == UD_UTF8 || f->type == UD_BINARY) {
        int large = schema->format[0] == 'U' || schema->format[0] == 'Z';
        if (k == INT64_MAX)
            return UD_CORRUPT;
        int64_t start = large ? ((const int64_t *)a->buffers[1])[k]
                              : ((const int32_t *)a->buffers[1])[k],
                end = large ? ((const int64_t *)a->buffers[1])[k + 1]
                            : ((const int32_t *)a->buffers[1])[k + 1];
        if (start < 0 || end < start || end - start > UINT32_MAX || (!a->buffers[2] && end))
            return UD_CORRUPT;
        v->text = a->buffers[2] ? (const char *)a->buffers[2] + start : "";
        v->text_length = (uint32_t)(end - start);
    } else if (f->type >= UD_STRUCT) {
        if (!a->children)
            return UD_CORRUPT;
        uint32_t count = f->child_count;
        int64_t start = k;
        if (f->type >= UD_LIST) {
            if (k == INT64_MAX)
                return UD_CORRUPT;
            int large = !strcmp(schema->format, "+L");
            int64_t begin = large ? ((const int64_t *)a->buffers[1])[k]
                                  : ((const int32_t *)a->buffers[1])[k],
                    end = large ? ((const int64_t *)a->buffers[1])[k + 1]
                                : ((const int32_t *)a->buffers[1])[k + 1];
            if (begin < 0 || end < begin || end - begin > UINT32_MAX)
                return UD_CORRUPT;
            start = begin;
            count = (uint32_t)(end - begin);
        }
        if ((size_t)count > SIZE_MAX / sizeof(ud_value))
            return UD_OVERFLOW;
        ud_value *children = count ? ud_alloc(ctx, (size_t)count * sizeof(*children)) : NULL;
        if (count && !children)
            return UD_RESOURCE;
        v->children = children;
        v->child_count = count;
        for (uint32_t j = 0; j < count; j++) {
            uint32_t field = f->type == UD_STRUCT ? j : 0;
            UD_TRY(import_value(ctx, f->children + field, a->children[field],
                                schema->children[field], f->type == UD_STRUCT ? start : start + j,
                                children + j, depth + 1));
        }
    } else {
        const char *format = schema->format;
        switch (format[0]) {
        case 'c':
            v->integer = ((const int8_t *)a->buffers[1])[k];
            break;
        case 'C':
            v->integer = ((const uint8_t *)a->buffers[1])[k];
            break;
        case 's':
            v->integer = ((const int16_t *)a->buffers[1])[k];
            break;
        case 'S':
            v->integer = ((const uint16_t *)a->buffers[1])[k];
            break;
        case 'i':
            v->integer = ((const int32_t *)a->buffers[1])[k];
            break;
        case 'I':
            v->integer = ((const uint32_t *)a->buffers[1])[k];
            break;
        default:
            v->integer =
                format[0] == 't' && format[1] == 't' && (format[2] == 's' || format[2] == 'm')
                    ? ((const int32_t *)a->buffers[1])[k]
                    : ((const int64_t *)a->buffers[1])[k];
        }
        if (f->type == UD_TIME64 || f->type == UD_TIMESTAMP || f->type == UD_DURATION) {
            char unit = format[2];
            if (unit == 'n') {
                if (v->integer % 1000)
                    return UD_SCHEMA;
                v->integer /= 1000;
            } else if (unit == 's' || unit == 'm') {
                int64_t factor = unit == 's' ? 1000000 : 1000;
                if (v->integer > INT64_MAX / factor || v->integer < INT64_MIN / factor)
                    return UD_OVERFLOW;
                v->integer *= factor;
            } else if (unit != 'u')
                return UD_UNSUPPORTED;
        }
    }
    return UD_OK;
}
ud_status ud_batch_import_arrow(ud_context *ctx, const struct ArrowArray *a,
                                const struct ArrowSchema *s, ud_batch **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!a || !s || !a->release || !s->release || !s->format || strcmp(s->format, "+s") ||
        a->dictionary || s->dictionary || a->length < 0 || a->length > INT32_MAX || a->offset < 0 ||
        a->offset > INT64_MAX - a->length || a->n_children != s->n_children || a->n_children <= 0 ||
        a->n_children > 65536 || !a->children || !s->children || a->null_count != 0 ||
        a->n_buffers != 1)
        return ud_fail(ctx, UD_UNSUPPORTED, "Arrow root must be a non-null struct batch");
    uint32_t n = (uint32_t)a->n_children;
    ud_field *fields = ud_alloc(ctx, n * sizeof(*fields));
    ud_value *values = ud_alloc(ctx, n * sizeof(*values));
    if (!fields || !values) {
        ud_free(fields);
        ud_free(values);
        return UD_RESOURCE;
    }
    ud_status code = UD_OK;
    for (uint32_t j = 0; !code && j < n; j++)
        code = import_field(ctx, s->children[j], fields + j, 0);
    if (!code)
        code = ud_batch_create(ctx, fields, n, (uint32_t)a->length, out);
    for (uint32_t r = 0; !code && r < (uint32_t)a->length; r++) {
        memset(values, 0, n * sizeof(*values));
        for (uint32_t j = 0; !code && j < n; j++)
            code = import_value(ctx, fields + j, a->children[j], s->children[j], a->offset + r,
                                values + j, 0);
        if (!code)
            code = ud_batch_append(*out, values, n);
        for (uint32_t j = 0; j < n; j++)
            clear_import_value(values + j);
    }
    for (uint32_t j = 0; j < n; j++)
        clear_field_tree(fields + j);
    ud_free(fields);
    ud_free(values);
    if (code) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return code;
}
