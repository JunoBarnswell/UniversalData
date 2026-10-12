#include "../internal.h"
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
static ud_status export_column(ud_context *ctx, const ud_column *c, uint32_t rows,
                                struct ArrowArray *a, struct ArrowSchema *s) {
    a->release = release_array; s->release = release_schema;
    a->length = rows; s->flags = c->field.nullable ? 2 : 0;
    s->name = ud_strdup_n(ctx, c->field.name, c->field.name_length);
    const char *format = c->field.type == UD_BOOL ? "b" : c->field.type == UD_INT64 ? "l"
        : c->field.type == UD_FLOAT64 ? "g" : c->field.type == UD_UTF8 ? "u"
        : c->field.type == UD_BINARY ? "z" : c->field.type == UD_DATE32 ? "tdD"
        : c->field.type == UD_TIME64 ? "ttu" : c->field.type == UD_STRUCT ? "+s"
        : c->field.type == UD_LIST ? "+l" : c->field.type == UD_MAP ? "+m" : NULL;
    char decimal[32];
    if (c->field.type == UD_DECIMAL128) {
        snprintf(decimal, sizeof(decimal), "d:%d,%d", c->field.precision, c->field.scale); format = decimal;
    }
    if (c->field.type == UD_TIMESTAMP) {
        if (c->field.timezone_length > SIZE_MAX - 5) return UD_OVERFLOW;
        char *timestamp = ud_alloc(ctx, (size_t)c->field.timezone_length + 5);
        if (!timestamp) return UD_RESOURCE;
        memcpy(timestamp, "tsu:", 4); memcpy(timestamp + 4, c->field.timezone, c->field.timezone_length);
        s->format = timestamp;
    } else if (format) s->format = ud_strdup_n(ctx, format, strlen(format));
    else return UD_UNSUPPORTED;
    if (!s->name || !s->format) return UD_RESOURCE;
    a->n_buffers = c->field.type == UD_STRUCT ? 1 : c->field.type >= UD_LIST ? 2 : c->offsets ? 3 : 2;
    a->buffers = ud_alloc(ctx, (size_t)a->n_buffers * sizeof(void *));
    if (!a->buffers) return UD_RESOURCE;
    size_t sizes[3] = {((size_t)rows + 7) / 8,
        c->offsets ? ((size_t)rows + 1) * sizeof(int32_t)
        : c->field.type == UD_BOOL ? ((size_t)rows + 7) / 8
        : c->field.type == UD_DATE32 ? (size_t)rows * 4
        : c->field.type == UD_DECIMAL128 ? (size_t)rows * 16 : (size_t)rows * 8, c->string_bytes};
    const void *src[3] = {c->validity, c->offsets ? (const void *)c->offsets : c->values, c->strings};
    for (int j = 0; j < a->n_buffers; j++) {
        void *buffer = ud_alloc(ctx, sizes[j] + 1);
        if (!buffer) return UD_RESOURCE;
        if (sizes[j]) memcpy(buffer, src[j], sizes[j]);
        a->buffers[j] = buffer;
    }
    for (uint32_t r = 0; r < rows; r++) if (!(c->validity[r / 8] & (1u << (r % 8)))) a->null_count++;
    if (c->nested) {
        a->n_children = s->n_children = c->field.child_count;
        a->children = ud_alloc(ctx, c->field.child_count * sizeof(*a->children));
        s->children = ud_alloc(ctx, c->field.child_count * sizeof(*s->children));
        if (!a->children || !s->children) return UD_RESOURCE;
        for (uint32_t j = 0; j < c->field.child_count; j++) {
            a->children[j] = ud_alloc(ctx, sizeof(struct ArrowArray));
            s->children[j] = ud_alloc(ctx, sizeof(struct ArrowSchema));
            if (!a->children[j] || !s->children[j]) return UD_RESOURCE;
            UD_TRY(export_column(ctx, c->nested->column + j, c->nested->rows, a->children[j], s->children[j]));
        }
    }
    return UD_OK;
}
ud_status ud_batch_export_arrow(const ud_batch *b, struct ArrowArray *a, struct ArrowSchema *s) {
    if (!b || !a || !s) return UD_INVALID;
    memset(a, 0, sizeof(*a)); memset(s, 0, sizeof(*s));
    a->release = release_array; s->release = release_schema;
    a->length = b->rows; a->n_buffers = 1;
    a->buffers = ud_alloc(b->ctx, sizeof(void *));
    s->format = ud_strdup_n(b->ctx, "+s", 2);
    a->n_children = s->n_children = b->columns;
    a->children = ud_alloc(b->ctx, b->columns * sizeof(*a->children));
    s->children = ud_alloc(b->ctx, b->columns * sizeof(*s->children));
    ud_status code = !a->buffers || !s->format || !a->children || !s->children ? UD_RESOURCE : UD_OK;
    for (uint32_t j = 0; !code && j < b->columns; j++) {
        a->children[j] = ud_alloc(b->ctx, sizeof(struct ArrowArray));
        s->children[j] = ud_alloc(b->ctx, sizeof(struct ArrowSchema));
        if (!a->children[j] || !s->children[j]) code = UD_RESOURCE;
        else code = export_column(b->ctx, b->column + j, b->rows, a->children[j], s->children[j]);
    }
    if (code) { release_array(a); release_schema(s); }
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
    for (uint32_t j = 0; j < f->child_count; j++) clear_field_tree((ud_field *)f->children + j);
    ud_free((void *)f->children); f->children = NULL; f->child_count = 0;
}
static ud_status import_field(ud_context *ctx, const struct ArrowSchema *s, ud_field *f, uint32_t depth) {
    if (depth > 32 || !s || !s->format || !s->name || s->dictionary || s->n_children < 0 || s->n_children > 65536)
        return UD_SCHEMA;
    f->name = s->name; f->name_length = (uint32_t)strlen(s->name); f->nullable = !!(s->flags & 2);
    f->type = !strcmp(s->format, "b") ? UD_BOOL : !strcmp(s->format, "l") ? UD_INT64
        : !strcmp(s->format, "g") ? UD_FLOAT64 : !strcmp(s->format, "u") ? UD_UTF8
        : !strcmp(s->format, "z") ? UD_BINARY : !strcmp(s->format, "tdD") ? UD_DATE32
        : !strcmp(s->format, "ttu") ? UD_TIME64 : !strncmp(s->format, "tsu:", 4) ? UD_TIMESTAMP
        : !strncmp(s->format, "d:", 2) ? UD_DECIMAL128 : !strcmp(s->format, "+s") ? UD_STRUCT
        : !strcmp(s->format, "+l") ? UD_LIST : !strcmp(s->format, "+m") ? UD_MAP : 0;
    if (!f->type) return UD_UNSUPPORTED;
    if (f->type == UD_DECIMAL128) {
        const char *comma = strchr(s->format + 2, ','); ud_value precision, scale;
        if (!comma) return UD_SCHEMA;
        const char *bits = strchr(comma + 1, ',');
        if (bits && strcmp(bits + 1,"128")) return UD_UNSUPPORTED;
        UD_TRY(ud_value_parse(ctx, UD_INT64, s->format + 2, (uint32_t)(comma - s->format - 2), &precision));
        UD_TRY(ud_value_parse(ctx, UD_INT64, comma + 1, bits ? (uint32_t)(bits-comma-1) : (uint32_t)strlen(comma + 1), &scale));
        if (precision.integer < 1 || precision.integer > 38 || scale.integer < -38 || scale.integer > 38) return UD_SCHEMA;
        f->precision = (int32_t)precision.integer; f->scale = (int32_t)scale.integer;
    } else if (f->type == UD_TIMESTAMP) {
        f->timezone = s->format + 4; f->timezone_length = (uint32_t)strlen(f->timezone);
    }
    if (f->type < UD_STRUCT) return s->n_children ? UD_SCHEMA : UD_OK;
    if (!s->n_children || !s->children || (f->type >= UD_LIST && s->n_children != 1)) return UD_SCHEMA;
    ud_field *children = ud_alloc(ctx, (size_t)s->n_children * sizeof(*children));
    if (!children) return UD_RESOURCE;
    f->children = children; f->child_count = (uint32_t)s->n_children;
    for (uint32_t j = 0; j < f->child_count; j++) UD_TRY(import_field(ctx, s->children[j], children + j, depth + 1));
    return UD_OK;
}
static void clear_import_value(ud_value *v) {
    for (uint32_t j = 0; j < v->child_count; j++) clear_import_value((ud_value *)v->children + j);
    ud_free((void *)v->children); v->children = NULL; v->child_count = 0;
}
static ud_status import_value(ud_context *ctx, const ud_field *f, const struct ArrowArray *a,
                              int64_t index, ud_value *v, uint32_t depth) {
    if (depth > 32 || !a || a->dictionary || a->offset < 0 || index < 0 || index >= a->length ||
        a->offset > INT64_MAX - index || !a->buffers || a->n_children != f->child_count) return UD_CORRUPT;
    int64_t k = a->offset + index;
    v->type = f->type; v->precision = f->precision; v->scale = f->scale;
    int buffers = f->type == UD_STRUCT ? 1 : f->type >= UD_LIST ? 2 :
        f->type == UD_UTF8 || f->type == UD_BINARY ? 3 : 2;
    if (a->n_buffers != buffers) return UD_SCHEMA;
    v->is_null = a->buffers[0] ? !(((const uint8_t *)a->buffers[0])[k / 8] & (1u << (k % 8))) : 0;
    if (v->is_null) return UD_OK;
    if (f->type != UD_STRUCT && !a->buffers[1]) return UD_CORRUPT;
    if (f->type == UD_BOOL) v->integer = !!(((const uint8_t *)a->buffers[1])[k / 8] & (1u << (k % 8)));
    else if (f->type == UD_FLOAT64) v->real = ((const double *)a->buffers[1])[k];
    else if (f->type == UD_DATE32) v->integer = ((const int32_t *)a->buffers[1])[k];
    else if (f->type == UD_DECIMAL128) memcpy(&v->decimal, (const char *)a->buffers[1] + k * 16, 16);
    else if (f->type == UD_UTF8 || f->type == UD_BINARY) {
        const int32_t *offsets = a->buffers[1];
        if (k == INT64_MAX || offsets[k] < 0 || offsets[k + 1] < offsets[k] || (!a->buffers[2] && offsets[k + 1])) return UD_CORRUPT;
        v->text = a->buffers[2] ? (const char *)a->buffers[2] + offsets[k] : "";
        v->text_length = (uint32_t)(offsets[k + 1] - offsets[k]);
    } else if (f->type >= UD_STRUCT) {
        if (!a->children) return UD_CORRUPT;
        uint32_t count = f->child_count; int64_t start = k;
        if (f->type >= UD_LIST) {
            const int32_t *offsets = a->buffers[1];
            if (k == INT64_MAX || offsets[k] < 0 || offsets[k + 1] < offsets[k]) return UD_CORRUPT;
            start = offsets[k]; count = (uint32_t)(offsets[k + 1] - offsets[k]);
        }
        if ((size_t)count > SIZE_MAX / sizeof(ud_value)) return UD_OVERFLOW;
        ud_value *children = count ? ud_alloc(ctx, (size_t)count * sizeof(*children)) : NULL;
        if (count && !children) return UD_RESOURCE;
        v->children = children; v->child_count = count;
        for (uint32_t j = 0; j < count; j++) {
            uint32_t field = f->type == UD_STRUCT ? j : 0;
            UD_TRY(import_value(ctx, f->children + field, a->children[field],
                                f->type == UD_STRUCT ? start : start + j, children + j, depth + 1));
        }
    } else v->integer = ((const int64_t *)a->buffers[1])[k];
    return UD_OK;
}
ud_status ud_batch_import_arrow(ud_context *ctx, const struct ArrowArray *a,
                                const struct ArrowSchema *s, ud_batch **out) {
    if (!out) return UD_INVALID;
    *out = NULL;
    if (!a || !s || !a->release || !s->release || !s->format || strcmp(s->format, "+s") ||
        a->dictionary || s->dictionary || a->length < 0 || a->length > INT32_MAX || a->offset < 0 ||
        a->offset > INT64_MAX - a->length || a->n_children != s->n_children || a->n_children <= 0 ||
        a->n_children > 65536 || !a->children || !s->children || a->null_count != 0 || a->n_buffers != 1)
        return ud_fail(ctx, UD_UNSUPPORTED, "Arrow root must be a non-null struct batch");
    uint32_t n = (uint32_t)a->n_children;
    ud_field *fields = ud_alloc(ctx, n * sizeof(*fields));
    ud_value *values = ud_alloc(ctx, n * sizeof(*values));
    if (!fields || !values) { ud_free(fields); ud_free(values); return UD_RESOURCE; }
    ud_status code = UD_OK;
    for (uint32_t j = 0; !code && j < n; j++) code = import_field(ctx, s->children[j], fields + j, 0);
    if (!code) code = ud_batch_create(ctx, fields, n, (uint32_t)a->length, out);
    for (uint32_t r = 0; !code && r < (uint32_t)a->length; r++) {
        memset(values, 0, n * sizeof(*values));
        for (uint32_t j = 0; !code && j < n; j++) code = import_value(ctx, fields + j, a->children[j], a->offset + r, values + j, 0);
        if (!code) code = ud_batch_append(*out, values, n);
        for (uint32_t j = 0; j < n; j++) clear_import_value(values + j);
    }
    for (uint32_t j = 0; j < n; j++) clear_field_tree(fields + j);
    ud_free(fields); ud_free(values);
    if (code) { ud_batch_destroy(*out); *out = NULL; }
    return code;
}
