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
ud_status ud_batch_export_arrow(const ud_batch *b, struct ArrowArray *a, struct ArrowSchema *s) {
    if (!b || !a || !s)
        return UD_INVALID;
    memset(a, 0, sizeof(*a));
    memset(s, 0, sizeof(*s));
    a->release = release_array;
    s->release = release_schema;
    a->length = b->rows;
    a->n_buffers = 1;
    a->buffers = ud_alloc(b->ctx, sizeof(void *));
    a->n_children = s->n_children = b->columns;
    s->format = "+s";
    a->children = ud_alloc(b->ctx, b->columns * sizeof(*a->children));
    s->children = ud_alloc(b->ctx, b->columns * sizeof(*s->children));
    if (!a->buffers || !a->children || !s->children)
        goto fail;
    for (uint32_t i = 0; i < b->columns; i++) {
        const ud_column *c = b->column + i;
        struct ArrowArray *v = ud_alloc(b->ctx, sizeof(*v));
        struct ArrowSchema *f = ud_alloc(b->ctx, sizeof(*f));
        a->children[i] = v;
        s->children[i] = f;
        if (!v || !f)
            goto fail;
        v->release = release_array;
        f->release = release_schema;
        v->length = b->rows;
        f->format = c->field.type == UD_BOOL      ? "b"
                    : c->field.type == UD_INT64   ? "l"
                    : c->field.type == UD_FLOAT64 ? "g"
                                                  : "u";
        f->name = ud_strdup_n(b->ctx, c->field.name, c->field.name_length);
        f->flags = c->field.nullable ? 2 : 0;
        v->n_buffers = c->field.type == UD_UTF8 ? 3 : 2;
        v->buffers = ud_alloc(b->ctx, (size_t)v->n_buffers * sizeof(void *));
        if (!f->name || !v->buffers)
            goto fail;
        size_t sizes[3] = {((size_t)b->rows + 7) / 8,
                           c->field.type == UD_UTF8   ? ((size_t)b->rows + 1) * 4
                           : c->field.type == UD_BOOL ? ((size_t)b->rows + 7) / 8
                                                      : (size_t)b->rows * 8,
                           c->string_bytes};
        const void *src[3] = {c->validity, c->offsets ? (void *)c->offsets : c->values, c->strings};
        for (int j = 0; j < v->n_buffers; j++) {
            void *p = ud_alloc(b->ctx, sizes[j] + 1);
            if (!p)
                goto fail;
            if (sizes[j])
                memcpy(p, src[j], sizes[j]);
            v->buffers[j] = p;
        }
        for (uint32_t r = 0; r < b->rows; r++)
            if (!(c->validity[r / 8] & (1u << (r % 8))))
                v->null_count++;
    }
    return UD_OK;
fail:
    release_array(a);
    release_schema(s);
    return UD_RESOURCE;
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
ud_status ud_batch_import_arrow(ud_context *ctx, const struct ArrowArray *a,
                                const struct ArrowSchema *s, ud_batch **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!a || !s || !a->release || !s->release || !s->format || strcmp(s->format, "+s") ||
        a->length < 0 || a->length > INT32_MAX || a->offset < 0 || a->n_children != s->n_children ||
        a->n_children <= 0 || a->n_children > 65536 || !a->children || !s->children ||
        a->null_count != 0)
        return ud_fail(ctx, UD_UNSUPPORTED, "Arrow root must be a non-null struct batch");
    uint32_t n = (uint32_t)a->n_children;
    ud_field *f = ud_alloc(ctx, n * sizeof(*f));
    ud_value *v = ud_alloc(ctx, n * sizeof(*v));
    if (!f || !v) {
        ud_free(f);
        ud_free(v);
        return UD_RESOURCE;
    }
    ud_status code = UD_OK;
    for (uint32_t i = 0; i < n; i++) {
        const struct ArrowSchema *x = s->children[i];
        const struct ArrowArray *c = a->children[i];
        if (!x || !c || !x->format || !x->name || c->offset < 0 ||
            c->length < a->offset + a->length || !c->buffers || x->dictionary || c->dictionary) {
            code = UD_SCHEMA;
            break;
        }
        ud_type t = !strcmp(x->format, "b")   ? UD_BOOL
                    : !strcmp(x->format, "l") ? UD_INT64
                    : !strcmp(x->format, "g") ? UD_FLOAT64
                    : !strcmp(x->format, "u") ? UD_UTF8
                                              : 0;
        if (!t || c->n_buffers != (t == UD_UTF8 ? 3 : 2) || !c->buffers[1]) {
            code = UD_UNSUPPORTED;
            break;
        }
        f[i] = (ud_field){x->name, (uint32_t)strlen(x->name), t, !!(x->flags & 2)};
    }
    if (!code)
        code = ud_batch_create(ctx, f, n, (uint32_t)a->length, out);
    for (uint32_t r = 0; !code && r < (uint32_t)a->length; r++) {
        for (uint32_t i = 0; i < n; i++) {
            const struct ArrowArray *c = a->children[i];
            int64_t k = c->offset + a->offset + r;
            memset(v + i, 0, sizeof(*v));
            v[i].type = f[i].type;
            v[i].is_null =
                c->buffers[0] ? !(((const uint8_t *)c->buffers[0])[k / 8] & (1u << (k % 8))) : 0;
            if (v[i].is_null)
                continue;
            if (f[i].type == UD_BOOL)
                v[i].integer = !!(((const uint8_t *)c->buffers[1])[k / 8] & (1u << (k % 8)));
            else if (f[i].type == UD_INT64)
                v[i].integer = ((const int64_t *)c->buffers[1])[k];
            else if (f[i].type == UD_FLOAT64)
                v[i].real = ((const double *)c->buffers[1])[k];
            else {
                const int32_t *o = c->buffers[1];
                if (o[k] < 0 || o[k + 1] < o[k] || (!c->buffers[2] && o[k + 1])) {
                    code = UD_CORRUPT;
                    break;
                }
                v[i].text = c->buffers[2] ? (const char *)c->buffers[2] + o[k] : "";
                v[i].text_length = (uint32_t)(o[k + 1] - o[k]);
            }
        }
        if (!code)
            code = ud_batch_append(*out, v, n);
    }
    ud_free(f);
    ud_free(v);
    if (code && *out) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return code;
}
