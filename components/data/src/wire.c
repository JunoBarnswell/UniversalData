#include "../internal.h"
ud_status ud_encode_batch(ud_bytes *b, const ud_batch *table) {
    UD_TRY(ud_put_u64(b, 1));
    UD_TRY(ud_put_u64(b, table->columns));
    UD_TRY(ud_put_u64(b, table->rows));
    for (uint32_t i = 0; i < table->columns; i++) {
        ud_field f;
        ud_batch_field(table, i, &f);
        UD_TRY(ud_put_string(b, f.name, f.name_length));
        UD_TRY(ud_put_u64(b, (uint64_t)f.type));
        UD_TRY(ud_put_u64(b, (uint64_t)f.nullable));
    }
    for (uint32_t r = 0; r < table->rows; r++)
        for (uint32_t i = 0; i < table->columns; i++) {
            ud_value v;
            ud_batch_get(table, r, i, &v);
            UD_TRY(ud_put_u64(b, (uint64_t)v.is_null));
            if (v.is_null)
                continue;
            if (v.type == UD_UTF8) {
                UD_TRY(ud_put_string(b, v.text, v.text_length));
            } else {
                uint64_t bits;
                if (v.type == UD_FLOAT64)
                    memcpy(&bits, &v.real, 8);
                else
                    bits = (uint64_t)v.integer;
                UD_TRY(ud_put_u64(b, bits));
            }
        }
    return UD_OK;
}
ud_status ud_decode_batch(ud_bytes *b, ud_batch **out) {
    *out = NULL;
    uint64_t ver, n, rows;
    UD_TRY(ud_get_u64(b, &ver));
    UD_TRY(ud_get_u64(b, &n));
    UD_TRY(ud_get_u64(b, &rows));
    if (ver != 1 || !n || n > 65536 || rows > INT32_MAX)
        return UD_INCOMPATIBLE;
    ud_field *f = ud_alloc(b->ctx, (size_t)n * sizeof(*f));
    ud_value *values = ud_alloc(b->ctx, (size_t)n * sizeof(*values));
    if (!f || !values) {
        ud_free(f);
        ud_free(values);
        return UD_RESOURCE;
    }
    ud_status s = UD_OK;
    for (uint32_t i = 0; i < n && !s; i++) {
        uint64_t t, nullable;
        char *name = NULL;
        s = ud_get_string(b, &name);
        f[i].name = name;
        if (s)
            break;
        f[i].name_length = (uint32_t)strlen(name);
        s = ud_get_u64(b, &t);
        if (s)
            break;
        s = ud_get_u64(b, &nullable);
        if (s)
            break;
        if (t < 1 || t > 4 || nullable > 1) {
            s = UD_CORRUPT;
            break;
        }
        f[i].type = (ud_type)t;
        f[i].nullable = (int32_t)nullable;
    }
    if (!s)
        s = ud_batch_create(b->ctx, f, (uint32_t)n, (uint32_t)rows, out);
    for (uint32_t r = 0; !s && r < rows; r++) {
        memset(values, 0, (size_t)n * sizeof(*values));
        for (uint32_t i = 0; i < n && !s; i++) {
            ud_value *v = values + i;
            uint64_t null;
            s = ud_get_u64(b, &null);
            if (s || null > 1) {
                if (!s)
                    s = UD_CORRUPT;
                break;
            }
            v->type = f[i].type;
            v->is_null = (int32_t)null;
            if (null)
                continue;
            if (v->type == UD_UTF8) {
                char *text = NULL; /* binary UTF-8 can contain NUL, unlike metadata strings */
                uint64_t len;
                s = ud_get_u64(b, &len);
                if (s)
                    break;
                if (len > UINT32_MAX || len > b->size - b->pos) {
                    s = UD_CORRUPT;
                    break;
                }
                text = ud_strdup_n(b->ctx, (char *)b->data + b->pos, (size_t)len);
                if (!text) {
                    s = UD_RESOURCE;
                    break;
                }
                b->pos += (size_t)len;
                v->text = text;
                v->text_length = (uint32_t)len;
            } else {
                uint64_t bits;
                s = ud_get_u64(b, &bits);
                if (!s) {
                    if (v->type == UD_FLOAT64)
                        memcpy(&v->real, &bits, 8);
                    else
                        memcpy(&v->integer, &bits, 8);
                }
            }
        }
        if (!s)
            s = ud_batch_append(*out, values, (uint32_t)n);
        for (uint32_t i = 0; i < n; i++)
            if (values[i].type == UD_UTF8)
                ud_free((void *)values[i].text);
    }
    for (uint32_t i = 0; i < n; i++)
        ud_free((void *)f[i].name);
    ud_free(f);
    ud_free(values);
    if (s && *out) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}
