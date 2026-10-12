#include "../internal.h"
static ud_status fields_of(ud_context *c, const ud_batch *b, ud_field **out) {
    uint32_t n = ud_batch_columns(b);
    *out = ud_alloc(c, n * sizeof(**out));
    if (!*out)
        return UD_RESOURCE;
    for (uint32_t i = 0; i < n; i++)
        ud_batch_field(b, i, *out + i);
    return UD_OK;
}
static ud_status append_row(ud_batch *out, const ud_batch *in, uint32_t row, ud_value *v) {
    uint32_t n = ud_batch_columns(in);
    for (uint32_t i = 0; i < n; i++)
        UD_TRY(ud_batch_get(in, row, i, v + i));
    return ud_batch_append(out, v, n);
}
ud_status ud_compute_slice(ud_context *c, const ud_batch *b, uint64_t offset, uint64_t limit,
                         ud_batch **out) {
    if (!b || !out)
        return UD_INVALID;
    *out = NULL;
    uint32_t rows = ud_batch_rows(b), n = ud_batch_columns(b);
    uint64_t count = offset < rows ? rows - offset : 0;
    if (count > limit)
        count = limit;
    ud_field *f = NULL;
    UD_TRY(fields_of(c, b, &f));
    ud_status s = ud_batch_create(c, f, n, (uint32_t)count, out);
    ud_free(f);
    ud_value *v = ud_alloc(c, n * sizeof(*v));
    if (!v && !s)
        s = UD_RESOURCE;
    for (uint32_t r = 0; !s && r < count; r++) {
        if (ud_is_cancelled(c)) {
            s = UD_CANCELLED;
            break;
        }
        s = append_row(*out, b, (uint32_t)offset + r, v);
    }
    ud_free(v);
    if (s && *out) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}
ud_status ud_compute_sort(ud_context *c, const ud_batch *b, uint32_t key, int32_t descending,
                        ud_batch **out) {
    if (!b || !out || key >= ud_batch_columns(b) || (descending != 0 && descending != 1))
        return UD_INVALID;
    *out = NULL;
    uint32_t rows = ud_batch_rows(b), n = ud_batch_columns(b);
    uint32_t *order = ud_alloc(c, (size_t)rows * sizeof(*order) + 1),
             *scratch = ud_alloc(c, (size_t)rows * sizeof(*scratch) + 1);
    if (!order || !scratch) {
        ud_free(order);
        ud_free(scratch);
        return UD_RESOURCE;
    }
    for (uint32_t i = 0; i < rows; i++)
        order[i] = i;
    for (uint64_t width = 1; width < rows; width *= 2) {
        for (uint64_t lo = 0; lo < rows; lo += 2 * width) {
            uint64_t mid = lo + width < rows ? lo + width : rows,
                     hi = lo + 2 * width < rows ? lo + 2 * width : rows, a = lo, z = mid, k = lo;
            while (a < mid && z < hi) {
                ud_value x, y;
                ud_batch_get(b, order[a], key, &x);
                ud_batch_get(b, order[z], key, &y);
                int cmp = ud_value_compare(&x, &y);
                if (descending)
                    cmp = -cmp;
                scratch[k++] = cmp <= 0 ? order[a++] : order[z++];
            }
            while (a < mid)
                scratch[k++] = order[a++];
            while (z < hi)
                scratch[k++] = order[z++];
        }
        uint32_t *swap = order;
        order = scratch;
        scratch = swap;
        if (ud_is_cancelled(c)) {
            ud_free(order);
            ud_free(scratch);
            return UD_CANCELLED;
        }
    }
    ud_field *f = NULL;
    ud_status s = fields_of(c, b, &f);
    if (!s)
        s = ud_batch_create(c, f, n, rows, out);
    ud_free(f);
    ud_value *v = ud_alloc(c, n * sizeof(*v));
    if (!v && !s)
        s = UD_RESOURCE;
    for (uint32_t i = 0; !s && i < rows; i++)
        s = append_row(*out, b, order[i], v);
    ud_free(v);
    ud_free(order);
    ud_free(scratch);
    if (s && *out) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}
ud_status ud_compute_inner_join(ud_context *c, const ud_batch *left, uint32_t lk,
                              const ud_batch *right, uint32_t rk, ud_batch **out) {
    if (!left || !right || !out || lk >= ud_batch_columns(left) || rk >= ud_batch_columns(right))
        return UD_INVALID;
    *out = NULL;
    ud_field a, b;
    ud_batch_field(left, lk, &a);
    ud_batch_field(right, rk, &b);
    b.nullable = a.nullable;
    if (!ud_field_equal(&a, &b, 0))
        return UD_SCHEMA;
    uint32_t ln = ud_batch_columns(left), rn = ud_batch_columns(right), n = ln + rn;
    if (n < ln || n > 65536)
        return UD_RESOURCE;
    ud_field *f = ud_alloc(c, n * sizeof(*f));
    ud_value *v = ud_alloc(c, n * sizeof(*v));
    if (!f || !v) {
        ud_free(f);
        ud_free(v);
        return UD_RESOURCE;
    }
    for (uint32_t i = 0; i < ln; i++)
        ud_batch_field(left, i, f + i);
    for (uint32_t i = 0; i < rn; i++)
        ud_batch_field(right, i, f + ln + i);
    uint64_t matches = 0;
    ud_status s = UD_OK;
    for (uint32_t l = 0; l < ud_batch_rows(left); l++) {
        if (ud_is_cancelled(c)) {
            s = UD_CANCELLED;
            break;
        }
        ud_value x;
        ud_batch_get(left, l, lk, &x);
        if (x.is_null)
            continue;
        for (uint32_t r = 0; r < ud_batch_rows(right); r++) {
            ud_value y;
            ud_batch_get(right, r, rk, &y);
            if (!y.is_null && !ud_value_compare(&x, &y))
                matches++;
            if (matches > INT32_MAX) {
                s = UD_RESOURCE;
                break;
            }
        }
        if (s)
            break;
    }
    if (!s)
        s = ud_batch_create(c, f, n, (uint32_t)matches, out);
    for (uint32_t l = 0; !s && l < ud_batch_rows(left); l++) {
        if (ud_is_cancelled(c)) {
            s = UD_CANCELLED;
            break;
        }
        ud_value x;
        ud_batch_get(left, l, lk, &x);
        if (x.is_null)
            continue;
        for (uint32_t r = 0; !s && r < ud_batch_rows(right); r++) {
            ud_value y;
            ud_batch_get(right, r, rk, &y);
            if (y.is_null || ud_value_compare(&x, &y))
                continue;
            for (uint32_t i = 0; i < ln; i++)
                ud_batch_get(left, l, i, v + i);
            for (uint32_t i = 0; i < rn; i++)
                ud_batch_get(right, r, i, v + ln + i);
            s = ud_batch_append(*out, v, n);
        }
    }
    ud_free(f);
    ud_free(v);
    if (s && *out) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}
