#include "../internal.h"
#include <stdatomic.h>
#include <math.h>
#include <errno.h>
#include <locale.h>
#include <float.h>
struct ud_context {
    ud_context_options options;
    atomic_uint_fast64_t used;
    atomic_int cancelled;
    char error[1024];
};
typedef union allocation {
    long double alignment;
    struct {
        ud_context *ctx;
        size_t bytes;
    } h;
} allocation;
static void *default_alloc(void *user, size_t n) {
    (void)user;
    return malloc(n);
}
static void default_free(void *user, void *p) {
    (void)user;
    free(p);
}
const char *ud_version(void) {
    return "0.1.0";
}
const char *ud_status_name(ud_status s) {
    static const char *names[] = {"ok",           "end",   "invalid",    "io",        "resource",
                                  "schema",       "parse", "overflow",   "cancelled", "corrupt",
                                  "incompatible", "busy",  "unsupported"};
    return s >= 0 && s < 13 ? names[s] : "unknown";
}
const char *ud_error(const ud_context *ctx) {
    return ctx ? ctx->error : "no context";
}
ud_status ud_fail(ud_context *ctx, ud_status code, const char *format, ...) {
    if (ctx) {
        va_list args;
        va_start(args, format);
        vsnprintf(ctx->error, sizeof(ctx->error), format, args);
        va_end(args);
    }
    return code;
}
ud_status ud_context_create(const ud_context_options *options, ud_context **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (options &&
        (options->struct_size < sizeof(*options) || options->abi_version != UD_ABI_VERSION ||
         (!!options->allocate != !!options->deallocate)))
        return UD_INCOMPATIBLE;
    ud_context *c = calloc(1, sizeof(*c));
    if (!c)
        return UD_RESOURCE;
    if (options)
        c->options = *options;
    if (!c->options.memory_limit)
        c->options.memory_limit = UINT64_C(268435456);
    if (!c->options.allocate) {
        c->options.allocate = default_alloc;
        c->options.deallocate = default_free;
    }
    atomic_init(&c->used, 0);
    atomic_init(&c->cancelled, 0);
    *out = c;
    return UD_OK;
}
ud_status ud_context_destroy(ud_context *c) {
    if (!c)
        return UD_OK;
    if (atomic_load(&c->used))
        return ud_fail(c, UD_BUSY, "context has live allocations");
    free(c);
    return UD_OK;
}
uint64_t ud_memory_used(const ud_context *c) {
    return c ? atomic_load(&c->used) : 0;
}
void *ud_alloc(ud_context *c, size_t n) {
    if (!c || n > SIZE_MAX - sizeof(allocation))
        return NULL;
    size_t total = n + sizeof(allocation);
    uint64_t old = atomic_load(&c->used);
    do {
        if (old > c->options.memory_limit || total > c->options.memory_limit - old) {
            ud_fail(c, UD_RESOURCE, "memory quota exceeded: requested %zu bytes", total);
            return NULL;
        }
    } while (!atomic_compare_exchange_weak(&c->used, &old, old + total));
    allocation *a = c->options.allocate(c->options.allocator_user, total);
    if (!a) {
        atomic_fetch_sub(&c->used, total);
        ud_fail(c, UD_RESOURCE, "allocator rejected %zu bytes", total);
        return NULL;
    }
    a->h.ctx = c;
    a->h.bytes = total;
    void *p = a + 1;
    memset(p, 0, n);
    return p;
}
void ud_free(void *p) {
    if (!p)
        return;
    allocation *a = ((allocation *)p) - 1;
    ud_context *c = a->h.ctx;
    size_t n = a->h.bytes;
    c->options.deallocate(c->options.allocator_user, a);
    atomic_fetch_sub(&c->used, n);
}
void ud_cancel(ud_context *c) {
    if (c)
        atomic_store(&c->cancelled, 1);
}
void ud_reset_cancel(ud_context *c) {
    if (c)
        atomic_store(&c->cancelled, 0);
}
int32_t ud_is_cancelled(const ud_context *c) {
    return c ? atomic_load(&c->cancelled) : 0;
}
char *ud_strdup_n(ud_context *c, const char *s, size_t n) {
    if (!s || n == SIZE_MAX)
        return NULL;
    char *p = ud_alloc(c, n + 1);
    if (p)
        memcpy(p, s, n);
    return p;
}
int ud_utf8_valid(const char *s, size_t n) {
    size_t i = 0;
    while (i < n) {
        uint32_t cp;
        unsigned char b = (unsigned char)s[i++];
        unsigned extra;
        if (b < 128)
            continue;
        if (b >= 0xc2 && b <= 0xdf) {
            cp = b & 31;
            extra = 1;
        } else if (b >= 0xe0 && b <= 0xef) {
            cp = b & 15;
            extra = 2;
        } else if (b >= 0xf0 && b <= 0xf4) {
            cp = b & 7;
            extra = 3;
        } else
            return 0;
        if (extra > n - i)
            return 0;
        unsigned count = extra;
        while (extra--) {
            b = (unsigned char)s[i++];
            if ((b & 0xc0) != 0x80)
                return 0;
            cp = (cp << 6) | (b & 63);
        }
        if ((count == 2 && cp < 0x800) || (count == 3 && cp < 0x10000) || cp > 0x10ffff ||
            (cp >= 0xd800 && cp <= 0xdfff))
            return 0;
    }
    return 1;
}
ud_status ud_bytes_put(ud_bytes *b, const void *p, size_t n) {
    if (n > SIZE_MAX - b->size)
        return UD_OVERFLOW;
    size_t need = b->size + n;
    if (need > b->capacity) {
        size_t cap = b->capacity ? b->capacity : 256;
        while (cap < need) {
            if (cap > SIZE_MAX / 2) {
                cap = need;
                break;
            }
            cap *= 2;
        }
        uint8_t *v = ud_alloc(b->ctx, cap);
        if (!v)
            return UD_RESOURCE;
        if (b->size)
            memcpy(v, b->data, b->size);
        ud_free(b->data);
        b->data = v;
        b->capacity = cap;
    }
    if (n)
        memcpy(b->data + b->size, p, n);
    b->size = need;
    return UD_OK;
}
ud_status ud_bytes_get(ud_bytes *b, void *p, size_t n) {
    if (b->pos > b->size || n > b->size - b->pos)
        return ud_fail(b->ctx, UD_CORRUPT, "truncated wire value at %zu", b->pos);
    if (n)
        memcpy(p, b->data + b->pos, n);
    b->pos += n;
    return UD_OK;
}
ud_status ud_put_u64(ud_bytes *b, uint64_t n) {
    uint8_t x[8];
    for (unsigned i = 0; i < 8; i++)
        x[i] = (uint8_t)(n >> (i * 8));
    return ud_bytes_put(b, x, 8);
}
ud_status ud_get_u64(ud_bytes *b, uint64_t *n) {
    uint8_t x[8];
    UD_TRY(ud_bytes_get(b, x, 8));
    *n = 0;
    for (unsigned i = 0; i < 8; i++)
        *n |= (uint64_t)x[i] << (i * 8);
    return UD_OK;
}
ud_status ud_put_string(ud_bytes *b, const char *s, size_t n) {
    UD_TRY(ud_put_u64(b, n));
    return ud_bytes_put(b, s, n);
}
ud_status ud_get_string(ud_bytes *b, char **s) {
    uint64_t n;
    *s = NULL;
    UD_TRY(ud_get_u64(b, &n));
    if (n > SIZE_MAX - 1 || n > b->size - b->pos || memchr(b->data + b->pos, 0, (size_t)n))
        return UD_CORRUPT;
    *s = ud_strdup_n(b->ctx, (char *)b->data + b->pos, (size_t)n);
    if (!*s)
        return UD_RESOURCE;
    b->pos += (size_t)n;
    return UD_OK;
}
void ud_bytes_clear(ud_bytes *b) {
    ud_free(b->data);
    b->data = NULL;
    b->size = b->pos = b->capacity = 0;
}
ud_status ud_read_file(ud_context *c, const char *path, ud_bytes *b) {
    b->ctx = c;
    FILE *f = ud_fopen(path, "rb");
    if (!f)
        return ud_fail(c, UD_IO, "cannot read %s", path);
    uint8_t buf[8192];
    size_t n;
    ud_status s = UD_OK;
    while ((n = fread(buf, 1, sizeof(buf), f))) {
        s = ud_bytes_put(b, buf, n);
        if (s)
            break;
    }
    if (ferror(f))
        s = UD_IO;
    if (fclose(f) && !s)
        s = UD_IO;
    return s ? ud_fail(c, s, "read failed: %s", path) : UD_OK;
}
ud_status ud_write_file(ud_context *c, const char *path, const void *p, size_t n) {
    FILE *f = ud_fopen(path, "wb");
    if (!f)
        return ud_fail(c, UD_IO, "cannot write %s", path);
    ud_status s = fwrite(p, 1, n, f) == n ? ud_flush(c, f) : UD_IO;
    if (fclose(f) && !s)
        s = UD_IO;
    return s ? ud_fail(c, s, "write failed: %s", path) : UD_OK;
}
ud_status ud_value_parse(ud_context *c, ud_type type, const char *text, uint32_t n, ud_value *out) {
    if (!out || !text || !ud_utf8_valid(text, n))
        return ud_fail(c, UD_PARSE, "invalid UTF-8 value");
    memset(out, 0, sizeof(*out));
    out->type = type;
    if (type == UD_UTF8) {
        out->text = text;
        out->text_length = n;
        return UD_OK;
    }
    if (!n || memchr(text, 0, n))
        return ud_fail(c, UD_PARSE, "empty or NUL numeric value");
    if (type == UD_BOOL) {
        if ((n == 4 && !memcmp(text, "true", 4)) || (n == 1 && *text == '1'))
            out->integer = 1;
        else if (!((n == 5 && !memcmp(text, "false", 5)) || (n == 1 && *text == '0')))
            return ud_fail(c, UD_PARSE, "invalid boolean");
        return UD_OK;
    }
    if (type == UD_INT64) {
        uint32_t i = 0;
        int neg = text[0] == '-';
        if (neg || text[0] == '+')
            i++;
        if (i == n)
            return UD_PARSE;
        uint64_t v = 0, limit = neg ? (uint64_t)INT64_MAX + 1 : INT64_MAX;
        for (; i < n; i++) {
            unsigned d = (unsigned char)text[i] - '0';
            if (d > 9)
                return ud_fail(c, UD_PARSE, "invalid int64");
            if (v > (limit - d) / 10)
                return ud_fail(c, UD_OVERFLOW, "int64 overflow");
            v = v * 10 + d;
        }
        out->integer = neg ? (v == (uint64_t)INT64_MAX + 1 ? INT64_MIN : -(int64_t)v) : (int64_t)v;
        return UD_OK;
    }
    if (type != UD_FLOAT64)
        return ud_fail(c, UD_UNSUPPORTED, "unsupported type %d", type);
    /* Explicit decimal grammar; locale-independent conversion, finite values only. */
    uint32_t i = 0;
    int sign = 1;
    if (text[i] == '-' || text[i] == '+') {
        if (text[i] == '-')
            sign = -1;
        i++;
    }
    long double v = 0;
    int frac = 0, exp = 0, exp_sign = 1, digits = 0;
    while (i < n && text[i] >= '0' && text[i] <= '9') {
        v = v * 10 + (text[i++] - '0');
        digits++;
    }
    if (i < n && text[i] == '.') {
        i++;
        while (i < n && text[i] >= '0' && text[i] <= '9') {
            v = v * 10 + (text[i++] - '0');
            frac++;
            digits++;
        }
    }
    if (!digits)
        return UD_PARSE;
    if (i < n && (text[i] == 'e' || text[i] == 'E')) {
        i++;
        if (i < n && (text[i] == '+' || text[i] == '-')) {
            if (text[i] == '-')
                exp_sign = -1;
            i++;
        }
        uint32_t start = i;
        while (i < n && text[i] >= '0' && text[i] <= '9') {
            if (exp < 100000)
                exp = exp * 10 + (text[i] - '0');
            i++;
        }
        if (start == i)
            return UD_PARSE;
    }
    if (i != n)
        return ud_fail(c, UD_PARSE, "invalid float64");
    long double result = sign * v * powl(10.0L, exp_sign * exp - frac);
    out->real = (double)result;
    if (!isfinite(out->real) || (v != 0 && out->real == 0))
        return ud_fail(c, UD_OVERFLOW, "float64 range exceeded");
    return UD_OK;
}
