#include "../internal.h"
#include <stdatomic.h>
#include <math.h>
#include <errno.h>
#include <locale.h>
#include <float.h>
#include "../domain.h"
struct ud_context {
    ud_context_options options;
    atomic_uint_fast64_t used;
    atomic_int cancelled;
};
/* Diagnostics belong to the calling thread. Shared Context allocation/cancel
 * operations must not race while writing an unrelated task's error string. */
static _Thread_local const ud_context *diagnostic_context;
static _Thread_local char diagnostic_text[1024];
struct ud_memory_domain {
    ud_context *ctx;
    uint64_t limit;
    atomic_uint_fast64_t used, peak;
};
static _Thread_local ud_memory_domain *allocation_domain;
typedef union allocation {
    long double alignment;
    struct {
        ud_context *ctx;
        size_t bytes;
        ud_memory_domain *domain;
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
    return UD_SDK_VERSION;
}
const char *ud_status_name(ud_status s) {
    static const char *names[] = {
        "ok",          "end",      "invalid",   "io",        "resource",       "schema",
        "parse",       "overflow", "cancelled", "corrupt",   "incompatible",   "busy",
        "unsupported", "auth",     "quality",   "transient", "commit_unknown", "fenced",
        "timeout",     "more",     "truncated"};
    return s >= 0 && s < 21 ? names[s] : "unknown";
}
const char *ud_error(const ud_context *ctx) {
    return !ctx ? "no context" : diagnostic_context == ctx ? diagnostic_text : "";
}
ud_status ud_fail(ud_context *ctx, ud_status code, const char *format, ...) {
    if (ctx) {
        va_list args;
        va_start(args, format);
        diagnostic_context = ctx;
        vsnprintf(diagnostic_text, sizeof(diagnostic_text), format, args);
        va_end(args);
    }
    return code;
}
ud_status ud_context_create(const ud_context_options *options, ud_context **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (options &&
        (options->struct_size != sizeof(*options) || options->abi_version != UD_ABI_VERSION ||
         (!!options->allocate != !!options->deallocate)))
        return UD_INCOMPATIBLE;
    ud_context_options effective = {0};
    if (options)
        effective = *options;
    if (!effective.memory_limit)
        effective.memory_limit = UINT64_C(268435456);
    if (!effective.allocate) {
        effective.allocate = default_alloc;
        effective.deallocate = default_free;
    }
    ud_context *c = effective.allocate(effective.allocator_user, sizeof(*c));
    if (!c)
        return UD_RESOURCE;
    memset(c, 0, sizeof(*c));
    c->options = effective;
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
    void *user = c->options.allocator_user;
    ud_deallocate_fn deallocate = c->options.deallocate;
    deallocate(user, c);
    return UD_OK;
}
uint64_t ud_memory_used(const ud_context *c) {
    return c ? atomic_load(&c->used) : 0;
}
void *ud_alloc(ud_context *c, size_t n) {
    if (!c || n > SIZE_MAX - sizeof(allocation))
        return NULL;
    size_t total = n + sizeof(allocation);
    ud_memory_domain *domain =
        allocation_domain && allocation_domain->ctx == c ? allocation_domain : NULL;
    if (domain) {
        uint64_t owned = atomic_load(&domain->used);
        do {
            if (owned > domain->limit || total > domain->limit - owned) {
                ud_fail(c, UD_RESOURCE, "allocation domain quota exceeded");
                return NULL;
            }
        } while (!atomic_compare_exchange_weak(&domain->used, &owned, owned + total));
        uint64_t peak = atomic_load(&domain->peak);
        while (peak < owned + total &&
               !atomic_compare_exchange_weak(&domain->peak, &peak, owned + total)) {
        }
    }
    uint64_t old = atomic_load(&c->used);
    do {
        if (old > c->options.memory_limit || total > c->options.memory_limit - old) {
            ud_fail(c, UD_RESOURCE, "memory quota exceeded: requested %zu bytes", total);
            if (domain)
                atomic_fetch_sub(&domain->used, total);
            return NULL;
        }
    } while (!atomic_compare_exchange_weak(&c->used, &old, old + total));
    allocation *a = c->options.allocate(c->options.allocator_user, total);
    if (!a) {
        atomic_fetch_sub(&c->used, total);
        if (domain)
            atomic_fetch_sub(&domain->used, total);
        ud_fail(c, UD_RESOURCE, "allocator rejected %zu bytes", total);
        return NULL;
    }
    a->h.ctx = c;
    a->h.bytes = total;
    a->h.domain = domain;
    void *p = a + 1;
    memset(p, 0, n);
    return p;
}
size_t ud_allocation_bytes(const void *ptr) {
    return ptr ? ((const allocation *)ptr - 1)->h.bytes : 0;
}
void ud_free(void *p) {
    if (!p)
        return;
    allocation *a = ((allocation *)p) - 1;
    ud_context *c = a->h.ctx;
    size_t n = a->h.bytes;
    ud_memory_domain *domain = a->h.domain;
    c->options.deallocate(c->options.allocator_user, a);
    atomic_fetch_sub(&c->used, n);
    if (domain)
        atomic_fetch_sub(&domain->used, n);
}
ud_status ud_memory_domain_create(ud_context *ctx, uint64_t limit, ud_memory_domain **out) {
    if (!ctx || !limit || !out)
        return UD_INVALID;
    *out = ud_alloc(ctx, sizeof(**out));
    if (!*out)
        return UD_RESOURCE;
    (*out)->ctx = ctx;
    (*out)->limit = limit;
    atomic_init(&(*out)->used, 0);
    atomic_init(&(*out)->peak, 0);
    return UD_OK;
}
void ud_memory_domain_enter(ud_memory_domain *domain, ud_memory_scope *scope) {
    scope->previous = allocation_domain;
    allocation_domain = domain;
}
void ud_memory_domain_leave(const ud_memory_scope *scope) {
    allocation_domain = scope->previous;
}
uint64_t ud_memory_domain_used(const ud_memory_domain *domain) {
    return domain ? atomic_load(&domain->used) : 0;
}
uint64_t ud_memory_domain_peak(const ud_memory_domain *domain) {
    return domain ? atomic_load(&domain->peak) : 0;
}
ud_status ud_memory_domain_destroy(ud_memory_domain *domain) {
    if (!domain)
        return UD_OK;
    if (atomic_load(&domain->used))
        return UD_BUSY;
    ud_free(domain);
    return UD_OK;
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
    if (b->limit && need > b->limit)
        return ud_fail(b->ctx, UD_RESOURCE, "buffer byte limit exceeded");
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
ud_status ud_append_file(ud_context *c, const char *path, const void *p, size_t n) {
    FILE *file = ud_fopen(path, "ab");
    if (!file)
        return UD_IO;
    ud_status result = fwrite(p, 1, n, file) == n ? ud_flush(c, file) : UD_IO;
    if (fclose(file) && !result)
        result = UD_IO;
    return result;
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
