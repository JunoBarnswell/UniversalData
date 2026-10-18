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
    atomic_uint children;
    struct ud_memory_domain *parent;
};
static ud_status domain_reserve(ud_memory_domain *domain, uint64_t bytes) {
    for (ud_memory_domain *at = domain; at; at = at->parent) {
        uint64_t used = atomic_load(&at->used);
        do {
            if (used > at->limit || bytes > at->limit - used) {
                for (ud_memory_domain *prior = domain; prior != at; prior = prior->parent)
                    atomic_fetch_sub(&prior->used, bytes);
                return UD_RESOURCE;
            }
        } while (!atomic_compare_exchange_weak(&at->used, &used, used + bytes));
        uint64_t peak = atomic_load(&at->peak);
        while (peak < used + bytes &&
               !atomic_compare_exchange_weak(&at->peak, &peak, used + bytes)) {
        }
    }
    return UD_OK;
}
static void domain_release(ud_memory_domain *domain, uint64_t bytes) {
    for (ud_memory_domain *at = domain; at; at = at->parent)
        atomic_fetch_sub(&at->used, bytes);
}
static _Thread_local ud_memory_domain *allocation_domain;
struct ud_memory_reservation {
    ud_context *ctx;
    ud_memory_domain *domain;
    uint64_t bytes;
};
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
ud_status ud_context_report(ud_context *ctx, ud_status code, ud_string message) {
    if (!ctx || code <= UD_END || code > UD_TRUNCATED || code == UD_MORE || !message.data ||
        !message.length || message.length > 16384 || memchr(message.data, 0, message.length) ||
        !ud_utf8_valid(message.data, message.length))
        return UD_INVALID;
    return ud_fail(ctx, code, "%.*s", (int)message.length, message.data);
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
ud_status ud_base64_encode(ud_bytes *output, const uint8_t *bytes, uint32_t length,
                           uint32_t line_width) {
    static const char alphabet[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    if (!output || (length && !bytes) || (line_width && (line_width < 4 || line_width % 4)))
        return UD_INVALID;
    uint32_t column = 0;
    for (uint32_t at = 0; at < length;) {
        uint32_t left = length - at, value = (uint32_t)bytes[at++] << 16;
        if (left > 1)
            value |= (uint32_t)bytes[at++] << 8;
        if (left > 2)
            value |= bytes[at++];
        char encoded[4] = {alphabet[value >> 18], alphabet[(value >> 12) & 63],
                           left > 1 ? alphabet[(value >> 6) & 63] : '=',
                           left > 2 ? alphabet[value & 63] : '='};
        UD_TRY(ud_bytes_put(output, encoded, 4));
        column += 4;
        if (line_width && column == line_width) {
            UD_TRY(ud_bytes_put(output, "\r\n", 2));
            column = 0;
        }
    }
    return line_width && column ? ud_bytes_put(output, "\r\n", 2) : UD_OK;
}
ud_status ud_memory_reserve(ud_context *ctx, uint64_t bytes, ud_memory_reservation **out) {
    if (!ctx || !out)
        return UD_INVALID;
    *out = NULL;
    ud_memory_reservation *reservation = ud_alloc(ctx, sizeof(*reservation));
    if (!reservation)
        return UD_RESOURCE;
    ud_memory_domain *domain =
        allocation_domain && allocation_domain->ctx == ctx ? allocation_domain : NULL;
    if (domain_reserve(domain, bytes)) {
        ud_free(reservation);
        return ud_fail(ctx, UD_RESOURCE, "external codec domain quota exceeded");
    }
    uint64_t used = atomic_load(&ctx->used);
    do {
        if (used > ctx->options.memory_limit || bytes > ctx->options.memory_limit - used) {
            if (domain)
                domain_release(domain, bytes);
            ud_free(reservation);
            return ud_fail(ctx, UD_RESOURCE, "external codec memory quota exceeded");
        }
    } while (!atomic_compare_exchange_weak(&ctx->used, &used, used + bytes));
    reservation->ctx = ctx;
    reservation->domain = domain;
    reservation->bytes = bytes;
    *out = reservation;
    return UD_OK;
}
void ud_memory_release(ud_memory_reservation *reservation) {
    if (!reservation)
        return;
    atomic_fetch_sub(&reservation->ctx->used, reservation->bytes);
    if (reservation->domain)
        domain_release(reservation->domain, reservation->bytes);
    ud_free(reservation);
}
void *ud_alloc(ud_context *c, size_t n) {
    if (!c || n > SIZE_MAX - sizeof(allocation))
        return NULL;
    size_t total = n + sizeof(allocation);
    ud_memory_domain *domain =
        allocation_domain && allocation_domain->ctx == c ? allocation_domain : NULL;
    if (domain_reserve(domain, total)) {
        ud_fail(c, UD_RESOURCE, "allocation domain quota exceeded");
        return NULL;
    }
    uint64_t old = atomic_load(&c->used);
    do {
        if (old > c->options.memory_limit || total > c->options.memory_limit - old) {
            ud_fail(c, UD_RESOURCE, "memory quota exceeded: requested %zu bytes", total);
            if (domain)
                domain_release(domain, total);
            return NULL;
        }
    } while (!atomic_compare_exchange_weak(&c->used, &old, old + total));
    allocation *a = c->options.allocate(c->options.allocator_user, total);
    if (!a) {
        atomic_fetch_sub(&c->used, total);
        if (domain)
            domain_release(domain, total);
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
        domain_release(domain, n);
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
    atomic_init(&(*out)->children, 0);
    return UD_OK;
}
ud_status ud_memory_domain_create_child(ud_context *ctx, uint64_t limit, ud_memory_domain *parent,
                                        ud_memory_domain **out) {
    if (parent && parent->ctx != ctx)
        return UD_INVALID;
    UD_TRY(ud_memory_domain_create(ctx, limit, out));
    (*out)->parent = parent;
    if (parent)
        atomic_fetch_add(&parent->children, 1);
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
    if (atomic_load(&domain->used) || atomic_load(&domain->children))
        return UD_BUSY;
    if (domain->parent)
        atomic_fetch_sub(&domain->parent->children, 1);
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
