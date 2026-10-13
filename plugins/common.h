#ifndef UD_PLUGIN_COMMON_H
#define UD_PLUGIN_COMMON_H
#include "ud/plugin.h"
#include <string.h>
#include <stdlib.h>
#include <limits.h>
static inline char *plugin_string(const ud_host_api *h, const char *s, uint32_t n) {
    if ((!s && n) || (n && memchr(s, 0, n)))
        return NULL;
    char *p = h->allocate(h->context, (size_t)n + 1);
    if (p && n)
        memcpy(p, s, n);
    return p;
}
/* Binary values may contain zero bytes; text validation never owns them. */
static inline char *plugin_bytes(const ud_host_api *h, const void *s, uint32_t n) {
    if (!s && n)
        return NULL;
    char *p = h->allocate(h->context, (size_t)n + 1);
    if (p && n)
        memcpy(p, s, n);
    return p;
}
static inline ud_status plugin_fields(const ud_host_api *h, const ud_batch *b, ud_field **out) {
    if (!b)
        return UD_INVALID;
    uint32_t n = h->columns(b);
    *out = h->allocate(h->context, n * sizeof(ud_field));
    if (!*out)
        return UD_RESOURCE;
    for (uint32_t i = 0; i < n; i++)
        h->field(b, i, *out + i);
    return UD_OK;
}
static inline void plugin_shutdown(void) {}
static inline char *plugin_identifier(const ud_host_api *h, ud_string name) {
    if (!name.data || !name.length || memchr(name.data, 0, name.length))
        return NULL;
#if SIZE_MAX <= UINT32_MAX
    if (name.length > (SIZE_MAX - 3) / 2)
        return NULL;
#endif
    char *quoted = h->allocate(h->context, (size_t)name.length * 2 + 3);
    if (!quoted)
        return NULL;
    size_t n = 0;
    quoted[n++] = '"';
    for (uint32_t j = 0; j < name.length; j++) {
        if (name.data[j] == '"')
            quoted[n++] = '"';
        quoted[n++] = name.data[j];
    }
    quoted[n++] = '"';
    quoted[n] = 0;
    return quoted;
}
#endif
