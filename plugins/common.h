#ifndef UD_PLUGIN_COMMON_H
#define UD_PLUGIN_COMMON_H
#include "ud/plugin.h"
#include <string.h>
#include <stdlib.h>
#include <limits.h>
static char *plugin_string(const ud_host_api *h, const char *s, uint32_t n) {
    if ((!s && n) || (n && memchr(s, 0, n)))
        return NULL;
    char *p = h->allocate(h->context, (size_t)n + 1);
    if (p && n)
        memcpy(p, s, n);
    return p;
}
static ud_status plugin_fields(const ud_host_api *h, const ud_batch *b, ud_field **out) {
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
static void plugin_shutdown(void) {}
#endif
