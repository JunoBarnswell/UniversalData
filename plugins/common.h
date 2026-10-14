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
static inline int plugin_record_conversion(ud_status status) {
    return status == UD_PARSE || status == UD_OVERFLOW || status == UD_TRUNCATED ||
           status == UD_QUALITY;
}
static inline ud_status plugin_record_finish(const ud_host_api *host,
                                             const ud_read_options *options, ud_batch *batch,
                                             const ud_field *fields, const ud_value *values,
                                             const ud_value *raw, uint32_t columns,
                                             ud_status conversion, uint32_t bad) {
    for (uint32_t at = 0; !conversion && at < columns; at++)
        if (values[at].is_null && !fields[at].nullable) {
            conversion = UD_QUALITY;
            bad = at;
        }
    if (!conversion)
        conversion = host->append(batch, values, columns);
    if (options->record && (!conversion || plugin_record_conversion(conversion)))
        return options->record(options->record_user, batch, conversion, bad, fields, raw, columns);
    return conversion;
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
