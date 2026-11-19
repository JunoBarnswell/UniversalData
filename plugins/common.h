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
/* A discovered result shape is owned twice: the driver keeps one copy for
 * cursor conversion and the caller receives an independent zero-row copy. */
static inline ud_status plugin_schema_outputs(const ud_host_api *h, const ud_field *fields,
                                              uint32_t columns, ud_batch **internal,
                                              ud_batch **published) {
    *internal = NULL;
    if (published)
        *published = NULL;
    ud_status status = h->batch_create(h->context, fields, columns, 0, internal);
    if (!status && published)
        status = h->batch_create(h->context, fields, columns, 0, published);
    if (status) {
        h->batch_destroy(*internal);
        *internal = NULL;
        if (published) {
            h->batch_destroy(*published);
            *published = NULL;
        }
    }
    return status;
}
/* Every SQL driver uses the same chunk contract. The compiler owns dialect
 * limits; the driver only applies its parameter-width and local hard bound.
 * A zero callback result means that the host has no stricter limit. */
static inline uint32_t plugin_write_chunk_rows(const ud_host_api *h, int32_t dialect,
                                               const ud_write_options *options,
                                               const ud_batch *batch, uint32_t width,
                                               uint32_t hard_limit) {
    uint32_t limit = width ? h->parameter_limit(dialect) / width : 0;
    if (h->write_chunk_rows) {
        uint32_t compiled = h->write_chunk_rows(dialect, options, batch);
        if (compiled && (!limit || compiled < limit))
            limit = compiled;
    }
    if (options->chunk_rows && (!limit || options->chunk_rows < limit))
        limit = options->chunk_rows;
    if (hard_limit && (!limit || hard_limit < limit))
        limit = hard_limit;
    return limit;
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
        return options->record(options->record_user, batch, conversion, bad, fields, values, raw,
                               columns);
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
