#ifndef UD_CURSOR_H
#define UD_CURSOR_H
#include "data.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_cursor ud_cursor;
/* Native policy: mode=snapshot/watermark/composite, keys=IDs/names, optional
 * typed after values. Incremental modes require explicit unique and monotonic.
 * Schema, key/order/policy are immutable; emitted rows must be strictly ordered. */
UD_API ud_status ud_cursor_create(ud_context *, const ud_batch *schema, ud_string policy,
                                  ud_cursor **);
UD_API ud_status ud_cursor_filter(ud_cursor *, const ud_batch *, ud_batch **);
UD_API ud_status ud_cursor_serialize(const ud_cursor *, char **, uint32_t *);
UD_API ud_status ud_cursor_diagnostics(const ud_cursor *, char **, uint32_t *);
UD_API ud_status ud_cursor_confirm(ud_cursor *); /* Only after the Pipeline receipt barrier. */
UD_API void ud_cursor_destroy(ud_cursor *);
#ifdef __cplusplus
}
#endif
#endif
