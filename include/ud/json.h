#ifndef UD_JSON_H
#define UD_JSON_H
#include "memory.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_json ud_json;
enum {
    UD_JSON_NULL = 0,
    UD_JSON_BOOL = 1,
    UD_JSON_NUMBER = 2,
    UD_JSON_STRING = 3,
    UD_JSON_ARRAY = 4,
    UD_JSON_OBJECT = 5
};
/* Immutable owned document. Strict RFC 8259, UTF8, no duplicate object members.
 * Depth is limited to 64, node count to 1,000,000 and memory to the host quota.
 * Numbers retain their exact lexical representation; no floating conversion.
 * A byte offset is reported on parse errors; no input values enter error text. */
UD_API ud_status ud_json_parse(ud_context *, const char *, uint32_t length, ud_json **,
                               uint32_t *error_offset);
UD_API void ud_json_destroy(ud_json *);
UD_API int32_t ud_json_kind(const ud_json *);
/* Borrowed until document destruction; string/number may contain embedded NUL.
 * Bool text is true/false; array/object/null text returns SCHEMA. */
UD_API ud_status ud_json_text(const ud_json *, const char **, uint32_t *length);
UD_API uint32_t ud_json_size(const ud_json *);
UD_API const ud_json *ud_json_at(const ud_json *, uint32_t index);
UD_API const ud_json *ud_json_find(const ud_json *, const char *key, uint32_t length);
UD_API ud_status ud_json_key(const ud_json *, uint32_t index, const char **, uint32_t *);
/* Compact UTF8 JSON. Caller releases with ud_free. Output is NUL terminated;
 * length excludes terminator. Key insertion order is preserved. */
UD_API ud_status ud_json_serialize(const ud_json *, char **, uint32_t *length);
#ifdef __cplusplus
}
#endif
#endif
