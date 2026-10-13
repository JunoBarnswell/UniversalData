#ifndef UD_TEXT_H
#define UD_TEXT_H
#include "data.h"
#ifdef __cplusplus
extern "C" {
#endif
enum { UD_TEXT_TRIM=1,UD_TEXT_NFC=2,UD_TEXT_NFKC=4,UD_TEXT_FOLD=8,UD_TEXT_UPPER=16,UD_TEXT_LOWER=32 };
/* Locale-independent Unicode normalization/casing. Embedded NUL survives.
 * Returned strings and token batches use the Native context quota. */
UD_API ud_status ud_text_transform(ud_context *,ud_string,uint32_t,char **,uint32_t *);
UD_API ud_status ud_text_terms(ud_context *,ud_string,uint32_t maximum_tokens,ud_batch **);
#ifdef __cplusplus
}
#endif
#endif
