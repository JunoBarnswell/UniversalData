#ifndef UD_REGEX_H
#define UD_REGEX_H
#include "memory.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_regex ud_regex;
/* UTF8 code-point regex with literals, ., ^, $, groups, |, *, +, ?, character
 * classes/ranges, negated classes and ASCII \d/\w/\s (uppercase negations).
 * Search semantics; use ^...$ for whole-value validation. Dot excludes newline.
 * Backreferences, lookarounds, counted/lazy repetitions and unsupported escapes
 * return UNSUPPORTED; invalid grammar returns PARSE. No process locale used.
 * Thompson NFA: O(pattern states * text code points), no recursive backtracking.
 * Maximum pattern bytes 4096, group depth 64, states 8192; host quota applies. */
UD_API ud_status ud_regex_compile(ud_context *, const char *, uint32_t length, ud_regex **);
UD_API ud_status ud_regex_test(const ud_regex *, const char *, uint32_t length, int32_t *matched);
UD_API void ud_regex_destroy(ud_regex *);
#ifdef __cplusplus
}
#endif
#endif
