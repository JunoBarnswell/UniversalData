#ifndef UD_REGEX_H
#define UD_REGEX_H
#include "memory.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_regex ud_regex;
typedef struct ud_regex_options {
    uint32_t struct_size,abi_version,max_steps,max_depth;
    uint64_t heap_bytes,timeout_ms;
    ud_status (*check)(void *); void *user;
} ud_regex_options;
typedef struct ud_regex_span {uint32_t begin,end;} ud_regex_span;
/* One UTF8/UCP kernel with named captures. Native quota allocations, explicit
 * work/depth/heap bounds and cancellation callouts bound every match. Missing
 * captures have UINT32_MAX spans. Compiled code is immutable; matches own state. */
UD_API ud_status ud_regex_compile(ud_context *,const char *,uint32_t,const ud_regex_options *,ud_regex **);
UD_API uint32_t ud_regex_capture_count(const ud_regex *);
UD_API ud_status ud_regex_group_index(const ud_regex *,ud_string,uint32_t *);
UD_API ud_status ud_regex_search(const ud_regex *,const char *,uint32_t,uint32_t,ud_regex_span *,uint32_t,int32_t *);
UD_API ud_status ud_regex_test(const ud_regex *,const char *,uint32_t,int32_t *);
UD_API ud_status ud_regex_replace(const ud_regex *,ud_string,ud_string,char **,uint32_t *);
UD_API void ud_regex_destroy(ud_regex *);
#ifdef __cplusplus
}
#endif
#endif
