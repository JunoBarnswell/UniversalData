#ifndef UD_HTTP_H
#define UD_HTTP_H
#include "component.h"
#include "connection_manager.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_http_options {uint32_t struct_size,abi_version;ud_string method,body,content_type,ca,url,idempotency_key;uint64_t timeout,maximum,attempts,delay,expected;} ud_http_options;
typedef struct ud_http_response {int32_t status;ud_string body,headers;uint64_t retry_after_ms;} ud_http_response;
/* One Native transport owner for HTTP Task and formal REST components. Every
 * transfer rechecks the existing ConnectionManager lease and resource grant. */
UD_API ud_status ud_http_call(const ud_component_context*,ud_connection_lease*,const ud_http_options*,ud_http_response*);
UD_API void ud_http_response_clear(ud_http_response*);
#ifdef __cplusplus
}
#endif
#endif
