#ifndef UD_HTTP_H
#define UD_HTTP_H
#include "component.h"
#include "connection_manager.h"
#include "json.h"
#define UD_HTTP_HEADER_PROPERTIES                                                               \
    "\"responseHeaders\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}},"                                 \
    "\"headers\":{\"type\":\"array\",\"items\":{\"type\":\"object\",\"required\":[\"name\"],"                 \
    "\"additionalProperties\":false,\"properties\":{\"name\":{\"type\":\"string\"},"                        \
    "\"value\":{\"type\":\"string\"},\"variable\":{\"type\":\"string\"},"                                     \
    "\"credentialReference\":{\"type\":\"string\"}}}}"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_http_options {
    uint32_t struct_size, abi_version;
    ud_string method, body, content_type, ca, url, idempotency_key;
    uint64_t timeout, maximum, attempts, delay, expected;
    const ud_json *headers;
    const ud_json *response_headers;
} ud_http_options;
typedef struct ud_http_response {
    int32_t status;
    int32_t transport_code;
    ud_string body, headers;
    uint64_t retry_after_ms;
    int32_t attempted; /* Transport started; cancellation/timeout may hide a side effect. */
    int32_t completed; /* Complete HTTP response, including explicitly rejected status codes. */
} ud_http_response;
/* One Native transport owner for HTTP Task and formal REST components. Every
 * transfer rechecks the existing ConnectionManager lease and resource grant. */
UD_API ud_status ud_http_call(const ud_component_context *, ud_connection_lease *,
                              const ud_http_options *, ud_http_response *);
ud_status ud_http_resolve_url(ud_context *, ud_string base, ud_string requested, ud_string *);
UD_API void ud_http_response_clear(ud_http_response *);
UD_API ud_status ud_http_validate_headers(const ud_json *, const ud_json *);
#ifdef __cplusplus
}
#endif
#endif
