#ifndef UD_HTTP_INTERNAL_H
#define UD_HTTP_INTERNAL_H
#include "ud/http.h"

typedef struct ud_http_oauth_cache {
    ud_string token;
    uint64_t expires_at;
} ud_http_oauth_cache;

const ud_json *ud_http_connection_oauth(ud_connection_lease *, ud_http_oauth_cache **);
ud_status ud_http_resolve_reference(ud_connection_lease *, ud_string resource,
                                    ud_string reference, ud_string *);
ud_status ud_http_oauth_validate(const ud_json *);
void ud_http_clear_secret(ud_string *);
ud_status ud_http_oauth_bearer(const ud_component_context *, ud_connection_lease *,
                              const ud_http_options *, const ud_json *, ud_http_oauth_cache *,
                              ud_string *);
ud_status ud_http_token_exchange(const ud_component_context *, ud_connection_lease *,
                                 const ud_http_options *, ud_string endpoint, ud_string body,
                                 ud_string authorization, ud_http_response *);
#endif
