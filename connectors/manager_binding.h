#ifndef UD_MANAGER_BINDING_H
#define UD_MANAGER_BINDING_H
#include "ud/package.h"
#include "ud/json.h"

/* Resolve a host-bound ud:<name> driver before connector acquisition. */
ud_status ud_execution_resolve_driver(ud_execution *, ud_string *driver);

typedef struct ud_session_binding {
    uint8_t family[32], key[32];
    uint64_t max_leases, acquire_timeout, idle_timeout;
} ud_session_binding;

ud_status ud_manager_bind_session(ud_execution *, const ud_json *definition, ud_string driver,
                                  ud_string resource, ud_session_binding *, ud_string *configuration);

void ud_manager_clear_configuration(ud_string configuration);

#endif
