#ifndef UD_CATALOG_H
#define UD_CATALOG_H
#include "package.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_catalog ud_catalog;
enum {
    UD_CATALOG_PROJECT = 1,
    UD_CATALOG_PACKAGE,
    UD_CATALOG_ENVIRONMENT,
    UD_CATALOG_DEPLOYMENT,
    UD_CATALOG_EXECUTION,
    UD_CATALOG_EVENT,
    UD_CATALOG_AUDIT
};
typedef struct ud_catalog_key {
    uint32_t struct_size, abi_version;
    int32_t kind;
    ud_string id;
    uint64_t revision;
} ud_catalog_key;
typedef struct ud_catalog_options {
    uint32_t struct_size, abi_version;
    ud_string directory, tenant;
    ud_authorize_fn authorize;
    void *authorize_user;
    int32_t create_directory;
} ud_catalog_options;
/* Optional embedded immutable document repository, not an executor. Host owns
 * ctx and serializes catalog calls. Metadata stores only supplied documents;
 * it never reads process credentials or invokes a credential resolver.
 * Versions are append-only; same key/same canonical document is idempotent,
 * different document is CORRUPT. Tenant is part of each physical identity. */
UD_API ud_status ud_catalog_open(ud_context *, const ud_catalog_options *, ud_catalog **);
UD_API ud_status ud_catalog_put(ud_catalog *, const ud_catalog_key *, ud_string json);
/* Owned canonical JSON, released with ud_free. Missing key returns END. */
UD_API ud_status ud_catalog_get(ud_catalog *, const ud_catalog_key *, char **json,
                                uint32_t *length);
/* Owned JSON array of {id,revision}; validates all returned envelopes. */
UD_API ud_status ud_catalog_list(ud_catalog *, int32_t kind, char **json, uint32_t *length);
/* Stores versioned identifiers/state/result/time/kind only, no row/SQL/config
 * data. sequence is explicit; returned errors belong to the host log policy. */
UD_API ud_status ud_catalog_record_event(ud_catalog *, const ud_execution_event *,
                                         uint64_t sequence);
UD_API void ud_catalog_close(ud_catalog *);
#ifdef __cplusplus
}
#endif
#endif
