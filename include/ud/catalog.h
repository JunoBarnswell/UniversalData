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
/* Native SQLite indexed immutable document repository. Host owns
 * ctx and serializes calls on each handle; independent handles/processes share
 * the transactional WAL database. Metadata stores only supplied documents;
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
/* Indexed bounded query. Strict native JSON: {kind,id?,afterId?,afterRevision?,
 * sinceMs?,untilMs?,limit?}. Tuple (id,revision) is the stable pagination cursor;
 * afterRevision without afterId requires an exact id filter. limit defaults to
 * 100 and must be 1..1000. Returns [{id,revision,document}] as owned JSON.
 * Time bounds select explicit timestampMs values in stored documents.
 * Only the current tenant/kind/filter/page is inspected; no directory scan. */
UD_API ud_status ud_catalog_query(ud_catalog *, ud_string query, char **json, uint32_t *length);
UD_API ud_status ud_catalog_latest_revision(ud_catalog *, int32_t kind, ud_string id, uint64_t *);
/* Stores versioned identifiers/state/result/time/kind only, no row/SQL/config
 * data. sequence is explicit; returned errors belong to the host log policy. */
UD_API ud_status ud_catalog_record_event(ud_catalog *, const ud_execution_event *,
                                         uint64_t sequence);
typedef struct ud_catalog_binding {
    uint32_t struct_size, abi_version;
    ud_string deployment_id;
    uint64_t deployment_revision;
    ud_string package_id, project_parameters, package_parameters;
} ud_catalog_binding;
/* Resolves exact immutable Deployment/Project/Environment/Package revisions,
 * delegates binding to the native Project model, and returns one Package for
 * the existing Execution API. No latest-version races or implicit activation. */
UD_API ud_status ud_catalog_bind_package(ud_catalog *, const ud_catalog_binding *, ud_package **,
                                         char **parameters, uint32_t *length);
/* Host records immutable Ready/terminal snapshots at explicit revisions.
 * Running/Draining return BUSY. Serialize with start/destroy; tenant must
 * match. Persists identifiers/fingerprints/metrics, never parameter/config/
 * row/credential values. Errors belong to the host logging policy. */
UD_API ud_status ud_catalog_record_execution(ud_catalog *, const ud_execution *, uint64_t revision);
UD_API void ud_catalog_close(ud_catalog *);
#ifdef __cplusplus
}
#endif
#endif
