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
/* Native-only atomic ownership. The Catalog server's persisted UTC clock is
 * monotone; clients must also stop their Worker on a missed renewal. Expiry
 * revokes authority, it NEVER proves that external side effects were absent. */
typedef struct ud_catalog_lease_request {
    uint32_t struct_size, abi_version;
    int32_t kind;
    ud_string id, owner, pool;
    uint32_t duration_ms, capacity; /* empty pool requires capacity=0. */
} ud_catalog_lease_request;
typedef struct ud_catalog_lease {
    uint32_t struct_size, abi_version;
    int32_t kind;
    ud_string id, owner;
    uint64_t generation; /* durable increasing fencing token; never reused. */
} ud_catalog_lease;
/* Acquire never returns a live lease, even to the same owner. Pool capacity is
 * fixed by its first acquisition and checked on every later acquisition.
 * ID/owner strings remain caller-owned; no returned pointer payloads. */
UD_API ud_status ud_catalog_lease_acquire(ud_catalog *, const ud_catalog_lease_request *,
                                          uint64_t *generation, uint64_t *until_ms);
UD_API ud_status ud_catalog_lease_renew(ud_catalog *, const ud_catalog_lease *,
                                        uint32_t duration_ms, uint64_t *until_ms);
UD_API ud_status ud_catalog_lease_release(ud_catalog *, const ud_catalog_lease *);
UD_API ud_status ud_catalog_lease_active(ud_catalog *, int32_t kind, ud_string id, int32_t *active);
/* Append revision expected+1 atomically, or FENCED. After an object first uses
 * a lease, every conditional write requires its current live fencing token.
 * Unconditional versioned documents remain available for immutable Package /
 * execution facts, but cannot overwrite a lease-protected object. */
UD_API ud_status ud_catalog_compare_exchange(ud_catalog *, const ud_catalog_key *, ud_string json,
                                             uint64_t expected_revision, const ud_catalog_lease *);
/* Owned canonical JSON, released with ud_free. Missing key returns END. */
UD_API ud_status ud_catalog_get(ud_catalog *, const ud_catalog_key *, char **json,
                                uint32_t *length);
/* Owned JSON array of {id,revision}; validates all returned envelopes. */
UD_API ud_status ud_catalog_list(ud_catalog *, int32_t kind, char **json, uint32_t *length);
/* Indexed bounded query. Strict native JSON: {kind,id?,afterId?,afterRevision?,
 * sinceMs?,untilMs?,limit?,latestOnly?}. latestOnly selects current object heads.
 * Tuple (id,revision) is the stable pagination cursor;
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
/* Immutable document transport for an isolated Worker. No runtime parameter
 * values are resolved here. Import in the returned order, then use the existing
 * Native binder. The host must authorize every exported object in its scope. */
UD_API ud_status ud_catalog_binding_snapshot(ud_catalog *, const ud_catalog_binding *, char **, uint32_t *);
/* Explicit historical secret migration. Only removes legacy audit Parameters;
 * installs an opaque credential reference and rebuilds the integrity digest.
 * Requires DELETE and WRITE authorization, and a subsequent secure compaction. */
UD_API ud_status ud_catalog_protect_parameters(ud_catalog *, const ud_catalog_key *, ud_string reference);
UD_API ud_status ud_catalog_secure_compact(ud_catalog *);
/* Host records immutable Ready/terminal snapshots at explicit revisions.
 * Running/Draining return BUSY. Serialize with start/destroy; tenant must
 * match. Persists identifiers/fingerprints/metrics, never parameter/config/
 * row/credential values. Errors belong to the host logging policy. */
UD_API ud_status ud_catalog_record_execution(ud_catalog *, const ud_execution *, uint64_t revision);
/* Bounded explicit log retention. The last sequence for every execution is
 * retained so resume cannot reuse event identities. Journal/recovery facts and
 * package/deployment/job documents are never pruned by log retention. */
UD_API ud_status ud_catalog_prune_events(ud_catalog *, uint64_t before_ms, uint32_t limit,
                                         uint64_t *removed);
UD_API void ud_catalog_close(ud_catalog *);
#ifdef __cplusplus
}
#endif
#endif
