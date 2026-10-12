#ifndef UD_CONNECTOR_H
#define UD_CONNECTOR_H
#include "data.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_plugin ud_plugin;
typedef struct ud_connection ud_connection;
enum {
    UD_CONNECT_READ = 1, UD_CONNECT_APPEND = 2, UD_CONNECT_UPSERT = 4,
    UD_CONNECT_DELETE = 8, UD_CONNECT_OVERWRITE = 16, UD_CONNECT_CDC = 32,
    UD_CONNECT_SNAPSHOT = 64, UD_CONNECT_RECEIPTS = 128
};
enum { UD_WRITE_APPEND = 1, UD_WRITE_UPSERT = 2, UD_WRITE_DELETE = 3 };
enum { UD_ACCESS_OPEN = 1, UD_ACCESS_DISCOVER = 2, UD_ACCESS_READ = 3,
       UD_ACCESS_WRITE = 4, UD_ACCESS_RECEIPT = 5 };
/* Only UD_OK is an authorization grant. Missing callback is a denial. The host
 * controls the credential scope; this callback never implements an identity provider. */
typedef ud_status (*ud_authorize_fn)(void *, ud_string tenant, ud_string resource, int32_t action);
/* Resolver returns configuration in an allocation from ctx (ud_alloc), released
 * by the SDK after connector creation. Reference / secret values are never logged. */
typedef ud_status (*ud_credential_resolver_fn)(void *, ud_context *ctx, ud_string tenant,
                                               ud_string resource, ud_string reference, ud_string *);
typedef struct ud_connection_options {
    uint32_t struct_size, abi_version;
    ud_string tenant, resource, credential_reference;
    void *authorize_user;
    ud_authorize_fn authorize;
    void *resolve_user;
    ud_credential_resolver_fn resolve;
} ud_connection_options;
typedef struct ud_read_options {
    uint32_t struct_size, abi_version, batch_size;
    ud_string sql; /* Explicit readonly statement. Cursor retains a bounded batch. */
} ud_read_options;
typedef struct ud_write_options {
    uint32_t struct_size, abi_version;
    int32_t mode;
    ud_string table, job, batch_id, source_position;
    const uint32_t *keys;
    uint32_t key_count;
} ud_write_options;
typedef struct ud_commit_receipt {
    int32_t committed;
    uint8_t digest[32];
    ud_string source_position; /* Owned allocation: caller releases data with ud_free. */
} ud_commit_receipt;
/* Independent plugin SDK. All returned batches and strings use host allocations.
 * No implicit transaction across connections. CommitUnknown prohibits cursor advance. */
typedef struct ud_connector_api {
    uint32_t struct_size, abi_version;
    uint64_t capabilities;
    ud_status (*test)(void *);
    ud_status (*discover)(void *, ud_string table, ud_batch **);
    ud_status (*read_open)(void *, const ud_read_options *, const ud_batch *schema);
    ud_status (*read_next)(void *, ud_batch **);
    ud_status (*read_close)(void *);
    ud_status (*write)(void *, ud_string tenant, const ud_write_options *, const ud_batch *, const uint8_t digest[32]);
    ud_status (*receipt)(void *, ud_string tenant, ud_string job, ud_string batch_id, ud_commit_receipt *);
    void (*cancel)(void *);
} ud_connector_api;
UD_API ud_status ud_connection_open(ud_plugin *, const ud_connection_options *, ud_connection **);
UD_API uint64_t ud_connection_capabilities(const ud_connection *);
UD_API ud_status ud_connection_test(ud_connection *);
UD_API ud_status ud_connection_discover(ud_connection *, ud_string table, ud_batch **);
UD_API ud_status ud_connection_read_open(ud_connection *, const ud_read_options *, const ud_batch *schema);
UD_API ud_status ud_connection_read_next(ud_connection *, ud_batch **);
UD_API ud_status ud_connection_read_close(ud_connection *);
UD_API ud_status ud_connection_write(ud_connection *, const ud_write_options *, const ud_batch *);
UD_API ud_status ud_connection_receipt(ud_connection *, ud_string job, ud_string batch_id, ud_commit_receipt *);
/* Cancel may run concurrently with a read/write, but not destruction. */
UD_API void ud_connection_cancel(ud_connection *);
UD_API ud_status ud_connection_close(ud_connection *);
#ifdef __cplusplus
}
#endif
#endif
