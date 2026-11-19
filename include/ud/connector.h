#ifndef UD_CONNECTOR_H
#define UD_CONNECTOR_H
#include "data.h"
#include "cdc.h"
#include "message.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_plugin ud_plugin;
typedef struct ud_connection ud_connection;
enum {
    UD_CONNECT_READ = 1,
    UD_CONNECT_APPEND = 2,
    UD_CONNECT_UPSERT = 4,
    UD_CONNECT_DELETE = 8,
    UD_CONNECT_OVERWRITE = 16,
    UD_CONNECT_CDC = 32,
    UD_CONNECT_SNAPSHOT = 64,
    UD_CONNECT_RECEIPTS = 128,
    UD_CONNECT_EXECUTE_SQL = 256,
    UD_CONNECT_EXECUTE_PROCESS = 512,
    UD_CONNECT_HTTP = 1024,
    UD_CONNECT_TRANSACTION = 2048,
    UD_CONNECT_METADATA = 4096,
    UD_CONNECT_POOL = 8192,
    UD_CONNECT_CHANGESET = 16384,
    UD_CONNECT_PREPARED = 32768,
    UD_CONNECT_BATCH_WRITE = 65536,
    UD_CONNECT_NATIVE_CDC = 131072,
    UD_CONNECT_PROVISION = 262144,
    UD_CONNECT_NATIVE_BULK = 524288, /* Genuine native append transport; not parameter batching. */
    UD_CONNECT_TRANSACTION_PARTICIPANT = 1048576,
    UD_CONNECT_PARAMETER_ARRAY = 2097152,
    UD_CONNECT_RECORD_ERRORS = 4194304, /* Complete SQL record notifications and isolation. */
    UD_CONNECT_TRANSACTIONAL_DML =
        8388608, /* Accepted row DML is governed by transaction/savepoint rollback. */
    UD_CONNECT_READONLY_SCAN =
        16777216, /* Table scans cannot execute external writes; enforced by the driver. */
    UD_CONNECT_READ_SNAPSHOT = 33554432, /* Repeatable read-only view across scans. */
    UD_CONNECT_MESSAGE_SUBSCRIBE = 67108864,
    UD_CONNECT_MESSAGE_PUBLISH = 134217728,
    UD_CONNECT_MESSAGE_TRANSACTION = 268435456
};
enum {
    UD_ISOLATION_READ_COMMITTED = 1, UD_ISOLATION_REPEATABLE_READ, UD_ISOLATION_SERIALIZABLE,
    UD_ISOLATION_READ_SNAPSHOT
};
enum {
    UD_TX_UNKNOWN = 0,
    UD_TX_ACTIVE = 1,
    UD_TX_PREPARED = 2,
    UD_TX_COMMITTED = 3,
    UD_TX_ROLLED_BACK = 4,
    UD_TX_NOT_FOUND = 5
};
enum { UD_TX_COMMIT = 1, UD_TX_ROLLBACK = 2 };
typedef struct ud_transaction_branch {
    uint32_t struct_size, abi_version;
    ud_string id; /* Native-generated 64 lowercase hex bytes; stable across recovery. */
    int32_t isolation;
    uint8_t digest[32]; /* Coordinator/participant binding proof stored with business effects. */
} ud_transaction_branch;
enum {
    UD_METADATA_CATALOGS = 1,
    UD_METADATA_SCHEMAS,
    UD_METADATA_TABLES,
    UD_METADATA_COLUMNS,
    UD_METADATA_PRIMARY_KEYS,
    UD_METADATA_FOREIGN_KEYS,
    UD_METADATA_INDEXES
};
typedef struct ud_metadata_options {
    uint32_t struct_size, abi_version;
    int32_t kind;
    ud_string parent;
    uint64_t offset;
    uint32_t limit;
} ud_metadata_options;
enum { UD_WRITE_APPEND = 1, UD_WRITE_UPSERT = 2, UD_WRITE_DELETE = 3, UD_WRITE_CHANGESET = 4,
       UD_WRITE_UPDATE = 5 };
enum { UD_LOAD_AUTO = 0, UD_LOAD_PARAMETERS = 1, UD_LOAD_NATIVE = 2 };
enum { UD_DELIVERY_RESPONSE = 1, UD_DELIVERY_IDEMPOTENT = 2, UD_DELIVERY_RECEIPT = 3 };
enum {
    UD_CHANGE_INSERT = 1,
    UD_CHANGE_UPDATE = 2,
    UD_CHANGE_DELETE = 3,
    UD_CHANGE_UPSERT = 4,
    UD_CHANGE_TRUNCATE = 5
};
enum {
    UD_ACCESS_OPEN = 1,
    UD_ACCESS_DISCOVER = 2,
    UD_ACCESS_READ = 3,
    UD_ACCESS_WRITE = 4,
    UD_ACCESS_RECEIPT = 5,
    UD_ACCESS_DELETE = 8, /* 6/7 are reserved. */
    UD_ACCESS_EXECUTE = 9,
    UD_ACCESS_NETWORK = 10,
    /* Engine-owned recovery storage has a separate purpose. Package file
     * leases never request these grants, even for the same physical path. */
    UD_ACCESS_CHECKPOINT_OPEN = 11,
    UD_ACCESS_CHECKPOINT_READ = 12,
    UD_ACCESS_CHECKPOINT_WRITE = 13
};
/* Only UD_OK is an authorization grant. Missing callback is a denial. The host
 * controls the credential scope; this callback never implements an identity provider. */
typedef ud_status (*ud_authorize_fn)(void *, ud_string tenant, ud_string resource, int32_t action);
/* Resolver returns configuration in an allocation from ctx (ud_alloc), released
 * by the SDK after consuming it. Reference / secret values are never logged. */
typedef ud_status (*ud_credential_resolver_fn)(void *, ud_context *ctx, ud_string tenant,
                                               ud_string resource, ud_string reference,
                                               ud_string *);
typedef struct ud_connection_options {
    uint32_t struct_size, abi_version;
    ud_string tenant, resource, credential_reference;
    void *authorize_user;
    ud_authorize_fn authorize;
    void *resolve_user;
    ud_credential_resolver_fn resolve;
    ud_status (*check)(void *authorize_user); /* Factory deadline/cancellation control. */
} ud_connection_options;
/* Called once for each completely fetched SQL record. On OK, batch already
 * contains its last row. On a row conversion failure, raw_cells are complete
 * nullable BINARY representations in provider result order; boundaries are known.
 * A callback may isolate conversion/constraint failures, never execution failures. */
typedef ud_status (*ud_read_record_fn)(void *, ud_batch *, ud_status, uint32_t column,
                                       const ud_field *, const ud_value *decoded,
                                       const ud_value *raw_cells, uint32_t count);
enum { UD_READ_TYPED = 0, UD_READ_DISCOVER = 1, UD_READ_SCALAR = 2 };
typedef struct ud_read_options {
    uint32_t struct_size, abi_version, batch_size;
    ud_string sql;              /* Prepared SQL text; no value interpolation. */
    const ud_batch *parameters; /* Borrowed sealed typed parameter batch. */
    uint32_t parameter_row;     /* Read selects one row; execute applies all supplied rows. */
    void *record_user;
    ud_read_record_fn record;
    int32_t result_mode;
    ud_batch **result_schema; /* Optional owned zero-row result shape. */
} ud_read_options;
enum {
    UD_COMMAND_NOT_EXECUTED = 0,
    UD_COMMAND_PENDING,
    UD_COMMAND_APPLIED,
    UD_COMMAND_COMMITTED,
    UD_COMMAND_REJECTED,
    UD_COMMAND_UNKNOWN
};
/* APPLIED belongs to the caller's still-open transaction; COMMITTED is a
 * completed local transaction. A rejection is safe only after rollback proof.
 * provider_code is owned Native memory; release with result_clear. */
typedef struct ud_command_result {
    uint32_t struct_size, abi_version;
    int32_t state;
    ud_status result;
    uint64_t affected_rows;
    ud_string provider_code;
} ud_command_result;
UD_API void ud_command_result_clear(ud_command_result *);
UD_API ud_status ud_connection_execute_record(ud_connection *, const ud_read_options *,
                                              ud_command_result *);
typedef struct ud_write_options {
    uint32_t struct_size, abi_version;
    int32_t mode;
    ud_string table, job, batch_id, source_position;
    const uint32_t *keys;
    uint32_t key_count;
    const int32_t *operations; /* CHANGESET: one typed action per row, in supplied order. */
    const ud_batch *before;    /* Optional exact preimages; same shape/count when supplied. */
    uint32_t
        current_column; /* UINT32_MAX for ordinary Changesets; SCD supplies its current BOOL. */
    int32_t
        load_mode; /* AUTO selects a proven native transport; explicit NATIVE never falls back. */
    int32_t allow_truncate; /* Explicit whole-table data reset; compiled as transactional DELETE. */
    int32_t delivery; /* Zero declares the default receipted SQL delivery. */
    uint32_t chunk_rows; /* Optional upper bound per physical execution. */
    uint64_t *affected_rows; /* Optional directly affected rows, published on success. */
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
    ud_status (*write)(void *, ud_string tenant, const ud_write_options *, const ud_batch *,
                       const uint8_t digest[32]);
    ud_status (*receipt)(void *, ud_string tenant, ud_string job, ud_string batch_id,
                         ud_commit_receipt *);
    void (*cancel)(void *);
    ud_status (*execute)(void *, const ud_read_options *, uint64_t *);
    ud_status (*begin)(void *, int32_t isolation);
    ud_status (*commit)(void *);
    ud_status (*rollback)(void *);
    ud_status (*metadata)(void *, const ud_metadata_options *, ud_batch **);
    /* Driver must call check during lengthy I/O; cancellation is concurrent.
     * Passing NULL detaches the Activation on return to the idle pool. */
    void (*set_control)(void *, void *user, ud_status (*check)(void *));
    /* POOL requires a clean-session proof. No cursor, transaction or mutable
     * per-session SQL state may cross leases. Failure retires the session. */
    ud_status (*reset_session)(void *);
    uint64_t (*session_capabilities)(
        void *);                    /* Actual immutable probe results, not a driver-name guess. */
    ud_status (*provision)(void *); /* Explicit authorized SDK receipt schema provisioning. */
    int32_t metadata_dialect;       /* UD_SQL_*; provider declares its metadata grammar. */
    int32_t (*session_dialect)(void *);
    int32_t parameter_style;
    ud_status (*cdc_open)(void *, const ud_cdc_options *);
    ud_status (*cdc_next)(void *, ud_cdc_event *);
    ud_status (*cdc_confirm)(void *, ud_string position);
    ud_status (*cdc_close)(void *);
    /* Real durable participant protocol, independent of ordinary local begin.
     * A committed branch must be proved by its receipt when the prepared ID is
     * no longer listed. NOT_FOUND alone never proves a COMMIT decision. */
    ud_status (*transaction_begin)(void *, const ud_transaction_branch *);
    ud_status (*transaction_prepare)(void *, const ud_transaction_branch *);
    ud_status (*transaction_resolve)(void *, const ud_transaction_branch *, int32_t decision);
    ud_status (*transaction_recover)(void *, const ud_transaction_branch *, int32_t *state);
    ud_status (*record_diagnostic)(void *,
                                   ud_string *provider_code); /* Owned code only; consumes it. */
    ud_status (*cdc_rebind)(void *, const ud_batch *, uint64_t schema_version);
    /* Driver primitive for Native-compiled transaction control. Required by TRANSACTIONAL_DML. It
     * bypasses user SQL restrictions, never authorizes arbitrary product SQL. */
    ud_status (*savepoint)(void *, ud_string native_statement);
    /* Optional provider proof inside the row transaction, before business mutation. */
    ud_status (*record_validate)(void *, const ud_read_options *);
    const ud_message_api *message;
} ud_connector_api;
UD_API ud_status ud_connection_savepoint(ud_connection *, ud_string, int32_t);
UD_API ud_status ud_connection_record_diagnostic(ud_connection *, ud_string *);
UD_API ud_status ud_connection_transaction_begin(ud_connection *, const ud_transaction_branch *);
UD_API ud_status ud_connection_transaction_prepare(ud_connection *, const ud_transaction_branch *);
UD_API ud_status ud_connection_transaction_resolve(ud_connection *, const ud_transaction_branch *,
                                                   int32_t);
UD_API ud_status ud_connection_transaction_recover(ud_connection *, const ud_transaction_branch *,
                                                   int32_t *);
UD_API ud_status ud_connection_open(ud_plugin *, const ud_connection_options *, ud_connection **);
UD_API ud_status ud_connection_provision(ud_connection *);
UD_API int32_t ud_connection_dialect(const ud_connection *);
UD_API int32_t ud_connection_parameter_style(const ud_connection *);
UD_API uint64_t ud_connection_capabilities(const ud_connection *);
UD_API ud_status ud_connection_execute_command(ud_connection *, const ud_read_options *,
                                               uint64_t *processed_rows);
UD_API ud_status ud_connection_begin(ud_connection *, int32_t isolation);
UD_API ud_status ud_connection_commit(ud_connection *);
UD_API ud_status ud_connection_rollback(ud_connection *);
UD_API ud_status ud_connection_metadata(ud_connection *, const ud_metadata_options *, ud_batch **);
UD_API ud_status ud_connection_cdc_open(ud_connection *, const ud_cdc_options *);
UD_API ud_status ud_connection_cdc_next(ud_connection *, ud_cdc_event *);
UD_API ud_status ud_connection_cdc_confirm(ud_connection *, ud_string);
UD_API ud_status ud_connection_cdc_close(ud_connection *);
UD_API ud_status ud_connection_cdc_rebind(ud_connection *, const ud_batch *, uint64_t);
UD_API ud_status ud_connection_cdc_schema(ud_connection *, ud_batch **);
UD_API ud_status ud_connection_test(ud_connection *);
UD_API ud_status ud_connection_schema(ud_connection *, ud_string table, ud_string name_space,
                                      ud_batch **);
UD_API ud_status ud_connection_discover(ud_connection *, ud_string table, ud_batch **);
UD_API ud_status ud_connection_read_open(ud_connection *, const ud_read_options *,
                                         const ud_batch *schema);
UD_API ud_status ud_connection_read_next(ud_connection *, ud_batch **);
UD_API ud_status ud_connection_read_close(ud_connection *);
UD_API ud_status ud_connection_digest(ud_connection *, const ud_write_options *, const ud_batch *,
                                      uint8_t out[32]);
UD_API ud_status ud_connection_write(ud_connection *, const ud_write_options *, const ud_batch *);
UD_API ud_status ud_connection_receipt(ud_connection *, ud_string job, ud_string batch_id,
                                       ud_commit_receipt *);
/* Cancel may run concurrently with a read/write, but not destruction. */
UD_API void ud_connection_cancel(ud_connection *);
UD_API ud_status ud_connection_close(ud_connection *);
#ifdef __cplusplus
}
#endif
#endif
