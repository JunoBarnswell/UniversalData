#ifndef UD_ODBC_INTERNAL_H
#define UD_ODBC_INTERNAL_H
#include "common.h"
#ifdef _WIN32
#include <windows.h>
#endif
#include <sql.h>
#include <sqlext.h>
#include "../components/foundation/thread.h"
#include <stdatomic.h>
typedef struct odbc_state {
    ud_host_api host;
    struct odbc_state *bulk_sessions, *bulk_next;
    struct ud_mssql_cdc *cdc;
    char *configuration;
    uint32_t configuration_length;
    SQLHENV environment;
    SQLHDBC connection;
    SQLHSTMT cursor;
    ud_mutex handles_lock;
    SQLHSTMT handles[16];
    ud_batch *schema;
    uint32_t batch_size;
    ud_read_options record_options;
    uint64_t capabilities;
    int dialect, ansi, utc, transaction, broken, array_parameters, bulk_session;
    atomic_int cancelled;
    void *check_user;
    ud_status (*check)(void *);
    char dbms[128], version[128], client[128], quote;
    SQLINTEGER native_error;
    char sqlstate[6];
    /* A prepared XA branch outlives this transport; close cannot decide it. */
    char branch_id[65];
    uint8_t branch_digest[32];
    int xa_phase, resolving;
} odbc_state;
enum {
    DIALECT_GENERIC = 0,
    DIALECT_MSSQL,
    DIALECT_MYSQL,
    DIALECT_POSTGRES,
    DIALECT_SQLITE,
    DIALECT_ORACLE,
    DIALECT_DB2,
    DIALECT_DUCKDB
};

ud_status ud_odbc_control(odbc_state *);
ud_status ud_odbc_diagnostic(odbc_state *);
ud_status ud_odbc_read_open(void *, const ud_read_options *, const ud_batch *);
ud_status ud_odbc_read_next(void *, ud_batch **);
ud_status ud_odbc_read_close(void *);
ud_status ud_odbc_begin(void *, int32_t);
ud_status ud_odbc_commit(void *);
ud_status ud_odbc_rollback(void *);
ud_status ud_odbc_command(odbc_state *, const char *);
ud_status ud_odbc_execute(void *, const ud_read_options *, uint64_t *);
ud_status ud_odbc_initialize(const ud_host_api *, const char *, uint32_t, void **);
ud_status ud_odbc_initialize_bulk(const ud_host_api *, const char *, uint32_t, void **);
#ifdef UD_WITH_MSSQL_BCP
ud_status ud_mssql_bulk_stage(odbc_state *, const ud_write_options *, const ud_batch *,
                              const uint8_t digest[32], odbc_state **, ud_string *);
#endif
void ud_odbc_destroy(void *);
void ud_odbc_cancel(void *);
ud_status ud_odbc_discover(void *, ud_string, ud_batch **);
ud_status ud_mssql_cdc_enabled(odbc_state *, int *);
ud_status ud_mssql_cdc_provision(odbc_state *);
ud_status ud_mssql_cdc_open(void *, const ud_cdc_options *);
ud_status ud_mssql_cdc_next(void *, ud_cdc_event *);
ud_status ud_mssql_cdc_confirm(void *, ud_string);
ud_status ud_mssql_cdc_close(void *);
void ud_mssql_cdc_cancel(odbc_state *);
ud_status ud_mssql_xa_probe(odbc_state *);
ud_status ud_mssql_xa_begin(void *, const ud_transaction_branch *);
ud_status ud_mssql_xa_prepare(void *, const ud_transaction_branch *);
ud_status ud_mssql_xa_resolve(void *, const ud_transaction_branch *, int32_t);
ud_status ud_mssql_xa_recover(void *, const ud_transaction_branch *, int32_t *);
void ud_mssql_xa_close(odbc_state *);
void ud_odbc_bulk_close(odbc_state *);
ud_status ud_odbc_tx_receipt(odbc_state *, const ud_transaction_branch *, int insert, int *found);
#endif
