#ifndef UD_MYSQL_INTERNAL_H
#define UD_MYSQL_INTERNAL_H
#include "mysql_family.h"
#include "binlog.h"
#include <stdbool.h>
#include <stdatomic.h>
#include "../components/foundation/thread.h"
#if defined(UD_MARIADB_CLIENT)
#include <mariadb_rpl.h>
#endif

#if defined(UD_MYSQL_CLIENT)
typedef bool ud_mysql_bool;
#else
typedef my_bool ud_mysql_bool;
#endif

typedef struct mysql_cdc {
    MYSQL *stream;
    ud_binlog *decoder;
    ud_batch *schema;
    char *file, *table, *gtid, *incarnation;
    uint64_t position, confirmed;
    uint32_t phase, rows;
    char *schema_position;
    int snapshot, opened, checksum, schema_changed;
#if defined(UD_MYSQL_CLIENT)
    MYSQL_RPL rpl;
#else
    MARIADB_RPL *rpl;
    MARIADB_RPL_EVENT *event;
#endif
} mysql_cdc;

typedef struct mysql_state {
    ud_host_api host;
    MYSQL *db, *cancel_db;
    mysql_cdc *cdc;
    MYSQL_STMT *cursor;
    ud_batch *schema;
    uint32_t batch_size;
    ud_read_options record_options;
    char *host_name, *user, *password, *database, *ca, *cert, *key;
    uint32_t port, timeout;
    int utc, transaction, broken, bulk_allowed, cursor_readonly, scalar;
    bool ssl_disabled;
    int cleanup, xa_state;
    char branch_id[65];
    unsigned int record_code;
    ud_bulk_reader *bulk;
    ud_status bulk_status;
    int bulk_requested;
    atomic_int cancelled;
    ud_mutex cancellation;
    void *check_user;
    ud_status (*check)(void *);
} mysql_state;

typedef struct bindings {
    mysql_state *state;
    MYSQL_BIND *bind;
    unsigned long *length;
    ud_mysql_bool *nulls;
    char **owned;
    ud_value *values;
    uint32_t count;
} bindings;

ud_status ud_mysql_check(mysql_state *);
ud_status ud_mysql_failure(mysql_state *, MYSQL_STMT *);
void ud_mysql_bindings_free(bindings *);
ud_status ud_mysql_bindings_create(mysql_state *, uint32_t, bindings *);
ud_status ud_mysql_bind_value(bindings *, uint32_t, ud_value);
ud_status ud_mysql_statement(mysql_state *, ud_string, MYSQL_STMT **);
ud_status ud_mysql_execute_bound(mysql_state *, MYSQL_STMT *, bindings *, uint64_t *);
ud_status ud_mysql_begin(mysql_state *, int32_t);
ud_status ud_mysql_commit(mysql_state *);
ud_status ud_mysql_rollback(mysql_state *);
ud_status ud_mysql_receipt(mysql_state *, ud_string, ud_string, ud_string, ud_commit_receipt *);
ud_status ud_mysql_prove_target(mysql_state *, const ud_write_options *, const ud_batch *);
int ud_mysql_bulk_init(void **, const char *, void *);
int ud_mysql_bulk_read(void *, char *, unsigned int);
void ud_mysql_bulk_end(void *);
int ud_mysql_bulk_error(void *, char *, unsigned int);
uint64_t ud_mysql_session_capabilities(void *);
ud_status ud_mysql_write_batch(void *, ud_string, const ud_write_options *, const ud_batch *,
                               const uint8_t *);

#endif
