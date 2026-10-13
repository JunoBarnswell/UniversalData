#ifndef UD_ORACLE_INTERNAL_H
#define UD_ORACLE_INTERNAL_H
#include "common.h"
#include <oci.h>
#include <xa.h>
#include <pthread.h>
#include <stdatomic.h>
typedef struct cell {
    OCIBind *bind;
    OCIDefine *define;
    void *data;
    void *descriptor;
    sb2 indicator;
    ub4 length;
    ub2 code, type;
    ud_field field;
    int64_t integer;
    double real;
} cell;
typedef struct oracle {
    ud_host_api host;
    char *configuration;
    uint32_t configuration_length;
    struct ud_oci_bulk_drop *bulk_drops;
    struct ud_oracle_cdc *cdc;
    OCIEnv *env;
    OCIError *error;
    OCISvcCtx *service;
    OCIStmt *cursor;
    ud_batch *schema;
    cell *columns;
    uint32_t count, batch, write_count;
    ub2 *write_types;
    int utc, transaction, broken;
    int cleanup, participant_enabled, branch_state;
    char branch_id[65];
    sb4 record_code;
    OCITrans *branch_handle;
    atomic_int cancelled;
    pthread_mutex_t ownership;
    void *user;
    ud_status (*check)(void *);
} oracle;

ud_status ud_oci_control(oracle *);
ud_status ud_oci_read_open(void *, const ud_read_options *, const ud_batch *);
ud_status ud_oci_read_next(void *, ud_batch **);
ud_status ud_oci_read_close(void *);
ud_status ud_oci_query(oracle *, const char *, const ud_string *, uint32_t, const ud_field *,
                       uint32_t, ud_batch **);
ud_status ud_oci_state_command(oracle *, const ud_read_options *);
void ud_oci_destroy(void *);
ud_status ud_oci_initialize(const ud_host_api *, const char *, uint32_t, void **);
ud_status ud_oci_bulk_stage(oracle *, const ud_write_options *, const ud_batch *, oracle **,
                            ud_string *);
ud_status ud_oci_bulk_dispose(oracle *, const ud_write_options *, const ud_batch *, ud_string);
ud_status ud_oci_bulk_defer(oracle *, const ud_write_options *, const ud_batch *, ud_string);
ud_status ud_oci_bulk_cleanup(oracle *);
void ud_oci_bulk_forget(oracle *);
ud_status ud_oracle_cdc_open(void *, const ud_cdc_options *);
ud_status ud_oracle_cdc_next(void *, ud_cdc_event *);
ud_status ud_oracle_cdc_confirm(void *, ud_string);
ud_status ud_oracle_cdc_close(void *);
#endif
