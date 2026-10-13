#ifndef UD_ORACLE_INTERNAL_H
#define UD_ORACLE_INTERNAL_H
#include "common.h"
#include <oci.h>
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
ud_status ud_oracle_cdc_open(void *, const ud_cdc_options *);
ud_status ud_oracle_cdc_next(void *, ud_cdc_event *);
ud_status ud_oracle_cdc_confirm(void *, ud_string);
ud_status ud_oracle_cdc_close(void *);
#endif
