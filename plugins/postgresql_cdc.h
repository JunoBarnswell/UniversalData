#ifndef UD_POSTGRESQL_CDC_H
#define UD_POSTGRESQL_CDC_H
#include "common.h"
typedef struct ud_pg_cdc ud_pg_cdc;
ud_status ud_pg_cdc_open(const ud_host_api *, const char *, const ud_cdc_options *, ud_pg_cdc **);
ud_status ud_pg_cdc_rebind(ud_pg_cdc *, const ud_batch *, uint64_t);
ud_status ud_pg_cdc_next(ud_pg_cdc *, ud_cdc_event *);
ud_status ud_pg_cdc_confirm(ud_pg_cdc *, ud_string);
ud_status ud_pg_cdc_close(ud_pg_cdc *);
void ud_pg_cdc_cancel(ud_pg_cdc *);
#endif
