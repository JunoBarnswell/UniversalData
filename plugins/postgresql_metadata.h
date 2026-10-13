#ifndef UD_POSTGRESQL_METADATA_H
#define UD_POSTGRESQL_METADATA_H
#include "common.h"
#include <libpq-fe.h>
const char *ud_pg_metadata_query(void);
ud_status ud_pg_metadata_decode(ud_host_api *, PGresult *, ud_batch **);
#endif
