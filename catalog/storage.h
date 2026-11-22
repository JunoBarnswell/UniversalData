#ifndef UD_CATALOG_STORAGE_H
#define UD_CATALOG_STORAGE_H
#include "ud/status.h"
#include "ud/memory.h"
#include <sqlite3.h>
#ifdef UD_WITH_POSTGRESQL_CATALOG
#include <libpq-fe.h>
#endif

typedef struct catalog_statement catalog_statement;
struct ud_catalog;

ud_status catalog_sqlite_status(int);
ud_status catalog_prepare(struct ud_catalog *, const char *, catalog_statement **);
ud_status catalog_text(catalog_statement *, int, ud_string);
ud_status catalog_int(catalog_statement *, int, int64_t);
ud_status catalog_blob(catalog_statement *, int, const void *, int);
ud_status catalog_step(catalog_statement *);
ud_status catalog_execute(catalog_statement *);
const char *catalog_column_text(catalog_statement *, int);
const void *catalog_column_blob(catalog_statement *, int);
int catalog_column_bytes(catalog_statement *, int);
int64_t catalog_column_int(catalog_statement *, int);
void catalog_finalize(catalog_statement *);
ud_status catalog_postgresql_open(struct ud_catalog *, ud_string);
#ifdef UD_WITH_POSTGRESQL_CATALOG
ud_status catalog_postgresql_query(ud_catalog *, const char *, int, const Oid *,
                                  const char *const *, const int *, const int *, PGresult **);
ud_status catalog_postgresql_begin(ud_catalog *, const char *);
ud_status catalog_postgresql_admit(ud_catalog *);
ud_status catalog_postgresql_finish(ud_catalog *, ud_status);
#endif
#endif
