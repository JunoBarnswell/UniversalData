#ifndef UD_CATALOG_INTERNAL_H
#define UD_CATALOG_INTERNAL_H
#include "ud/catalog.h"
#include "../engines/package/internal.h"
#include "storage.h"
struct ud_catalog {
    ud_context *ctx;
    ud_catalog_options options;
    char *directory, *tenant;
    sqlite3 *database;
#ifdef UD_WITH_POSTGRESQL_CATALOG
    PGconn *postgresql;
#endif
    int remote;
    uint64_t changed;
    char *leader_owner;
    uint64_t leader_generation;
};
ud_status ud_catalog_definition_check(ud_catalog *, const ud_catalog_key *, const ud_json *);
ud_status ud_catalog_sql(ud_catalog *, const char *);
ud_status ud_catalog_begin(ud_catalog *, int election);
ud_status ud_catalog_finish(ud_catalog *, ud_status);
ud_status ud_catalog_append_document(ud_catalog *, const ud_catalog_key *, ud_string, int conditional,
                                     uint64_t expected, const ud_catalog_lease *, int own_transaction);
#endif
