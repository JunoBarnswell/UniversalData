#ifndef UD_CATALOG_INTERNAL_H
#define UD_CATALOG_INTERNAL_H
#include "ud/catalog.h"
#include "../engines/package/internal.h"
#include <sqlite3.h>
struct ud_catalog {
    ud_context *ctx;
    ud_catalog_options options;
    char *directory, *tenant;
    sqlite3 *database;
};
ud_status ud_catalog_definition_check(ud_catalog *, const ud_catalog_key *, const ud_json *);
ud_status ud_catalog_sql(ud_catalog *, const char *);
ud_status ud_catalog_append_document(ud_catalog *, const ud_catalog_key *, ud_string, int conditional,
                                     uint64_t expected, const ud_catalog_lease *, int own_transaction);
#endif
