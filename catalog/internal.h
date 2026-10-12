#ifndef UD_CATALOG_INTERNAL_H
#define UD_CATALOG_INTERNAL_H
#include "ud/catalog.h"
#include "../engines/package/internal.h"
struct ud_catalog {
    ud_context *ctx;
    ud_catalog_options options;
    char *directory, *tenant;
};
ud_status ud_catalog_definition_check(ud_catalog *, const ud_catalog_key *, const ud_json *);
#endif
