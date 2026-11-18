#ifndef UD_SCHEMA_REGISTRY_H
#define UD_SCHEMA_REGISTRY_H
#include "ud/component.h"

typedef struct ud_schema_registry ud_schema_registry;
ud_status ud_schema_registry_open(const ud_component_context *, ud_string connection,
                                   ud_string subject, uint32_t version, ud_schema_registry **);
uint32_t ud_schema_registry_id(const ud_schema_registry *);
ud_string ud_schema_registry_schema(const ud_schema_registry *);
/* Schema strings are borrowed until close. A proven unknown writer ID is UD_QUALITY. */
ud_status ud_schema_registry_find(ud_schema_registry *, uint32_t writer_id, ud_string *schema);
void ud_schema_registry_close(ud_schema_registry *);
#endif
