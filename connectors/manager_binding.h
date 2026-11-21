#ifndef UD_MANAGER_BINDING_H
#define UD_MANAGER_BINDING_H
#include "ud/package.h"

/* Resolve a host-bound ud:<name> driver before connector acquisition. */
ud_status ud_execution_resolve_driver(ud_execution *, ud_string *driver);

#endif
