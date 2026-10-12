#ifndef UD_REGISTRY_INTERNAL_H
#define UD_REGISTRY_INTERNAL_H
#include "ud/component.h"
ud_status ud_registry_acquire(ud_registry *);
void ud_registry_release(ud_registry *);
#endif
