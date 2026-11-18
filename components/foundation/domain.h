#ifndef UD_MEMORY_DOMAIN_H
#define UD_MEMORY_DOMAIN_H
#include "internal.h"
typedef struct ud_memory_domain ud_memory_domain;
typedef struct ud_memory_scope {
    ud_memory_domain *previous;
} ud_memory_scope;
ud_status ud_memory_domain_create(ud_context *, uint64_t limit, ud_memory_domain **);
ud_status ud_memory_domain_create_child(ud_context *, uint64_t, ud_memory_domain *,
                                        ud_memory_domain **);
/* Charge retention of existing allocations without charging the Context twice. */
ud_status ud_memory_domain_charge(ud_memory_domain *, uint64_t);
void ud_memory_domain_credit(ud_memory_domain *, uint64_t);
int ud_allocation_in_domain(const void *, const ud_memory_domain *);
void ud_memory_domain_enter(ud_memory_domain *, ud_memory_scope *);
void ud_memory_domain_leave(const ud_memory_scope *);
uint64_t ud_memory_domain_used(const ud_memory_domain *);
uint64_t ud_memory_domain_peak(const ud_memory_domain *);
ud_status ud_memory_domain_destroy(ud_memory_domain *);
#endif
