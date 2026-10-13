#ifndef UD_RELATIONAL_INTERNAL_H
#define UD_RELATIONAL_INTERNAL_H
#include "ud/operator.h"
typedef struct ud_relational ud_relational;
ud_status ud_relational_create(ud_context *,const ud_operator_options *,const ud_batch *const *,ud_relational **);
ud_status ud_relational_schema(const ud_relational *,uint32_t,ud_batch **);
ud_status ud_relational_order(const ud_relational *,ud_sort_contract *);
uint64_t ud_relational_demand(const ud_relational *);
ud_status ud_relational_push(ud_relational *,uint32_t,const ud_batch *);
ud_status ud_relational_end(ud_relational *,uint32_t);
ud_status ud_relational_pull(ud_relational *,ud_batch **,uint32_t *);
ud_status ud_relational_diagnostics(const ud_relational *,char **,uint32_t *);
void ud_relational_destroy(ud_relational *);
#endif
