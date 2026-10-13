#ifndef UD_ADVANCED_INTERNAL_H
#define UD_ADVANCED_INTERNAL_H
#include "state.h"
#include "ud/operator.h"
#include "ud/regex.h"
#include "ud/text.h"
#include "ud/cache.h"
typedef struct ud_advanced ud_advanced;
ud_status ud_advanced_create(ud_context *,const ud_operator_options *,const ud_batch *const *,ud_advanced **);
ud_status ud_advanced_schema(const ud_advanced *,uint32_t,ud_batch **);
ud_status ud_advanced_order(const ud_advanced *,ud_sort_contract *);
ud_status ud_advanced_push(ud_advanced *,uint32_t,const ud_batch *);
ud_status ud_advanced_end(ud_advanced *,uint32_t);
ud_status ud_advanced_pull(ud_advanced *,ud_batch **,uint32_t *);
uint64_t ud_advanced_demand(const ud_advanced *);
ud_status ud_advanced_diagnostics(const ud_advanced *,char **,uint32_t *);
uint32_t ud_advanced_algorithm(const ud_advanced *);
uint32_t ud_advanced_output_count(const ud_advanced *);
void ud_advanced_destroy(ud_advanced *);
#endif
