#ifndef UD_COMPUTE_INTERNAL_H
#define UD_COMPUTE_INTERNAL_H
#include "ud/compute.h"
#include "../foundation/internal.h"
typedef struct {
    uint32_t column;
    int op;
    ud_value value;
} ud_predicate;
typedef struct {
    uint32_t column;
    int kind;
    char *name;
} ud_aggregate_def;
struct ud_plan {
    ud_context *ctx;
    ud_field *fields;
    uint32_t n;
    uint32_t *projection, project_n, *keys, key_n;
    ud_predicate *predicates;
    uint32_t predicate_n;
    ud_aggregate_def *aggs;
    uint32_t agg_n;
    char *schema_text, *commands;
    uint8_t fingerprint[32];
};
int ud_value_compare(const ud_value *, const ud_value *);
ud_status ud_plan_check_batch(const ud_plan *, const ud_batch *);
ud_status ud_plan_accept(const ud_plan *, const ud_batch *, uint32_t, int *);
#endif
