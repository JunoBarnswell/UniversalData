#ifndef UD_COMPUTE_INTERNAL_H
#define UD_COMPUTE_INTERNAL_H
#include "ud/compute.h"
#include "ud/expression.h"
#include "../data/internal.h"
typedef struct {
    uint32_t column;
    int kind;
    char *name;
    ud_expression *condition;
    uint32_t maximum_distinct;
    int distinct, rounding;
} ud_aggregate_def;
struct ud_plan {
    ud_context *ctx;
    ud_field *fields;
    uint32_t n;
    uint32_t *projection, project_n, *keys, key_n;
    ud_expression *predicate;
    ud_aggregate_def *aggs;
    uint32_t agg_n;
    ud_json *definition;
    uint8_t fingerprint[32];
    ud_batch *schema_owner;
    ud_batch *output_schema;
    atomic_uint references;
};
int ud_value_compare(const ud_value *, const ud_value *);
ud_status ud_plan_check_batch(const ud_plan *, const ud_batch *);
ud_status ud_plan_accept(const ud_plan *, const ud_batch *, uint32_t, int *);
ud_status ud_accumulator_update_accepted(ud_accumulator *, const ud_batch *, uint32_t, int);
ud_status ud_accumulator_state_schema(const ud_plan *,ud_batch **);
ud_status ud_accumulator_partial_snapshot(const ud_accumulator *,ud_batch **);
ud_status ud_accumulator_merge_row(ud_accumulator *,const ud_batch *,uint32_t);
ud_status ud_plan_retain(ud_plan *);
ud_status ud_partition_hash_row(const ud_batch *, uint32_t row, const uint32_t *keys,
                                uint32_t key_count, uint32_t partitions, uint32_t *partition);
int ud_compute_stage_order(const ud_batch *, uint32_t, const ud_batch *, uint32_t);
static inline int ud_key_descending(int descending, uint32_t directions, uint32_t index) {
    return descending || (index < 16 && (directions & (UINT32_C(1) << index)) != 0);
}
int ud_compute_compare_keys(const ud_batch *, uint32_t, const ud_batch *, uint32_t, const uint32_t *,
                            uint32_t, int, uint32_t, int);
ud_status ud_compute_sort_keys(ud_context *, const ud_batch *, const uint32_t *, uint32_t,
                               int, uint32_t, int, ud_batch **);
#endif
