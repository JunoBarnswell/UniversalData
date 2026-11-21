#ifndef UD_COMPUTE_STAT_H
#define UD_COMPUTE_STAT_H
#include "state.h"
#include "sketch.h"
typedef struct ud_aggregate_state {
    ud_context *ctx;
    ud_sum_state sum;
    ud_value extremum;
    ud_value_set distinct;
    uint8_t *hll;
} ud_aggregate_state;
ud_status ud_stat_add(ud_aggregate_state *, const ud_aggregate_def *, const ud_value *);
int ud_stat_retractable(const ud_aggregate_def *);
ud_status ud_stat_retract(ud_aggregate_state *, const ud_aggregate_def *, const ud_value *);
ud_status ud_stat_result(const ud_aggregate_state *, const ud_aggregate_def *, const ud_field *,
                         ud_value *);
ud_status ud_stat_encode(const ud_aggregate_state *, const ud_aggregate_def *, const ud_field *,
                         ud_value *);
ud_status ud_stat_merge(ud_aggregate_state *, const ud_aggregate_def *, const ud_field *,
                        const ud_value *);
void ud_stat_clear(ud_aggregate_state *);
#endif
