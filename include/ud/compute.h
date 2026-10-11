#ifndef UD_COMPUTE_H
#define UD_COMPUTE_H
#include "data.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_plan ud_plan;
typedef struct ud_accumulator ud_accumulator;
/* Schema/plan command grammar is specified in docs/api.md. Strings are copied. */
UD_API ud_status ud_plan_create(ud_context *, const char *schema, uint32_t schema_length,
                                const char *commands, uint32_t commands_length, ud_plan **);
UD_API void ud_plan_destroy(ud_plan *);
UD_API ud_status ud_plan_schema(const ud_plan *, const ud_field **, uint32_t *);
UD_API void ud_plan_fingerprint(const ud_plan *, uint8_t out[32]);
UD_API ud_status ud_compute_apply(const ud_plan *, const ud_batch *, ud_batch **);
UD_API ud_status ud_accumulator_create(const ud_plan *, ud_accumulator **);
UD_API ud_status ud_accumulator_add(ud_accumulator *, const ud_batch *);
UD_API ud_status ud_accumulator_snapshot(const ud_accumulator *, ud_batch **);
UD_API ud_status ud_accumulator_restore(ud_accumulator *, const ud_batch *);
UD_API void ud_accumulator_destroy(ud_accumulator *);
/* SQL three-valued logic: -1 unknown, 0 false, 1 true. */
UD_API int32_t ud_truth_and(int32_t, int32_t);
UD_API int32_t ud_truth_or(int32_t, int32_t);
UD_API int32_t ud_truth_not(int32_t);
#ifdef __cplusplus
}
#endif
#endif
