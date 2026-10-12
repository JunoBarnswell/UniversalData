#ifndef UD_COMPUTE_H
#define UD_COMPUTE_H
#include "data.h"
#ifdef __cplusplus
extern "C" {
#endif
/* Stable sort, NULLS FIRST ascending / NULLS LAST descending. */
UD_API ud_status ud_compute_sort(ud_context *, const ud_batch *, uint32_t key, int32_t descending,
                               ud_batch **);
UD_API ud_status ud_compute_slice(ud_context *, const ud_batch *, uint64_t offset, uint64_t limit,
                                ud_batch **);
/* Same key type required; NULL never joins NULL. Names must be unique across inputs. */
UD_API ud_status ud_compute_inner_join(ud_context *, const ud_batch *left, uint32_t left_key,
                                     const ud_batch *right, uint32_t right_key, ud_batch **);

typedef struct ud_plan ud_plan;
typedef struct ud_accumulator ud_accumulator;
#define UD_SCHEMA_HEADER "ud-schema 3\n"
#define UD_PLAN_HEADER "ud-plan 3\n"
enum { UD_MAP_TRIM_ASCII=1, UD_MAP_UPPER_ASCII=2, UD_MAP_LOWER_ASCII=4, UD_MAP_DEFAULT=8 };
typedef struct ud_mapping {
    uint32_t struct_size, version;
    ud_field target;
    int32_t source_column; /* -1 selects constant; otherwise input column index. */
    ud_value constant, default_value;
    uint32_t flags;
} ud_mapping;
/* Fields are mapped in output order, renamed and explicitly converted. Text
 * normalization affects ASCII only; UTF8 code points are retained. */
UD_API ud_status ud_compute_map(ud_context *, const ud_batch *, const ud_mapping *,
                                uint32_t columns, int32_t rounding, ud_batch **);
/* Schema/plan command grammar is specified in docs/api.md. Strings are copied. */
UD_API ud_status ud_plan_create(ud_context *, const char *schema, uint32_t schema_length,
                                const char *commands, uint32_t commands_length, ud_plan **);
/* Typed C construction covers recursive fields. Version must be UD_PLAN_VERSION. */
UD_API ud_status ud_plan_create_fields(ud_context *, uint32_t version, const ud_field *,
                                       uint32_t columns, const char *commands, uint32_t length,
                                       ud_plan **);
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
