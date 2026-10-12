#ifndef UD_COMPUTE_H
#define UD_COMPUTE_H
#include "data.h"
#include "expression.h"
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
/* Canonical output metadata used by both binding and execution. Join outputs
 * acquire distinct identities and preserve each input's origin set. */
UD_API ud_status ud_compute_join_schema(ud_context *, const ud_batch *, const ud_batch *,
                                        ud_batch **);
UD_API ud_status ud_compute_union_schema(ud_context *, const ud_batch *, const ud_batch *,
                                         ud_batch **);

typedef struct ud_plan ud_plan;
typedef struct ud_accumulator ud_accumulator;
enum { UD_MAP_TRIM_ASCII = 1, UD_MAP_UPPER_ASCII = 2, UD_MAP_LOWER_ASCII = 4, UD_MAP_DEFAULT = 8 };
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
/* Canonical JSON plan: version, optional filter AST, projection/group arrays of
 * field names, aggregates [{function:count|sum|min|max,column:name|"*",name:alias}].
 * The same Expression binder evaluates the filter; no text predicate language.
 * Typed fields cover recursive schemas. Registry/resolvers borrow the supplied
 * binding options; the expression retains the Registry module lease. */
UD_API ud_status ud_plan_create_fields(ud_context *, uint32_t version, const ud_field *,
                                       uint32_t columns, ud_string definition,
                                       const ud_expression_options *bindings, ud_plan **);
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
