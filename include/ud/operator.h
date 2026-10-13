#ifndef UD_OPERATOR_H
#define UD_OPERATOR_H
#include "compute.h"
#include "buffer.h"
#include "partition.h"
#include "error.h"
typedef struct ud_executor ud_executor;
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_operator ud_operator;
enum { UD_PHYSICAL_STREAM = 1, UD_PHYSICAL_SORT, UD_PHYSICAL_HASH,
       UD_PHYSICAL_GRACE_HASH, UD_PHYSICAL_MERGE, UD_PHYSICAL_INDEX };

enum {
    UD_OPERATOR_SORT = 1,
    UD_OPERATOR_AGGREGATE,
    UD_OPERATOR_JOIN,
    UD_OPERATOR_WINDOW,
    UD_OPERATOR_MERGE,
    UD_OPERATOR_MERGE_JOIN,
    UD_OPERATOR_PIVOT,
    UD_OPERATOR_UNPIVOT,
    UD_OPERATOR_ROW_SAMPLING,
    UD_OPERATOR_PERCENTAGE_SAMPLING,
    UD_OPERATOR_SCD,
    UD_OPERATOR_DEDUPLICATE, UD_OPERATOR_TOP_N, UD_OPERATOR_RANK,
    UD_OPERATOR_SURROGATE_KEY, UD_OPERATOR_ALTER_ROW, UD_OPERATOR_FLATTEN,
    UD_OPERATOR_EXPLODE, UD_OPERATOR_PARSE_JSON, UD_OPERATOR_JSON_PATH,
    UD_OPERATOR_STANDARDIZE, UD_OPERATOR_REGEX_EXTRACT, UD_OPERATOR_REGEX_REPLACE,
    UD_OPERATOR_TERM_EXTRACT, UD_OPERATOR_TERM_LOOKUP, UD_OPERATOR_DATA_PROFILE,
    UD_OPERATOR_MULTIWAY_JOIN, UD_OPERATOR_ASOF_JOIN, UD_OPERATOR_TEMPORAL_JOIN,
    UD_OPERATOR_DIFF_COMPARE, UD_OPERATOR_FUZZY_LOOKUP, UD_OPERATOR_FUZZY_GROUPING
};
typedef struct ud_sort_key {
    ud_id field;
    int32_t descending, nulls_first;
} ud_sort_key;
typedef struct ud_sort_contract {
    uint32_t count, stable;
    ud_sort_key keys[16]; /* Canonical typed comparison; UTF8 compares unsigned bytes. */
} ud_sort_contract;
typedef struct ud_cache ud_cache;
enum { UD_WINDOW_ROW_NUMBER = 1, UD_WINDOW_RUNNING_SUM, UD_WINDOW_RANK,
       UD_WINDOW_DENSE_RANK, UD_WINDOW_LAG, UD_WINDOW_LEAD, UD_WINDOW_FIRST,
       UD_WINDOW_LAST, UD_WINDOW_MOVING_SUM, UD_WINDOW_MOVING_AVERAGE };
enum { UD_FRAME_DEFAULT = 0, UD_FRAME_ROWS = 1, UD_FRAME_RANGE = 2 };
typedef struct ud_operator_options {
    uint32_t struct_size, abi_version;
    int32_t kind;
    uint32_t max_batch_rows;
    const uint32_t *keys;
    uint32_t key_count;
    const uint32_t *right_keys; /* Same key_count as the left keys. */
    int32_t descending;
    ud_plan *aggregation;       /* Retained immutable plan, AGGREGATE only. */
    ud_buffer_manager *buffers; /* Borrowed until operator destruction. */
    ud_status (*check)(void *);
    void *user;
    uint32_t partition_key_count, window_column;
    int32_t window_function;
    ud_string window_name;
    uint32_t partition_count; /* 0 selects 1; <=64, same algorithms at every width. */
    ud_executor *executor;    /* Borrowed root pool; required above one partition. */
    ud_string configuration;  /* Native binding for relational, sampling and SCD operators. */
    const ud_sort_contract *input_order; /* One contract per input; copied on binding. */
    ud_cache *reference; /* Sealed SCD current-dimension asset, retained by operator. */
    uint32_t window_offset;
    int32_t frame_kind, average_rounding;
    uint64_t frame_preceding, frame_following;
    uint64_t estimated_rows[64], memory_budget, spill_budget;
    uint32_t input_count;
    const ud_expression_options *expression_bindings;
    ud_error_context errors;
} ud_operator_options;
typedef struct ud_physical_plan_info {
    uint32_t struct_size, abi_version, algorithm, input_count, output_count, partitions;
    uint64_t estimated_rows, row_width, memory_budget, spill_budget, estimated_cost;
    ud_sort_contract sort;
    ud_string decision; /* Borrowed immutable diagnostic text. */
} ud_physical_plan_info;
UD_API ud_status ud_operator_physical_plan(const ud_operator *, ud_physical_plan_info *);
UD_API ud_status ud_operator_configuration_schema(int32_t kind, ud_string *);
/* Pure binding copies schemas. Separate instances may share Buffer Manager. */
UD_API ud_status ud_operator_create(ud_context *, const ud_operator_options *,
                                    const ud_batch *const *, ud_operator **);
UD_API uint32_t ud_operator_partition_count(const ud_operator *);
UD_API ud_status ud_operator_schema(const ud_operator *, uint32_t output, ud_batch **);
UD_API ud_status ud_operator_order(const ud_operator *, ud_sort_contract *);
/* A demand bit admits another DATA/END event on that input. This is part of the
 * same operator protocol, not a second scheduler or an unbounded input queue. */
UD_API uint64_t ud_operator_input_demand(const ud_operator *);
UD_API ud_status ud_operator_diagnostics(const ud_operator *, char **, uint32_t *);
/* push borrows sealed input; success owns retained/spilled blocks. End is once
 * per port. pull returns owned bounded batches, END only after all inputs end.
 * One controller per instance. Full-result materialization is not required. */
UD_API ud_status ud_operator_push(ud_operator *, uint32_t port, const ud_batch *);
UD_API ud_status ud_operator_finish_input(ud_operator *, uint32_t port);
UD_API ud_status ud_operator_pull(ud_operator *, ud_batch **, uint32_t *output);
UD_API ud_status ud_operator_destroy(ud_operator *);
#ifdef __cplusplus
}
#endif
#endif
