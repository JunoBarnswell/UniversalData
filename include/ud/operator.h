#ifndef UD_OPERATOR_H
#define UD_OPERATOR_H
#include "compute.h"
#include "buffer.h"
#include "partition.h"
typedef struct ud_executor ud_executor;
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_operator ud_operator;
enum { UD_OPERATOR_SORT = 1, UD_OPERATOR_AGGREGATE, UD_OPERATOR_JOIN, UD_OPERATOR_WINDOW,
       UD_OPERATOR_MERGE, UD_OPERATOR_MERGE_JOIN, UD_OPERATOR_PIVOT, UD_OPERATOR_UNPIVOT,
       UD_OPERATOR_ROW_SAMPLING, UD_OPERATOR_PERCENTAGE_SAMPLING, UD_OPERATOR_SCD };
typedef struct ud_sort_key {
    ud_id field;
    int32_t descending, nulls_first;
} ud_sort_key;
typedef struct ud_sort_contract {
    uint32_t count, stable;
    ud_sort_key keys[16]; /* Canonical typed comparison; UTF8 compares unsigned bytes. */
} ud_sort_contract;
typedef struct ud_cache ud_cache;
enum { UD_WINDOW_ROW_NUMBER = 1, UD_WINDOW_RUNNING_SUM };
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
    ud_string configuration; /* Native binding for relational, sampling and SCD operators. */
    const ud_sort_contract *input_order; /* One contract per input; copied on binding. */
    ud_cache *reference; /* Sealed SCD current-dimension asset, retained by operator. */
} ud_operator_options;
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
