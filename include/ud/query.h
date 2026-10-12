#ifndef UD_QUERY_H
#define UD_QUERY_H
#include "compute.h"
#ifdef __cplusplus
extern "C" {
#endif
UD_API ud_status ud_query_execute(const ud_plan *, const ud_batch *, ud_batch **);
/* Bounded-memory, 8-way merge passes; temporary directory must already exist.
 * Output must not exist. Input/output may not alias. Core has no DB dependency. */
UD_API ud_status ud_query_sort_csv(ud_context *, const char *input, const char *output,
                                   const char *temporary_directory, const ud_plan *, uint32_t key,
                                   int32_t descending, uint32_t run_rows);
typedef struct ud_external_options {
    uint32_t struct_size, version, block_rows, max_record_bytes;
    uint64_t spill_limit; /* Logical bytes written to SDK-owned temporary files. */
    ud_string temporary_directory;
} ud_external_options;
/* Typed NDJSON spill operators. Input fields are explicit; output is staged and
 * atomically published to a new path. Temporary directory must exist. Allocation
 * quota applies to all SDK state. External aggregation adaptively repartitions
 * hash buckets up to depth 8 when group state cannot fit. Join uses hash buckets
 * and bounded block nested loops to handle skew without buffering a whole group.
 * No order is promised. Existing destination / spill files are never replaced. */
UD_API ud_status ud_query_aggregate_ndjson(ud_context *, ud_string input, ud_string output,
                                           const ud_plan *, const ud_external_options *);
UD_API ud_status ud_query_join_ndjson(ud_context *, ud_string left, const ud_batch *left_schema,
                                      uint32_t left_key, ud_string right, const ud_batch *right_schema,
                                      uint32_t right_key, ud_string output, const ud_external_options *);
#ifdef __cplusplus
}
#endif
#endif
