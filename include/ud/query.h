#ifndef UD_QUERY_H
#define UD_QUERY_H
#include "compute.h"
#ifdef __cplusplus
extern "C" {
#endif
UD_API ud_status ud_query_execute(const ud_plan *, const ud_batch *, ud_batch **);
/* Stable sort, NULLS FIRST ascending / NULLS LAST descending. */
UD_API ud_status ud_query_sort(ud_context *, const ud_batch *, uint32_t key, int32_t descending,
                               ud_batch **);
UD_API ud_status ud_query_slice(ud_context *, const ud_batch *, uint64_t offset, uint64_t limit,
                                ud_batch **);
/* Same key type required; NULL never joins NULL. Names must be unique across inputs. */
UD_API ud_status ud_query_inner_join(ud_context *, const ud_batch *left, uint32_t left_key,
                                     const ud_batch *right, uint32_t right_key, ud_batch **);
/* Bounded-memory, 8-way merge passes; temporary directory must already exist.
 * Output must not exist. Input/output may not alias. Core has no DB dependency. */
UD_API ud_status ud_query_sort_csv(ud_context *, const char *input, const char *output,
                                   const char *temporary_directory, const ud_plan *, uint32_t key,
                                   int32_t descending, uint32_t run_rows);
#ifdef __cplusplus
}
#endif
#endif
