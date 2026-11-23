#ifndef UD_INTERNAL_PARQUET_SCAN_H
#define UD_INTERNAL_PARQUET_SCAN_H
#include "ud/columnar_file.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Read only the flat columns in expected. Date32 bounds prune row groups
 * whose calendarMonth statistics exclude the whole group. The query's
 * normal row predicate still applies to each surviving group. */
ud_status ud_parquet_scan_open(ud_context *context, const char *path, const ud_batch *expected,
                               const ud_columnar_file_options *options, int32_t month_from,
                               int32_t month_before, ud_columnar_reader **out);

#ifdef __cplusplus
}
#endif
#endif
