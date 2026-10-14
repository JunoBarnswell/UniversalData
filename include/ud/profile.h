#ifndef UD_PROFILE_H
#define UD_PROFILE_H
#include "data.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_profile ud_profile;
enum { UD_PROFILE_ROWS = 1, UD_PROFILE_NULLS, UD_PROFILE_MINIMUM, UD_PROFILE_MAXIMUM, UD_PROFILE_DISTINCT,
    UD_PROFILE_TOPK, UD_PROFILE_HISTOGRAM, UD_PROFILE_LENGTH_DISTRIBUTION,
    UD_PROFILE_APPROXIMATE_CARDINALITY, UD_PROFILE_ANOMALIES, UD_PROFILE_REPORT };
typedef struct ud_profile_statistic {
    uint32_t struct_size, abi_version;
    int32_t kind;
    uint32_t column; /* Ignored for row count. */
} ud_profile_statistic;
typedef ud_status (*ud_profile_check_fn)(void *);
UD_API ud_status ud_profile_create(ud_context *, const ud_batch *schema,
                                   const ud_profile_statistic *, uint32_t count,
                                   uint64_t maximum_rows, ud_profile_check_fn, void *,
                                   ud_profile **);
/* Streaming shared Columnar comparison, without retaining input batches.
 * A failed add poisons the instance; partial results are never available. */
UD_API ud_status ud_profile_add(ud_profile *, const ud_batch *);
/* Borrowed until next add/destroy. Empty/all-null extrema are typed NULL;
 * counts are int64 including zero. Report statistics return canonical JSON as UTF8,
 * with explicit method/error metadata. Context cancellation is always checked. */
UD_API ud_status ud_profile_get(ud_profile *, uint32_t statistic, ud_value *);
/* Bounded Native report: HLL (RSE 0.0325), logarithmic histograms,
 * SpaceSaving TopK with per-entry error, lengths and stable anomaly RowIds. */
UD_API ud_status ud_profile_report(const ud_profile *, uint32_t column, char **, uint32_t *);
UD_API void ud_profile_destroy(ud_profile *);
#ifdef __cplusplus
}
#endif
#endif
