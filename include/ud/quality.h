#ifndef UD_QUALITY_H
#define UD_QUALITY_H
#include "data.h"
#include "buffer.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_quality ud_quality;
enum {
    UD_RULE_NOT_NULL = 1,
    UD_RULE_UNIQUE = 2,
    UD_RULE_RANGE = 3,
    UD_RULE_PRIMARY_KEY = 4,
    UD_RULE_FOREIGN_KEY = 5,
    UD_RULE_REGEX = 6,
    UD_RULE_ROW_COUNT = 7
};
enum { UD_ERROR_INHERIT = -1 };
typedef ud_status (*ud_regex_match_fn)(void *user, const char *pattern, uint32_t pattern_length,
                                       const char *text, uint32_t length, int32_t *matched);
typedef struct ud_quality_rule {
    uint32_t struct_size, abi_version, rule_version;
    int32_t policy;
    const char *id;
    uint32_t id_length;
    int32_t kind;
    const uint32_t *columns;
    uint32_t column_count;
    ud_value minimum, maximum; /* Inclusive; NULL bound is unbounded. */
    const ud_batch *reference;
    const uint32_t *reference_columns;
    const char *pattern;
    uint32_t pattern_length;
    void *match_user;
    ud_regex_match_fn match; /* NULL uses SDK's bounded UTF8 PCRE2 engine. */
    uint64_t expected_rows;
} ud_quality_rule;
typedef struct ud_quality_violation {
    uint32_t input_row, rejected_row, rule_index;
    int32_t policy;
    ud_id column_id;
} ud_quality_violation;
/* Classification counters are not source acknowledgments or target commits. */
typedef struct ud_quality_metrics {
    uint64_t processed, accepted, rejected, violations, ignored;
} ud_quality_metrics;
typedef struct ud_quality_options {
    uint32_t struct_size, abi_version, max_batch_rows, max_violations;
    int32_t policy,record_state;
    ud_buffer_manager *buffers;
    ud_status (*check)(void *);
    void *user;
} ud_quality_options;
typedef struct ud_quality_rule_metrics {
    uint64_t evaluated, failed, rejected, ignored, reference_loaded, reference_hits,
        reference_misses, reference_duplicates, reference_pages;
} ud_quality_rule_metrics;
UD_API ud_status ud_quality_reference_push(ud_quality *, const ud_batch *);
UD_API ud_status ud_quality_reference_end(ud_quality *);
UD_API ud_status ud_quality_rule_statistics(const ud_quality *, uint32_t,
                                            ud_quality_rule_metrics *);
UD_API ud_status ud_quality_create(ud_context *, const ud_field *, uint32_t columns,
                                   const ud_quality_rule *, uint32_t rule_count,
                                   const ud_quality_options *, ud_quality **);
/* State spans all successful apply calls. Unique ignores keys containing NULL;
 * primary keys reject NULL. Foreign keys use MATCH SIMPLE semantics.
 * Supply all three rejection outputs together, or NULL for deliberate drop.
 * Results/violation array are owned; classification never acknowledges sources. */
/* Accepted port narrows columns proven non-NULL by Fail/Redirect rules. */
UD_API ud_status ud_quality_schema(const ud_quality *, ud_batch **);
UD_API ud_status ud_quality_apply(ud_quality *, const ud_batch *, ud_batch **accepted,
                                  ud_batch **rejected, ud_quality_violation **, uint32_t *);
UD_API ud_status ud_quality_checkpoint(ud_quality *,ud_batch **);
UD_API ud_status ud_quality_restore(ud_quality **,const ud_batch *);
UD_API ud_status ud_quality_finish(ud_quality *);
UD_API void ud_quality_get_metrics(const ud_quality *, ud_quality_metrics *);
UD_API void ud_quality_destroy(ud_quality *);
#ifdef __cplusplus
}
#endif
#endif
