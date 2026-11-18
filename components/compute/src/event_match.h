#ifndef UD_EVENT_MATCH_INTERNAL_H
#define UD_EVENT_MATCH_INTERNAL_H
#include "state.h"
#include "ud/operator.h"
typedef struct ud_event_match ud_event_match;
ud_status ud_event_match_create(ud_context *, const ud_operator_options *,
                                const ud_batch *const *, ud_event_match **);
ud_status ud_event_match_schema(const ud_event_match *, uint32_t, ud_batch **);
ud_status ud_event_match_push(ud_event_match *, uint32_t, const ud_batch *);
ud_status ud_event_match_end(ud_event_match *, uint32_t);
ud_status ud_event_match_watermark(ud_event_match *, int64_t);
ud_status ud_event_match_pull(ud_event_match *, ud_batch **, uint32_t *);
uint64_t ud_event_match_demand(const ud_event_match *);
ud_status ud_event_match_checkpoint(ud_event_match *, ud_batch **);
ud_status ud_event_match_restore(ud_event_match **, const ud_batch *);
ud_status ud_event_match_diagnostics(const ud_event_match *, char **, uint32_t *);
void ud_event_match_destroy(ud_event_match *);
#endif
