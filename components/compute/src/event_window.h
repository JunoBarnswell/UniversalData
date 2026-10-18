#ifndef UD_EVENT_WINDOW_INTERNAL_H
#define UD_EVENT_WINDOW_INTERNAL_H
#include "state.h"
#include "ud/operator.h"
typedef struct ud_event_window ud_event_window;
ud_status ud_event_window_create(ud_context *, const ud_operator_options *, const ud_batch *,
                                 ud_event_window **);
ud_status ud_event_window_schema(const ud_event_window *, uint32_t, ud_batch **);
ud_status ud_event_window_push(ud_event_window *, const ud_batch *);
ud_status ud_event_window_watermark(ud_event_window *, int64_t);
ud_status ud_event_window_end(ud_event_window *);
ud_status ud_event_window_pull(ud_event_window *, ud_batch **, uint32_t *);
uint64_t ud_event_window_demand(const ud_event_window *);
ud_status ud_event_window_checkpoint(ud_event_window *, ud_batch **);
ud_status ud_event_window_restore(ud_event_window **, const ud_batch *);
ud_status ud_event_window_diagnostics(const ud_event_window *, char **, uint32_t *);
void ud_event_window_destroy(ud_event_window *);
#endif
