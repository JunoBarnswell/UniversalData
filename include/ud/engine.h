#ifndef UD_ENGINE_H
#define UD_ENGINE_H
#include "data.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_queue ud_queue;
/* Single-threaded pull runtime. Push transfers ownership ONLY on success.
 * Full queues return BUSY (backpressure), empty queues return END. */
UD_API ud_status ud_queue_create(ud_context *, uint32_t capacity, ud_queue **);
UD_API ud_status ud_queue_push(ud_queue *, ud_batch *);
UD_API ud_status ud_queue_pop(ud_queue *, ud_batch **);
UD_API void ud_queue_destroy(ud_queue *);
UD_API ud_status ud_wait(ud_context *, uint32_t milliseconds);
#ifdef __cplusplus
}
#endif
#endif
