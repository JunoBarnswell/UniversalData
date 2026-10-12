#ifndef UD_ENGINE_H
#define UD_ENGINE_H
#include "buffer.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_queue ud_queue;
/* Bounded concurrent queue of owned Block Store handles. Push transfers
 * ownership ONLY on success; pop transfers it to the caller. A popped block
 * must be released after all its read views. Empty returns END, full BUSY. */
UD_API ud_status ud_queue_create(ud_context *, uint32_t capacity, ud_queue **);
UD_API ud_status ud_queue_push(ud_queue *, ud_buffer_block *);
UD_API ud_status ud_queue_pop(ud_queue *, ud_buffer_block **);
UD_API uint32_t ud_queue_count(const ud_queue *);
/* Call only after producers/consumers have joined. */
UD_API ud_status ud_queue_destroy(ud_queue *);
UD_API ud_status ud_wait(ud_context *, uint32_t milliseconds);
#ifdef __cplusplus
}
#endif
#endif
