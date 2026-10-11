#include "ud/engine.h"
#include "../../foundation/internal.h"
struct ud_queue {
    ud_context *ctx;
    ud_batch **batches;
    uint32_t capacity, head, count;
};
ud_status ud_queue_create(ud_context *c, uint32_t cap, ud_queue **out) {
    if (!out || !cap || cap > 65536)
        return UD_INVALID;
    *out = ud_alloc(c, sizeof(**out));
    if (!*out)
        return UD_RESOURCE;
    (*out)->ctx = c;
    (*out)->capacity = cap;
    (*out)->batches = ud_alloc(c, cap * sizeof(ud_batch *));
    if (!(*out)->batches) {
        ud_free(*out);
        *out = NULL;
        return UD_RESOURCE;
    }
    return UD_OK;
}
ud_status ud_queue_push(ud_queue *q, ud_batch *b) {
    if (!q || !b)
        return UD_INVALID;
    if (ud_is_cancelled(q->ctx))
        return UD_CANCELLED;
    if (q->count == q->capacity)
        return UD_BUSY;
    q->batches[(q->head + q->count) % q->capacity] = b;
    q->count++;
    return UD_OK;
}
ud_status ud_queue_pop(ud_queue *q, ud_batch **out) {
    if (!q || !out)
        return UD_INVALID;
    *out = NULL;
    if (ud_is_cancelled(q->ctx))
        return UD_CANCELLED;
    if (!q->count)
        return UD_END;
    *out = q->batches[q->head];
    q->batches[q->head] = NULL;
    q->head = (q->head + 1) % q->capacity;
    q->count--;
    return UD_OK;
}
void ud_queue_destroy(ud_queue *q) {
    if (!q)
        return;
    for (uint32_t i = 0; i < q->count; i++)
        ud_batch_destroy(q->batches[(q->head + i) % q->capacity]);
    ud_free(q->batches);
    ud_free(q);
}
ud_status ud_wait(ud_context *c, uint32_t ms) {
    while (ms) {
        if (ud_is_cancelled(c))
            return UD_CANCELLED;
        uint32_t step = ms < 25 ? ms : 25;
        ud_sleep_ms(step);
        ms -= step;
    }
    return ud_is_cancelled(c) ? UD_CANCELLED : UD_OK;
}
