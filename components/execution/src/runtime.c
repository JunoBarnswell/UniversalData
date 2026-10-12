#include "ud/engine.h"
#include "../../foundation/internal.h"
#include "../../foundation/thread.h"
struct ud_queue {
    ud_context *ctx;
    ud_buffer_block **blocks;
    uint32_t capacity, head, count;
    ud_mutex mutex;
};
ud_status ud_queue_create(ud_context *ctx, uint32_t capacity, ud_queue **out) {
    if (!ctx || !out || !capacity || capacity > 65536)
        return UD_INVALID;
    *out = ud_alloc(ctx, sizeof(**out));
    if (!*out)
        return UD_RESOURCE;
    (*out)->ctx = ctx;
    (*out)->capacity = capacity;
    ud_mutex_init(&(*out)->mutex);
    (*out)->blocks = ud_alloc(ctx, (size_t)capacity * sizeof(ud_buffer_block *));
    if (!(*out)->blocks) {
        ud_mutex_destroy(&(*out)->mutex);
        ud_free(*out);
        *out = NULL;
        return UD_RESOURCE;
    }
    return UD_OK;
}
ud_status ud_queue_push(ud_queue *queue, ud_buffer_block *block) {
    if (!queue || !block)
        return UD_INVALID;
    if (ud_is_cancelled(queue->ctx))
        return UD_CANCELLED;
    ud_mutex_lock(&queue->mutex);
    ud_status result = queue->count == queue->capacity ? UD_BUSY : UD_OK;
    if (!result) {
        queue->blocks[(queue->head + queue->count) % queue->capacity] = block;
        queue->count++;
    }
    ud_mutex_unlock(&queue->mutex);
    return result;
}
ud_status ud_queue_pop(ud_queue *queue, ud_buffer_block **out) {
    if (!queue || !out)
        return UD_INVALID;
    *out = NULL;
    if (ud_is_cancelled(queue->ctx))
        return UD_CANCELLED;
    ud_mutex_lock(&queue->mutex);
    ud_status result = queue->count ? UD_OK : UD_END;
    if (!result) {
        *out = queue->blocks[queue->head];
        queue->blocks[queue->head] = NULL;
        queue->head = (queue->head + 1) % queue->capacity;
        queue->count--;
    }
    ud_mutex_unlock(&queue->mutex);
    return result;
}
uint32_t ud_queue_count(const ud_queue *queue) {
    if (!queue)
        return 0;
    ud_mutex_lock((ud_mutex *)&queue->mutex);
    uint32_t count = queue->count;
    ud_mutex_unlock((ud_mutex *)&queue->mutex);
    return count;
}
ud_status ud_queue_destroy(ud_queue *queue) {
    if (!queue)
        return UD_OK;
    ud_status result = UD_OK;
    while (queue->count) {
        ud_status status = ud_buffer_release(queue->blocks[queue->head]);
        if (status == UD_BUSY)
            return status; /* Live reader: caller retains queue ownership. */
        if (status && !result)
            result = status;
        queue->head = (queue->head + 1) % queue->capacity;
        queue->count--;
    }
    ud_mutex_destroy(&queue->mutex);
    ud_free(queue->blocks);
    ud_free(queue);
    return result;
}
ud_status ud_wait(ud_context *ctx, uint32_t milliseconds) {
    while (milliseconds) {
        if (ud_is_cancelled(ctx))
            return UD_CANCELLED;
        uint32_t step = milliseconds < 25 ? milliseconds : 25;
        ud_sleep_ms(step);
        milliseconds -= step;
    }
    return ud_is_cancelled(ctx) ? UD_CANCELLED : UD_OK;
}
