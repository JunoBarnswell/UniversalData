#include "ud/engine.h"
#include "../../foundation/internal.h"
#include "../../foundation/thread.h"
struct ud_queue {
    ud_context *ctx;
    ud_stream_event *events;
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
    (*out)->events = ud_alloc(ctx, (size_t)capacity * sizeof(ud_stream_event));
    if (!(*out)->events) {
        ud_mutex_destroy(&(*out)->mutex);
        ud_free(*out);
        *out = NULL;
        return UD_RESOURCE;
    }
    return UD_OK;
}
ud_status ud_queue_push(ud_queue*queue,const ud_stream_event*event){
    if(!queue||!event||event->struct_size!=sizeof(*event)||event->abi_version!=UD_ABI_VERSION)return UD_INCOMPATIBLE;
    if(event->kind<UD_STREAM_DATA||event->kind>UD_STREAM_SCHEMA_CHANGE||event->kind==UD_STREAM_POLL||(event->kind==UD_STREAM_DATA&&!event->block)||(event->kind==UD_STREAM_BARRIER&&!event->epoch)||(event->source_position.length&&!event->source_position.data))return UD_INVALID;
    if(ud_is_cancelled(queue->ctx))return UD_CANCELLED;
    ud_mutex_lock(&queue->mutex);ud_status result=queue->count==queue->capacity?UD_BUSY:UD_OK;ud_stream_event copy=*event;copy.source_position.data=NULL;
    if(!result&&event->source_position.length){copy.source_position.data=ud_strdup_n(queue->ctx,event->source_position.data,event->source_position.length);if(!copy.source_position.data)result=UD_RESOURCE;}
    if(!result){queue->events[(queue->head+queue->count)%queue->capacity]=copy;queue->count++;}ud_mutex_unlock(&queue->mutex);return result;
}
ud_status ud_queue_pop(ud_queue*queue,ud_stream_event*out){if(!queue||!out)return UD_INVALID;memset(out,0,sizeof(*out));if(ud_is_cancelled(queue->ctx))return UD_CANCELLED;ud_mutex_lock(&queue->mutex);ud_status result=queue->count?UD_OK:UD_END;if(!result){*out=queue->events[queue->head];memset(queue->events+queue->head,0,sizeof(*out));queue->head=(queue->head+1)%queue->capacity;queue->count--;}ud_mutex_unlock(&queue->mutex);return result;}
ud_status ud_stream_event_clear(ud_stream_event*event){if(!event)return UD_INVALID;ud_status s=event->block?ud_buffer_release(event->block):UD_OK;if(s==UD_BUSY)return s;ud_free((void*)event->source_position.data);memset(event,0,sizeof(*event));return s;}
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
        ud_status status = ud_stream_event_clear(queue->events+queue->head);
        if (status == UD_BUSY)
            return status; /* Live reader: caller retains queue ownership. */
        if (status && !result)
            result = status;
        queue->head = (queue->head + 1) % queue->capacity;
        queue->count--;
    }
    ud_mutex_destroy(&queue->mutex);
    ud_free(queue->events);
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
