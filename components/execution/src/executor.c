#include "../executor.h"
#include "../../foundation/thread.h"
#include <stdatomic.h>
#ifdef _WIN32
#include <process.h>
#endif

enum { JOB_WAITING, JOB_READY, JOB_RUNNING, JOB_DONE };
typedef struct work_item {
    uint32_t next;
    uint8_t state, signalled;
} work_item;
typedef struct worker {
    struct ud_executor *executor;
#ifdef _WIN32
    HANDLE thread;
#else
    pthread_t thread;
#endif
} worker;
struct ud_work_group {
    ud_executor *executor;
    ud_work_quantum quantum;
    void *user;
    work_item *items;
    uint32_t count, done, active, concurrency, head, tail;
    ud_status result;
    int published, queued, joined;
    struct ud_work_group *next_ready;
};
struct ud_executor {
    ud_context *ctx;
    uint32_t limit, started, groups, busy, high_water;
    int running, closing;
    ud_mutex mutex;
    ud_condition changed;
    worker *workers;
    ud_work_group *head, *tail;
};
typedef struct work_frame {
    ud_executor *executor;
    struct work_frame *parent;
} work_frame;
static _Thread_local work_frame *current_frame;

/* Every queue operation shares this lock. No callback runs under it. */
static void publish_ready(ud_work_group *g) {
    ud_executor *e = g->executor;
    if (g->published && !g->result && g->concurrency > 1 && !g->queued &&
        g->head != UINT32_MAX && g->active < g->concurrency) {
        g->queued = 1;
        g->next_ready = NULL;
        if (e->tail) e->tail->next_ready = g;
        else e->head = g;
        e->tail = g;
    }
}
static void remove_ready(ud_work_group *g) {
    if (!g->queued) return;
    ud_executor *e = g->executor;
    ud_work_group *previous = NULL, *at = e->head;
    while (at && at != g) { previous = at; at = at->next_ready; }
    if (!at) return;
    if (previous) previous->next_ready = at->next_ready;
    else e->head = at->next_ready;
    if (e->tail == at) e->tail = previous;
    g->queued = 0;
    g->next_ready = NULL;
}
static uint32_t take(ud_work_group *g) {
    if (!g->published || g->result || g->head == UINT32_MAX || g->active >= g->concurrency)
        return UINT32_MAX;
    uint32_t index = g->head;
    work_item *item = g->items + index;
    g->head = item->next;
    if (g->head == UINT32_MAX) g->tail = UINT32_MAX;
    item->next = UINT32_MAX;
    item->state = JOB_RUNNING;
    item->signalled = 0;
    g->active++;
    remove_ready(g);
    publish_ready(g);
    return index;
}
static void enqueue(ud_work_group *g, uint32_t index) {
    work_item *item = g->items + index;
    item->state = JOB_READY;
    item->next = UINT32_MAX;
    if (g->tail != UINT32_MAX) g->items[g->tail].next = index;
    else g->head = index;
    g->tail = index;
    publish_ready(g);
}
static void run_work(ud_work_group *g, uint32_t index) {
    ud_executor *e = g->executor;
    int outer = !current_frame || current_frame->executor != e;
    work_frame frame = {e, current_frame};
    if (outer) {
        e->busy++;
        if (e->busy > e->high_water) e->high_water = e->busy;
    }
    ud_mutex_unlock(&e->mutex);
    current_frame = &frame;
    ud_status status = ud_is_cancelled(e->ctx) ? UD_CANCELLED : g->quantum(g->user, index);
    current_frame = frame.parent;
    ud_mutex_lock(&e->mutex);
    if (outer) e->busy--;
    g->active--;
    work_item *item = g->items + index;
    if (status == UD_OK) {
        item->state = JOB_DONE;
        g->done++;
    } else if (status == UD_MORE || (status == UD_BUSY && item->signalled))
        enqueue(g, index);
    else if (status == UD_BUSY)
        item->state = JOB_WAITING;
    else if (!g->result)
        g->result = status;
    if (g->result) remove_ready(g);
    else publish_ready(g);
    ud_condition_wake(&e->changed);
}
#ifdef _WIN32
static unsigned __stdcall worker_main(void *user) {
#else
static void *worker_main(void *user) {
#endif
    worker *w = user;
    ud_executor *e = w->executor;
    ud_mutex_lock(&e->mutex);
    while (!e->closing) {
        if (!e->head) {
            ud_condition_wait(&e->changed, &e->mutex);
            continue;
        }
        ud_work_group *g = e->head;
        uint32_t index = take(g);
        if (index == UINT32_MAX) { remove_ready(g); continue; }
        run_work(g, index);
    }
    ud_mutex_unlock(&e->mutex);
#ifdef _WIN32
    return 0;
#else
    return NULL;
#endif
}
ud_status ud_executor_create(ud_context *ctx, uint32_t concurrency, ud_executor **out) {
    if (!ctx || !out || !concurrency || concurrency > 64) return UD_INVALID;
    *out = NULL;
    ud_executor *e = ud_alloc(ctx, sizeof(*e));
    if (!e) return UD_RESOURCE;
    e->ctx = ctx;
    e->limit = concurrency;
    ud_mutex_init(&e->mutex);
    ud_condition_init(&e->changed);
    e->workers = ud_alloc(ctx, (size_t)concurrency * sizeof(*e->workers));
    if (!e->workers) {
        ud_condition_destroy(&e->changed);
        ud_mutex_destroy(&e->mutex);
        ud_free(e);
        return UD_RESOURCE;
    }
    *out = e;
    return UD_OK;
}
ud_status ud_executor_start(ud_executor *e) {
    if (!e) return UD_INVALID;
    if (e->closing) return UD_INVALID;
    e->running = 1;
    return UD_OK;
}
static ud_status ensure_workers(ud_executor *e,uint32_t concurrency) {
    if (concurrency>e->limit) concurrency=e->limit;
    ud_status status=UD_OK;
    ud_mutex_lock(&e->mutex);
    while (!status && e->started+1<concurrency) {
        worker *w=e->workers+e->started;w->executor=e;
#ifdef _WIN32
        w->thread=(HANDLE)_beginthreadex(NULL,0,worker_main,w,0,NULL);
        int failed=!w->thread;
#else
        int failed=pthread_create(&w->thread,NULL,worker_main,w)!=0;
#endif
        if (failed) status=UD_RESOURCE;else e->started++;
    }
    ud_mutex_unlock(&e->mutex);
    return status;
}
ud_status ud_work_group_create(ud_executor *e, uint32_t count, uint32_t concurrency,
                              ud_work_quantum quantum, void *user, ud_work_group **out) {
    if (!e || !out || !count || !concurrency || !quantum) return UD_INVALID;
    *out = NULL;
    ud_work_group *g = ud_alloc(e->ctx, sizeof(*g));
    if (!g) return UD_RESOURCE;
    g->executor = e;
    g->count = count;
    g->concurrency = concurrency < e->limit ? concurrency : e->limit;
    g->quantum = quantum;
    g->user = user;
    g->head = g->tail = UINT32_MAX;
    g->items = ud_alloc(e->ctx, (size_t)count * sizeof(*g->items));
    if (!g->items) { ud_free(g); return UD_RESOURCE; }
    ud_mutex_lock(&e->mutex);
    e->groups++;
    ud_mutex_unlock(&e->mutex);
    *out = g;
    return UD_OK;
}
void ud_work_group_ready(ud_work_group *g, uint32_t index) {
    if (!g || index >= g->count) return;
    ud_executor *e = g->executor;
    ud_mutex_lock(&e->mutex);
    if (!g->result && !g->joined) {
        work_item *item = g->items + index;
        if (item->state == JOB_RUNNING) item->signalled = 1;
        else if (item->state == JOB_WAITING) enqueue(g, index);
        ud_condition_wake(&e->changed);
    }
    ud_mutex_unlock(&e->mutex);
}
ud_status ud_work_group_wait(ud_work_group *g) {
    if (!g) return UD_INVALID;
    ud_executor *e = g->executor;
    if (!e->running || e->closing) return UD_INVALID;
    uint32_t width=g->count<g->concurrency ? g->count : g->concurrency;
    UD_TRY(ensure_workers(e,width));
    ud_mutex_lock(&e->mutex);
    if (g->published || g->joined) { ud_mutex_unlock(&e->mutex); return UD_INVALID; }
    g->published = 1;
    publish_ready(g);
    ud_condition_wake(&e->changed);
    for (;;) {
        if (ud_is_cancelled(e->ctx) && !g->result) {
            g->result = UD_CANCELLED;
            remove_ready(g);
        }
        if ((g->result || g->done == g->count) && !g->active) break;
        uint32_t index = take(g);
        if (index != UINT32_MAX) { run_work(g, index); continue; }
        if (!g->active && !g->result) {
            g->result = UD_CORRUPT; /* All producers are in this finite group. */
            remove_ready(g);
            continue;
        }
        ud_condition_wait(&e->changed, &e->mutex);
    }
    remove_ready(g);
    g->published = 0;
    g->joined = 1;
    ud_status status = g->result;
    ud_mutex_unlock(&e->mutex);
    return status;
}
ud_status ud_work_group_destroy(ud_work_group *g) {
    if (!g) return UD_OK;
    ud_executor *e = g->executor;
    ud_mutex_lock(&e->mutex);
    if (g->published || g->active || g->queued) {
        ud_mutex_unlock(&e->mutex);
        return UD_BUSY;
    }
    e->groups--;
    ud_mutex_unlock(&e->mutex);
    ud_free(g->items);
    ud_free(g);
    return UD_OK;
}
void ud_executor_cancel(ud_executor *e) {
    if (!e) return;
    ud_mutex_lock(&e->mutex);
    ud_condition_wake(&e->changed);
    ud_mutex_unlock(&e->mutex);
}
uint32_t ud_executor_high_water(const ud_executor *executor) {
    if (!executor) return 0;
    ud_executor *e = (ud_executor *)executor;
    ud_mutex_lock(&e->mutex);
    uint32_t result = e->high_water;
    ud_mutex_unlock(&e->mutex);
    return result;
}
ud_status ud_executor_destroy(ud_executor *e) {
    if (!e) return UD_OK;
    ud_mutex_lock(&e->mutex);
    if (e->groups || e->busy) { ud_mutex_unlock(&e->mutex); return UD_BUSY; }
    e->closing = 1;
    ud_condition_wake(&e->changed);
    ud_mutex_unlock(&e->mutex);
    for (uint32_t i = 0; i < e->started; i++) {
#ifdef _WIN32
        if (WaitForSingleObject(e->workers[i].thread, INFINITE) != WAIT_OBJECT_0) return UD_IO;
        CloseHandle(e->workers[i].thread);
#else
        if (pthread_join(e->workers[i].thread, NULL)) return UD_IO;
#endif
    }
    ud_free(e->workers);
    ud_condition_destroy(&e->changed);
    ud_mutex_destroy(&e->mutex);
    ud_free(e);
    return UD_OK;
}
