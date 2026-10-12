#include "../executor.h"
#include "../../foundation/thread.h"
#include <stdatomic.h>
#ifdef _WIN32
#include <process.h>
#endif

enum { JOB_READY, JOB_RUNNING, JOB_WAITING, JOB_DONE };
struct ud_executor {
    ud_context *ctx;
    uint32_t limit;
    atomic_uint workers, high_water, groups;
};
typedef struct work_group {
    ud_executor *executor;
    ud_work_quantum quantum;
    void *user;
    uint8_t *states;
    uint32_t count, done, active, next;
    ud_status result;
    int started;
    ud_mutex mutex;
    ud_condition changed;
} work_group;
typedef struct worker {
    work_group *group;
#ifdef _WIN32
    HANDLE thread;
#else
    pthread_t thread;
#endif
} worker;
static void high_water(ud_executor *executor, unsigned count) {
    unsigned previous = atomic_load(&executor->high_water);
    while (count > previous &&
           !atomic_compare_exchange_weak(&executor->high_water, &previous, count)) {}
}
ud_status ud_executor_create(ud_context *ctx, uint32_t concurrency, ud_executor **out) {
    if (!ctx || !out || !concurrency || concurrency > 64)
        return UD_INVALID;
    *out = ud_alloc(ctx, sizeof(**out));
    if (!*out)
        return UD_RESOURCE;
    (*out)->ctx = ctx;
    (*out)->limit = concurrency;
    atomic_init(&(*out)->workers, 0);
    atomic_init(&(*out)->high_water, 0);
    atomic_init(&(*out)->groups, 0);
    return UD_OK;
}
void ud_executor_enter(ud_executor *executor) {
    high_water(executor, atomic_fetch_add(&executor->workers, 1) + 1);
}
void ud_executor_leave(ud_executor *executor) {
    atomic_fetch_sub(&executor->workers, 1);
}
static int reserve(ud_executor *executor) {
    unsigned workers = atomic_load(&executor->workers);
    while (workers < executor->limit)
        if (atomic_compare_exchange_weak(&executor->workers, &workers, workers + 1)) {
            high_water(executor, workers + 1);
            return 1;
        }
    return 0;
}
static void execute_group(work_group *group) {
    ud_mutex_lock(&group->mutex);
    while (!group->started && !group->result)
        ud_condition_wait(&group->changed, &group->mutex);
    while (!group->result && group->done < group->count) {
        uint32_t selected = UINT32_MAX;
        for (uint32_t offset = 0; offset < group->count; offset++) {
            uint32_t index = (group->next + offset) % group->count;
            if (group->states[index] == JOB_READY) {
                selected = index;
                break;
            }
        }
        if (selected == UINT32_MAX) {
            if (!group->active) {
                group->result = UD_CORRUPT; /* No external producer; true plan deadlock. */
                ud_condition_wake(&group->changed);
                break;
            }
            ud_condition_wait(&group->changed, &group->mutex);
            continue;
        }
        group->next = (selected + 1) % group->count;
        group->states[selected] = JOB_RUNNING;
        group->active++;
        ud_mutex_unlock(&group->mutex);
        ud_status status = group->quantum(group->user, selected);
        ud_mutex_lock(&group->mutex);
        group->active--;
        if (status == UD_OK) {
            group->states[selected] = JOB_DONE;
            group->done++;
        } else if (status == UD_MORE)
            group->states[selected] = JOB_READY;
        else if (status == UD_BUSY)
            group->states[selected] = JOB_WAITING;
        else if (!group->result)
            group->result = status;
        if (status == UD_OK || status == UD_MORE)
            for (uint32_t i = 0; i < group->count; i++)
                if (group->states[i] == JOB_WAITING)
                    group->states[i] = JOB_READY;
        ud_condition_wake(&group->changed);
    }
    ud_mutex_unlock(&group->mutex);
}
#ifdef _WIN32
static unsigned __stdcall worker_main(void *user) {
#else
static void *worker_main(void *user) {
#endif
    worker *work = user;
    execute_group(work->group);
    ud_executor_leave(work->group->executor);
#ifdef _WIN32
    return 0;
#else
    return NULL;
#endif
}
ud_status ud_executor_run(ud_executor *executor, uint32_t count, uint32_t concurrency,
                         ud_work_quantum quantum, void *user) {
    if (!executor || !count || !quantum || !concurrency)
        return UD_INVALID;
    if (concurrency > executor->limit)
        concurrency = executor->limit;
    work_group group = {.executor = executor, .quantum = quantum, .user = user, .count = count};
    ud_mutex_init(&group.mutex);
    ud_condition_init(&group.changed);
    group.states = ud_alloc(executor->ctx, count);
    uint32_t maximum = count < concurrency ? count : concurrency;
    worker *workers = ud_alloc(executor->ctx, (size_t)maximum * sizeof(*workers));
    if (!group.states || !workers)
        group.result = UD_RESOURCE;
    atomic_fetch_add(&executor->groups, 1);
    uint32_t started = 0;
    for (uint32_t i = 1; !group.result && i < maximum; i++) {
        if (!reserve(executor))
            break;
        worker *work = workers + started;
        work->group = &group;
#ifdef _WIN32
        work->thread = (HANDLE)_beginthreadex(NULL, 0, worker_main, work, 0, NULL);
        int failed = !work->thread;
#else
        int failed = pthread_create(&work->thread, NULL, worker_main, work) != 0;
#endif
        if (failed) {
            ud_executor_leave(executor);
            ud_mutex_lock(&group.mutex);
            group.result = UD_RESOURCE;
            ud_mutex_unlock(&group.mutex);
        } else
            started++;
    }
    ud_mutex_lock(&group.mutex);
    group.started = 1;
    ud_condition_wake(&group.changed);
    ud_mutex_unlock(&group.mutex);
    execute_group(&group);
    for (uint32_t i = 0; i < started; i++) {
#ifdef _WIN32
        if (WaitForSingleObject(workers[i].thread, INFINITE) != WAIT_OBJECT_0) {
            ud_mutex_lock(&group.mutex);
            group.result = UD_IO;
            ud_mutex_unlock(&group.mutex);
        }
        CloseHandle(workers[i].thread);
#else
        if (pthread_join(workers[i].thread, NULL)) {
            ud_mutex_lock(&group.mutex);
            group.result = UD_IO;
            ud_mutex_unlock(&group.mutex);
        }
#endif
    }
    atomic_fetch_sub(&executor->groups, 1);
    ud_free(group.states);
    ud_free(workers);
    ud_condition_destroy(&group.changed);
    ud_mutex_destroy(&group.mutex);
    return group.result;
}
uint32_t ud_executor_high_water(const ud_executor *executor) {
    return executor ? atomic_load(&executor->high_water) : 0;
}
ud_status ud_executor_destroy(ud_executor *executor) {
    if (!executor)
        return UD_OK;
    if (atomic_load(&executor->workers) || atomic_load(&executor->groups))
        return UD_BUSY;
    ud_free(executor);
    return UD_OK;
}
