#ifndef UD_THREAD_H
#define UD_THREAD_H
#include "internal.h"
#ifdef _WIN32
#include <windows.h>
typedef SRWLOCK ud_mutex;
typedef CONDITION_VARIABLE ud_condition;
#define UD_MUTEX_INITIALIZER SRWLOCK_INIT
static inline void ud_mutex_init(ud_mutex *m) {
    InitializeSRWLock(m);
}
static inline void ud_mutex_lock(ud_mutex *m) {
    AcquireSRWLockExclusive(m);
}
static inline void ud_mutex_unlock(ud_mutex *m) {
    ReleaseSRWLockExclusive(m);
}
static inline void ud_mutex_destroy(ud_mutex *m) {
    (void)m;
}
static inline void ud_condition_init(ud_condition *condition) {
    InitializeConditionVariable(condition);
}
static inline void ud_condition_wait(ud_condition *condition, ud_mutex *mutex) {
    SleepConditionVariableSRW(condition, mutex, INFINITE, 0);
}
static inline void ud_condition_wait_ms(ud_condition *condition, ud_mutex *mutex, uint32_t ms) {
    SleepConditionVariableSRW(condition, mutex, ms, 0);
}
static inline void ud_condition_wake(ud_condition *condition) {
    WakeAllConditionVariable(condition);
}
static inline void ud_condition_destroy(ud_condition *condition) {
    (void)condition;
}
#else
#include <pthread.h>
#include <time.h>
typedef pthread_mutex_t ud_mutex;
typedef pthread_cond_t ud_condition;
#define UD_MUTEX_INITIALIZER PTHREAD_MUTEX_INITIALIZER
static inline void ud_mutex_init(ud_mutex *m) {
    pthread_mutex_init(m, NULL);
}
static inline void ud_mutex_lock(ud_mutex *m) {
    pthread_mutex_lock(m);
}
static inline void ud_mutex_unlock(ud_mutex *m) {
    pthread_mutex_unlock(m);
}
static inline void ud_mutex_destroy(ud_mutex *m) {
    pthread_mutex_destroy(m);
}
static inline void ud_condition_init(ud_condition *condition) {
    pthread_cond_init(condition, NULL);
}
static inline void ud_condition_wait(ud_condition *condition, ud_mutex *mutex) {
    pthread_cond_wait(condition, mutex);
}
static inline void ud_condition_wait_ms(ud_condition *condition, ud_mutex *mutex, uint32_t ms) {
    struct timespec until;
    clock_gettime(CLOCK_REALTIME, &until);
    until.tv_sec += ms / 1000;
    until.tv_nsec += (long)(ms % 1000) * 1000000L;
    if (until.tv_nsec >= 1000000000L) { until.tv_sec++; until.tv_nsec -= 1000000000L; }
    pthread_cond_timedwait(condition, mutex, &until);
}
static inline void ud_condition_wake(ud_condition *condition) {
    pthread_cond_broadcast(condition);
}
static inline void ud_condition_destroy(ud_condition *condition) {
    pthread_cond_destroy(condition);
}
#endif
#endif
