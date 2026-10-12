#ifndef UD_THREAD_H
#define UD_THREAD_H
#include "internal.h"
#ifdef _WIN32
#include <windows.h>
typedef SRWLOCK ud_mutex;
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
#else
#include <pthread.h>
typedef pthread_mutex_t ud_mutex;
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
#endif
#endif
