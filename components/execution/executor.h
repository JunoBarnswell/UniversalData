#ifndef UD_EXECUTOR_INTERNAL_H
#define UD_EXECUTOR_INTERNAL_H
#include "../foundation/internal.h"
typedef struct ud_executor ud_executor;
/* OK completes work; MORE made progress and remains runnable; BUSY waits for
 * another job's progress. Any other status fails the whole group. */
typedef ud_status (*ud_work_quantum)(void *, uint32_t);
ud_status ud_executor_create(ud_context *, uint32_t concurrency, ud_executor **);
void ud_executor_enter(ud_executor *);
void ud_executor_leave(ud_executor *);
ud_status ud_executor_run(ud_executor *, uint32_t count, uint32_t concurrency,
                         ud_work_quantum, void *);
uint32_t ud_executor_high_water(const ud_executor *);
ud_status ud_executor_destroy(ud_executor *);
#endif
