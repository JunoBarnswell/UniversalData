#ifndef UD_EXECUTOR_INTERNAL_H
#define UD_EXECUTOR_INTERNAL_H
#include "../foundation/internal.h"
typedef struct ud_executor ud_executor;
typedef struct ud_work_group ud_work_group;
/* OK completes; MORE remains ready; BUSY parks until an explicit ready event.
 * Readiness received during a running quantum is retained, never lost. */
typedef ud_status (*ud_work_quantum)(void *, uint32_t);
ud_status ud_executor_create(ud_context *, uint32_t concurrency, ud_executor **);
ud_status ud_executor_start(ud_executor *);
ud_status ud_work_group_create(ud_executor *, uint32_t count, uint32_t concurrency,
                              ud_work_quantum, void *, ud_work_group **);
void ud_work_group_ready(ud_work_group *, uint32_t);
/* The caller participates, and nested joins help only their own child group.
 * A serial group stays on its caller for native instance thread affinity. */
ud_status ud_work_group_wait(ud_work_group *);
ud_status ud_work_group_destroy(ud_work_group *);
void ud_executor_cancel(ud_executor *);
uint32_t ud_executor_high_water(const ud_executor *);
ud_status ud_executor_destroy(ud_executor *);
#endif
