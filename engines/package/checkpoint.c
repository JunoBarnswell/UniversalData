#include "internal.h"
/* Durable recovery is capability-gated until the unified journal milestone.
 * Reject before executing tasks; never silently treat resume as a fresh run. */
ud_status ud_checkpoint_open(ud_execution *e) {
    return e->options.state_directory.length || e->options.resume ? UD_UNSUPPORTED : UD_OK;
}
ud_status ud_checkpoint_task(ud_execution *e, ud_string identity, int *completed) {
    (void)e;
    (void)identity;
    if (completed)
        *completed = 0;
    return UD_UNSUPPORTED;
}
void ud_checkpoint_close(ud_execution *e) {
    (void)e;
}
