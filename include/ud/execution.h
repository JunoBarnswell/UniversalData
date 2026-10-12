#ifndef UD_EXECUTION_H
#define UD_EXECUTION_H
#include "memory.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef int32_t ud_execution_state;
enum {
    UD_EXEC_CREATED,
    UD_EXEC_VALIDATING,
    UD_EXEC_READY,
    UD_EXEC_RUNNING,
    UD_EXEC_DRAINING,
    UD_EXEC_SUCCEEDED,
    UD_EXEC_FAILED,
    UD_EXEC_CANCELLED,
    UD_EXEC_SKIPPED,
    UD_EXEC_RECOVERY_REQUIRED
};
UD_API int32_t ud_execution_terminal(ud_execution_state);
UD_API int32_t ud_execution_transition_allowed(ud_execution_state from, ud_execution_state to);
UD_API const char *ud_execution_state_name(ud_execution_state);
#ifdef __cplusplus
}
#endif
#endif
