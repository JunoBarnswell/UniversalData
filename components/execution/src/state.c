#include "ud/execution.h"
int32_t ud_execution_terminal(ud_execution_state s) {
    return s >= UD_EXEC_SUCCEEDED && s <= UD_EXEC_RECOVERY_REQUIRED;
}
int32_t ud_execution_transition_allowed(ud_execution_state from, ud_execution_state to) {
    if (from < UD_EXEC_CREATED || from > UD_EXEC_RECOVERY_REQUIRED || to < UD_EXEC_CREATED ||
        to > UD_EXEC_RECOVERY_REQUIRED || ud_execution_terminal(from))
        return 0;
    if (to == UD_EXEC_FAILED || to == UD_EXEC_CANCELLED || to == UD_EXEC_RECOVERY_REQUIRED)
        return 1;
    if (from == UD_EXEC_READY && to == UD_EXEC_SKIPPED)
        return 1;
    return to == from + 1 && to <= UD_EXEC_SUCCEEDED;
}
const char *ud_execution_state_name(ud_execution_state s) {
    static const char *names[] = {"Created",  "Validating",      "Ready",  "Running",
                                  "Draining", "Succeeded",       "Failed", "Cancelled",
                                  "Skipped",  "RecoveryRequired"};
    return s >= 0 && s <= UD_EXEC_RECOVERY_REQUIRED ? names[s] : "Invalid";
}
