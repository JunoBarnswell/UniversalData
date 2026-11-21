#include "../include/ud/scheduler.h"

#include <limits.h>

ud_status ud_scheduler_select_due(ud_scheduler_due_request *request) {
    if (!request)
        return UD_INVALID;
    if (request->struct_size != sizeof(*request) || request->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!request->count || request->count > 64 || !request->next_due_ms || !request->enabled ||
        request->now_ms < 0)
        return UD_INVALID;
    request->index = UINT32_MAX;
    request->due_ms = 0;
    for (uint32_t at = 0; at < request->count; at++) {
        if (request->enabled[at] != 0 && request->enabled[at] != 1)
            return UD_INVALID;
        if (!request->enabled[at] || request->next_due_ms[at] < 0 ||
            request->next_due_ms[at] > request->now_ms)
            continue;
        if (request->index == UINT32_MAX || request->next_due_ms[at] < request->due_ms) {
            request->index = at;
            request->due_ms = request->next_due_ms[at];
        }
    }
    return request->index == UINT32_MAX ? UD_END : UD_OK;
}

static int valid_flag(int32_t value) { return value == 0 || value == 1; }

/* Only failures that describe a retryable transport/admission condition may
 * consume the durable job attempt budget.  Invalid input, authorization,
 * protocol and data-quality failures are terminal; treating those as generic
 * retryable errors would turn an expired identity or malformed package into
 * an unbounded scheduler loop. */
static int retryable_result(int32_t result) {
    return result == UD_IO || result == UD_BUSY || result == UD_TRANSIENT ||
           result == UD_TIMEOUT || result == UD_FENCED || result == UD_WORKER_LOST;
}

ud_status ud_scheduler_retry(const ud_scheduler_retry_request *request,
                             ud_scheduler_retry_decision *decision) {
    if (!request || !decision)
        return UD_INVALID;
    if (request->struct_size != sizeof(*request) || request->abi_version != UD_ABI_VERSION ||
        decision->struct_size != sizeof(*decision) || decision->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!valid_flag(request->cancelled) || !valid_flag(request->started) ||
        !valid_flag(request->resumed) || !valid_flag(request->broker_restore) ||
        !valid_flag(request->remote) || request->result < 0 || request->result > UD_STATUS_LAST ||
        !request->attempt || !request->max_attempts || request->attempt > request->max_attempts ||
        request->now_ms < 0)
        return UD_INVALID;

    *decision = (ud_scheduler_retry_decision){sizeof(*decision), UD_ABI_VERSION,
                                              UD_SCHEDULER_FAILED, request->result, 0,
                                              request->now_ms};
    /* Broker restore is an explicit recovery path.  Even a cancellation or
     * ordinary Worker error cannot prove that a broker-side effect was absent;
     * preserve that uncertainty for the Journal instead of retrying it. */
    if (request->broker_restore && request->result != UD_OK) {
        decision->state = UD_SCHEDULER_RECOVERY_REQUIRED;
        decision->result = UD_COMMIT_UNKNOWN;
        return UD_OK;
    }
    /* The Native Journal is authoritative after a Worker could have started
     * or a Broker restore was requested.  A Catalog lease cannot establish
     * that the external side effect did not happen. */
    if (request->started || request->resumed || request->result == UD_COMMIT_UNKNOWN) {
        decision->state = UD_SCHEDULER_RECOVERY_REQUIRED;
        decision->result = UD_COMMIT_UNKNOWN;
        return UD_OK;
    }
    if (request->cancelled) {
        decision->state = UD_SCHEDULER_CANCELLED;
        decision->result = UD_CANCELLED;
        return UD_OK;
    }
    if (request->result == UD_OK) {
        decision->state = UD_SCHEDULER_SUCCEEDED;
        decision->result = UD_OK;
        return UD_OK;
    }
    /* A terminal Native failure is safe to retry when no Worker start or
     * resume was observed.  The job's durable max-attempt policy, rather than
     * a managed exception-type table, is the retry authority. */
    if ((request->result == UD_WORKER_LOST && request->remote) ||
        (retryable_result(request->result) && request->attempt < request->max_attempts)) {
        uint64_t delay = (uint64_t)(request->result == UD_WORKER_LOST && request->remote
                                        ? request->retry_delay_seconds ? request->retry_delay_seconds : 1
                                        : request->retry_delay_seconds) * UINT64_C(1000);
        if ((uint64_t)request->now_ms > (uint64_t)INT64_MAX - delay)
            return UD_OVERFLOW;
        decision->state = UD_SCHEDULER_QUEUED;
        decision->result = 0;
        decision->retry = 1;
        decision->not_before_ms = request->now_ms + (int64_t)delay;
    }
    return UD_OK;
}
