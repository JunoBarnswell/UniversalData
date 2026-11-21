#ifndef UD_SCHEDULER_H
#define UD_SCHEDULER_H
#include "memory.h"

/* Native scheduling contracts are deliberately data-only.  The Management
 * host supplies identity, package and node snapshots; Native owns calendar
 * interpretation and the deterministic decisions made from those snapshots.
 * Every structure is ABI tagged so a host cannot accidentally reinterpret a
 * schedule or a placement produced by a different Native revision. */
#ifdef __cplusplus
extern "C" {
#endif

enum {
    UD_SCHEDULE_ONCE = 0,
    UD_SCHEDULE_INTERVAL = 1,
    UD_SCHEDULE_DAILY = 2,
    UD_SCHEDULE_WEEKLY = 3,
    UD_SCHEDULE_MONTHLY = 4
};

/* Weekday bits use the .NET/ICU convention: Sunday is bit 0 and Saturday is
 * bit 6.  local_ticks is the number of 100 ns ticks since local midnight,
 * matching DateTime/TimeOnly without making the host timezone part of ABI. */
typedef struct ud_scheduler_schedule {
    uint32_t struct_size, abi_version;
    ud_string id;
    int32_t frequency;
    int64_t start_ms;
    int32_t enabled;
    uint32_t interval_seconds;
    ud_string time_zone;
    int32_t has_local_time;
    uint64_t local_ticks;
    uint32_t weekdays_mask, weekdays_count;
    int32_t day_of_month;
} ud_scheduler_schedule;

/* Validate one schedule.  Invalid timezone IDs and date/time shape are
 * rejected by the same ICU calendar used by ud_scheduler_next.  A valid
 * calendar may still have an occasional DST gap; ud_scheduler_next skips it. */
UD_API ud_status ud_scheduler_validate(const ud_scheduler_schedule *schedule);

/* Return the first occurrence strictly after after_ms.  UD_END means that the
 * finite schedule has no remaining occurrence; next_ms is zero in that case.
 * Calendar occurrences use the first UTC instant for an ambiguous local time
 * and skip local times in a daylight-saving gap. */
UD_API ud_status ud_scheduler_next(const ud_scheduler_schedule *schedule, int64_t after_ms,
                                   int64_t *next_ms);

typedef struct ud_scheduler_due_request {
    uint32_t struct_size, abi_version;
    uint32_t count;
    int64_t now_ms;
    const int64_t *next_due_ms;
    const int32_t *enabled;
    uint32_t index;
    int64_t due_ms;
} ud_scheduler_due_request;

/* Select the earliest enabled cursor due at or before now_ms.  Equal due
 * times retain their durable cursor order, making selection deterministic
 * without a managed LINQ/order implementation. */
UD_API ud_status ud_scheduler_select_due(ud_scheduler_due_request *request);

enum {
    UD_SCHEDULER_QUEUED = 0,
    UD_SCHEDULER_CLAIMED = 1,
    UD_SCHEDULER_RUNNING = 2,
    UD_SCHEDULER_SUCCEEDED = 3,
    UD_SCHEDULER_FAILED = 4,
    UD_SCHEDULER_CANCELLED = 5,
    UD_SCHEDULER_RECOVERY_REQUIRED = 6,
    UD_SCHEDULER_STOPPED = 7
};

typedef struct ud_scheduler_budget {
    uint32_t struct_size, abi_version;
    uint32_t cpu_slots, consumer_lanes;
    uint64_t native_memory_bytes, process_memory_bytes;
} ud_scheduler_budget;

/* A node is a Management-owned immutable heartbeat snapshot.  Native never
 * opens a node, inspects its package or retains any pointer beyond the call. */
typedef struct ud_scheduler_node {
    uint32_t struct_size, abi_version;
    ud_string id;
    int32_t remote, online, eligible, supports_package;
    uint32_t max_cpu, max_processes, used_cpu, used_processes, available_cpu,
        available_processes;
    uint64_t max_memory_bytes, used_memory_bytes, available_memory_bytes;
} ud_scheduler_node;

typedef struct ud_scheduler_placement {
    uint32_t struct_size, abi_version;
    ud_string node_id;
    int32_t remote;
    ud_scheduler_budget budget;
} ud_scheduler_placement;

typedef struct ud_scheduler_placement_request {
    uint32_t struct_size, abi_version;
    ud_scheduler_budget default_budget;
    uint64_t max_native_memory_bytes;
    uint32_t count, node_count;
    const ud_scheduler_node *nodes;
    const ud_scheduler_budget *budgets;
    const ud_scheduler_placement *pins;
    const ud_scheduler_placement *excluded;
} ud_scheduler_placement_request;

/* Deterministic placement from immutable node snapshots.  Output node_id
 * strings borrow the corresponding input node strings and are valid until the
 * caller releases its input snapshot.  No reservation is acquired here; the
 * host performs the actual transport/resource admission after this decision. */
UD_API ud_status ud_scheduler_place(const ud_scheduler_placement_request *request,
                                     ud_scheduler_placement *placements);

typedef struct ud_scheduler_retry_request {
    uint32_t struct_size, abi_version;
    int32_t result, cancelled, started, resumed, broker_restore, remote;
    uint32_t attempt, max_attempts, retry_delay_seconds;
    int64_t now_ms;
} ud_scheduler_retry_request;

typedef struct ud_scheduler_retry_decision {
    uint32_t struct_size, abi_version;
    int32_t state, result, retry;
    int64_t not_before_ms;
} ud_scheduler_retry_decision;

/* Resolve one terminal Worker result.  Commit-unknown and every operation
 * whose side effect may have started become RecoveryRequired and are never
 * retried automatically; broker_restore has the same precedence over
 * cancellation or an ordinary failure.  Other failed jobs use their durable
 * job policy. */
UD_API ud_status ud_scheduler_retry(const ud_scheduler_retry_request *request,
                                     ud_scheduler_retry_decision *decision);

#ifdef __cplusplus
}
#endif
#endif
