#ifndef UD_PACKAGE_H
#define UD_PACKAGE_H
#include "component.h"
#include "connector.h"
#include "execution.h"
#ifdef __cplusplus
extern "C" {
#endif
#define UD_PACKAGE_VERSION 4u
typedef struct ud_package ud_package;
enum {
    UD_EXEC_EVENT_STATE = 1,
    UD_EXEC_EVENT_BEFORE_CHECKPOINT,
    UD_EXEC_EVENT_AFTER_CHECKPOINT,
    UD_EXEC_EVENT_TARGET_COMMITTED
};
typedef struct ud_execution_event {
    uint32_t struct_size, abi_version;
    ud_string package_id, execution_id, task_id, pipeline_id, component_id;
    int32_t state;
    ud_status result;
    uint64_t timestamp_ms;
    int32_t kind;      /* State changes or durability boundary; no data/secret values. */
    uint64_t sequence; /* Serialized within the root Execution. */
    const ud_activation *activation; /* Borrowed during observer only. */
} ud_execution_event;
typedef void (*ud_execution_observer)(void *, const ud_execution_event *);
typedef struct ud_execution_options {
    uint32_t struct_size, abi_version;
    ud_context_options context;
    uint32_t edge_capacity, max_batch_rows, max_depth;
    uint64_t timeout_ms;
    ud_string parameters, tenant, execution_id;
    ud_registry *registry; /* Optional host registry, leased for Execution lifetime. */
    ud_authorize_fn authorize;
    void *authorize_user;
    ud_credential_resolver_fn resolve;
    void *resolve_user;
    ud_execution_observer observe;
    void *observe_user;
    ud_string state_directory; /* Optional authorized durable journal; absolute path. */
    int32_t resume;
    uint32_t max_concurrency; /* 0 selects 1; upper bound on native task workers. */
} ud_execution_options;
typedef struct ud_execution_metrics {
    uint32_t struct_size, abi_version;
    int32_t state;
    ud_status result;
    uint64_t input_rows, output_rows, rejected_rows, batches, queued_batches, queue_high_water,
        memory_used, completed_tasks, spill_bytes;
} ud_execution_metrics;
/* Package owns an immutable parsed document. Serializer returns owned ud_free
 * text. Version 4 only; old Job/DAG formats are incompatible. */
UD_API ud_status ud_package_create(ud_context *, ud_string document, ud_package **);
UD_API ud_status ud_package_serialize(const ud_package *, char **, uint32_t *);
UD_API void ud_package_destroy(ud_package *);
UD_API ud_status ud_execution_create(const ud_package *, const ud_execution_options *,
                                     ud_execution **);
UD_API ud_status ud_execution_create_json(ud_string, const ud_execution_options *, ud_execution **);
UD_API ud_status ud_execution_execute(ud_execution *);
UD_API ud_status ud_execution_start(ud_execution *);
UD_API ud_status ud_execution_wait(ud_execution *, uint32_t timeout_ms);
UD_API void ud_execution_cancel(ud_execution *);
UD_API ud_status ud_execution_get_metrics(const ud_execution *, ud_execution_metrics *);
/* One controller; Running/Draining return BUSY. Context allocation leaks are
 * reported at teardown. cancel/metrics are safe concurrently with execution. */
UD_API ud_status ud_execution_destroy(ud_execution *);
/* Root parameter/variable inspection. Running/Draining return BUSY; callbacks
 * inspect their event/component Activation instead. Parameters are read-only. */
UD_API ud_status ud_execution_variable(const ud_execution *, ud_string, ud_value *);
UD_API ud_status ud_execution_variable_field(const ud_execution *, ud_string, ud_field *);
#ifdef __cplusplus
}
#endif
#endif
