#ifndef UD_PACKAGE_H
#define UD_PACKAGE_H
#include "component.h"
#include "connector.h"
#include "execution.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_package ud_package;
/* Pure Native template expansion; $template substitutes typed JSON values. */
UD_API ud_status ud_package_instantiate(ud_context *, ud_string, char **, uint32_t *);
enum {
    UD_EXEC_EVENT_STATE = 1,
    UD_EXEC_EVENT_BEFORE_CHECKPOINT,
    UD_EXEC_EVENT_AFTER_CHECKPOINT,
    UD_EXEC_EVENT_TARGET_COMMITTED,
    UD_EXEC_EVENT_TASK_EXECUTED,
    UD_EXEC_EVENT_TASK_CONFIRMED,
    UD_EXEC_EVENT_TASK_UNCERTAIN,
    UD_EXEC_EVENT_WARNING,
    UD_EXEC_EVENT_ERROR
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
    ud_string error_chain; /* Bounded Native JSON frames, no SQL/raw data; callback lifetime. */
} ud_execution_event;
typedef void (*ud_execution_observer)(void *, const ud_execution_event *);
/* Observer failure latches a real execution fault without recursively invoking
 * the observer. Safe inside the borrowed callback; first failure wins. */
UD_API ud_status ud_activation_report_fault(const ud_activation *, ud_status, ud_string);
typedef struct ud_execution_options {
    uint32_t struct_size, abi_version;
    ud_context_options context;
    uint32_t edge_capacity, max_batch_rows, max_depth;
    uint64_t timeout_ms;
    ud_string parameters, tenant,
        execution_id;      /* Empty new ID: Native generates once. Resume requires original ID. */
    ud_registry *registry; /* Optional host registry, leased for Execution lifetime. */
    ud_authorize_fn authorize;
    void *authorize_user;
    ud_credential_resolver_fn resolve;
    void *resolve_user;
    ud_execution_observer observe;
    void *observe_user;
    ud_string state_directory; /* Optional authorized durable journal; absolute path. */
    int32_t resume;
    uint32_t max_concurrency;      /* 0 selects 1; shared Control/Tree native worker budget. */
    uint64_t event_sequence_start; /* Persisted host/Catalog sequence when resuming the same ID. */
} ud_execution_options;
typedef struct ud_execution_metrics {
    uint32_t struct_size, abi_version;
    int32_t state;
    ud_status result;
    uint64_t input_rows, output_rows, rejected_rows, batches, queued_batches, queue_high_water,
        memory_used, executed_tasks, durable_tasks, recovery_tasks, resumed_tasks, spill_bytes,
        pipeline_trees, tree_high_water, worker_high_water, committed_rows, acknowledged_rows,
        error_count, error_rows, committed_error_rows, acknowledged_error_rows;
} ud_execution_metrics;
/* Package owns an immutable parsed document. Serializer returns owned ud_free
 * text. Current version only; old Job/DAG formats are incompatible. */
UD_API ud_status ud_package_create(ud_context *, ud_string document, ud_package **);
UD_API ud_status ud_package_serialize(const ud_package *, char **, uint32_t *);
UD_API void ud_package_destroy(ud_package *);
UD_API ud_status ud_execution_create(const ud_package *, const ud_execution_options *,
                                     ud_execution **);
UD_API ud_status ud_execution_create_json(ud_string, const ud_execution_options *, ud_execution **);
/* Immutable identity borrowed until Execution destruction; available in every state.
 * UUID v4 for an omitted new ID, otherwise the exact validated host-supplied ID. */
UD_API ud_status ud_execution_id(const ud_execution *, ud_string *);
/* Owned JSON, released with ud_free. Both use the bound native model; neither
 * acquires external resources. Diagnostics are available after execution joins. */
UD_API ud_status ud_activation_raise(ud_activation *, int32_t event_kind,
                                     ud_status original_result);
/* Pure callback-owned field namespace. Returns owned Native text. */
UD_API ud_status ud_component_source_identity(const ud_component_context *, ud_batch *, uint32_t,
                                              uint64_t, ud_string);
UD_API ud_status ud_component_schema_namespace(const ud_component_context *, char **, uint32_t *);
UD_API ud_status ud_execution_compile_expression(ud_execution *, ud_string schema, ud_string source,
                                                 char **, uint32_t *);
UD_API ud_status ud_execution_component_catalog(ud_execution *, char **, uint32_t *);
/* Pure metadata binding for designer ports; uses the same pipeline binder. */
UD_API ud_status ud_execution_component_ports(ud_execution *, ud_string pipeline_id,
                                              ud_string component_id, ud_string kind,
                                              ud_string config, char **, uint32_t *);
UD_API ud_status ud_execution_explain(ud_execution *, char **, uint32_t *);
UD_API ud_status ud_execution_diagnostics(ud_execution *, char **, uint32_t *);
/* Debug sessions explicitly use one Native scheduling worker. Pause/step only
 * at Task/Batch or resolved Epoch boundaries; active transactions never park. */
UD_API ud_status ud_execution_debug_configure(ud_execution *, ud_string configuration);
UD_API ud_status ud_execution_debug_control(ud_execution *, ud_string command);
UD_API ud_status ud_execution_debug_snapshot(ud_execution *, char **, uint32_t *);
/* One Data Schema result. Database resources discover a table; file resources
 * accept csv/ndjson/jsonArray or infer the framing from the filename. Sampling
 * is bounded to 256 complete records; unsupported unions never narrow silently.
 * File discovery also accepts {"format":"csv","options":{...}} so the same
 * fileFormat contract governs inference, decoding and encoding. */
UD_API ud_status ud_execution_discover_schema(ud_execution *, ud_string connection, ud_string table,
                                              char **, uint32_t *);
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
