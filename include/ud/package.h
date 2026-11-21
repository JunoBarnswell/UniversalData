#ifndef UD_PACKAGE_H
#define UD_PACKAGE_H
#include "component.h"
#include "connector.h"
#include "execution.h"
#include "message.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_package ud_package;
typedef struct ud_sql_session ud_sql_session;
typedef struct ud_connection_pool ud_connection_pool;
UD_API ud_status ud_package_requires_journal(const ud_package *, int32_t *);
UD_API ud_status ud_execution_sql_session_open(ud_execution *, ud_string connection,
                                                 ud_sql_session **);
/* Borrows the enrolled physical connection until the handle is closed. Close
 * it before the activation callback returns. Explicit local transaction control
 * is unavailable on a coordinator-owned session. */
UD_API ud_status ud_activation_sql_session_open(ud_activation *, ud_string connection,
                                                ud_sql_session **);
/* Resolve the Journal of a single required Task without running its callback.
 * The execution must be READY and built with resume and the original binding.
 * A successful decision is zero (no manifest), UD_TX_COMMIT, or UD_TX_ROLLBACK. */
UD_API ud_status ud_execution_recover_transaction(ud_execution *, int32_t *decision);
/* Convert the provider's client syntax into its Native connector credentials. */
UD_API ud_status ud_sql_connection_config(ud_context *, int32_t dialect, ud_string,
                                          ud_string *);
UD_API ud_status ud_sql_session_open(int32_t dialect, ud_string plugin_path,
                                      ud_string connection_string, ud_sql_session **);
UD_API ud_context *ud_sql_session_context(ud_sql_session *);
UD_API int32_t ud_sql_session_dialect(const ud_sql_session *);
UD_API ud_status ud_sql_session_set_auto_close(ud_sql_session *, int32_t enabled);
/* Supply SQL text or a structured table query; schema is optional for discovery and scalar reads. */
UD_API ud_status ud_sql_session_read_open(ud_sql_session *, ud_string sql, ud_string query_json,
                                         ud_string schema_json, const ud_batch *parameters,
                                         uint32_t batch_rows, int32_t result_mode, ud_batch **);
UD_API ud_status ud_sql_session_read_next(ud_sql_session *, ud_batch **);
UD_API ud_status ud_sql_session_read_close(ud_sql_session *);
UD_API ud_status ud_sql_session_execute(ud_sql_session *, ud_string sql, const ud_batch *, uint64_t *);
UD_API ud_status ud_sql_session_write(ud_sql_session *, int32_t mode, int32_t load_mode,
                                       ud_string table, const uint32_t *keys, uint32_t key_count,
                                       uint32_t chunk_rows, const ud_batch *, uint64_t *);
/* Supply a sealed batch from this session's context, valid until the next call.
 * Return UD_END for empty input, or set last on the final nonempty batch. */
typedef ud_status (*ud_sql_batch_source)(void *user, uint32_t capacity,
                                         const ud_batch **batch, int32_t *last);
UD_API ud_status ud_sql_session_write_stream(ud_sql_session *, int32_t mode, int32_t load_mode,
                                              ud_string table, const uint32_t *keys,
                                              uint32_t key_count, uint32_t chunk_rows,
                                              ud_sql_batch_source next, void *user, uint64_t *);
enum { UD_SQL_SESSION_BEGIN = 1, UD_SQL_SESSION_COMMIT = 2, UD_SQL_SESSION_ROLLBACK = 3 };
UD_API ud_status ud_sql_session_transaction(ud_sql_session *, int32_t operation, int32_t isolation);
UD_API void ud_sql_session_cancel(ud_sql_session *);
UD_API ud_status ud_sql_session_close(ud_sql_session *);
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
    uint32_t worker_member, worker_members, consumer_lanes; /* Frozen host dispatch; zero means direct execution. */
    int32_t broker_restore; /* Requires verified committed Kafka state before processing. */
    ud_string worker_run_id; /* Frozen parent run identity shared by Kafka stage members. */
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
/* Open before starting. Each successful read returns one owned wire batch.
 * BUSY means no batch yet; END means the stream is drained. Check terminal
 * execution status separately. */
UD_API ud_status ud_execution_open_results(ud_execution *);
UD_API ud_status ud_execution_read_result(ud_execution *, uint32_t timeout_ms, uint8_t **, uint64_t *);
/* A host declares the local path for an installed ud:<name> connector.
 * Bind before starting. The mapped path still requires normal Native authorization. */
UD_API ud_status ud_execution_bind_connector(ud_execution *, ud_string logical_id, ud_string path);
/* Host defaults cap each source and govern wait when the Package has no explicit acquisition timeout. */
UD_API ud_status ud_execution_set_connection_limits(ud_execution *, uint32_t max_connections_per_source,
                                                   uint32_t default_wait_ms);
/* Owns physical connector sessions while each Execution retains its own grants,
 * credential resolver, cancellation and Journal. Destroy only after every
 * borrowing Execution (and any borrowed result) has been released. */
UD_API ud_status ud_connection_pool_create(uint32_t max_sessions, uint64_t memory_limit,
                                           ud_connection_pool **);
/* Hosted pools sharing this existing lock file share one relational session
 * limit per tenant/resource. Configure before creating any Execution. */
UD_API ud_status ud_connection_pool_set_node_limits(ud_connection_pool *, ud_string lock_file,
                                                    uint32_t max_source_sessions);
UD_API ud_status ud_connection_pool_trim_idle(ud_connection_pool *);
UD_API ud_status ud_connection_pool_destroy(ud_connection_pool *);
UD_API ud_status ud_execution_create_json_in_pool(ud_connection_pool *, ud_string,
                                                  const ud_execution_options *, ud_execution **);
typedef struct ud_kafka_partition_assignment {
    int32_t partition, require_checkpoint;
    int64_t start_offset;
} ud_kafka_partition_assignment;
/* One process runs a bounded set of independent Native consumer lanes. The
 * parent run identity and partition offsets remain stable across grouping. */
UD_API ud_status ud_execution_create_partitioned(ud_string, const ud_execution_options *,
    ud_string run_id, ud_string topic, const ud_kafka_partition_assignment *, uint32_t count, ud_execution **);
UD_API ud_status ud_execution_create_partitioned_in_pool(ud_connection_pool *, ud_string,
    const ud_execution_options *, ud_string run_id, ud_string topic,
    const ud_kafka_partition_assignment *, uint32_t count, ud_execution **);
UD_API ud_status ud_execution_message_partition_positions(ud_execution *, ud_string connection,
    ud_string topic, ud_string group, ud_message_partition_position *, uint32_t capacity, uint32_t *);
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
UD_API ud_status ud_execution_test_message_connection(ud_execution *, ud_string connection,
                                                       uint64_t *capabilities, uint64_t *elapsed_ms);
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
/* Concurrent, atomic component counters. Includes no data or mutable schema. */
UD_API ud_status ud_execution_progress(ud_execution *, char **, uint32_t *);
UD_API ud_status ud_execution_debug_snapshot(ud_execution *, char **, uint32_t *);
/* One-shot bounded read-only slice of the regular Native Pipeline. Keeps the
 * selected node and its ancestors, omits destinations, tasks and confirmation.
 * source_rows 1..10000 per source; sample_rows 1..32 per output. Results include
 * cumulative port counts, typed Schema and original Row Identity. The execution
 * must be READY, non-durable and have no extension registry. */
UD_API ud_status ud_execution_preview(ud_execution *, ud_string pipeline, ud_string component,
                                      uint32_t source_rows, uint32_t sample_rows, char **,
                                      uint32_t *);
/* One Data Schema result. Database resources discover a table; file resources
 * accept csv/ndjson/jsonArray or infer the framing from the filename. Sampling
 * is bounded to 256 complete records; unsupported unions never narrow silently.
 * File discovery also accepts {"format":"csv","options":{...}} so the same
 * fileFormat contract governs inference, decoding and encoding. */
UD_API ud_status ud_execution_metadata(ud_execution *, ud_string connection, int32_t kind,
                                       ud_string parent, uint64_t offset, char **, uint32_t *);
/* Authorized absolute plugin paths -> actual Native plugin identity/capabilities.
 * No connections or credentials are opened. Failed modules retain their status. */
UD_API ud_status ud_execution_connector_catalog(ud_execution *, ud_string paths, char **,
                                                uint32_t *);
/* Clone a READY builtin Package under a new identity. Native-bound path ports
 * become resolved indices before the namespace changes. No task/component is
 * prepared or executed. Extension registries are explicitly unsupported. */
UD_API ud_status ud_execution_reidentify_package(ud_execution *, ud_string new_id, char **,
                                                 uint32_t *);
UD_API ud_status ud_execution_inspect_file(ud_execution *, ud_string connection,
                                           ud_string configuration, char **, uint32_t *);
UD_API ud_status ud_execution_discover_schema(ud_execution *, ud_string connection, ud_string table,
                                              char **, uint32_t *);
UD_API ud_status ud_execution_execute(ud_execution *);
UD_API ud_status ud_execution_start(ud_execution *);
/* Start within one CPU granted to this Worker by its Linux process budget. */
UD_API ud_status ud_execution_start_on_cpu(ud_execution *, uint32_t cpu);
UD_API ud_status ud_execution_wait(ud_execution *, uint32_t timeout_ms);
/* Graceful source boundary, drain and final confirmation; distinct from Cancel. */
UD_API void ud_execution_stop(ud_execution *);
UD_API void ud_execution_cancel(ud_execution *);
UD_API ud_status ud_execution_get_metrics(const ud_execution *, ud_execution_metrics *);
UD_API ud_status ud_execution_error_details(ud_execution *, ud_error_detail *);
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
