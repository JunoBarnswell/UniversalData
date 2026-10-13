#ifndef UD_PACKAGE_INTERNAL_H
#define UD_PACKAGE_INTERNAL_H
#include "ud/package.h"
#include "ud/cache.h"
#include "ud/cursor.h"
#include "ud/json.h"
#include "ud/format.h"
#include "../../components/data/internal.h"
#include "../../components/contracts/registry_internal.h"
#include "../../components/foundation/thread.h"
#include "ud/buffer.h"
#include "../../components/execution/executor.h"
#include <stdatomic.h>
#ifdef _WIN32
#include <windows.h>
#else
#include <pthread.h>
#endif
typedef struct ud_variable {
    ud_string name;
    ud_value value;
    ud_batch *box; /* One sealed owned value, including immutable type metadata. */
    int readonly;
    struct ud_variable *next;
} ud_variable;
typedef struct ud_scope {
    ud_id identity; /* Logical lexical path; independent of iteration. */
    ud_variable *values;
    ud_variable *parameter_values;
    struct ud_scope *parent;
    const ud_json *parameters, *provided;
    const ud_activation *owner;
} ud_scope;
struct ud_package {
    ud_context *ctx;
    ud_json *document;
    ud_string id;
    uint8_t fingerprint[32];
};
struct ud_activation {
    ud_execution *execution;
    struct ud_activation *parent;
    struct ud_activation *definition;
    ud_package *package;
    ud_json *parameters;
    ud_scope *scope;
    ud_scope globals, project_scope;
    ud_string task_id, pipeline_id, component_id;
    uint64_t deadline, invocation;
    uint32_t depth;
    atomic_int cancelled;
    int parallel;
    struct ud_activation *scope_authority;
    int stepping;
    ud_status pipeline_result;
    struct ud_transaction *transaction;
    const ud_json *event_handlers, *task_definition;
    uint32_t event_depth;
    int transaction_serial;
    ud_string transaction_connection;
    void *cancel_instance;
    void (*cancel_callback)(void *);
    uint32_t cancel_pins;
    int cancel_sent;
    struct ud_activation *next_cancel;
    uint32_t task_facts;
    struct ud_rule_fact *rule_facts;
    atomic_uint_fast64_t input_rows, output_rows, rejected_rows, committed_rows, acknowledged_rows,
        error_rows, committed_error_rows, acknowledged_error_rows;
};
struct ud_execution {
    ud_context *ctx;
    ud_package *package;
    ud_execution_options options;
    ud_registry *registry, *external;
    ud_json *parameters;
    uint8_t parameters_fingerprint[32];
    ud_string deployment_metadata; /* Immutable, cached before cancellation. */
    ud_activation root;
    atomic_int state, result;
    atomic_uint_fast64_t input_rows, output_rows, rejected_rows, batches, queued, high_water,
        executed_tasks, durable_tasks, recovery_tasks, resumed_tasks, spill_bytes, committed_rows,
        acknowledged_rows, error_rows, committed_error_rows, acknowledged_error_rows;
    atomic_uint_fast64_t pipeline_trees;
    atomic_uint tree_active, tree_high_water;
    ud_buffer_metrics buffer_metrics;
    ud_executor *executor;
    struct ud_connection_manager *connections;
    struct ud_source_family *source_families;
    struct ud_source_grant *source_grants;
    struct ud_rule_fact *rule_facts;
    struct ud_cache_asset *assets;
    uint32_t asset_count;
    ud_mutex asset_lock;
    ud_condition finished;
    atomic_uint error_count;
    atomic_int error_limit_reason;
    ud_bytes pipeline_reports, bound_plans, control_reports, schema_reports;
    uint64_t control_record_count, control_records_dropped, pipeline_record_count,
        pipeline_records_dropped;
    ud_mutex diagnostics_lock, cancellation_lock;
    ud_condition cancellation_changed;
    ud_activation *cancellable;
    ud_mutex events_lock, journal_lock, serial_tasks_lock;
    uint64_t event_sequence;
    ud_buffer_manager *buffers;
    struct ud_connection_lease *buffer_lease;
    int threaded;
    int disposed;
    void *journal;
#ifdef _WIN32
    HANDLE thread;
#else
    pthread_t thread;
#endif
};
const ud_json *ud_member(const ud_json *, const char *);
int ud_named(ud_string, const char *);
int ud_same(ud_string, ud_string);
ud_status ud_json_string(const ud_json *, ud_string *);
ud_status ud_json_uint(const ud_json *, uint64_t *);
ud_status ud_package_check(const ud_package *);
ud_status ud_control_validate(ud_activation *, const ud_json *, uint32_t);
ud_status ud_control_handler(ud_activation *, const ud_json *, const char *);
ud_status ud_enumerators_register(ud_registry *);
ud_status ud_foreach(ud_activation *, const ud_json *, const ud_json *, int, uint32_t);
ud_status ud_control_iteration(ud_activation *, const ud_json *, uint64_t);
ud_status ud_checkpoint_enumerator(ud_activation *, const uint8_t digest[32]);
ud_status ud_control_execute(ud_activation *, const ud_json *);
ud_status ud_pipeline_validate(ud_activation *, const ud_json *);
ud_status ud_pipeline_bind_validate(ud_activation *, const ud_json *);
ud_status ud_control_explain(ud_activation *, const ud_json *, ud_bytes *);
ud_status ud_pipeline_execute(ud_activation *, const ud_json *);
ud_status ud_connection_field_namespace(ud_activation *, ud_string, ud_string, char[33]);
ud_status ud_pipeline_field_namespace(const ud_component_context *, char output[33]);
ud_status ud_transforms_register(ud_registry *);
ud_status ud_builtin_register(ud_registry *);
ud_status ud_advanced_register(ud_registry *);
ud_status ud_relational_register(ud_registry *);
ud_status ud_pipeline_write(const ud_component_context *, struct ud_connection_lease *, const ud_write_options *, const ud_batch *);
ud_status ud_component_lookup(ud_activation *, ud_string, ud_component_descriptor *);
void ud_activation_notify_kind(ud_activation *, int32_t kind, ud_execution_state, ud_status);
void ud_activation_notify(ud_activation *, ud_execution_state, ud_status);
enum { UD_TASK_EXECUTED = 1, UD_TASK_DURABLE = 2, UD_TASK_UNCERTAIN = 4, UD_TASK_RESUMED = 8 };
void ud_task_cancel_register(ud_activation *, void *, void (*)(void *));
void ud_task_cancel_unregister(ud_activation *);
void ud_task_fact(ud_activation *, uint32_t);
void ud_target_rows(ud_activation *, uint64_t, uint64_t, int acknowledged);
ud_status ud_transaction_delivery(ud_activation *, const ud_batch *);
int ud_transaction_rows(ud_activation *, uint64_t, uint64_t);
void ud_task_rows(ud_activation *, uint64_t input, uint64_t output, uint64_t rejected);
uint32_t ud_execution_concurrency(const ud_activation *);
ud_status ud_scope_enter(ud_activation *, ud_string name, const ud_json *, ud_scope *);
void ud_scope_leave(ud_activation *, ud_scope *);
ud_status ud_resolve_value(void *, ud_string, ud_value *);
ud_status ud_expression_json(ud_activation *, const ud_json *, const ud_batch *, ud_expression **);
ud_status ud_bound_config(ud_activation *, const ud_json *, ud_json **);
ud_status ud_scalar_json(ud_bytes *, const ud_field *, const ud_value *);
ud_status ud_scope_bind_parameters(ud_activation *, ud_scope *);
ud_status ud_definition_box(ud_context *, ud_string name_space, ud_string name, const ud_json *,
                            const ud_json *, int required, ud_batch **);
ud_status ud_resolve_field(void *, ud_string, ud_field *);
ud_status ud_activation_bind_roots(ud_activation *);
ud_status ud_activation_validate_definition(ud_activation *, uint32_t depth);
ud_status ud_activation_run_definition(ud_activation *);
ud_status ud_child_package(ud_activation *, const ud_json *, int execute, uint32_t depth);
ud_status ud_parameters_check(ud_context *, const ud_json *, const ud_json *, int require_values);
ud_status ud_variable_assign(ud_context *, ud_variable *, const ud_value *, const ud_field *);
ud_status ud_variable_writable(ud_activation *, ud_string, ud_variable **);
ud_status ud_connection_definition(ud_activation *, ud_string, const ud_json **,
                                   int *project_scope);
ud_status ud_connection_definitions_check(const ud_json *);
typedef struct ud_connection_lease ud_connection_lease;
ud_status ud_connections_create(ud_execution *);
ud_status ud_connections_destroy(ud_execution *);
void ud_connections_cancel(ud_execution *);
ud_status ud_connections_release_activation(ud_activation *);
ud_status ud_connections_diagnostics(ud_execution *, ud_bytes *);
ud_status ud_transaction_enter(ud_activation *, const ud_json *, int validate);
ud_status ud_transaction_leave(ud_activation *, ud_status);
int ud_transaction_active(const ud_activation *);
ud_status ud_acquire_file_connection(ud_activation *, ud_string, ud_connection_lease **);
ud_status ud_filesystem_register(ud_registry *);
ud_status ud_process_register(ud_registry *);
ud_status ud_http_register(ud_registry *);
ud_status ud_profile_register(ud_registry *);
ud_status ud_acquire_http_connection(ud_activation *, ud_string, ud_connection_lease **);
ud_status ud_http_connection_resource(ud_connection_lease *, ud_string *);
ud_status ud_http_connection_credentials(ud_connection_lease *, ud_string *);
ud_status ud_acquire_process_connection(ud_activation *, ud_string, ud_connection_lease **);
ud_status ud_process_connection_path(ud_connection_lease *, ud_string *);
ud_status ud_checkpoint_open(ud_execution *);
ud_status ud_checkpoint_task_begin(ud_activation *, ud_string kind, int *completed);
ud_status ud_checkpoint_task_done(ud_activation *);
ud_status ud_checkpoint_source(ud_activation *, const char *path);
ud_status ud_checkpoint_stage(ud_activation *, const char *target, const char *temporary,
                              int *replaying);
ud_status ud_checkpoint_file_done(ud_activation *, const char *target);
int ud_checkpoint_batch_seen(ud_activation *, ud_string identity);
ud_status ud_checkpoint_batch(ud_activation *, ud_string identity, const uint8_t digest[32],
                              int committed);

void ud_checkpoint_close(ud_execution *);
ud_status ud_execution_buffer_options(ud_execution *,ud_buffer_options *);
ud_status ud_component_operator_options(const ud_component_context *,ud_operator_options *);
ud_status ud_component_operator_plan(const ud_operator *,ud_component_plan *);
ud_status ud_execution_prepare_buffers(ud_execution *);
ud_status ud_execution_release_buffers(ud_execution *);
/* Persistent Source snapshots and cursor facts share the existing journal owner. */
ud_status ud_checkpoint_snapshot_read(ud_activation *, ud_string, uint64_t, ud_batch **);
ud_status ud_checkpoint_snapshot_write(ud_activation *, ud_string, uint64_t, const ud_batch *);
ud_status ud_checkpoint_snapshot_info(ud_activation *, ud_string, ud_string, char **, uint32_t *);
ud_status ud_checkpoint_snapshot_seal(ud_activation *, ud_string, ud_string);
ud_status ud_checkpoint_cursor(ud_activation *, ud_string, ud_string);
ud_status ud_checkpoint_schema(ud_activation *, ud_string, const ud_batch *);

ud_status ud_assets_create(ud_execution *);
void ud_assets_destroy(ud_execution *);
ud_status ud_assets_release(ud_execution *);
ud_status ud_assets_diagnostics(ud_execution *, ud_bytes *);
ud_status ud_asset_schema(ud_activation *, ud_string, ud_batch **, int *, int *);
ud_status ud_asset_source(ud_activation *, ud_string, ud_string *);
ud_status ud_asset_keys(ud_activation *, ud_string, const uint32_t **, uint32_t *);
ud_status ud_asset_build(ud_activation *, ud_string, ud_cache **);
ud_status ud_assets_bind_source(ud_activation *, ud_string, struct ud_connection_lease *);
ud_status ud_asset_publish(ud_activation *, ud_string, ud_cache *);
ud_status ud_asset_acquire(ud_activation *, ud_string, ud_cache **);

ud_status ud_error_policy(const ud_json *, const char *, int, int *);

ud_status ud_schema_report(ud_activation *, ud_string, ud_status);
ud_status ud_quality_component_register(ud_registry *);
ud_status ud_cache_components_register(ud_registry *);
struct ud_rule_delivery;
ud_status ud_rule_stage(ud_activation *, const ud_batch *, struct ud_rule_delivery **);
ud_status ud_rule_delivered(ud_activation *, const struct ud_rule_delivery *, int);
void ud_rule_delivery_destroy(struct ud_rule_delivery *);
void ud_rule_facts_destroy(struct ud_rule_fact **);
ud_status ud_rule_facts_json(const struct ud_rule_fact *, ud_bytes *);
ud_status ud_rule_facts_restore(ud_execution *, const ud_json *, int);
ud_status ud_rule_token(const ud_component_context *, ud_string, uint32_t, ud_id *);
ud_status ud_rule_statistics(ud_execution *, ud_id, uint64_t *, uint64_t *, uint64_t *);
ud_status ud_source_version(ud_activation *, ud_id, const uint8_t[32]);
void ud_source_versions_destroy(ud_execution *);
#endif
