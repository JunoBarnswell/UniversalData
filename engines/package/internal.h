#ifndef UD_PACKAGE_INTERNAL_H
#define UD_PACKAGE_INTERNAL_H
#include "ud/package.h"
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
    ud_column_id identity; /* Logical lexical path; independent of iteration. */
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
    uint32_t task_facts;
    atomic_uint_fast64_t input_rows, output_rows, rejected_rows;
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
        executed_tasks, durable_tasks, recovery_tasks, resumed_tasks, spill_bytes;
    atomic_uint_fast64_t pipeline_trees;
    atomic_uint tree_active, tree_high_water;
    ud_executor *executor;
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
ud_status ud_json_keys(const ud_json *, const char *const *, uint32_t);
ud_status ud_package_check(const ud_package *);
ud_status ud_control_validate(ud_activation *, const ud_json *, uint32_t);
ud_status ud_control_handler(ud_activation *, const ud_json *, const char *);
ud_status ud_control_execute(ud_activation *, const ud_json *);
ud_status ud_pipeline_validate(ud_activation *, const ud_json *);
ud_status ud_pipeline_bind_validate(ud_activation *, const ud_json *);
ud_status ud_pipeline_execute(ud_activation *, const ud_json *);
ud_status ud_pipeline_field_namespace(const ud_component_context *, char output[33]);
ud_status ud_transforms_register(ud_registry *);
ud_status ud_builtin_register(ud_registry *);
ud_status ud_component_lookup(ud_activation *, ud_string, ud_component_descriptor *);
void ud_activation_notify_kind(ud_activation *, int32_t kind, ud_execution_state, ud_status);
void ud_activation_notify(ud_activation *, ud_execution_state, ud_status);
enum { UD_TASK_EXECUTED = 1, UD_TASK_DURABLE = 2, UD_TASK_UNCERTAIN = 4, UD_TASK_RESUMED = 8 };
void ud_task_fact(ud_activation *, uint32_t);
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
ud_status ud_execution_prepare_buffers(ud_execution *);
ud_status ud_execution_release_buffers(ud_execution *);
#endif
