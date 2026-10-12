#ifndef UD_PACKAGE_INTERNAL_H
#define UD_PACKAGE_INTERNAL_H
#include "ud/package.h"
#include "ud/json.h"
#include "../../components/data/internal.h"
#include "../../components/contracts/registry_internal.h"
#include <stdatomic.h>
#ifdef _WIN32
#include <windows.h>
#else
#include <pthread.h>
#endif
typedef struct ud_variable {
    ud_string name;
    ud_value value;
    int readonly;
    struct ud_variable *next;
} ud_variable;
typedef struct ud_scope {
    ud_variable *values;
    struct ud_scope *parent;
    const ud_json *parameters, *provided;
} ud_scope;
struct ud_package {
    ud_context *ctx;
    ud_json *document;
    ud_string id;
    uint8_t fingerprint[32];
};
struct ud_execution {
    ud_context *ctx;
    ud_package *package;
    ud_execution_options options;
    ud_registry *registry, *external;
    ud_json *parameters;
    ud_scope *scope;
    ud_scope globals, project_scope;
    atomic_int state, result;
    atomic_uint_fast64_t input_rows, output_rows, rejected_rows, batches, queued, high_water,
        completed_tasks, spill_bytes;
    uint64_t deadline, invocation;
    ud_string task_id, pipeline_id, component_id;
    uint32_t depth;
    int threaded;
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
ud_status ud_control_validate(ud_execution *, const ud_json *, uint32_t);
ud_status ud_control_execute(ud_execution *, const ud_json *);
ud_status ud_pipeline_validate(ud_execution *, const ud_json *);
ud_status ud_pipeline_bind_validate(ud_execution *, const ud_json *);
ud_status ud_pipeline_execute(ud_execution *, const ud_json *);
ud_status ud_builtin_register(ud_registry *);
ud_status ud_component_lookup(ud_execution *, ud_string, ud_component_descriptor *);
ud_status ud_execution_check(ud_execution *);
void ud_execution_notify_kind(ud_execution *, int32_t kind, ud_execution_state, ud_status);
void ud_execution_notify(ud_execution *, ud_execution_state, ud_status);
ud_status ud_scope_enter(ud_execution *, const ud_json *, ud_scope *);
void ud_scope_leave(ud_execution *, ud_scope *);
ud_status ud_resolve_value(void *, ud_string, ud_value *);
ud_status ud_expression_json(ud_execution *, const ud_json *, const ud_batch *, ud_expression **);
ud_status ud_bound_config(ud_execution *, const ud_json *, ud_json **);
ud_status ud_scalar_value(const ud_json *, ud_type, ud_value *);
ud_type ud_scalar_type(ud_string);
ud_status ud_parameters_check(const ud_json *, const ud_json *, int require_values);
ud_status ud_variable_writable(ud_execution *, ud_string, ud_variable **);
ud_status ud_connection_definition(ud_execution *, ud_string, const ud_json **, int *project_scope);
ud_status ud_connection_definitions_check(const ud_json *);
ud_status ud_checkpoint_open(ud_execution *);
ud_status ud_checkpoint_task_begin(ud_execution *, ud_string kind, int *completed);
ud_status ud_checkpoint_task_done(ud_execution *);
ud_status ud_checkpoint_source(ud_execution *, const char *path);
ud_status ud_checkpoint_stage(ud_execution *, const char *target, const char *temporary,
                              int *replaying);
ud_status ud_checkpoint_file_done(ud_execution *, const char *target);
int ud_checkpoint_batch_seen(ud_execution *, ud_string identity);
ud_status ud_checkpoint_batch(ud_execution *, ud_string identity, const uint8_t digest[32],
                              int committed);

void ud_checkpoint_close(ud_execution *);
#endif
