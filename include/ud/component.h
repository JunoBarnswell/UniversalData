#ifndef UD_COMPONENT_H
#define UD_COMPONENT_H
#include "expression.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_registry ud_registry;
typedef struct ud_execution ud_execution;
enum {
    UD_COMPONENT_TASK = 1,
    UD_COMPONENT_SOURCE,
    UD_COMPONENT_TRANSFORM,
    UD_COMPONENT_DESTINATION,
    UD_COMPONENT_CONNECTION,
    UD_COMPONENT_ENUMERATOR,
    UD_COMPONENT_LOG_PROVIDER
};
enum { UD_COMPONENT_DATA = 1, UD_COMPONENT_INPUT_END, UD_COMPONENT_POLL };
typedef struct ud_component_event {
    uint32_t struct_size, abi_version;
    int32_t kind;
    uint32_t input;
    const ud_batch *batch; /* Borrowed sealed Buffer during callback only. */
} ud_component_event;
/* Successful emit retains a sealed reference; it does not take the caller's
 * reference. At most one Buffer per output per step. Only invoke in step. */
typedef ud_status (*ud_component_emit)(void *, uint32_t output, ud_batch *);
typedef struct ud_component_context {
    uint32_t struct_size, abi_version;
    ud_context *memory;
    ud_execution *execution; /* Borrowed; cannot start/wait/destroy in callbacks. */
    ud_string task_id, pipeline_id, component_id;
    uint32_t max_batch_rows;
    ud_component_emit emit;
    void *emit_user;
    ud_expression_resolver resolve;
    void *resolve_user;
} ud_component_context;
typedef struct ud_component_descriptor {
    uint32_t struct_size, abi_version;
    ud_string id, name, version, configuration_schema;
    int32_t category;
    uint32_t inputs, outputs;
    int32_t synchronous_input, error_output; /* -1 = no synchronous/error port. */
    uint64_t capabilities;
    void *user;
    /* Configure must not perform external I/O. Output schemas are owned empty
     * batches allocated from context memory. Inputs borrow upstream schemas. */
    ud_status (*configure)(void *, const ud_component_context *, ud_string config,
                           const ud_batch *const *, ud_batch **output_schemas);
    /* Prepare acquires resources. On failure clean its partial instance itself. */
    ud_status (*prepare)(void *, const ud_component_context *, ud_string config,
                         const ud_batch *const *, const ud_batch *const *, void **);
    ud_status (*step)(void *, const ud_component_event *);
    /* finish may commit only after all inputs end and output drains; no emits. */
    ud_status (*finish)(void *);
    void (*dispose)(void *, ud_status outcome);
    ud_status (*invoke)(void *, const ud_component_context *, ud_string config); /* Task only. */
} ud_component_descriptor;
UD_API ud_status ud_registry_create(ud_context *, ud_registry **);
/* Copies descriptor metadata, retains callback user by contract (host lifetime).
 * Duplicate IDs reject. Built-in IDs cannot be shadowed. Registry mutations and
 * destruction return BUSY while any Execution holds a lease. */
UD_API ud_status ud_registry_register(ud_registry *, const ud_component_descriptor *);
UD_API uint32_t ud_registry_count(const ud_registry *);
UD_API ud_status ud_registry_get(const ud_registry *, uint32_t, ud_component_descriptor *);
UD_API ud_status ud_registry_find(const ud_registry *, ud_string id, ud_component_descriptor *);
UD_API ud_status ud_registry_destroy(ud_registry *);
#ifdef __cplusplus
}
#endif
#endif
