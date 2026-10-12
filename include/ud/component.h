#ifndef UD_COMPONENT_H
#define UD_COMPONENT_H
#include "expression.h"
#include "activation.h"
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
    UD_COMPONENT_FUNCTION,
    UD_COMPONENT_ENUMERATOR
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
    ud_execution *execution;   /* Borrowed; cannot start/wait/destroy in callbacks. */
    ud_activation *activation; /* Isolated lexical/deadline frame. */
    ud_string task_id, pipeline_id, component_id;
    uint32_t max_batch_rows;
    ud_component_emit emit;
    void *emit_user;
    ud_expression_resolver resolve;
    void *resolve_user;
    ud_status (*resolve_field)(void *, ud_string, ud_field *);
} ud_component_context;
/* Task promises its effects may safely be replayed after an interrupted invoke.
 * This is a host/plugin contract, not a claim inferred by the runtime. */
#define UD_COMPONENT_REPLAY_SAFE UINT64_C(0x8000000000000000)
#define UD_COMPONENT_THREAD_SAFE UINT64_C(0x4000000000000000)
#define UD_COMPONENT_STATEFUL UINT64_C(0x0800000000000000)
#define UD_COMPONENT_BLOCKING UINT64_C(0x0400000000000000)
#define UD_COMPONENT_PARTIALLY_BLOCKING UINT64_C(0x0200000000000000)
#define UD_COMPONENT_THREAD_AFFINE UINT64_C(0x0100000000000000)
#define UD_COMPONENT_CLONEABLE UINT64_C(0x0080000000000000)
#define UD_COMPONENT_MERGEABLE UINT64_C(0x0040000000000000)
#define UD_ENUMERATOR_DETERMINISTIC UINT64_C(0x0020000000000000)
#define UD_ENUMERATOR_SNAPSHOT UINT64_C(0x0010000000000000)
#define UD_ENUMERATOR_RESUMABLE UINT64_C(0x0008000000000000)
#define UD_FUNCTION_PURE UINT64_C(0x2000000000000000)
#define UD_FUNCTION_DETERMINISTIC UINT64_C(0x1000000000000000)
typedef struct ud_component_plan {
    uint32_t struct_size, abi_version, partitions;
    int32_t ordering; /* UD_ORDER_*; dynamic physical policy, pure binding only. */
} ud_component_plan;
typedef struct ud_component_descriptor {
    uint32_t struct_size, abi_version;
    ud_string id, name, version, configuration_schema;
    int32_t category;
    uint32_t inputs, outputs;
    int32_t synchronous_input, error_output; /* -1 = no synchronous/error port. */
    uint64_t capabilities;
    void *user;
    /* Configure must not perform external I/O. Output schemas are owned empty
     * batches allocated from context memory. Inputs borrow upstream schemas.
     * Task configure is optional pure validation and receives NULL schemas. */
    ud_status (*configure)(void *, const ud_component_context *, ud_string config,
                           const ud_batch *const *, ud_batch **output_schemas);
    /* Prepare acquires resources. On failure clean its partial instance itself. */
    ud_status (*prepare)(void *, const ud_component_context *, ud_string config,
                         const ud_batch *const *, const ud_batch *const *, void **);
    /* MORE schedules POLL before another input. It is never a Task result or
     * a success/commit indication. Emission limits still apply. */
    ud_status (*step)(void *, const ud_component_event *);
    /* finish may commit only after all inputs end and output drains; no emits. */
    ud_status (*finish)(void *);
    void (*dispose)(void *, ud_status outcome);
    ud_status (*invoke)(void *, const ud_component_context *, ud_string config); /* Task only. */
    uint32_t min_arguments, max_arguments; /* FUNCTION only, <= 16. */
    /* Pure binding borrows argument metadata; evaluator returns owned pointer
     * payloads allocated in memory. Expression releases them with ud_free. */
    ud_status (*function_bind)(void *, const ud_field *, uint32_t, ud_field *);
    ud_status (*function_evaluate)(void *, ud_context *, const ud_value *, uint32_t,
                                   const ud_field *, ud_value *);
    /* Optional task lifecycle: prepare returns invoke's instance, finish commits,
     * cancel is safe concurrently, and dispose consumes exactly once. */
    void (*cancel)(void *);
    /* Enumerator configure returns one ValueSet schema; prepare opens it. next
     * returns an owned sealed single-row ValueSet, END once iteration completes. */
    ud_status (*enumerate_next)(void *, ud_batch **);
    ud_status (*enumerate_position)(void *, uint64_t *);
    uint64_t optional_inputs;
    /* Runs after every finish succeeds: Source cursor confirmation barrier. */
    ud_status (*confirm)(void *);
    ud_status (*diagnostics)(void *, char **, uint32_t *);
    uint64_t optional_outputs; /* Unconnected optional ports never accept emitted data. */
    ud_status (*describe_plan)(void *, const ud_component_context *, ud_string,
                               const ud_batch *const *, ud_component_plan *);
} ud_component_descriptor;
UD_API ud_status ud_registry_create(ud_context *, ud_registry **);
/* Copies descriptor metadata, retains callback user by contract (host lifetime).
 * Duplicate IDs reject. Built-in IDs cannot be shadowed. Registry mutations and
 * destruction return BUSY while any Execution holds a lease. */
UD_API ud_status ud_registry_register(ud_registry *, const ud_component_descriptor *);
typedef ud_status (*ud_component_owner_retain)(void *);
typedef void (*ud_component_owner_release)(void *);
/* Atomic metadata registration. Each entry retains its callback owner until
 * registry destruction; retain/release must be paired, or both NULL. Only an
 * OK retain acquires a reference. Ownership
 * callbacks must not mutate the registry or control executions. Host serializes
 * registration/destruction; it is not concurrent with registry lifetime changes. */
UD_API ud_status ud_registry_register_owned(ud_registry *, const ud_component_descriptor *,
                                            uint32_t count, void *owner, ud_component_owner_retain,
                                            ud_component_owner_release);
UD_API uint32_t ud_registry_count(const ud_registry *);
UD_API ud_status ud_registry_get(const ud_registry *, uint32_t, ud_component_descriptor *);
UD_API ud_status ud_registry_find(const ud_registry *, ud_string id, ud_component_descriptor *);
UD_API ud_status ud_registry_destroy(ud_registry *);
#ifdef __cplusplus
}
#endif
#endif
