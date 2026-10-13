#ifndef UD_COMPONENT_H
#define UD_COMPONENT_H
#include "expression.h"
#include "json.h"
#include "error.h"
#include "activation.h"
#include "operator.h"
#include "stream.h"
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
enum {
    UD_COMPONENT_DATA = UD_STREAM_DATA,
    UD_COMPONENT_INPUT_END = UD_STREAM_END,
    UD_COMPONENT_POLL = UD_STREAM_POLL,
    UD_COMPONENT_WATERMARK = UD_STREAM_WATERMARK,
    UD_COMPONENT_CHECKPOINT_BARRIER = UD_STREAM_BARRIER,
    UD_COMPONENT_SCHEMA_CHANGE = UD_STREAM_SCHEMA_CHANGE
};
typedef struct ud_component_event {
    uint32_t struct_size, abi_version;
    int32_t kind;
    uint32_t input;
    const ud_batch *batch; /* Borrowed sealed Buffer during callback only. */
    uint64_t epoch, schema_version;
    int64_t watermark;
    ud_string source_position;
} ud_component_event;
/* Successful emit retains a sealed reference; it does not take the caller's
 * reference. At most one Buffer per output per step. Only invoke in step. */
typedef ud_status (*ud_component_emit)(void *, uint32_t output, ud_batch *);
typedef ud_status (*ud_component_emit_event)(void *, uint32_t, const ud_component_event *);
typedef struct ud_component_plan ud_component_plan;
enum { UD_PORT_INPUT = 1, UD_PORT_OUTPUT = 2, UD_PORT_REQUIRED = 1, UD_PORT_ERROR = 2 };
typedef struct ud_component_port {
    ud_id id;
    ud_string name;
    uint32_t direction, index, flags;
    const ud_batch *schema; /* Borrowed Binder-owned schema. */
    ud_sort_contract sort;
    uint32_t partition_count, partition_key_count;
    ud_id partition_keys[16];
} ud_component_port;
typedef struct ud_component_ports {
    uint32_t input_count, output_count;
    ud_component_port *inputs, *outputs;
} ud_component_ports;
typedef struct ud_component_context {
    uint32_t struct_size, abi_version;
    ud_context *memory;
    ud_execution *execution;   /* Borrowed; cannot start/wait/destroy in callbacks. */
    ud_activation *activation; /* Isolated lexical/deadline frame. */
    ud_string task_id, pipeline_id, component_id;
    uint32_t max_batch_rows;
    uint32_t (*recommended_batch_rows)(void *emit_user);
    ud_component_emit emit;
    void *emit_user;
    ud_expression_resolver resolve;
    void *resolve_user;
    ud_status (*resolve_field)(void *, ud_string, ud_field *);
    const ud_component_plan *input_plans; /* Borrowed for the instance lifetime. */
    uint32_t input_count, output_count;
    uint64_t connected_outputs;            /* Binding knows which regular ports have edges. */
    const ud_batch *const *output_schemas; /* Resolved output identities at pure binding. */
    ud_error_context errors;
    ud_component_emit_event emit_event;
    uint64_t recovery_epoch;
    ud_string recovery_position;
    int32_t recovery_terminal; /* Borrowed committed Source position, pure prepare input. */
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
struct ud_component_plan {
    uint32_t struct_size, abi_version, partitions;
    int32_t ordering; /* UD_ORDER_*; dynamic physical policy, pure binding only. */
    ud_sort_contract sort;
    uint64_t estimated_rows, row_width, memory_budget, spill_budget, estimated_cost;
    double selectivity;
    uint32_t algorithm, state_version, partition_key_count, effects;
    ud_id partition_keys[16];
    ud_string physical_tree; /* Owned by the plan; input_plans borrow it. */
};
enum {
    UD_EFFECT_EXTERNAL_READ = 1,
    UD_EFFECT_EXTERNAL_WRITE = 2,
    UD_EFFECT_TRANSACTIONAL = 4,
    UD_EFFECT_RECEIPTED = 8,
    UD_EFFECT_COMMIT_UNKNOWN = 16
};
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
    ud_status (*bind_ports)(void *, const ud_component_context *, ud_string, ud_component_ports *);
    uint64_t (*input_demand)(void *); /* NULL admits every input. */
    uint32_t state_version, effects;
    /* State snapshots use canonical typed Batch / Wire / Journal ownership.
     * Callback returns an owned sealed batch; restore borrows it. */
    ud_status (*checkpoint)(void *, ud_batch **);
    ud_status (*restore)(void *, const ud_batch *);
    ud_status (*confirm_epoch)(void *, uint64_t, ud_string source_position);
} ud_component_descriptor;
UD_API ud_status ud_component_plan_copy(ud_context *, const ud_component_plan *,
                                        ud_component_plan *);
UD_API void ud_component_plan_clear(ud_component_plan *);
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
/* Pure descriptor binding materializes defaults and validates the one declared
 * configuration schema before any configure/prepare callback. */
UD_API ud_status ud_component_bind_configuration(ud_context *, const ud_component_descriptor *,
                                                 const ud_json *, ud_json **);
/* Owned Native Catalog JSON, identical for C, CLI, .NET and Java. */
UD_API ud_status ud_registry_describe(const ud_registry *, char **, uint32_t *);
#ifdef __cplusplus
}
#endif
#endif
