#ifndef UD_RUN_H
#define UD_RUN_H
#include "dag.h"
#include "connector.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_run ud_run;
enum { UD_RUN_CREATED=0, UD_RUN_RUNNING=1, UD_RUN_COMPLETE=2, UD_RUN_FAILED=3 };
enum { UD_NODE_READY=0, UD_NODE_ACTIVE=1, UD_NODE_DRAINING=2, UD_NODE_COMPLETE=3, UD_NODE_FAILED=4 };
enum { UD_EVENT_DATA=1, UD_EVENT_INPUT_END=2, UD_EVENT_POLL=3 };
enum { UD_RUN_ACCESS_READ=101, UD_RUN_ACCESS_WRITE=102 };
typedef struct ud_node_event {
    int32_t kind;
    uint32_t port;
    const ud_batch *batch; /* Borrowed only during the call; NULL for other events. */
} ud_node_event;
/* Node instances and returned batches belong to the independent run context.
 * Callbacks run on one execution thread, never concurrently within a run.
 * open returns an owned empty output schema, or NULL for Sink. It must not
 * publish results. A failing open must clean up its own partial instance.
 * event returns OK with zero/one owned output batch, or END with no output
 * after all output has drained. Source receives POLL only; all other nodes
 * receive DATA and one INPUT_END per port, then POLL to drain. END is valid
 * only for Source or after all ports have ended. Sink never returns batches.
 * No automatic retries: event cannot return BUSY (mapped to INVALID), and a
 * TRANSIENT result terminates this run. close frees all instance resources.
 * Host user data must remain valid through destruction and must not share a
 * mutable SDK context between runs. */
typedef struct ud_node_handler {
    uint32_t struct_size, abi_version;
    int32_t kind;
    ud_string operation;
    void *user;
    ud_status (*open)(void *, ud_context *, const ud_dag_node *,
                      const ud_batch *const *input_schemas, uint32_t inputs,
                      void **instance, ud_batch **output_schema);
    ud_status (*event)(void *instance, const ud_node_event *, ud_batch **output);
    void (*close)(void *instance, ud_status outcome);
} ud_node_handler;
typedef struct ud_run_options {
    uint32_t struct_size, abi_version;
    ud_context_options context;
    uint32_t edge_capacity, max_batch_rows;
    const ud_node_handler *handlers;
    uint32_t handler_count;
    ud_string parameter_values; /* NULL/zero selects defaults. */
    ud_string tenant;
    ud_authorize_fn authorize;
    void *authorize_user;
} ud_run_options;
typedef struct ud_run_metrics {
    int32_t state;
    ud_status result;
    uint64_t input_rows, output_rows, batches, queued_batches, queue_high_water, memory_used;
} ud_run_metrics;
/* Copies/binds the graph and registry; creates its own allocator/quota/cancel
 * context. DAG and options may be released after creation (callback users live
 * longer). Built-ins: source csv/ndjson, transform pass/cast, sink ndjson.
 * File built-ins require explicit tenant authorization before every operation.
 * Authorization callbacks may cancel/inspect metrics but must not wait, execute,
 * start or destroy runs. Queue capacity is per edge; callbacks are invoked only
 * with output capacity. File sources must be regular files at absolute paths.
 * The pull scheduler drains downstream first. Fan-out copies own their rows.
 * NDJSON Sink publishes a new path atomically; an existing path is BUSY. Each
 * Sink is independent: multiple sinks do not form a global transaction.
 * This execution API currently has no durable DAG checkpoint or retry policy. */
UD_API ud_status ud_run_create(const ud_dag *, const ud_run_options *, ud_run **);
/* Equivalent constructor from the versioned JSON graph. A temporary parse
 * context is released before return; it is never shared with execution. */
UD_API ud_status ud_run_create_json(const char *, uint32_t length, const ud_run_options *, ud_run **);
UD_API ud_status ud_run_execute(ud_run *); /* Blocking, may be called once. */
UD_API ud_status ud_run_start(ud_run *); /* Dedicated background thread, once. */
/* One controller calls start/execute/wait/destroy. Cancellation and metrics may
 * run concurrently. Timeout returns BUSY; terminal returns the execution result. */
UD_API ud_status ud_run_wait(ud_run *, uint32_t timeout_ms);
UD_API void ud_run_cancel(ud_run *);
UD_API void ud_run_get_metrics(const ud_run *, ud_run_metrics *);
UD_API ud_status ud_run_node_state(const ud_run *, uint32_t node, int32_t *);
/* BUSY while running. Otherwise closes nodes, joins the native thread, releases
 * the run and its context. Except BUSY, the handle is invalid after this call;
 * a non-OK result can report leaked allocations from a custom node handler. */
UD_API ud_status ud_run_destroy(ud_run *);
#ifdef __cplusplus
}
#endif
#endif
