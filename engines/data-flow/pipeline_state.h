#ifndef UD_PIPELINE_STATE_H
#define UD_PIPELINE_STATE_H
#include "../package/internal.h"
#include "pipeline_frontier.h"
#include "ud/engine.h"
typedef struct edge {
    uint32_t from, to, output, input;
    ud_queue *queue;
    int ended;
    atomic_uint occupied;
    atomic_uint_fast64_t produced, consumed, high_water, spill_bytes;
} edge;
typedef struct execution_tree {
    uint32_t offset, count;
    uint64_t ready_ms, first_ms, end_ms, quanta, busy, callback_ms, cpu_ns;
} execution_tree;
enum { UD_CHANGES_APPEND, UD_CHANGES_UPSERT, UD_CHANGES_RETRACT };
typedef struct node {
    ud_string id;
    ud_component_descriptor descriptor;
    ud_json *configuration;
    char *json;
    uint32_t json_length;
    ud_batch **schemas;
    uint32_t *inputs, *outputs, output_edges, ended, next_port, tree;
    uint8_t *emitted;
    void *instance;
    int opened, emit_allowed, pending, control_pending, watermark_refresh;
    uint32_t watermark_refresh_input;
    ud_component_event deferred_control;
    uint64_t idle_started, idle_ms, sampled_rows;
    atomic_uint_fast64_t live_idle_ms;
    atomic_int idle;
    atomic_int_fast64_t live_watermark;
    atomic_int state, result;
    ud_activation activation;
    ud_component_context context;
    ud_component_plan binding, *input_plans;
    ud_component_ports ports;
    int change_mode, source_replay;
    const char *visibility, *confirmation;
    uint64_t steps, processed, produced, rejected, wait_input, wait_capacity, callback_ms, cpu_ns,
        first_ms, end_ms, ready_ms, prepare_ms, prepare_cpu_ns, finish_ms, finish_cpu_ns,
        blocked_ms, input_wait_ms, capacity_wait_ms;
    int blocked_kind;
    uint64_t batch_latency[64], batch_samples, batch_max_ns, batch_sum_ns;
    uint64_t barrier_mask, barrier_epoch, acknowledged_epoch, schema_version;
    uint64_t merge_base_rows;
    int merge_base_sealed;
    atomic_uint_fast64_t staged_epoch;
    atomic_uint_fast64_t live_processed, live_produced, live_rejected, live_callback_ms;
    atomic_uint_fast64_t *port_rows;
    ud_pipeline_frontier frontier;
    ud_string source_position;
} node;
typedef struct physical_plan {
    ud_activation *execution;
    node *nodes;
    edge *edges;
    uint32_t count, paths, *order, *tree_order, tree_count, concurrency;
    execution_tree *trees;
    ud_string id;
    ud_work_group *group;
    atomic_uint_fast64_t confirmed_epoch;
    ud_mutex epoch_lock;
    int epoch_lock_ready;
    int allow_replayable_publish;
    int32_t continuous_input;
    ud_string kafka_connection, kafka_topic, kafka_group;
    uint32_t kafka_sources;
    int32_t kafka_partition;
    int kafka_stage, kafka_merge;
    uint32_t kafka_merge_sort;
    struct ud_kafka_checkpoint *kafka;
    ud_json *epoch_record;
    struct physical_plan *progress_next;
    int progress_registered;
    uint64_t sampled_ms;
} physical_plan;
ud_status ud_pipeline_plan_json(physical_plan *, ud_bytes *, int, ud_status);
ud_status ud_pipeline_node_failed(node *, ud_status, const char *);
ud_status ud_pipeline_validate_contract(physical_plan *);
int ud_pipeline_port_change(const node *, uint32_t);
uint64_t ud_pipeline_input_mask(const physical_plan *, const node *, int);
int ud_pipeline_epoch_target_supported(const physical_plan *, const node *);
ud_status ud_pipeline_epoch_component_key(physical_plan *, node *, char [65]);
ud_status ud_pipeline_stage_epoch(physical_plan *, node *, uint64_t);
ud_status ud_pipeline_commit_epoch(physical_plan *, uint64_t, int);
ud_status ud_pipeline_kafka_bind(physical_plan *, const ud_json *);
ud_status ud_pipeline_kafka_open(physical_plan *);
ud_status ud_pipeline_kafka_commit(physical_plan *, uint64_t, ud_string);
ud_status ud_pipeline_kafka_verify(physical_plan *, node *);
int ud_pipeline_kafka_exhausted(const physical_plan *);
int ud_pipeline_kafka_terminal_pending(const physical_plan *);
ud_status ud_pipeline_parallel_bind(physical_plan *, const ud_json *);
ud_status ud_pipeline_parallel_run(ud_activation *, const ud_json *,
                                    ud_status (*)(ud_activation *, const ud_json *));
#endif
