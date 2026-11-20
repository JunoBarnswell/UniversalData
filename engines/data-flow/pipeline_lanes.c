#include "pipeline_lanes.h"
#include "../package/internal.h"

typedef struct pipeline_lane {
    ud_activation activation;
    ud_status result;
} pipeline_lane;

typedef struct pipeline_lanes {
    const ud_json *definition;
    ud_status (*run_one)(ud_activation *, const ud_json *);
    pipeline_lane *items;
    uint32_t count;
    atomic_int stopping;
} pipeline_lanes;

static ud_status prepare_lane(ud_activation *parent, pipeline_lane *lane, ud_string pipeline,
                             uint32_t index, uint32_t count) {
    ud_activation *a = &lane->activation;
    a->execution = parent->execution;
    a->parent = parent;
    a->definition = parent->definition;
    a->package = parent->package;
    a->parameters = parent->parameters;
    a->scope = parent->scope;
    a->pipeline_id = pipeline;
    a->task_definition = parent->task_definition;
    a->invocation = parent->invocation;
    a->depth = parent->depth;
    a->event_depth = parent->event_depth;
    a->parallel = 1;
    a->assigned_partition = a->execution->assignment_count ? &a->execution->assignments[index] : NULL;
    a->consumer_lane = index + 1;
    a->consumer_lane_count = count;
    atomic_init(&a->cancelled, 0);
    atomic_init(&a->input_rows, 0);
    atomic_init(&a->output_rows, 0);
    atomic_init(&a->rejected_rows, 0);
    atomic_init(&a->committed_rows, 0);
    atomic_init(&a->acknowledged_rows, 0);
    atomic_init(&a->error_rows, 0);
    atomic_init(&a->committed_error_rows, 0);
    atomic_init(&a->acknowledged_error_rows, 0);

    /* Control task paths append a length-prefixed task segment. The lane
     * segment is disjoint from that grammar and stable across task resumption. */
    char suffix[24];
    int n = a->assigned_partition
        ? snprintf(suffix, sizeof(suffix), "/~partition/%d", a->assigned_partition->partition)
        : snprintf(suffix, sizeof(suffix), "/~lane/%u", index);
    if (n < 0 || (size_t)n >= sizeof(suffix) ||
        parent->task_id.length > UINT32_MAX - (uint32_t)n - 1)
        return UD_OVERFLOW;
    uint32_t length = parent->task_id.length + (uint32_t)n;
    char *identity = ud_alloc(a->execution->ctx, (size_t)length + 1);
    if (!identity)
        return UD_RESOURCE;
    if (parent->task_id.length)
        memcpy(identity, parent->task_id.data, parent->task_id.length);
    memcpy(identity + parent->task_id.length, suffix, (size_t)n + 1);
    a->task_id = (ud_string){identity, length};
    return UD_OK;
}

static ud_status run_lane(void *user, uint32_t index) {
    pipeline_lanes *lanes = user;
    pipeline_lane *lane = &lanes->items[index];
    ud_activation *a = &lane->activation;
    ud_status result = ud_activation_check(a);
    if (!result && atomic_load(&lanes->stopping))
        result = UD_CANCELLED;
    if (!result) {
        ud_activation_notify(a, UD_EXEC_RUNNING, UD_OK);
        result = lanes->run_one(a, lanes->definition);
        ud_status released = ud_connections_release_activation(a);
        if (!result)
            result = released;
    }
    lane->result = result;
    if (result && !atomic_exchange(&lanes->stopping, 1)) {
        for (uint32_t i = 0; i < lanes->count; ++i)
            if (i != index)
                atomic_store(&lanes->items[i].activation.cancelled, 1);
        ud_executor_cancel(a->execution->executor);
    }
    ud_execution_state state = result == UD_COMMIT_UNKNOWN ? UD_EXEC_RECOVERY_REQUIRED :
                               result == UD_CANCELLED ? UD_EXEC_CANCELLED :
                               result ? UD_EXEC_FAILED : UD_EXEC_SUCCEEDED;
    ud_activation_notify(a, state, result);

    /* run_one owns a complete Pipeline lifecycle. Its result is a terminal
     * lane outcome, including BUSY, rather than a WorkGroup scheduling signal. */
    return UD_OK;
}

static void collect_lane(ud_activation *parent, ud_activation *lane) {
    atomic_fetch_add(&parent->input_rows, atomic_load(&lane->input_rows));
    atomic_fetch_add(&parent->output_rows, atomic_load(&lane->output_rows));
    atomic_fetch_add(&parent->rejected_rows, atomic_load(&lane->rejected_rows));
    atomic_fetch_add(&parent->committed_rows, atomic_load(&lane->committed_rows));
    atomic_fetch_add(&parent->acknowledged_rows, atomic_load(&lane->acknowledged_rows));
    atomic_fetch_add(&parent->error_rows, atomic_load(&lane->error_rows));
    atomic_fetch_add(&parent->committed_error_rows, atomic_load(&lane->committed_error_rows));
    atomic_fetch_add(&parent->acknowledged_error_rows, atomic_load(&lane->acknowledged_error_rows));
    ud_rule_facts_destroy(&lane->rule_facts);
    ud_free((void *)lane->task_id.data);
}

ud_status ud_pipeline_run_lanes(ud_activation *parent, const ud_json *definition, uint32_t count,
                               ud_status (*run_one)(ud_activation *, const ud_json *)) {
    if (count == 1 && !parent->execution->assignment_count)
        return run_one(parent, definition);
    if (!count || count > ud_execution_concurrency(parent))
        return UD_UNSUPPORTED;
    UD_TRY(ud_activation_check(parent));
    ud_string pipeline;
    UD_TRY(ud_json_string(ud_member(definition, "id"), &pipeline));
    pipeline_lanes lanes = {.definition = definition, .run_one = run_one, .count = count};
    atomic_init(&lanes.stopping, 0);
    lanes.items = ud_alloc(parent->execution->ctx, (size_t)count * sizeof(*lanes.items));
    if (!lanes.items)
        return UD_RESOURCE;

    ud_status status = UD_OK;
    uint32_t prepared = 0;
    while (!status && prepared < count) {
        status = prepare_lane(parent, lanes.items + prepared, pipeline, prepared, count);
        if (!status)
            ++prepared;
    }
    ud_work_group *group = NULL;
    if (!status)
        status = ud_work_group_create(parent->execution->executor, count, count,
                                      run_lane, &lanes, &group);
    if (!status) {
        for (uint32_t i = 0; i < count; ++i)
            ud_work_group_ready(group, i);
        status = ud_work_group_wait(group);
    }
    ud_status closed = ud_work_group_destroy(group);
    if (!status)
        status = closed;
    ud_status failure = UD_OK;
    for (uint32_t i = 0; i < prepared; ++i) {
        ud_status result = lanes.items[i].result;
        if (result == UD_COMMIT_UNKNOWN ||
            (result && (!failure || (failure == UD_CANCELLED && result != UD_CANCELLED))))
            failure = result;
        collect_lane(parent, &lanes.items[i].activation);
    }
    ud_free(lanes.items);
    return failure ? failure : status;
}
