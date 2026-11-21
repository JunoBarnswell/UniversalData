#include "builtins_internal.h"

struct ud_result_queue {
    ud_mutex gate;
    ud_condition changed;
    uint32_t capacity, head, count;
    ud_batch *batches[];
};

ud_status ud_execution_open_results(ud_execution *execution) {
    if (!execution)
        return UD_INVALID;
    if (execution->results || atomic_load(&execution->state) != UD_EXEC_READY)
        return UD_BUSY;
    uint32_t capacity = execution->options.edge_capacity;
    struct ud_result_queue *queue = ud_alloc(execution->ctx,
        sizeof(*queue) + (size_t)capacity * sizeof(*queue->batches));
    if (!queue)
        return UD_RESOURCE;
    queue->capacity = capacity;
    queue->head = queue->count = 0;
    ud_mutex_init(&queue->gate);
    ud_condition_init(&queue->changed);
    execution->results = queue;
    return UD_OK;
}

void ud_execution_destroy_results(ud_execution *execution) {
    struct ud_result_queue *queue = execution->results;
    if (!queue)
        return;
    for (uint32_t i = 0; i < queue->count; ++i)
        ud_batch_destroy(queue->batches[(queue->head + i) % queue->capacity]);
    ud_condition_destroy(&queue->changed);
    ud_mutex_destroy(&queue->gate);
    ud_free(queue);
    execution->results = NULL;
}

void ud_execution_wake_results(ud_execution *execution) {
    struct ud_result_queue *queue = execution->results;
    if (!queue)
        return;
    ud_mutex_lock(&queue->gate);
    ud_condition_wake(&queue->changed);
    ud_mutex_unlock(&queue->gate);
}

ud_status ud_execution_read_result(ud_execution *execution, uint32_t timeout_ms,
                                    uint8_t **wire, uint64_t *length) {
    if (!execution || !execution->results || !wire || !length)
        return UD_INVALID;
    *wire = NULL;
    *length = 0;
    struct ud_result_queue *queue = execution->results;
    uint64_t started = ud_now_ms();
    ud_mutex_lock(&queue->gate);
    while (!queue->count) {
        int state = atomic_load(&execution->state);
        uint64_t elapsed = ud_now_ms() - started;
        if (ud_execution_terminal(state) || elapsed >= timeout_ms) {
            ud_mutex_unlock(&queue->gate);
            return ud_execution_terminal(state) ? UD_END : UD_BUSY;
        }
        uint64_t left = timeout_ms - elapsed;
        ud_condition_wait_ms(&queue->changed, &queue->gate, (uint32_t)(left < 25 ? left : 25));
    }
    ud_batch *batch = queue->batches[queue->head];
    queue->head = (queue->head + 1) % queue->capacity;
    queue->count--;
    ud_condition_wake(&queue->changed);
    ud_mutex_unlock(&queue->gate);
    ud_status status = ud_batch_serialize(batch, wire, length);
    ud_batch_destroy(batch);
    return status;
}

static ud_status configure(void *user, const ud_component_context *context, ud_string configuration,
                           const ud_batch *const *inputs, ud_batch **outputs) {
    (void)user;
    (void)context;
    (void)configuration;
    (void)outputs;
    return inputs && inputs[0] ? UD_OK : UD_SCHEMA;
}

static ud_status prepare(void *user, const ud_component_context *context, ud_string configuration,
                         const ud_batch *const *inputs, const ud_batch *const *outputs, void **instance) {
    (void)user;
    (void)configuration;
    (void)inputs;
    (void)outputs;
    *instance = context->activation;
    return UD_OK;
}

static ud_status finish(void *instance) {
    (void)instance;
    return UD_OK;
}

static void dispose(void *instance, ud_status status) {
    (void)instance;
    (void)status;
}

static ud_status discard(void *instance, const ud_component_event *event) {
    (void)instance;
    return event->kind == UD_COMPONENT_POLL ? UD_END : UD_OK;
}

static ud_status step(void *instance, const ud_component_event *event) {
    if (event->kind == UD_COMPONENT_POLL)
        return UD_END;
    if (!instance || event->kind != UD_COMPONENT_DATA)
        return UD_OK;
    ud_activation *activation = instance;
    struct ud_result_queue *queue = activation->execution->results;
    if (!queue)
        return UD_OK;
    ud_mutex_lock(&queue->gate);
    ud_status status = UD_OK;
    while (!(status = ud_activation_check(activation)) && queue->count == queue->capacity)
        ud_condition_wait_ms(&queue->changed, &queue->gate, 25);
    if (!status)
        status = ud_batch_retain((ud_batch *)event->batch);
    if (!status) {
        queue->batches[(queue->head + queue->count) % queue->capacity] = (ud_batch *)event->batch;
        queue->count++;
        ud_condition_wake(&queue->changed);
    }
    ud_mutex_unlock(&queue->gate);
    return status;
}

ud_status ud_results_register(ud_registry *registry) {
    static const char schema[] = "{\"type\":\"object\",\"additionalProperties\":false}";
    ud_component_descriptor descriptor = {
        .struct_size = sizeof(descriptor), .abi_version = UD_ABI_VERSION,
        .id = {"discard", 7}, .name = {"Discard", 7},
        .version = {UD_SDK_VERSION, sizeof(UD_SDK_VERSION) - 1},
        .category = UD_COMPONENT_DESTINATION, .inputs = 1,
        .error_output = -1, .synchronous_input = -1,
        .capabilities = UD_COMPONENT_REPLAY_SAFE | UD_COMPONENT_THREAD_SAFE | UD_COMPONENT_STREAM_SAFE,
        .configuration_schema = {schema, sizeof(schema) - 1}, .configure = configure,
        .prepare = prepare, .step = discard, .finish = finish, .dispose = dispose
    };
    UD_TRY(ud_registry_register(registry, &descriptor));
    descriptor.id = (ud_string){"result.destination", 18};
    descriptor.name = (ud_string){"Result Destination", 18};
    descriptor.step = step;
    return ud_registry_register(registry, &descriptor);
}
