#include "mq_payload.h"
#include "kafka_stage.h"
#include "../package/kafka_checkpoint.h"
#include "../../components/compute/internal.h"

enum { MQ_PARTITION_KEYS = 16, MQ_PARTITIONS = 64 };

typedef struct mq_destination {
    ud_component_context context;
    ud_json *config;
    ud_mq_settings settings;
    ud_connection_lease *lease;
    ud_schema_registry *registry;
    struct ud_kafka_checkpoint *checkpoint;
    atomic_uint_fast64_t pending_committed;
    uint32_t partition_count, partition_key_count, partition_keys[MQ_PARTITION_KEYS];
    uint32_t stage_partitions;
    uint8_t stage_identity[32];
} mq_destination;

static ud_status bind_partition_keys(const ud_json *config, const ud_mq_settings *settings,
                                      const ud_batch *input, uint32_t *count, uint32_t *key_count,
                                      uint32_t keys[MQ_PARTITION_KEYS]) {
    const ud_json *repartition = ud_member(config, "repartition");
    *count = *key_count = 0;
    if (!repartition)
        return UD_OK;
    if (!input || settings->target.kind != UD_MESSAGE_TOPIC || settings->fixed_partition)
        return UD_SCHEMA;
    uint64_t partitions;
    UD_TRY(ud_json_uint(ud_member(repartition, "partitions"), &partitions));
    const ud_json *fields = ud_member(repartition, "keys");
    uint32_t length = ud_json_size(fields);
    if (!partitions || partitions > MQ_PARTITIONS || !length || length > MQ_PARTITION_KEYS)
        return UD_SCHEMA;
    for (uint32_t i = 0; i < length; i++)
        UD_TRY(ud_compute_field_index(input, ud_json_at(fields, i), keys + i));
    *count = (uint32_t)partitions;
    *key_count = length;
    return UD_OK;
}

static ud_status configure(void *user, const ud_component_context *context, ud_string text,
                           const ud_batch *const *inputs, ud_batch **outputs) {
    (void)user;
    (void)outputs;
    ud_json *config = NULL;
    UD_TRY(ud_builtin_parse(context, text, &config));
    ud_mq_settings settings;
    ud_status status = ud_mq_settings_parse(context, config, &settings);
    uint32_t partitions, key_count, keys[MQ_PARTITION_KEYS];
    if (!status)
        status = bind_partition_keys(config, &settings, inputs[0], &partitions, &key_count, keys);
    if (!status && settings.format == UD_MQ_BINARY) {
        const ud_batch *input = inputs[0];
        if (!input || input->columns < 1 || input->schema->fields[0].type != UD_BINARY)
            status = UD_SCHEMA;
    }
    ud_json_destroy(config);
    return status;
}

static void dispose(void *instance, ud_status result) {
    (void)result;
    mq_destination *destination = instance;
    if (!destination)
        return;
    ud_schema_registry_close(destination->registry);
    if (destination->lease && !destination->checkpoint)
        (void)ud_connection_lease_release(destination->lease);
    ud_json_destroy(destination->config);
    ud_free(destination);
}

static ud_status prepare(void *user, const ud_component_context *context, ud_string text,
                         const ud_batch *const *inputs, const ud_batch *const *outputs,
                         void **out) {
    (void)user;
    (void)outputs;
    mq_destination *destination = ud_alloc(context->memory, sizeof(*destination));
    if (!destination)
        return UD_RESOURCE;
    destination->context = *context;
    destination->checkpoint = context->activation->kafka_checkpoint;
    atomic_init(&destination->pending_committed, 0);
    ud_mq_settings *settings = &destination->settings;
    ud_status status = ud_builtin_parse(context, text, &destination->config);
    if (!status)
        status = ud_mq_settings_parse(context, destination->config, settings);
    if (!status)
        status = bind_partition_keys(destination->config, settings, inputs[0],
            &destination->partition_count, &destination->partition_key_count, destination->partition_keys);
    if (!status && ud_kafka_stage_enabled(ud_member(destination->config, "publishStage"))) {
        uint32_t members = context->execution->options.worker_members;
        destination->stage_partitions = settings->fixed_partition && !settings->partition_by_worker ? 1 : members;
        if (!destination->checkpoint || !context->execution->options.worker_run_id.length ||
            (destination->partition_count != members && !settings->partition_by_worker && settings->fixed_partition != 1))
            status = UD_SCHEMA;
        else
            ud_kafka_stage_identity(context, context->pipeline_id, destination->stage_identity);
    }
    if (!status && settings->format == UD_MQ_AVRO)
        status = ud_schema_registry_open(context, settings->registry_connection, settings->subject,
                                         settings->schema_version, &destination->registry);
    if (!status) {
        if (destination->checkpoint)
            destination->lease = ud_kafka_checkpoint_producer(destination->checkpoint);
        else {
            uint64_t capabilities = UD_CONNECT_MESSAGE_PUBLISH;
            if (settings->fixed_partition || destination->partition_count)
                capabilities |= UD_CONNECT_MESSAGE_TRANSACTION;
            status = ud_activation_acquire_connection(context->activation, settings->connection,
                                                      capabilities, &destination->lease);
        }
    }
    if (status)
        dispose(destination, status);
    else
        *out = destination;
    return status;
}

static ud_status publish(mq_destination *destination, const ud_batch *batch) {
    if (!batch->rows)
        return UD_OK;
    ud_context *ctx = destination->context.memory;
    ud_message *messages = ud_alloc(ctx, batch->rows * sizeof(*messages));
    ud_message_result *results = ud_alloc(ctx, batch->rows * sizeof(*results));
    ud_batch *payload = NULL;
    ud_status status = messages && results ? ud_mq_project_payload(batch, &payload) : UD_RESOURCE;
    int format = destination->settings.format;
    if (!status && format == UD_MQ_JSON && ud_batch_has_absent(payload))
        status = UD_INCOMPATIBLE;
    for (uint32_t row = 0; !status && row < batch->rows; row++) {
        status = ud_mq_encode(payload, row, format, destination->registry, messages + row);
        if (!status && messages[row].payload.length > destination->settings.maximum_bytes)
            status = UD_TRUNCATED;
        if (!status)
            status = ud_mq_encode_metadata(batch, row, messages + row);
        if (!status && destination->stage_partitions)
            status = ud_kafka_stage_tag(ctx, messages + row, destination->stage_identity,
                                        destination->context.execution->options.worker_member,
                                        UD_COMPONENT_DATA, 0);
        if (!status && destination->settings.fixed_partition)
            messages[row].partition = (int32_t)(destination->settings.fixed_partition - 1);
        if (!status && destination->partition_count) {
            uint32_t partition;
            status = ud_partition_hash_row(batch, row, destination->partition_keys,
                                            destination->partition_key_count,
                                            destination->partition_count, &partition);
            if (!status)
                messages[row].partition = (int32_t)partition;
        }
    }
    if (!status && destination->checkpoint)
        status = ud_kafka_checkpoint_begin(destination->checkpoint);
    if (!status) {
        status = ud_connection_lease_message_publish(destination->lease, &destination->settings.target,
                                                      messages, batch->rows, results);
        uint64_t accepted = 0;
        int uncertain = 0;
        for (uint32_t row = 0; row < batch->rows; row++) {
            accepted += results[row].state == UD_MESSAGE_ACCEPTED;
            uncertain |= results[row].state == UD_MESSAGE_UNCERTAIN;
            if (!status && results[row].state != UD_MESSAGE_ACCEPTED)
                status = results[row].result ? results[row].result : UD_IO;
        }
        if (!destination->checkpoint && (uncertain || (status && accepted)))
            status = UD_COMMIT_UNKNOWN;
        if (destination->checkpoint)
            atomic_fetch_add(&destination->pending_committed, accepted);
        else if (accepted)
            ud_target_rows(destination->context.activation, accepted, 0, 0);
    }
    for (uint32_t row = 0; messages && row < batch->rows; row++)
        ud_message_clear(messages + row);
    ud_batch_destroy(payload);
    ud_free(messages);
    ud_free(results);
    return status;
}

void ud_mq_destination_confirm(void *instance) {
    mq_destination *destination = instance;
    uint64_t accepted = atomic_exchange(&destination->pending_committed, 0);
    if (accepted)
        ud_target_rows(destination->context.activation, accepted, 0, 0);
}

static ud_status publish_stage_control(mq_destination *destination, const ud_component_event *event) {
    ud_message messages[MQ_PARTITIONS] = {{0}};
    ud_message_result results[MQ_PARTITIONS] = {{0}};
    ud_context *ctx = destination->context.memory;
    ud_status status = UD_OK;
    for (uint32_t i = 0; !status && i < destination->stage_partitions; i++) {
        messages[i].partition = (int32_t)i;
        messages[i].offset = messages[i].timestamp_ms = -1;
        status = ud_kafka_stage_tag(ctx, messages + i, destination->stage_identity,
                                   destination->context.execution->options.worker_member,
                                   event->kind, event->watermark);
    }
    if (!status)
        status = ud_kafka_checkpoint_begin(destination->checkpoint);
    if (!status)
        status = ud_connection_lease_message_publish(destination->lease, &destination->settings.target,
                                                      messages, destination->stage_partitions, results);
    for (uint32_t i = 0; i < destination->stage_partitions; i++) {
        if (!status && results[i].state != UD_MESSAGE_ACCEPTED)
            status = results[i].result ? results[i].result : UD_IO;
        ud_message_clear(messages + i);
    }
    return status;
}

static ud_status step(void *instance, const ud_component_event *event) {
    mq_destination *destination = instance;
    if (event->kind == UD_COMPONENT_DATA)
        return publish(destination, event->batch);
    if (destination->stage_partitions && (event->kind == UD_COMPONENT_INPUT_END ||
        event->kind == UD_COMPONENT_WATERMARK || event->kind == UD_COMPONENT_IDLE ||
        event->kind == UD_COMPONENT_ACTIVE))
        return publish_stage_control(destination, event);
    return event->kind == UD_COMPONENT_POLL ? UD_END : UD_OK;
}

static ud_status finish(void *instance) {
    (void)instance;
    return UD_OK;
}

ud_status ud_mq_destination_register(ud_registry *registry) {
    static const char schema[] = "{\"type\":\"object\",\"additionalProperties\":false,"
        "\"required\":[\"connection\",\"targetType\",\"address\"],\"properties\":{" UD_MQ_PAYLOAD_PROPERTIES ","
        "\"publishStage\":{\"type\":\"boolean\",\"default\":false},"
        "\"repartition\":{\"type\":\"object\",\"additionalProperties\":false,\"required\":[\"partitions\",\"keys\"],"
        "\"properties\":{\"partitions\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":64},"
        "\"keys\":{\"type\":\"array\",\"minItems\":1,\"maxItems\":16,\"items\":{\"anyOf\":[{\"type\":\"string\"},"
        "{\"type\":\"integer\",\"minimum\":0}]}}}}}}";
    const ud_component_descriptor descriptor = {
        .struct_size = sizeof(descriptor), .abi_version = UD_ABI_VERSION,
        .id = {"mq.destination", 14}, .name = {"Message Queue Destination", 25},
        .version = {UD_SDK_VERSION, sizeof(UD_SDK_VERSION) - 1},
        .configuration_schema = {schema, sizeof(schema) - 1}, .category = UD_COMPONENT_DESTINATION,
        .inputs = 1, .synchronous_input = -1, .error_output = -1,
        .capabilities = UD_COMPONENT_THREAD_SAFE | UD_COMPONENT_STREAM_SAFE | UD_COMPONENT_CONTROL_EVENTS,
        .effects = UD_EFFECT_EXTERNAL_WRITE | UD_EFFECT_REPLAYABLE_PUBLISH | UD_EFFECT_COMMIT_UNKNOWN,
        .configure = configure, .prepare = prepare, .step = step, .finish = finish, .dispose = dispose,
    };
    return ud_registry_register(registry, &descriptor);
}
