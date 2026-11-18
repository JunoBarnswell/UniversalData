#include "mq_payload.h"
#include "../package/kafka_checkpoint.h"

typedef struct mq_source {
    ud_component_context context;
    const ud_batch *schema;
    ud_json *config;
    ud_connection_lease *lease;
    ud_schema_registry *registry;
    ud_id identity;
    ud_message_partition_end *end_offsets;
    uint64_t *deliveries, epoch, records;
    uint32_t count, prefetch;
    int format, error_policy, include_input, exhausted;
    char position[32];
} mq_source;

static ud_status bind_source(const ud_component_context *context, const ud_json *config,
                             ud_mq_settings *settings, ud_message_subscription *subscription,
                             int *error_policy, int *include_input) {
    UD_TRY(ud_mq_settings_parse(context, config, settings));
    if (settings->target.kind == UD_MESSAGE_EXCHANGE)
        return UD_SCHEMA;
    *subscription = (ud_message_subscription){.struct_size = sizeof(*subscription),
                      .abi_version = UD_ABI_VERSION, .source = settings->target,
                      .max_message_bytes = settings->maximum_bytes,
                      .fixed_partition = settings->fixed_partition};
    const char *names[] = {"group", "subscription", "filter"};
    ud_string *values[] = {&subscription->group, &subscription->subscription, &subscription->filter};
    for (uint32_t i = 0; i < 3; i++)
        if (ud_member(config, names[i]))
            UD_TRY(ud_json_string(ud_member(config, names[i]), values[i]));
    ud_string initial;
    UD_TRY(ud_json_string(ud_member(config, "initialPosition"), &initial));
    subscription->initial_position = ud_named(initial, "earliest") ? UD_MESSAGE_POSITION_EARLIEST :
                                     ud_named(initial, "latest") ? UD_MESSAGE_POSITION_LATEST :
                                     UD_MESSAGE_POSITION_REQUIRE;
    uint64_t prefetch, bytes;
    UD_TRY(ud_json_uint(ud_member(config, "prefetch"), &prefetch));
    UD_TRY(ud_json_uint(ud_member(config, "maxInFlightBytes"), &bytes));
    if (bytes < settings->maximum_bytes)
        return UD_SCHEMA;
    subscription->prefetch = (uint32_t)prefetch;
    subscription->max_in_flight_bytes = bytes;
    const ud_json *ends = ud_member(config, "endOffsets");
    if (ends && (settings->target.kind != UD_MESSAGE_TOPIC || !ud_json_size(ends)))
        return UD_SCHEMA;
    if (subscription->fixed_partition) {
        if (!subscription->group.length)
            return UD_SCHEMA;
        if (ends) {
            uint64_t partition;
            UD_TRY(ud_json_uint(ud_member(ud_json_at(ends, 0), "partition"), &partition));
            if (ud_json_size(ends) != 1 || partition != subscription->fixed_partition - 1)
                return UD_SCHEMA;
        }
    }
    UD_TRY(ud_error_policy(config, "errorPolicy", UD_ERROR_FAIL, error_policy));
    if (*error_policy == UD_ERROR_REDIRECT && !(context->connected_outputs & 2))
        return UD_SCHEMA;
    *include_input = 0;
    if (ud_member(config, "includeInput")) {
        const char *text;
        uint32_t length;
        UD_TRY(ud_json_text(ud_member(config, "includeInput"), &text, &length));
        *include_input = length == 4 && !memcmp(text, "true", 4);
    }
    return UD_OK;
}

static int compare_end(const void *a, const void *b) {
    int32_t x = ((const ud_message_partition_end *)a)->partition;
    int32_t y = ((const ud_message_partition_end *)b)->partition;
    return (x > y) - (x < y);
}

static ud_status bind_ends(mq_source *source, ud_message_subscription *subscription) {
    const ud_json *ends = ud_member(source->config, "endOffsets");
    if (!ends)
        return UD_OK;
    uint32_t count = ud_json_size(ends);
    source->end_offsets = ud_alloc(source->context.memory, count * sizeof(*source->end_offsets));
    if (!source->end_offsets)
        return UD_RESOURCE;
    for (uint32_t i = 0; i < count; i++) {
        const ud_json *end = ud_json_at(ends, i);
        uint64_t partition, next;
        UD_TRY(ud_json_uint(ud_member(end, "partition"), &partition));
        UD_TRY(ud_json_uint(ud_member(end, "nextOffset"), &next));
        if (partition > INT32_MAX || next > INT64_MAX)
            return UD_SCHEMA;
        source->end_offsets[i] = (ud_message_partition_end){(int32_t)partition, (int64_t)next};
    }
    qsort(source->end_offsets, count, sizeof(*source->end_offsets), compare_end);
    for (uint32_t i = 1; i < count; i++)
        if (source->end_offsets[i - 1].partition == source->end_offsets[i].partition)
            return UD_SCHEMA;
    subscription->end_offsets = source->end_offsets;
    subscription->end_offset_count = count;
    return UD_OK;
}

static ud_status configure(void *user, const ud_component_context *context, ud_string text,
                           const ud_batch *const *inputs, ud_batch **outputs) {
    (void)user;
    (void)inputs;
    ud_json *config = NULL;
    UD_TRY(ud_builtin_parse(context, text, &config));
    ud_mq_settings settings;
    ud_message_subscription subscription;
    int error_policy, include_input;
    ud_status status = bind_source(context, config, &settings, &subscription, &error_policy,
                                   &include_input);
    if (!status)
        status = ud_mq_schema(context, config, &settings, outputs);
    if (!status)
        status = ud_error_schema(&context->errors, NULL, include_input, outputs + 1);
    ud_json_destroy(config);
    return status;
}

static void dispose(void *instance, ud_status result) {
    (void)result;
    mq_source *source = instance;
    if (!source)
        return;
    ud_schema_registry_close(source->registry);
    if (source->lease)
        (void)ud_connection_lease_release(source->lease);
    ud_json_destroy(source->config);
    ud_free(source->deliveries);
    ud_free(source->end_offsets);
    ud_free(source);
}

static ud_status prepare(void *user, const ud_component_context *context, ud_string text,
                         const ud_batch *const *inputs, const ud_batch *const *outputs,
                         void **out) {
    (void)user;
    (void)inputs;
    if (!context->execution->journal && !context->activation->kafka_checkpoint)
        return UD_UNSUPPORTED;
    mq_source *source = ud_alloc(context->memory, sizeof(*source));
    if (!source)
        return UD_RESOURCE;
    source->context = *context;
    source->schema = outputs[0];
    source->epoch = context->recovery_epoch;
    ud_mq_settings settings = {0};
    ud_message_subscription subscription = {0};
    ud_status status = ud_builtin_parse(context, text, &source->config);
    if (!status)
        status = bind_source(context, source->config, &settings, &subscription,
                             &source->error_policy, &source->include_input);
    if (!status)
        status = bind_ends(source, &subscription);
    if (!status && context->activation->kafka_checkpoint)
        subscription.exclusive_transaction = context->execution->options.worker_members <= 1 &&
            context->activation->consumer_lane_count <= 1;
    if (!status) {
        source->format = settings.format;
        source->prefetch = subscription.prefetch;
        source->deliveries = ud_alloc(context->memory, source->prefetch * sizeof(*source->deliveries));
        if (!source->deliveries)
            status = UD_RESOURCE;
    }
    if (!status)
        status = ud_id_make(context->memory, settings.connection, settings.target.name,
                            &source->identity);
    if (!status && settings.format == UD_MQ_AVRO)
        status = ud_schema_registry_open(context, settings.registry_connection, settings.subject,
                                         settings.schema_version, &source->registry);
    if (!status) {
        uint64_t capabilities = UD_CONNECT_MESSAGE_SUBSCRIBE;
        if (context->activation->kafka_checkpoint || subscription.end_offset_count ||
            subscription.fixed_partition)
            capabilities |= UD_CONNECT_MESSAGE_TRANSACTION;
        status = ud_activation_acquire_connection(context->activation, settings.connection,
                                                  capabilities, &source->lease);
    }
    if (!status)
        status = ud_connection_lease_message_subscribe(source->lease, &subscription);
    if (status)
        dispose(source, status);
    else
        *out = source;
    return status;
}

static ud_status source_position(mq_source *source, const ud_message *message, ud_row_info *out) {
    char scope[33], partition[28];
    ud_id_format(source->identity, scope);
    ud_id identity = source->identity;
    uint64_t position = 0;
    if (message->partition >= 0 && message->offset >= 0) {
        int size = snprintf(partition, sizeof(partition), "partition-%d", message->partition);
        UD_TRY(ud_id_make(source->context.memory, (ud_string){scope, 32},
                          (ud_string){partition, (uint32_t)size}, &identity));
        position = (uint64_t)message->offset;
    } else if (message->id.length)
        UD_TRY(ud_id_make(source->context.memory, (ud_string){scope, 32}, message->id, &identity));
    else {
        int size = snprintf(partition, sizeof(partition), "epoch-%llu",
                             (unsigned long long)source->epoch);
        UD_TRY(ud_id_make(source->context.memory, (ud_string){scope, 32},
                          (ud_string){partition, (uint32_t)size}, &identity));
        position = source->records;
    }
    return ud_row_info_source(identity, position, out);
}

static ud_status emit_barrier(mq_source *source) {
    int length = snprintf(source->position, sizeof(source->position), "mq-%llu",
                           (unsigned long long)(source->epoch + 1));
    ud_component_event event = {.struct_size = sizeof(event), .abi_version = UD_ABI_VERSION,
                                .kind = UD_COMPONENT_CHECKPOINT_BARRIER,
                                .epoch = source->epoch + 1,
                                .source_position = {source->position, (uint32_t)length}};
    UD_TRY(source->context.emit_event(source->context.emit_user, 0, &event));
    if (source->context.connected_outputs & 2)
        UD_TRY(source->context.emit_event(source->context.emit_user, 1, &event));
    source->epoch++;
    return UD_MORE;
}

static ud_status step(void *instance, const ud_component_event *event) {
    (void)event;
    mq_source *source = instance;
    if (source->count)
        return emit_barrier(source);
    if (source->exhausted || ud_activation_stopping(source->context.activation))
        return UD_END;
    uint32_t limit = source->context.recommended_batch_rows(source->context.emit_user);
    if (limit > source->prefetch)
        limit = source->prefetch;
    ud_batch *batch = NULL, *errors = NULL;
    ud_status status = ud_batch_create(source->context.memory, source->schema->schema->fields,
                                       source->schema->columns, limit, &batch);
    if (!status && source->error_policy == UD_ERROR_REDIRECT)
        status = ud_error_batch(&source->context.errors, source->include_input, limit, &errors);
    uint32_t rejected = 0;
    while (!status && source->count < limit) {
        ud_message message = {0};
        status = ud_connection_lease_message_receive(source->lease, 5, &message);
        if (status == UD_BUSY || status == UD_END) {
            source->exhausted = status == UD_END;
            if (source->count)
                status = UD_OK;
            break;
        }
        if (status)
            break;
        ud_row_info provenance = {0};
        ud_id column = {0};
        status = source_position(source, &message, &provenance);
        if (!status) {
            ud_status conversion = ud_mq_decode(&message, source->format, source->registry,
                                                 batch, &column);
            if (!conversion)
                status = ud_batch_set_row_info(batch, batch->rows - 1, &provenance);
            else if (source->error_policy == UD_ERROR_FAIL || !ud_record_error_recoverable(conversion))
                status = conversion;
            else {
                rejected++;
                if (errors)
                    status = ud_error_append(&source->context.errors, errors,
                        &(ud_record_error){.struct_size = sizeof(ud_record_error),
                          .abi_version = UD_ABI_VERSION, .source = provenance, .column = column,
                          .rule = {"mq.decode", 9}, .rule_version = 1, .code = conversion,
                          .policy = source->error_policy, .input_format = {"mq", 2},
                          .raw = (const uint8_t *)message.payload.data,
                          .raw_length = message.payload.length});
            }
        }
        if (!status) {
            source->deliveries[source->count++] = message.delivery;
            source->records++;
        }
        ud_message_clear(&message);
    }
    if (!status && batch->rows)
        status = source->context.emit(source->context.emit_user, 0, batch);
    if (!status && errors && errors->rows)
        status = source->context.emit(source->context.emit_user, 1, errors);
    if (!status && source->count)
        ud_task_rows(source->context.activation, source->count, 0, rejected);
    ud_batch_destroy(batch);
    ud_batch_destroy(errors);
    return status ? status : UD_MORE;
}

static ud_status confirm(void *instance, uint64_t epoch, ud_string position) {
    mq_source *source = instance;
    if (epoch != source->epoch || !source->count || !ud_named(position, source->position))
        return UD_FENCED;
    if (source->context.activation->kafka_checkpoint)
        UD_TRY(ud_connection_lease_message_release_epoch(source->lease, source->deliveries,
                                                         source->count));
    else
        UD_TRY(ud_connection_lease_message_acknowledge(source->lease, source->deliveries,
                                                       source->count));
    source->count = 0;
    return UD_OK;
}

ud_status ud_mq_source_capture(void *instance, ud_message_epoch *epoch) {
    mq_source *source = instance;
    return ud_connection_lease_message_capture_epoch(source->lease, source->deliveries,
                                                       source->count, epoch);
}

int ud_mq_source_exhausted(void *instance) {
    return ((mq_source *)instance)->exhausted;
}

ud_status ud_mq_source_verify(void *instance, const ud_message_epoch *epoch) {
    mq_source *source = instance;
    return ud_connection_lease_message_verify_epoch(source->lease, epoch);
}

static ud_status finish(void *instance) {
    return ((mq_source *)instance)->count ? UD_CORRUPT : UD_OK;
}

static ud_status describe(void *user, const ud_component_context *context, ud_string text,
                           const ud_batch *const *inputs, ud_component_plan *plan) {
    (void)user; (void)inputs;
    ud_json *config = NULL;
    UD_TRY(ud_builtin_parse(context, text, &config));
    plan->boundedness = ud_member(config, "endOffsets") ? UD_SOURCE_BOUNDED : UD_SOURCE_CONTINUOUS;
    ud_json_destroy(config);
    return UD_OK;
}

ud_status ud_mq_source_register(ud_registry *registry) {
    static const char schema[] = "{\"type\":\"object\",\"additionalProperties\":false,"
        "\"required\":[\"connection\",\"targetType\",\"address\"],\"properties\":{" UD_MQ_PAYLOAD_PROPERTIES ","
        "\"schema\":{\"type\":\"object\"},\"group\":{\"type\":\"string\"},\"subscription\":{\"type\":\"string\"},"
        "\"filter\":{\"type\":\"string\"},\"initialPosition\":{\"enum\":[\"require\",\"earliest\",\"latest\"],"
        "\"default\":\"require\"},\"prefetch\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":256,\"default\":32},"
        "\"maxInFlightBytes\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":67108864,\"default\":4194304},"
        "\"endOffsets\":{\"type\":\"array\",\"minItems\":1,\"maxItems\":65536,\"items\":{\"type\":\"object\","
        "\"required\":[\"partition\",\"nextOffset\"],\"additionalProperties\":false,\"properties\":{\"partition\":{"
        "\"type\":\"integer\",\"minimum\":0},\"nextOffset\":{\"type\":\"integer\",\"minimum\":0}}}},"
        "\"errorPolicy\":{\"enum\":[\"fail\",\"redirect\",\"ignore\"],\"default\":\"fail\"},"
        "\"includeInput\":{\"type\":\"boolean\",\"default\":false}}}";
    const ud_component_descriptor descriptor = {
        .struct_size = sizeof(descriptor), .abi_version = UD_ABI_VERSION,
        .id = {"mq.source", 9}, .name = {"Message Queue Source", 20},
        .version = {UD_SDK_VERSION, sizeof(UD_SDK_VERSION) - 1},
        .configuration_schema = {schema, sizeof(schema) - 1}, .category = UD_COMPONENT_SOURCE,
        .outputs = 2, .optional_outputs = 2, .synchronous_input = -1, .error_output = 1,
        .capabilities = UD_COMPONENT_REPLAY_SAFE | UD_COMPONENT_THREAD_SAFE | UD_COMPONENT_CONTINUOUS,
        .effects = UD_EFFECT_EXTERNAL_READ | UD_EFFECT_SOURCE_ACK,
        .configure = configure, .describe_plan = describe, .prepare = prepare, .step = step,
        .finish = finish, .dispose = dispose, .confirm_epoch = confirm,
    };
    return ud_registry_register(registry, &descriptor);
}
