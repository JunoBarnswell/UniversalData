#include "pipeline_state.h"
#include "../package/kafka_checkpoint.h"

ud_status ud_mq_source_capture(void *, ud_message_epoch *);
ud_status ud_mq_source_verify(void *, const ud_message_epoch *);
int ud_mq_source_exhausted(void *);
void ud_mq_destination_confirm(void *);

int ud_pipeline_kafka_exhausted(const physical_plan *plan) {
    return plan->kafka && ud_mq_source_exhausted(plan->nodes[plan->kafka_source].instance);
}

ud_status ud_pipeline_kafka_bind(physical_plan *plan, const ud_json *definition) {
    const ud_json *delivery = ud_member(definition, "delivery");
    if (!delivery)
        return UD_OK;
    static const char *keys[] = {"mode", "checkpointTopic", "checkpointPartition"};
    UD_TRY(ud_json_keys(delivery, keys, 3));
    ud_string mode;
    UD_TRY(ud_json_string(ud_member(delivery, "mode"), &mode));
    if (ud_named(mode, "atLeastOnce"))
        return ud_member(delivery, "checkpointTopic") || ud_member(delivery, "checkpointPartition")
                   ? UD_SCHEMA : UD_OK;
    if (!ud_named(mode, "kafkaTransaction"))
        return UD_SCHEMA;
    if (plan->execution->transaction_serial || plan->execution->execution->preview_pipeline)
        return UD_UNSUPPORTED;
    UD_TRY(ud_json_string(ud_member(delivery, "checkpointTopic"), &plan->kafka_topic));
    if (!plan->kafka_topic.length)
        return UD_SCHEMA;
    uint64_t partition = 0;
    if (ud_member(delivery, "checkpointPartition"))
        UD_TRY(ud_json_uint(ud_member(delivery, "checkpointPartition"), &partition));
    if (partition > INT32_MAX)
        return UD_SCHEMA;
    plan->kafka_partition = (int32_t)partition;
    uint32_t destinations = 0;
    plan->kafka_source = UINT32_MAX;
    for (uint32_t i = 0; i < plan->count; i++) {
        node *component = plan->nodes + i;
        uint32_t effects = component->binding.effects;
        int mq_source = ud_named(component->descriptor.id, "mq.source"),
            mq_destination = ud_named(component->descriptor.id, "mq.destination");
        if ((component->descriptor.category == UD_COMPONENT_SOURCE && !mq_source) ||
            ((effects & UD_EFFECT_EXTERNAL_WRITE) && !mq_destination))
            return UD_UNSUPPORTED;
        if ((component->descriptor.capabilities & UD_COMPONENT_STATEFUL) &&
            (!component->descriptor.checkpoint || !component->descriptor.restore ||
             !component->descriptor.state_version))
            return UD_UNSUPPORTED;
        if (!mq_source && !mq_destination)
            continue;
        ud_string connection, kind, address;
        UD_TRY(ud_json_string(ud_member(component->configuration, "connection"), &connection));
        UD_TRY(ud_json_string(ud_member(component->configuration, "targetType"), &kind));
        UD_TRY(ud_json_string(ud_member(component->configuration, "address"), &address));
        if (!ud_named(kind, "topic") || ud_same(address, plan->kafka_topic) ||
            (plan->kafka_connection.length && !ud_same(plan->kafka_connection, connection)))
            return UD_SCHEMA;
        plan->kafka_connection = connection;
        if (mq_source) {
            if (plan->kafka_source != UINT32_MAX)
                return UD_UNSUPPORTED;
            plan->kafka_source = i;
            UD_TRY(ud_json_string(ud_member(component->configuration, "group"), &plan->kafka_group));
        } else
            destinations++;
    }
    if (!destinations || !plan->kafka_group.length || plan->kafka_source == UINT32_MAX)
        return UD_SCHEMA;
    return UD_OK;
}

ud_status ud_pipeline_kafka_open(physical_plan *plan) {
    if (!plan->kafka_topic.length)
        return UD_OK;
    ud_activation *execution = plan->execution;
    uint32_t lanes = execution->consumer_lane_count ? execution->consumer_lane_count : 1;
    uint32_t lane = execution->consumer_lane ? execution->consumer_lane - 1 : 0;
    uint64_t partition = (uint64_t)plan->kafka_partition +
                         (uint64_t)execution->execution->options.worker_member * lanes + lane;
    if (partition > INT32_MAX)
        return UD_OVERFLOW;
    return ud_kafka_checkpoint_open(plan->execution, plan->kafka_connection, plan->kafka_topic,
                                    (int32_t)partition, plan->id, plan->kafka_group,
                                    plan->count, &plan->kafka);
}

ud_status ud_pipeline_kafka_commit(physical_plan *plan, uint64_t epoch, ud_string manifest) {
    ud_message_epoch offsets = {0};
    ud_status status = ud_mq_source_capture(plan->nodes[plan->kafka_source].instance, &offsets);
    ud_bytes payload = {.ctx = plan->execution->execution->ctx};
    const char cluster[] = ",\"kafka\":{\"cluster\":";
    const char array[] = ",\"offsets\":[";
    if (!status)
        status = ud_bytes_put(&payload, manifest.data, manifest.length - 1);
    if (!status)
        status = ud_bytes_put(&payload, cluster, sizeof(cluster) - 1);
    if (!status)
        status = ud_json_quote(&payload, offsets.cluster_id.data, offsets.cluster_id.length);
    if (!status)
        status = ud_bytes_put(&payload, array, sizeof(array) - 1);
    for (uint32_t i = 0; !status && i < offsets.offset_count; i++) {
        const ud_message_offset *offset = offsets.offsets + i;
        if (i)
            status = ud_bytes_put(&payload, ",", 1);
        if (!status)
            status = ud_bytes_put(&payload, "{\"topic\":", 9);
        if (!status)
            status = ud_json_quote(&payload, offset->topic.data, offset->topic.length);
        char text[104];
        int length = snprintf(text, sizeof(text), ",\"partition\":%d,\"next\":%lld}",
                              offset->partition, (long long)offset->next_offset);
        if (!status)
            status = ud_bytes_put(&payload, text, (size_t)length);
    }
    if (!status)
        status = ud_bytes_put(&payload, "]}}", 3);
    if (!status)
        status = ud_kafka_checkpoint_commit(plan->kafka, epoch,
                    (ud_string){(const char *)payload.data, (uint32_t)payload.size}, &offsets);
    ud_bytes_clear(&payload);
    ud_message_epoch_clear(&offsets);
    if (!status)
        for (uint32_t i = 0; i < plan->count; i++)
            if (ud_named(plan->nodes[i].descriptor.id, "mq.destination"))
                ud_mq_destination_confirm(plan->nodes[i].instance);
    return status;
}

ud_status ud_pipeline_kafka_verify(physical_plan *plan) {
    const ud_json *kafka = ud_member(plan->epoch_record, "kafka");
    const ud_json *saved = ud_member(kafka, "offsets");
    if (ud_json_kind(saved) != UD_JSON_ARRAY)
        return UD_CORRUPT;
    ud_message_epoch offsets = {.offset_count = ud_json_size(saved)};
    offsets.allow_advanced_offsets = plan->execution->consumer_lane_count > 1 ||
        plan->execution->execution->options.worker_members > 1;
    UD_TRY(ud_json_string(ud_member(kafka, "cluster"), &offsets.cluster_id));
    if (offsets.offset_count) {
        offsets.offsets = ud_alloc(plan->execution->execution->ctx,
                                   offsets.offset_count * sizeof(*offsets.offsets));
        if (!offsets.offsets)
            return UD_RESOURCE;
    }
    ud_status status = UD_OK;
    for (uint32_t i = 0; !status && i < offsets.offset_count; i++) {
        const ud_json *item = ud_json_at(saved, i);
        ud_message_offset *offset = offsets.offsets + i;
        uint64_t partition = 0, next = 0;
        status = ud_json_string(ud_member(item, "topic"), &offset->topic);
        if (!status)
            status = ud_json_uint(ud_member(item, "partition"), &partition);
        if (!status)
            status = ud_json_uint(ud_member(item, "next"), &next);
        if (!status && (partition > INT32_MAX || next > INT64_MAX))
            status = UD_CORRUPT;
        offset->partition = (int32_t)partition;
        offset->next_offset = (int64_t)next;
    }
    if (!status)
        status = ud_mq_source_verify(plan->nodes[plan->kafka_source].instance, &offsets);
    ud_free(offsets.offsets);
    return status == UD_FENCED ? UD_COMMIT_UNKNOWN : status;
}
