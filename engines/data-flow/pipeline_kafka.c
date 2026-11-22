#include "pipeline_state.h"
#include "kafka_stage.h"
#include "../package/kafka_checkpoint.h"

ud_status ud_mq_source_capture(void *, ud_message_epoch *);
ud_status ud_mq_source_verify(void *, const ud_message_epoch *);
int ud_mq_source_exhausted(void *);
void ud_mq_destination_confirm(void *);

int ud_pipeline_kafka_exhausted(const physical_plan *plan) {
    if (!plan->kafka)
        return 0;
    for (uint32_t i = 0; i < plan->count; i++)
        if (plan->nodes[i].descriptor.category == UD_COMPONENT_SOURCE &&
            !ud_mq_source_exhausted(plan->nodes[i].instance))
            return 0;
    return 1;
}

int ud_pipeline_kafka_terminal_pending(const physical_plan *plan) {
    if (!ud_pipeline_kafka_exhausted(plan))
        return 0;
    for (uint32_t i = 0; i < plan->count; i++)
        if (!plan->nodes[i].context.recovery_terminal)
            return 1;
    return 0;
}

ud_status ud_pipeline_kafka_bind(physical_plan *plan, const ud_json *delivery) {
    int assigned = plan->execution->execution->assignment_count != 0;
    static const char *keys[] = {"mode", "checkpointTopic", "checkpointPartition", "kafkaStage", "kafkaMerge"};
    UD_TRY(ud_json_keys(delivery, keys, 5));
    const ud_json *stage = ud_member(delivery, "kafkaStage");
    if ((stage && !ud_kafka_stage_enabled(stage)) ||
        (ud_member(delivery, "kafkaMerge") && (!stage || !ud_kafka_stage_enabled(ud_member(delivery, "kafkaMerge")))))
        return UD_SCHEMA;
    if (assigned && stage) return UD_UNSUPPORTED;
    plan->kafka_stage = stage != NULL;
    plan->kafka_merge = ud_member(delivery, "kafkaMerge") != NULL;
    if (plan->execution->transaction_isolation || plan->execution->execution->preview_pipeline)
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
    plan->kafka_merge_sort = UINT32_MAX;
    ud_string source_topics[2];
    for (uint32_t i = 0; i < plan->count; i++) {
        node *component = plan->nodes + i;
        if (ud_named(component->descriptor.id, "sort"))
            plan->kafka_merge_sort = i;
        uint32_t effects = component->binding.effects;
        int mq_source = ud_named(component->descriptor.id, "mq.source"),
            mq_destination = ud_named(component->descriptor.id, "mq.destination");
        if ((component->descriptor.category == UD_COMPONENT_SOURCE && !mq_source) ||
            ((effects & UD_EFFECT_EXTERNAL_WRITE) && !mq_destination))
            return UD_UNSUPPORTED;
        if (assigned && (component->descriptor.capabilities & UD_COMPONENT_BLOCKING))
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
            if (assigned && (!ud_same(address, plan->execution->execution->partition_topic) ||
                ud_member(component->configuration, "partition") || ud_member(component->configuration, "endOffsets")))
                return UD_SCHEMA;
            uint32_t index = plan->kafka_sources;
            if (index >= (plan->kafka_stage ? 2u : 1u))
                return UD_UNSUPPORTED;
            ud_string group;
            UD_TRY(ud_json_string(ud_member(component->configuration, "group"), &group));
            if (index && (ud_same(address, source_topics[0]) || !ud_same(group, plan->kafka_group)))
                return UD_SCHEMA;
            if (plan->kafka_stage && !ud_kafka_stage_enabled(ud_member(component->configuration, "partitionByWorker")))
                return UD_SCHEMA;
            source_topics[index] = address;
            plan->kafka_group = group;
            plan->kafka_sources++;
        } else {
            if (plan->kafka_merge) {
                uint64_t output_partition;
                UD_TRY(ud_json_uint(ud_member(component->configuration, "partition"), &output_partition));
                if (output_partition > INT32_MAX || ud_kafka_stage_enabled(ud_member(component->configuration, "publishStage")))
                    return UD_SCHEMA;
            }
            destinations++;
        }
    }
    if (!destinations || !plan->kafka_group.length || !plan->kafka_sources)
        return UD_SCHEMA;
    if (plan->kafka_merge && (plan->count != 3 || plan->paths != 2 || plan->kafka_sources != 1 ||
        destinations != 1 || plan->kafka_merge_sort == UINT32_MAX))
        return UD_SCHEMA;
    return UD_OK;
}

ud_status ud_pipeline_kafka_open(physical_plan *plan) {
    if (!plan->kafka_topic.length)
        return UD_OK;
    ud_activation *execution = plan->execution;
    uint32_t lanes = execution->consumer_lane_count ? execution->consumer_lane_count : 1;
    uint32_t lane = execution->consumer_lane ? execution->consumer_lane - 1 : 0;
    const ud_kafka_partition_assignment *assigned = ud_activation_partition(execution);
    uint64_t partition = (uint64_t)plan->kafka_partition + (assigned
        ? (uint64_t)assigned->partition : (uint64_t)execution->execution->options.worker_member * lanes + lane);
    if (partition > INT32_MAX)
        return UD_OVERFLOW;
    return ud_kafka_checkpoint_open(plan->execution, plan->kafka_connection, plan->kafka_topic,
                                    (int32_t)partition, plan->id, plan->kafka_group,
                                    plan->count, &plan->kafka);
}

static ud_status capture_inputs(physical_plan *plan, ud_message_epoch *parts, ud_message_epoch *out) {
    if (plan->kafka_sources == 1) {
        for (uint32_t i = 0; i < plan->count; i++)
            if (plan->nodes[i].descriptor.category == UD_COMPONENT_SOURCE) {
                UD_TRY(ud_mq_source_capture(plan->nodes[i].instance, parts));
                *out = *parts;
                return UD_OK;
            }
    }
    uint32_t index = 0;
    for (uint32_t i = 0; i < plan->count; i++)
        if (plan->nodes[i].descriptor.category == UD_COMPONENT_SOURCE)
            UD_TRY(ud_mq_source_capture(plan->nodes[i].instance, parts + index++));
    if (!ud_same(parts[0].cluster_id, parts[1].cluster_id) ||
        !ud_same(parts[0].group_metadata, parts[1].group_metadata))
        return UD_FENCED;
    out->cluster_id = parts[0].cluster_id;
    out->group_metadata = parts[0].group_metadata;
    out->offset_count = parts[0].offset_count + parts[1].offset_count;
    if (out->offset_count) {
        out->offsets = ud_alloc(plan->execution->execution->ctx, out->offset_count * sizeof(*out->offsets));
        if (!out->offsets)
            return UD_RESOURCE;
        for (uint32_t i = 0, at = 0; i < index; i++)
            for (uint32_t j = 0; j < parts[i].offset_count; j++)
                out->offsets[at++] = parts[i].offsets[j];
    }
    return UD_OK;
}

ud_status ud_pipeline_kafka_commit(physical_plan *plan, uint64_t epoch, ud_string manifest) {
    ud_message_epoch parts[2] = {{0}}, offsets = {0};
    ud_status status = capture_inputs(plan, parts, &offsets);
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
    if (plan->kafka_sources > 1)
        ud_free(offsets.offsets);
    for (uint32_t i = 0; i < plan->kafka_sources; i++)
        ud_message_epoch_clear(parts + i);
    if (!status)
        for (uint32_t i = 0; i < plan->count; i++)
            if (ud_named(plan->nodes[i].descriptor.id, "mq.destination"))
                ud_mq_destination_confirm(plan->nodes[i].instance);
    return status;
}

ud_status ud_pipeline_kafka_verify(physical_plan *plan, node *source) {
    const ud_json *kafka = ud_member(plan->epoch_record, "kafka");
    const ud_json *saved = ud_member(kafka, "offsets");
    if (ud_json_kind(saved) != UD_JSON_ARRAY)
        return UD_CORRUPT;
    ud_message_epoch offsets = {0};
    uint32_t count = ud_json_size(saved);
    offsets.allow_advanced_offsets = !plan->kafka_stage && (plan->execution->consumer_lane_count > 1 ||
        plan->execution->execution->options.worker_members > 1);
    UD_TRY(ud_json_string(ud_member(kafka, "cluster"), &offsets.cluster_id));
    ud_string topic;
    UD_TRY(ud_json_string(ud_member(source->configuration, "address"), &topic));
    if (count) {
        offsets.offsets = ud_alloc(plan->execution->execution->ctx,
                                   count * sizeof(*offsets.offsets));
        if (!offsets.offsets)
            return UD_RESOURCE;
    }
    ud_status status = UD_OK;
    for (uint32_t i = 0; !status && i < count; i++) {
        const ud_json *item = ud_json_at(saved, i);
        ud_message_offset offset = {0};
        uint64_t partition = 0, next = 0;
        status = ud_json_string(ud_member(item, "topic"), &offset.topic);
        if (!status)
            status = ud_json_uint(ud_member(item, "partition"), &partition);
        if (!status)
            status = ud_json_uint(ud_member(item, "next"), &next);
        if (!status && (partition > INT32_MAX || next > INT64_MAX))
            status = UD_CORRUPT;
        if (!status && (plan->kafka_sources == 1 || ud_same(topic, offset.topic))) {
            offset.partition = (int32_t)partition;
            offset.next_offset = (int64_t)next;
            offsets.offsets[offsets.offset_count++] = offset;
        }
    }
    if (!status)
        status = ud_mq_source_verify(source->instance, &offsets);
    ud_free(offsets.offsets);
    return status == UD_FENCED ? UD_COMMIT_UNKNOWN : status;
}
