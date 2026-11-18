#include "pipeline_state.h"
#include "../package/kafka_checkpoint.h"

int ud_pipeline_epoch_target_supported(const physical_plan *p, const node *n) {
    uint32_t effects = n->binding.effects;
    return !(effects & UD_EFFECT_EXTERNAL_WRITE) ||
           (effects & (UD_EFFECT_RECEIPTED | UD_EFFECT_IDEMPOTENT)) ||
           (p->execution->transaction_epoch && (effects & UD_EFFECT_TRANSACTIONAL)) ||
           (p->allow_replayable_publish && (effects & UD_EFFECT_REPLAYABLE_PUBLISH));
}
ud_status ud_pipeline_epoch_component_key(physical_plan *p, node *n, char out[65]) {
    ud_bytes b = {.ctx = p->execution->execution->ctx};
    ud_status s = ud_put_string(&b, p->id.data, p->id.length);
    if (!s)
        s = ud_put_string(&b, n->id.data, n->id.length);
    uint8_t hash[32];
    if (!s) {
        ud_hash(b.data, b.size, hash);
        for (uint32_t i = 0; i < 32; i++)
            snprintf(out + 2 * i, 3, "%02x", hash[i]);
    }
    ud_bytes_clear(&b);
    return s;
}
ud_status ud_pipeline_stage_epoch(physical_plan *p, node *n, uint64_t epoch) {
    if (atomic_load(&n->staged_epoch) == epoch)
        return UD_OK;
    if (ud_transaction_active(&n->activation) && !n->activation.transaction_epoch)
        return UD_UNSUPPORTED;
    if (!ud_pipeline_epoch_target_supported(p, n))
        return UD_UNSUPPORTED;
    if ((n->descriptor.capabilities & UD_COMPONENT_STATEFUL) && !n->descriptor.checkpoint)
        return UD_UNSUPPORTED;
    if (n->merge_base_sealed) {
        UD_TRY(ud_kafka_checkpoint_reuse(p->kafka, (uint32_t)(n - p->nodes), epoch));
        atomic_store(&n->staged_epoch, epoch);
        return UD_OK;
    }
    ud_batch *state = NULL;
    ud_status status = UD_OK;
    if (n->descriptor.checkpoint)
        status = n->descriptor.checkpoint(n->instance, &state);
    else {
        ud_field field = {.name = "stateless", .name_length = 9, .type = UD_INT64};
        ud_value value = {.type = UD_INT64};
        status = ud_batch_create(p->execution->execution->ctx, &field, 1, 1, &state);
        if (!status)
            status = ud_batch_append(state, &value, 1);
        if (!status)
            status = ud_batch_seal(state);
    }
    char key[65];
    if (!status)
        status = ud_pipeline_epoch_component_key(p, n, key);
    if (!status)
        status = p->kafka ? ud_kafka_checkpoint_stage(p->kafka, (uint32_t)(n - p->nodes), epoch, state)
                          : ud_checkpoint_snapshot_write(p->execution, (ud_string){key, 64}, epoch, state);
    ud_batch_destroy(state);
    if (!status) {
        if (p->kafka_merge && n == p->nodes + p->kafka_merge_sort && n->ended == n->descriptor.inputs) {
            n->merge_base_rows = n->produced;
            n->merge_base_sealed = 1;
        }
        atomic_store(&n->staged_epoch, epoch);
    }
    return status;
}
ud_status ud_pipeline_commit_epoch(physical_plan *p, uint64_t epoch, int terminal) {
    ud_mutex_lock(&p->epoch_lock);
    ud_status status = UD_OK;
    if (epoch <= atomic_load(&p->confirmed_epoch)) {
        ud_mutex_unlock(&p->epoch_lock);
        return UD_OK;
    }
    int active = terminal || (p->kafka_merge &&
        atomic_load(&p->nodes[p->kafka_merge_sort].staged_epoch) == epoch);
    for (uint32_t i = 0; !active && i < p->count; i++)
        if (p->nodes[i].descriptor.category == UD_COMPONENT_SOURCE &&
            p->nodes[i].descriptor.confirm_epoch && atomic_load(&p->nodes[i].staged_epoch) == epoch)
            active = 1;
    if (!active) {
        ud_mutex_unlock(&p->epoch_lock);
        return UD_OK;
    }
    for (uint32_t i = 0; !status && i < p->count; i++)
        if (atomic_load(&p->nodes[i].state) == UD_EXEC_SUCCEEDED &&
            atomic_load(&p->nodes[i].staged_epoch) != epoch)
            status = ud_pipeline_stage_epoch(p, p->nodes + i, epoch);
    if (status) {
        ud_mutex_unlock(&p->epoch_lock);
        return status;
    }

    for (uint32_t i = 0; i < p->count; i++)
        if (atomic_load(&p->nodes[i].staged_epoch) != epoch) {
            ud_mutex_unlock(&p->epoch_lock);
            return UD_OK;
        }
    ud_bytes payload = {.ctx = p->execution->execution->ctx, .limit = 16u * 1024u * 1024u};
    char header[96];
    int n = snprintf(header, sizeof(header), "{\"epoch\":%llu,\"sources\":[",
                     (unsigned long long)epoch);
    status = ud_bytes_put(&payload, header, (size_t)n);
    uint32_t sources = 0;
    for (uint32_t i = 0; !status && i < p->count; i++) {
        node *source = p->nodes + i;
        if (source->descriptor.category != UD_COMPONENT_SOURCE)
            continue;
        if (sources++)
            status = ud_bytes_put(&payload, ",", 1);
        if (!status)
            status = ud_bytes_put(&payload, "{\"id\":", 6);
        if (!status)
            status = ud_json_quote(&payload, source->id.data, source->id.length);
        if (!status)
            status = ud_bytes_put(&payload, ",\"position\":", 12);
        if (!status)
            status = ud_json_quote(&payload, source->source_position.data,
                                   source->source_position.length);
        char schema_version[64];
        int schema_size =
            snprintf(schema_version, sizeof(schema_version), ",\"schemaVersion\":%llu",
                     (unsigned long long)source->schema_version);
        if (!status)
            status = ud_bytes_put(&payload, schema_version, (size_t)schema_size);
        if (!status)
            status = ud_bytes_put(&payload, "}", 1);
    }
    const char *next = "],\"components\":[";
    if (!status)
        status = ud_bytes_put(&payload, next, strlen(next));
    for (uint32_t i = 0; !status && i < p->count; i++) {
        node *component = p->nodes + i;
        if (i)
            status = ud_bytes_put(&payload, ",", 1);
        if (!status)
            status = ud_bytes_put(&payload, "{\"id\":", 6);
        if (!status)
            status = ud_json_quote(&payload, component->id.data, component->id.length);
        char metadata[320];
        uint32_t ports = component->descriptor.inputs;
        uint64_t declared = ports == 64 ? UINT64_MAX : (UINT64_C(1) << ports) - 1;
        uint64_t ended = declared & ~ud_pipeline_input_mask(p, component, 1);
        int length = snprintf(
            metadata, sizeof(metadata),
            ",\"stateVersion\":%u,\"endedInputs\":%llu,\"terminal\":%s,\"producedRows\":%llu,"
            "\"processedRows\":%llu,\"frontier\":",
            component->descriptor.state_version, (unsigned long long)ended,
            atomic_load(&component->state) == UD_EXEC_SUCCEEDED ? "true" : "false",
            (unsigned long long)component->produced, (unsigned long long)component->processed);
        if (!status)
            status = ud_bytes_put(&payload, metadata, (size_t)length);
        if (!status)
            status = ud_pipeline_frontier_write(&payload, &component->frontier, ports);
        if (!status && component->merge_base_sealed) {
            length = snprintf(metadata, sizeof(metadata), ",\"mergeBaseRows\":%llu",
                              (unsigned long long)component->merge_base_rows);
            status = ud_bytes_put(&payload, metadata, (size_t)length);
        }
        if (!status)
            status = ud_bytes_put(&payload, "}", 1);
    }
    if (!status)
        status = ud_bytes_put(&payload, "]}", 2);
    ud_string manifest = {(char *)payload.data, (uint32_t)payload.size};
    if (!status)
        status = p->kafka ? ud_pipeline_kafka_commit(p, epoch, manifest)
                          : ud_checkpoint_epoch_commit(p->execution, p->id, manifest);
    if (!status && !p->kafka && ud_transaction_active(p->execution))
        status = ud_transaction_commit_epoch(p->execution);
    if (!status) {
        uint64_t previous = atomic_load(&p->confirmed_epoch);
        atomic_store(&p->confirmed_epoch, epoch);
        if (previous && !p->kafka)
            for (uint32_t i = 0; !status && i < p->count; i++)
                if (p->nodes[i].descriptor.checkpoint) {
                    char key[65];
                    status = ud_pipeline_epoch_component_key(p, p->nodes + i, key);
                    if (!status)
                        status = ud_checkpoint_snapshot_forget(p->execution, (ud_string){key, 64},
                                                               previous, 1);
                }
        if (p->group)
            for (uint32_t i = 0; i < p->tree_count; i++)
                ud_work_group_ready(p->group, i);
    }
    ud_bytes_clear(&payload);
    ud_mutex_unlock(&p->epoch_lock);
    return status;
}
