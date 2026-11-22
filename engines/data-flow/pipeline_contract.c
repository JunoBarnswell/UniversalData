#include "pipeline_state.h"
#include "builtins_internal.h"
#include "ud/cursor.h"
#include "../package/incremental.h"

static int configured(const node *n, const char *key, const char *expected) {
    ud_string value;
    return !ud_json_string(ud_member(n->configuration, key), &value) && ud_named(value, expected);
}

static ud_status bind_event_receipt(physical_plan *p, const ud_json *definition) {
    ud_activation *activation = p->execution;
    const ud_execution_options *options = &activation->execution->options;
    if (p->count != 2 || p->paths != 1 || ud_member(definition, "parallelism") ||
        options->worker_members > 1 || options->consumer_lanes > 1 ||
        activation->consumer_lane_count > 1 || activation->parallel ||
        activation->transaction_isolation || ud_transaction_active(activation))
        return ud_fail(activation->execution->ctx, UD_UNSUPPORTED,
                       "eventReceipt requires a direct single-worker flow outside a package transaction");

    edge *path = p->edges;
    node *source = p->nodes + path->from, *target = p->nodes + path->to;
    const char *metadata = NULL;
    uint32_t metadata_length = 0;
    int source_errors, target_errors;
    UD_TRY(ud_error_policy(source->configuration, "errorPolicy", UD_ERROR_FAIL, &source_errors));
    UD_TRY(ud_error_policy(target->configuration, "errorPolicy", UD_ERROR_FAIL, &target_errors));
    if (path->output != 0 || path->input != 0 ||
        !ud_named(source->descriptor.id, "mq.source") || !configured(source, "targetType", "queue") ||
        !configured(source, "initialPosition", "require") ||
        ud_json_text(ud_member(source->configuration, "includeMetadata"), &metadata, &metadata_length) ||
        !ud_named((ud_string){metadata, metadata_length}, "false") ||
        source_errors != UD_ERROR_FAIL || ud_member(source->configuration, "group") ||
        ud_member(source->configuration, "subscription") || ud_member(source->configuration, "filter") ||
        ud_member(source->configuration, "fromStage") || ud_member(source->configuration, "endOffsets"))
        return ud_pipeline_node_failed(source, UD_UNSUPPORTED, "eventReceipt queue source contract");
    if (!ud_named(target->descriptor.id, "sql.destination") || target_errors != UD_ERROR_FAIL ||
        (!configured(target, "mode", "append") &&
         !(configured(target, "mode", "upsert") && ud_json_size(ud_member(target->configuration, "keys")))) ||
        (ud_member(target->configuration, "delivery") && !configured(target, "delivery", "receipt")) ||
        ud_member(target->configuration, "beforeField"))
        return ud_pipeline_node_failed(target, UD_UNSUPPORTED, "eventReceipt SQL destination contract");

    ud_bytes binding = {.ctx = activation->execution->ctx};
    ud_status status = ud_bytes_put(&binding, activation->package->fingerprint,
                                   sizeof(activation->package->fingerprint));
    if (!status)
        status = ud_put_string(&binding, source->json, source->json_length);
    if (!status)
        status = ud_put_string(&binding, target->json, target->json_length);
    if (!status)
        ud_hash(binding.data, binding.size, p->event_receipt_binding);
    ud_bytes_clear(&binding);
    return status;
}

ud_status ud_pipeline_delivery_bind(physical_plan *p, const ud_json *definition) {
    const ud_json *delivery = ud_member(definition, "delivery");
    int assigned = p->execution->execution->assignment_count != 0;
    if (!delivery)
        return assigned ? UD_UNSUPPORTED : UD_OK;
    UD_TRY(ud_json_string(ud_member(delivery, "mode"), &p->delivery_mode));
    if (ud_named(p->delivery_mode, "kafkaTransaction"))
        return UD_OK;
    static const char *keys[] = {"mode"};
    UD_TRY(ud_json_keys(delivery, keys, 1));
    if (ud_named(p->delivery_mode, "eventReceipt"))
        return assigned ? UD_UNSUPPORTED : bind_event_receipt(p, definition);
    return !assigned && ud_named(p->delivery_mode, "atLeastOnce") ? UD_OK : UD_SCHEMA;
}

static int cdc_source(const node *n) {
    const char suffix[] = ".cdc.source";
    ud_string id = n->descriptor.id;
    return n->descriptor.category == UD_COMPONENT_SOURCE && id.length > sizeof(suffix) - 1 &&
           !memcmp(id.data + id.length - (sizeof(suffix) - 1), suffix, sizeof(suffix) - 1);
}

int ud_pipeline_port_change(const node *n, uint32_t output) {
    return (int32_t)output == n->descriptor.error_output ? UD_CHANGES_APPEND : n->change_mode;
}

static int input_changes(const physical_plan *p, const node *n) {
    int highest = UD_CHANGES_APPEND;
    for (uint32_t i = 0; i < n->ports.input_count; i++) {
        if (n->inputs[i] == UINT32_MAX)
            continue;
        const edge *v = p->edges + n->inputs[i];
        int changes = ud_pipeline_port_change(p->nodes + v->from, v->output);
        if (changes > highest)
            highest = changes;
    }
    return highest;
}

static void target_contract(const physical_plan *p, node *n) {
    uint32_t effects = n->binding.effects;
    n->visibility = "None";
    n->confirmation = "None";
    if (!(effects & UD_EFFECT_EXTERNAL_WRITE))
        return;
    if (ud_named(n->descriptor.id, "iceberg.destination")) {
        n->visibility = "CatalogSnapshot";
        n->confirmation = "Receipt";
    } else if (ud_named(n->descriptor.id, "mq.destination") && p->kafka_topic.length) {
        n->visibility = "TargetCommit";
        n->confirmation = "BrokerTransaction";
    } else {
        n->visibility = (effects & (UD_EFFECT_TRANSACTIONAL | UD_EFFECT_RECEIPTED)) ||
                         ud_named(n->descriptor.id, "sql.destination") ? "TargetCommit" :
                                                                        "ExternalResponse";
        n->confirmation = ud_named(p->delivery_mode, "eventReceipt") ? "EventReceipt" :
                          effects & UD_EFFECT_RECEIPTED ? "Receipt" : "Response";
    }
}

static ud_status bind_changes(physical_plan *p, node *n) {
    ud_string id = n->descriptor.id;
    if (n->descriptor.category == UD_COMPONENT_SOURCE) {
        n->source_replay = !!(n->descriptor.capabilities & UD_COMPONENT_REPLAY_SAFE) &&
                           !configured(n, "readMode", "stream");
        if (cdc_source(n)) {
            const char *value = NULL;
            uint32_t length = 0;
            const ud_json *flag = ud_member(n->configuration, "appendOnly");
            int append = flag && !ud_json_text(flag, &value, &length) &&
                         ud_named((ud_string){value, length}, "true");
            n->change_mode = append ? UD_CHANGES_APPEND : UD_CHANGES_RETRACT;
        }
        return UD_OK;
    }
    int incoming = input_changes(p, n);
    if (n->descriptor.category == UD_COMPONENT_DESTINATION) {
        target_contract(p, n);
        if (incoming == UD_CHANGES_APPEND || !(n->binding.effects & UD_EFFECT_EXTERNAL_WRITE))
            return UD_OK;
        int changeset = ud_named(id, "sql.destination") || ud_named(id, "iceberg.destination") ||
                        ud_named(id, "rest.destination");
        return changeset && configured(n, "mode", "changeset") &&
                       ud_json_size(ud_member(n->configuration, "keys")) ? UD_OK : UD_UNSUPPORTED;
    }
    if (ud_named(id, "eventWindow") && configured(n, "emit", "changes")) {
        n->change_mode = incoming == UD_CHANGES_RETRACT ? UD_CHANGES_RETRACT : UD_CHANGES_UPSERT;
        return UD_OK;
    }
    if (ud_named(id, "eventTime") || ud_named(id, "multicast") || ud_named(id, "unionAll") ||
        ud_named(id, "repartition")) {
        n->change_mode = incoming;
        return UD_OK;
    }
    return incoming == UD_CHANGES_APPEND ? UD_OK : UD_UNSUPPORTED;
}

static ud_status bind_cursor(node *n) {
    const ud_json *definition = ud_member(n->configuration, "incremental");
    if (!definition || !n->descriptor.outputs)
        return UD_OK;
    char *document = NULL;
    uint32_t length = 0;
    UD_TRY(ud_json_serialize(definition, &document, &length));
    ud_cursor *cursor = NULL;
    ud_status status = ud_cursor_create(n->context.memory, n->schemas[0],
                                         (ud_string){document, length}, &cursor);
    ud_cursor_destroy(cursor);
    ud_free(document);
    return status;
}

static ud_status validate_live_sql_effects(physical_plan *p) {
    int live_sql = 0;
    for (uint32_t index = 0; index < p->count; index++) {
        const node *n = p->nodes + index;
        ud_string mode = {0};
        if (ud_named(n->descriptor.id, "sql.source") &&
            !ud_json_string(ud_member(n->configuration, "readMode"), &mode) &&
            ud_named(mode, "stream"))
            live_sql = 1;
    }
    if (!live_sql)
        return UD_OK;
    for (uint32_t index = 0; index < p->count; index++) {
        node *n = p->nodes + index;
        int category = n->descriptor.category;
        uint32_t effects = n->descriptor.effects;
        /* A synchronous transform can inherit its input plan, including the
         * source's read effect. Only the transform's own effects apply here. */
        if (n->descriptor.describe_plan)
            effects |= n->binding.effects;
        int memory_output = ud_named(n->descriptor.id, "result.destination") ||
                            ud_named(n->descriptor.id, "discard");
        if ((category == UD_COMPONENT_SOURCE && !ud_named(n->descriptor.id, "sql.source")) ||
            (category == UD_COMPONENT_DESTINATION && !memory_output &&
             !ud_named(n->descriptor.id, "sql.destination")))
            return ud_pipeline_node_failed(n, UD_UNSUPPORTED, "streaming SQL requires SQL or in-memory output");
        if (category == UD_COMPONENT_DESTINATION && (effects & UD_EFFECT_RECEIPTED))
            return ud_pipeline_node_failed(n, UD_UNSUPPORTED, "a live SQL source cannot request recovery receipts");
        /* SQL Command owns its per-record database transaction and can deliver
         * its result without a Journal. The transform restriction applies to
         * other components that could retain state or write outside this path. */
        if (category == UD_COMPONENT_TRANSFORM && !ud_named(n->descriptor.id, "sql.command") &&
            ((n->descriptor.capabilities &
              (UD_COMPONENT_STATEFUL | UD_COMPONENT_BLOCKING | UD_COMPONENT_PARTIALLY_BLOCKING)) ||
             (effects &
              (UD_EFFECT_EXTERNAL_READ | UD_EFFECT_EXTERNAL_WRITE | UD_EFFECT_TRANSACTIONAL |
               UD_EFFECT_RECEIPTED | UD_EFFECT_SOURCE_ACK))))
            return ud_pipeline_node_failed(n, UD_UNSUPPORTED,
                               "streaming SQL requires stateless transforms without external effects");
    }
    return UD_OK;
}

ud_status ud_pipeline_validate_contract(physical_plan *p) {
    UD_TRY(validate_live_sql_effects(p));
    int managed_cursor = 0, receipt_targets = 0;
    for (uint32_t i = 0; i < p->count; i++) {
        node *n = p->nodes + p->order[i];
        ud_status status = bind_cursor(n);
        if (status)
            return ud_pipeline_node_failed(n, status, "incremental key binding");
        status = bind_changes(p, n);
        if (status)
            return ud_pipeline_node_failed(n, status, "change delivery contract");
        managed_cursor |= ud_incremental_source(&n->activation, n->id);
    }
    if (managed_cursor) {
        for (uint32_t i = 0; i < p->count; i++) {
            node *n = p->nodes + i;
            if (n->descriptor.category != UD_COMPONENT_DESTINATION ||
                !(n->binding.effects & UD_EFFECT_EXTERNAL_WRITE))
                continue;
            if (!n->confirmation || strcmp(n->confirmation, "Receipt"))
                return ud_pipeline_node_failed(n, UD_UNSUPPORTED, "incremental targets require durable receipts");
            receipt_targets++;
        }
        if (!receipt_targets)
            return UD_UNSUPPORTED;
    }
    uint8_t *reachable = ud_alloc(p->execution->execution->ctx, p->count);
    if (!reachable)
        return UD_RESOURCE;
    ud_status status = UD_OK;
    int epochs = 0, other_confirming_sources = 0;
    for (uint32_t index = 0; index < p->count; index++) {
        const node *n = p->nodes + index;
        if (n->descriptor.category == UD_COMPONENT_SOURCE && n->descriptor.confirm_epoch) {
            epochs = 1;
            other_confirming_sources |= (n->binding.effects & UD_EFFECT_SOURCE_ACK) == 0;
        }
    }
    p->allow_replayable_publish = !other_confirming_sources;
    for (uint32_t index = 0; !status && index < p->count; index++) {
        node *n = p->nodes + index;
        int publish = p->allow_replayable_publish && (n->binding.effects & UD_EFFECT_REPLAYABLE_PUBLISH);
        if ((p->execution->execution->options.state_directory.length &&
             !(n->descriptor.capabilities & UD_COMPONENT_REPLAY_SAFE) && !publish) ||
            (epochs && !ud_pipeline_epoch_target_supported(p, n))) {
            status = UD_UNSUPPORTED;
            break;
        }
        if (n->descriptor.category == UD_COMPONENT_SOURCE && n->descriptor.confirm_epoch &&
            p->execution->transaction_isolation && !p->execution->transaction_epoch) {
            status = UD_UNSUPPORTED;
            break;
        }
        int cache_build = ud_named(n->descriptor.id, "cacheBuild");
        uint64_t overlap = 0;
        const ud_json *incremental = ud_member(n->configuration, "incremental");
        if (ud_member(incremental, "overlap"))
            status = ud_json_uint(ud_member(incremental, "overlap"), &overlap);
        int redirect = 0;
        int policy = UD_ERROR_FAIL;
        if (!status)
            status = ud_error_policy(n->configuration, "errorPolicy", UD_ERROR_FAIL, &policy);
        redirect = policy == UD_ERROR_REDIRECT;
        const ud_json *rules = ud_member(n->configuration, "rules");
        for (uint32_t r = 0; !status && r < ud_json_size(rules); r++) {
            int effective = policy;
            status = ud_error_policy(ud_json_at(rules, r), "policy", policy, &effective);
            redirect |= effective == UD_ERROR_REDIRECT;
        }
        ud_string reference = {0};
        if (!status && cache_build) {
            ud_string id;
            status = ud_json_string(ud_member(n->configuration, "cache"), &id);
            ud_batch *schema = NULL;
            int cache_policy = 0;
            if (!status)
                status = ud_asset_schema(n->activation.parent, id, &schema, NULL, &cache_policy);
            ud_batch_destroy(schema);
            redirect |= cache_policy == UD_ERROR_REDIRECT;
            if (!status)
                status = ud_asset_source(n->activation.parent, id, &reference);
        }
        if (!status && redirect) {
            int connected = 0;
            for (uint32_t i = 0; i < n->output_edges; i++)
                if ((int32_t)p->edges[n->outputs[i]].output == n->descriptor.error_output)
                    connected = 1;
            if (!connected && !p->execution->execution->preview_pipeline)
                status = UD_SCHEMA;
        }
        if (status || (!cache_build && !overlap))
            continue;
        memset(reachable, 0, p->count);
        reachable[index] = 1;
        for (uint32_t round = 0; round < p->count; round++) {
            int changed = 0;
            for (uint32_t i = 0; i < p->paths; i++) {
                edge *v = p->edges + i;
                uint32_t from = cache_build ? v->to : v->from, to = cache_build ? v->from : v->to;
                if (reachable[from] && !reachable[to]) {
                    reachable[to] = 1;
                    changed = 1;
                }
            }
            if (!changed)
                break;
        }
        for (uint32_t i = 0; !status && i < p->count; i++)
            if (reachable[i] && i != index) {
                node *other = p->nodes + i;
                if (cache_build && other->descriptor.category == UD_COMPONENT_SOURCE) {
                    ud_string id = {0};
                    status = ud_json_string(ud_member(other->configuration, "connection"), &id);
                    if (!status && !ud_same(reference, id))
                        status = UD_SCHEMA;
                }
                if (overlap && other->descriptor.category == UD_COMPONENT_DESTINATION) {
                    const ud_json *keys = ud_member(other->configuration, "keys");
                    if (!(other->binding.effects & UD_EFFECT_KEYED_REPLACE) ||
                        ud_json_kind(keys) != UD_JSON_ARRAY || !ud_json_size(keys))
                        status = UD_UNSUPPORTED;
                    /* Every cursor key must survive in a declared upsert key's lineage. */
                    const ud_json *source_keys = ud_member(incremental, "keys");
                    node *producer = other->inputs[0] == UINT32_MAX
                                         ? NULL
                                         : p->nodes + p->edges[other->inputs[0]].from;
                    const ud_batch *sink_schema =
                        producer ? producer->schemas[p->edges[other->inputs[0]].output] : NULL;
                    for (uint32_t k = 0; !status && k < ud_json_size(source_keys); k++) {
                        ud_string name;
                        status = ud_json_string(ud_json_at(source_keys, k), &name);
                        ud_id id = {0};
                        int found = 0;
                        for (uint32_t c = 0; !status && c < n->schemas[0]->columns; c++) {
                            const ud_field *f = &n->schemas[0]->column[c].field;
                            char text[33];
                            ud_id_format(f->identity, text);
                            if ((name.length == f->name_length &&
                                 !memcmp(name.data, f->name, name.length)) ||
                                (name.length == 32 && !memcmp(name.data, text, 32)))
                                id = f->identity;
                        }
                        for (uint32_t key = 0; !status && key < ud_json_size(keys); key++) {
                            uint64_t c;
                            status = ud_json_uint(ud_json_at(keys, key), &c);
                            if (!status && (!sink_schema || c >= sink_schema->columns))
                                status = UD_SCHEMA;
                            if (!status) {
                                const ud_field *f = &sink_schema->column[c].field;
                                found |= !ud_id_compare(f->identity, id);
                                for (uint32_t origin = 0; origin < f->lineage_count; origin++)
                                    found |= !ud_id_compare(f->lineage[origin], id);
                            }
                        }
                        if (!status && !found)
                            status = UD_UNSUPPORTED;
                    }
                }
            }
    }
    ud_free(reachable);
    return status;
}
