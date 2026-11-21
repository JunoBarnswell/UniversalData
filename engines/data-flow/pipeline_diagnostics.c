#include "pipeline_state.h"
#include "ud/engine.h"
#include "ud/partition.h"
static uint64_t batch_p95(const node *n) {
    uint64_t rank = n->batch_samples - n->batch_samples / 20, seen = 0;
    if (!rank)
        return 0;
    for (uint32_t bucket = 0; bucket < 64; bucket++) {
        seen += n->batch_latency[bucket];
        if (seen >= rank)
            return bucket == 63 ? UINT64_MAX : UINT64_C(1) << bucket;
    }
    return UINT64_MAX;
}
static ud_status json_number(ud_bytes *b, const char *name, uint64_t value) {
    char text[128];
    int n = snprintf(text, sizeof(text), "\"%s\":%llu", name, (unsigned long long)value);
    return ud_bytes_put(b, text, (size_t)n);
}
ud_status ud_pipeline_plan_json(physical_plan *p, ud_bytes *b, int runtime, ud_status outcome) {
    UD_TRY(ud_bytes_put(b, "{\"id\":", sizeof("{\"id\":") - 1));
    UD_TRY(ud_json_quote(b, p->id.data, p->id.length));
    UD_TRY(ud_bytes_put(b, ",\"task\":", sizeof(",\"task\":") - 1));
    UD_TRY(ud_json_quote(b, p->execution->task_id.data, p->execution->task_id.length));
    UD_TRY(ud_bytes_put(b, ",", 1));
    UD_TRY(json_number(b, "treeConcurrency", p->concurrency));
    if (p->execution->execution->options.worker_members) {
        UD_TRY(ud_bytes_put(b, ",", 1));
        UD_TRY(json_number(b, "workerMember", p->execution->execution->options.worker_member));
        UD_TRY(ud_bytes_put(b, ",", 1));
        UD_TRY(json_number(b, "workerMembers", p->execution->execution->options.worker_members));
    }
    if (p->execution->consumer_lane_count) {
        UD_TRY(ud_bytes_put(b, ",", 1));
        UD_TRY(json_number(b, "consumerLane", p->execution->consumer_lane));
        UD_TRY(ud_bytes_put(b, ",", 1));
        UD_TRY(json_number(b, "consumerLanes", p->execution->consumer_lane_count));
    }
    UD_TRY(ud_bytes_put(b, ",", 1));
    UD_TRY(json_number(b, "confirmedEpoch", atomic_load(&p->confirmed_epoch)));
    if (p->execution->execution->options.state_directory.length)
        UD_TRY(ud_bytes_put(
            b, ",\"recoveryOrdering\":\"concurrent-trees-and-serialized-journal-frames\"",
            sizeof(",\"recoveryOrdering\":\"concurrent-trees-and-serialized-journal-frames\"") -
                1));
    UD_TRY(ud_bytes_put(b, ",\"trees\":[", sizeof(",\"trees\":[") - 1));
    for (uint32_t t = 0; t < p->tree_count; t++) {
        if (t)
            UD_TRY(ud_bytes_put(b, ",", sizeof(",") - 1));
        UD_TRY(ud_bytes_put(b, "{", sizeof("{") - 1));
        UD_TRY(json_number(b, "id", t));
        UD_TRY(ud_bytes_put(b, ",\"nodes\":[", sizeof(",\"nodes\":[") - 1));
        execution_tree *tree = p->trees + t;
        for (uint32_t i = 0; i < tree->count; i++) {
            if (i)
                UD_TRY(ud_bytes_put(b, ",", sizeof(",") - 1));
            node *n = p->nodes + p->tree_order[tree->offset + i];
            UD_TRY(ud_bytes_put(b, "{\"id\":", sizeof("{\"id\":") - 1));
            UD_TRY(ud_json_quote(b, n->id.data, n->id.length));
            UD_TRY(ud_bytes_put(b, ",\"component\":", sizeof(",\"component\":") - 1));
            UD_TRY(ud_json_quote(b, n->descriptor.id.data, n->descriptor.id.length));
            UD_TRY(ud_bytes_put(b, ",", sizeof(",") - 1));
            UD_TRY(json_number(b, "capabilities", n->descriptor.capabilities));
            UD_TRY(ud_bytes_put(b, ",", 1));
            UD_TRY(json_number(b, "boundedness", n->binding.boundedness));
            if (n->descriptor.category == UD_COMPONENT_SOURCE) {
                const char *replay = n->source_replay ? ",\"sourceReplay\":true" : ",\"sourceReplay\":false";
                UD_TRY(ud_bytes_put(b, replay, strlen(replay)));
            }
            if (n->visibility) {
                UD_TRY(ud_bytes_put(b, ",\"delivery\":{\"visibility\":", sizeof(",\"delivery\":{\"visibility\":") - 1));
                UD_TRY(ud_json_quote(b, n->visibility, (uint32_t)strlen(n->visibility)));
                UD_TRY(ud_bytes_put(b, ",\"confirmation\":", sizeof(",\"confirmation\":") - 1));
                UD_TRY(ud_json_quote(b, n->confirmation, (uint32_t)strlen(n->confirmation)));
                UD_TRY(ud_bytes_put(b, "}", 1));
            }
            const char *mode = n->descriptor.capabilities & UD_COMPONENT_BLOCKING ? "blocking"
                               : n->descriptor.capabilities & UD_COMPONENT_PARTIALLY_BLOCKING
                                   ? "partiallyBlocking"
                                   : "streaming";
            UD_TRY(ud_bytes_put(b, ",\"stateMode\":", sizeof(",\"stateMode\":") - 1));
            UD_TRY(ud_json_quote(b, mode, (uint32_t)strlen(mode)));
            UD_TRY(ud_bytes_put(b, ",\"ports\":[", 10));
            for (uint32_t direction = 0, at = 0; direction < 2; direction++)
                for (uint32_t k = 0; k < (direction ? n->ports.output_count : n->ports.input_count);
                     k++) {
                    const ud_component_port *port =
                        (direction ? n->ports.outputs : n->ports.inputs) + k;
                    if (at++)
                        UD_TRY(ud_bytes_put(b, ",", 1));
                    char id[33];
                    ud_id_format(port->id, id);
                    UD_TRY(ud_bytes_put(b, "{\"id\":", 6));
                    UD_TRY(ud_json_quote(b, id, 32));
                    UD_TRY(ud_bytes_put(b, ",\"name\":", 8));
                    UD_TRY(ud_json_quote(b, port->name.data, port->name.length));
                    UD_TRY(ud_bytes_put(b, ",", 1));
                    UD_TRY(json_number(b, "direction", port->direction));
                    UD_TRY(ud_bytes_put(b, ",", 1));
                    UD_TRY(json_number(b, "index", port->index));
                    UD_TRY(ud_bytes_put(b, ",", 1));
                    UD_TRY(json_number(b, "flags", port->flags));
                    if (direction) {
                        const char *modes[] = {"Append", "Upsert", "Retract"};
                        const char *mode = modes[ud_pipeline_port_change(n, k)];
                        UD_TRY(ud_bytes_put(b, ",\"changeMode\":", sizeof(",\"changeMode\":") - 1));
                        UD_TRY(ud_json_quote(b, mode, (uint32_t)strlen(mode)));
                    }
                    if (runtime && direction) {
                        UD_TRY(ud_bytes_put(b, ",", 1));
                        UD_TRY(json_number(b, "rows", atomic_load(n->port_rows + k)));
                    }
                    UD_TRY(ud_bytes_put(b, "}", 1));
                }
            UD_TRY(ud_bytes_put(b, "],", 2));
            UD_TRY(json_number(b, "inputCount", n->descriptor.inputs));
            UD_TRY(ud_bytes_put(b, ",", 1));
            UD_TRY(json_number(b, "outputCount", n->descriptor.outputs));
            UD_TRY(ud_bytes_put(b, ",", 1));
            UD_TRY(json_number(b, "optionalOutputs", n->descriptor.optional_outputs));
            uint64_t partitions = n->binding.partitions;
            UD_TRY(ud_bytes_put(b, ",", 1));
            UD_TRY(json_number(b, "partitions", partitions));
            UD_TRY(ud_bytes_put(b, ",", 1));
            UD_TRY(json_number(b, "ordering", n->binding.ordering));
            const char *physical_names[] = {"algorithm",    "estimatedRows", "rowWidthEstimate",
                                            "memoryBudget", "spillBudget",   "estimatedCost",
                                            "stateVersion", "effects"};
            const uint64_t physical_values[] = {n->binding.algorithm,     n->binding.estimated_rows,
                                                n->binding.row_width,     n->binding.memory_budget,
                                                n->binding.spill_budget,  n->binding.estimated_cost,
                                                n->binding.state_version, n->binding.effects};
            for (uint32_t field = 0; field < 8; field++) {
                UD_TRY(ud_bytes_put(b, ",", 1));
                UD_TRY(json_number(b, physical_names[field], physical_values[field]));
            }
            if (n->binding.physical_tree.length) {
                UD_TRY(ud_bytes_put(b, ",\"physicalPlan\":", 16));
                UD_TRY(ud_bytes_put(b, n->binding.physical_tree.data,
                                    n->binding.physical_tree.length));
            }
            UD_TRY(ud_bytes_put(b, ",\"sortKeys\":[", 13));
            for (uint32_t k = 0; k < n->binding.sort.count; k++) {
                char id[33];
                ud_id_format(n->binding.sort.keys[k].field, id);
                if (k)
                    UD_TRY(ud_bytes_put(b, ",", 1));
                UD_TRY(ud_json_quote(b, id, 32));
            }
            UD_TRY(ud_bytes_put(b, "]", 1));
            UD_TRY(ud_bytes_put(b, ",\"inputs\":[", sizeof(",\"inputs\":[") - 1));
            for (uint32_t at = 0; at < n->ports.input_count; at++) {
                if (at)
                    UD_TRY(ud_bytes_put(b, ",", 1));
                const ud_batch *schema = n->ports.inputs[at].schema;
                if (!schema) {
                    UD_TRY(ud_bytes_put(b, "null", 4));
                    continue;
                }
                char *text = NULL;
                uint32_t size = 0;
                ud_status status = ud_schema_to_json(schema, &text, &size);
                if (!status)
                    status = ud_bytes_put(b, text, size);
                ud_free(text);
                if (status)
                    return status;
            }
            UD_TRY(ud_bytes_put(b, "]", 1));
            UD_TRY(ud_bytes_put(b, ",\"outputs\":[", sizeof(",\"outputs\":[") - 1));
            for (uint32_t j = 0; j < n->descriptor.outputs; j++) {
                if (j)
                    UD_TRY(ud_bytes_put(b, ",", sizeof(",") - 1));
                char *schema = NULL;
                uint32_t size = 0;
                ud_status status = ud_schema_to_json(n->schemas[j], &schema, &size);
                if (!status)
                    status = ud_bytes_put(b, schema, size);
                ud_free(schema);
                if (status)
                    return status;
            }
            UD_TRY(ud_bytes_put(b, "]", sizeof("]") - 1));
            if (runtime) {
                UD_TRY(ud_bytes_put(b, ",", 1));
                UD_TRY(json_number(b, "result", (uint64_t)atomic_load(&n->result)));
                const char *names[] = {"state",          "steps",        "processedRows",
                                       "producedRows",   "rejectedRows", "inputWaits",
                                       "capacityWaits",  "callbackMs",   "cpuNs",
                                       "firstMs",        "endMs",        "readyMs",
                                       "prepareMs",      "prepareCpuNs", "inputWaitMs",
                                       "capacityWaitMs", "finishMs",     "finishCpuNs"};
                uint64_t values[] = {atomic_load(&n->state),
                                     n->steps,
                                     n->processed,
                                     n->produced,
                                     n->rejected,
                                     n->wait_input,
                                     n->wait_capacity,
                                     n->callback_ms,
                                     n->cpu_ns,
                                     n->first_ms,
                                     n->end_ms,
                                     n->ready_ms,
                                     n->prepare_ms,
                                     n->prepare_cpu_ns,
                                     n->input_wait_ms,
                                     n->capacity_wait_ms,
                                     n->finish_ms,
                                     n->finish_cpu_ns};
                for (uint32_t k = 0; k < 18; k++) {
                    UD_TRY(ud_bytes_put(b, ",", sizeof(",") - 1));
                    UD_TRY(json_number(b, names[k], values[k]));
                }
                UD_TRY(ud_bytes_put(b, ",\"batchLatency\":{", sizeof(",\"batchLatency\":{") - 1));
                UD_TRY(json_number(b, "samples", n->batch_samples));
                UD_TRY(ud_bytes_put(b, ",", 1));
                UD_TRY(json_number(b, "p95UpperNs", batch_p95(n)));
                UD_TRY(ud_bytes_put(b, ",", 1));
                UD_TRY(json_number(b, "maxNs", n->batch_max_ns));
                UD_TRY(ud_bytes_put(b, ",", 1));
                UD_TRY(json_number(b, "sumNs", n->batch_sum_ns));
                UD_TRY(ud_bytes_put(b,
                                    ",\"method\":\"log2-nanoseconds-upper-bound\",\"scope\":"
                                    "\"component-batch-callback\"}",
                                    sizeof(",\"method\":\"log2-nanoseconds-upper-bound\",\"scope\":"
                                           "\"component-batch-callback\"}") -
                                        1));
                if (n->opened && n->descriptor.diagnostics) {
                    char *facts = NULL;
                    uint32_t length = 0;
                    ud_status status = n->descriptor.diagnostics(n->instance, &facts, &length);
                    if (!status)
                        status = ud_bytes_put(b, ",\"facts\":", 9);
                    if (!status)
                        status = ud_bytes_put(b, facts, length);
                    ud_free(facts);
                    if (status)
                        return status;
                }
            }
            UD_TRY(ud_bytes_put(b, "}", sizeof("}") - 1));
        }
        UD_TRY(ud_bytes_put(b, "]", 1));
        if (runtime) {
            const char *names[] = {"readyMs",    "firstMs",    "endMs", "quanta",
                                   "busyQuanta", "callbackMs", "cpuNs"};
            uint64_t values[] = {tree->ready_ms, tree->first_ms,    tree->end_ms, tree->quanta,
                                 tree->busy,     tree->callback_ms, tree->cpu_ns};
            for (uint32_t k = 0; k < 7; k++) {
                UD_TRY(ud_bytes_put(b, ",", 1));
                UD_TRY(json_number(b, names[k], values[k]));
            }
        }
        UD_TRY(ud_bytes_put(b, "}", 1));
    }
    UD_TRY(ud_bytes_put(b, "],\"paths\":[", sizeof("],\"paths\":[") - 1));
    for (uint32_t i = 0; i < p->paths; i++) {
        edge *v = p->edges + i;
        if (i)
            UD_TRY(ud_bytes_put(b, ",", sizeof(",") - 1));
        UD_TRY(ud_bytes_put(b, "{\"from\":", sizeof("{\"from\":") - 1));
        UD_TRY(ud_json_quote(b, p->nodes[v->from].id.data, p->nodes[v->from].id.length));
        UD_TRY(ud_bytes_put(b, ",\"to\":", sizeof(",\"to\":") - 1));
        UD_TRY(ud_json_quote(b, p->nodes[v->to].id.data, p->nodes[v->to].id.length));
        UD_TRY(ud_bytes_put(b, ",", 1));
        const char *names[] = {"fromTree",     "toTree",        "output",
                               "input",        "capacity",      "producedRows",
                               "consumedRows", "queueHighWater", "spillBytes"};
        uint64_t values[] = {p->nodes[v->from].tree,
                             p->nodes[v->to].tree,
                             v->output,
                             v->input,
                             p->execution->execution->options.edge_capacity,
                             atomic_load(&v->produced),
                             atomic_load(&v->consumed),
                             atomic_load(&v->high_water),
                             atomic_load(&v->spill_bytes)};
        for (uint32_t j = 0; j < (runtime ? 9u : 5u); j++) {
            if (j)
                UD_TRY(ud_bytes_put(b, ",", sizeof(",") - 1));
            UD_TRY(json_number(b, names[j], values[j]));
        }
        UD_TRY(ud_bytes_put(b, "}", sizeof("}") - 1));
    }
    UD_TRY(ud_bytes_put(b, "]", sizeof("]") - 1));
    if (runtime) {
        UD_TRY(ud_bytes_put(b, ",", sizeof(",") - 1));
        UD_TRY(json_number(b, "outcome", outcome));
    }
    return ud_bytes_put(b, "}", sizeof("}") - 1);
}

ud_status ud_execution_progress(ud_execution *e, char **out, uint32_t *length) {
    if (!e || !out || !length)
        return UD_INVALID;
    *out = NULL;
    *length = 0;
    ud_bytes b = {.ctx = e->ctx, .limit = 4194304};
    const ud_json *options = ud_member(ud_member(e->package->document, "policy"), "execution");
    uint64_t prefetch = 0, source_limit = e->max_connections_per_source;
    if (ud_member(options, "messagePrefetch"))
        UD_TRY(ud_json_uint(ud_member(options, "messagePrefetch"), &prefetch));
    if (ud_member(options, "maxConnectionsPerSource"))
        UD_TRY(ud_json_uint(ud_member(options, "maxConnectionsPerSource"), &source_limit));
    ud_execution_metrics metrics = {.struct_size = sizeof(metrics), .abi_version = UD_ABI_VERSION};
    UD_TRY(ud_execution_get_metrics(e, &metrics));
    char configuration[768];
    int size = snprintf(configuration, sizeof(configuration),
        "{\"executionOptions\":{\"batchRows\":%u,\"edgeCapacity\":%u,\"parallelism\":%u,"
        "\"memoryBytes\":%llu,\"timeoutMilliseconds\":%llu,\"maxConnectionsPerSource\":%llu,"
        "\"messagePrefetch\":%llu},\"inputRows\":%llu,\"outputRows\":%llu,\"committedRows\":%llu,"
        "\"acknowledgedRows\":%llu,\"rejectedRows\":%llu,\"batches\":%llu,\"pipelines\":[",
        e->options.max_batch_rows, e->options.edge_capacity, e->options.max_concurrency,
        (unsigned long long)e->options.context.memory_limit, (unsigned long long)e->options.timeout_ms,
        (unsigned long long)source_limit, (unsigned long long)prefetch, (unsigned long long)metrics.input_rows,
        (unsigned long long)metrics.output_rows, (unsigned long long)metrics.committed_rows,
        (unsigned long long)metrics.acknowledged_rows, (unsigned long long)metrics.rejected_rows,
        (unsigned long long)metrics.batches);
    if (size < 0 || (size_t)size >= sizeof(configuration))
        return UD_OVERFLOW;
    ud_status s = ud_bytes_put(&b, configuration, (size_t)size);
    ud_mutex_lock(&e->diagnostics_lock);
    uint64_t sampled_at = ud_now_ms();
    uint32_t count = 0;
    for (physical_plan *p = e->active_progress; !s && p; p = p->progress_next) {
        if (count++)
            s = ud_bytes_put(&b, ",", 1);
        if (!s)
            s = ud_bytes_put(&b, "{\"id\":", sizeof("{\"id\":") - 1);
        if (!s)
            s = ud_json_quote(&b, p->id.data, p->id.length);
        if (!s) {
            char facts[256];
            int n = snprintf(
                facts, sizeof(facts),
                ",\"sampledAtMs\":%llu,\"sampleWindowMs\":%llu,\"confirmedEpoch\":%llu,\"nodes\":[",
                (unsigned long long)sampled_at,
                (unsigned long long)(p->sampled_ms ? sampled_at - p->sampled_ms : 0),
                (unsigned long long)atomic_load(&p->confirmed_epoch));
            s = ud_bytes_put(&b, facts, (size_t)n);
        }
        for (uint32_t i = 0; !s && i < p->count; i++) {
            node *n = p->nodes + i;
            if (i)
                s = ud_bytes_put(&b, ",", 1);
            if (!s)
                s = ud_bytes_put(&b, "{\"id\":", sizeof("{\"id\":") - 1);
            if (!s)
                s = ud_json_quote(&b, n->id.data, n->id.length);
            const char *names[] = {"state",        "result",       "processedRows",
                                   "producedRows", "rejectedRows", "callbackMs"};
            uint64_t values[] = {
                (uint64_t)atomic_load(&n->state), (uint64_t)atomic_load(&n->result),
                atomic_load(&n->live_processed),  atomic_load(&n->live_produced),
                atomic_load(&n->live_rejected),   atomic_load(&n->live_callback_ms)};
            for (uint32_t k = 0; !s && k < 6; k++) {
                s = ud_bytes_put(&b, ",", 1);
                if (!s)
                    s = json_number(&b, names[k], values[k]);
            }
            if (!s) {
                uint64_t rows = atomic_load(&n->live_produced),
                         elapsed = p->sampled_ms ? sampled_at - p->sampled_ms : 0;
                char facts[384];
                int size = snprintf(
                    facts, sizeof(facts),
                    ",\"boundedness\":%u,\"idle\":%s,\"idleMs\":%llu,\"watermark\":%lld,"
                    "\"rateRowsPerSecond\":%.3f,\"stagedEpoch\":%llu",
                    n->binding.boundedness, atomic_load(&n->idle) ? "true" : "false",
                    (unsigned long long)atomic_load(&n->live_idle_ms),
                    (long long)atomic_load(&n->live_watermark),
                    elapsed ? (double)(rows - n->sampled_rows) * 1000.0 / (double)elapsed : 0.0,
                    (unsigned long long)atomic_load(&n->staged_epoch));
                s = ud_bytes_put(&b, facts, (size_t)size);
                n->sampled_rows = rows;
            }
            if (!s)
                s = ud_bytes_put(&b, ",\"ports\":[", sizeof(",\"ports\":[") - 1);
            for (uint32_t k = 0; !s && k < n->descriptor.outputs; k++) {
                if (k)
                    s = ud_bytes_put(&b, ",", 1);
                if (!s)
                    s = ud_bytes_put(&b, "{", 1);
                if (!s)
                    s = json_number(&b, "index", k);
                if (!s)
                    s = ud_bytes_put(&b, ",", 1);
                if (!s)
                    s = json_number(&b, "rows", atomic_load(n->port_rows + k));
                if (!s)
                    s = ud_bytes_put(&b, "}", 1);
            }
            if (!s)
                s = ud_bytes_put(&b, "]}", 2);
        }
        if (!s)
            s = ud_bytes_put(&b, "],\"paths\":[", 11);
        for (uint32_t k = 0; !s && k < p->paths; k++) {
            edge *path = p->edges + k;
            ud_queue_metrics queue = {0};
            s = ud_queue_snapshot(path->queue, &queue);
            char facts[384];
            int size = snprintf(facts, sizeof(facts),
                                "%s{\"from\":%u,\"to\":%u,\"output\":%u,\"input\":%u,"
                                "\"queueBatches\":%llu,\"queueRows\":%llu,\"referencedBytes\":%llu,"
                                "\"capacity\":%llu,\"producedRows\":%llu,\"consumedRows\":%llu}",
                                k ? "," : "", path->from, path->to, path->output, path->input,
                                (unsigned long long)queue.batches, (unsigned long long)queue.rows,
                                (unsigned long long)queue.referenced_bytes,
                                (unsigned long long)queue.capacity,
                                (unsigned long long)atomic_load(&path->produced),
                                (unsigned long long)atomic_load(&path->consumed));
            if (!s)
                s = ud_bytes_put(&b, facts, (size_t)size);
        }
        p->sampled_ms = sampled_at;
        if (!s)
            s = ud_bytes_put(&b, "]}", 2);
    }
    ud_mutex_unlock(&e->diagnostics_lock);
    if (!s)
        s = ud_bytes_put(&b, "]}", 2);
    if (!s) {
        *out = ud_strdup_n(e->ctx, (char *)b.data, b.size);
        if (!*out)
            s = UD_RESOURCE;
        else
            *length = (uint32_t)b.size;
    }
    ud_bytes_clear(&b);
    return s;
}
