#include "pipeline_state.h"
#include "pipeline_lanes.h"
#include "kafka_stage.h"
#include "ud/compute.h"

static ud_status limits(const ud_json *definition, uint32_t *members, uint32_t *lanes,
                         uint32_t *threads) {
    *members = *lanes = *threads = 1;
    const ud_json *policy = ud_member(definition, "parallelism");
    if (!policy)
        return UD_OK;
    static const char *names[] = {"workerProcesses", "consumerLanesPerWorker", "threadsPerWorker"};
    uint32_t *values[] = {members, lanes, threads};
    UD_TRY(ud_json_keys(policy, names, 3));
    for (uint32_t i = 0; i < 3; i++) {
        const ud_json *value = ud_member(policy, names[i]);
        if (value) {
            uint64_t count;
            UD_TRY(ud_json_uint(value, &count));
            if (!count || count > 64)
                return UD_SCHEMA;
            *values[i] = (uint32_t)count;
        }
    }
    return *lanes > *threads ? UD_SCHEMA : UD_OK;
}

ud_status ud_pipeline_validate_stages(const ud_package *package) {
    const ud_json *flows = ud_member(package->document, "dataFlows");
    uint32_t count = ud_json_size(flows), stages = 0;
    for (uint32_t i = 0; i < count; i++)
        if (ud_member(ud_member(ud_json_at(flows, i), "delivery"), "kafkaStage"))
            stages++;
    if (!stages)
        return UD_OK;
    const ud_json *control = ud_member(package->document, "controlFlow");
    const ud_json *tasks = ud_member(control, "tasks"), *edges = ud_member(control, "constraints");
    if (stages != count || count > 64 || ud_json_size(tasks) != count || ud_json_size(edges) != count - 1 ||
        ud_json_size(ud_member(package->document, "eventHandlers")))
        return UD_SCHEMA;
    ud_string task_ids[64];
    uint32_t group = 0, threads = 0;
    for (uint32_t i = 0; i < count; i++) {
        const ud_json *flow = ud_json_at(flows, i), *task = ud_json_at(tasks, i);
        ud_string id, reference, kind;
        UD_TRY(ud_json_string(ud_member(flow, "id"), &id));
        UD_TRY(ud_json_string(ud_member(task, "id"), task_ids + i));
        UD_TRY(ud_json_string(ud_member(task, "kind"), &kind));
        UD_TRY(ud_json_string(ud_member(ud_member(task, "config"), "pipeline"), &reference));
        if (!ud_named(kind, "dataFlow") || !ud_same(id, reference) ||
            ud_member(task, "condition") || ud_member(task, "retry"))
            return UD_SCHEMA;
        const ud_json *delivery = ud_member(flow, "delivery");
        if (ud_member(delivery, "kafkaMerge") && i != count - 1)
            return UD_SCHEMA;
        uint32_t members, lanes, cpu;
        UD_TRY(limits(flow, &members, &lanes, &cpu));
        if (members < 2 || lanes != 1 || (i && (members != group || cpu != threads)))
            return UD_SCHEMA;
        group = members;
        threads = cpu;
    }
    uint64_t predecessors = 0;
    for (uint32_t i = 0; i < count - 1; i++) {
        const ud_json *edge = ud_json_at(edges, i);
        ud_string from, to, on;
        UD_TRY(ud_json_string(ud_member(edge, "from"), &from));
        UD_TRY(ud_json_string(ud_member(edge, "to"), &to));
        UD_TRY(ud_json_string(ud_member(edge, "on"), &on));
        if (!ud_named(on, "success") || ud_member(edge, "expression"))
            return UD_SCHEMA;
        uint32_t target = 1;
        while (target < count && !ud_same(to, task_ids[target]))
            target++;
        if (target == count || !ud_same(from, task_ids[target - 1]) || (predecessors & (UINT64_C(1) << target)))
            return UD_SCHEMA;
        predecessors |= UINT64_C(1) << target;
    }
    return UD_OK;
}

static ud_status validate_stage_keys(const physical_plan *plan, const uint32_t *sources,
                                     const ud_json *const *routes, uint32_t source_count) {
    for (uint32_t i = 0; i < plan->count; i++) {
        const node *n = plan->nodes + i;
        int window = ud_named(n->descriptor.id, "eventWindow");
        int join = ud_named(n->descriptor.id, "join") || ud_named(n->descriptor.id, "eventJoin") ||
                   ud_named(n->descriptor.id, "mergeJoin");
        if (!window && !join)
            continue;
        for (uint32_t port = 0; port < n->descriptor.inputs; port++) {
            uint32_t upstream = plan->edges[n->inputs[port]].from;
            uint32_t slot = 0;
            while (slot < source_count && sources[slot] != upstream)
                slot++;
            if (slot == source_count)
                return UD_UNSUPPORTED;
            const ud_json *keys = window ? ud_member(ud_member(n->configuration, "plan"), "group") :
                                  ud_member(n->configuration, port ? "rightKeys" : "leftKeys");
            uint32_t count = ud_json_size(keys);
            if (!count || count != ud_json_size(routes[slot]))
                return UD_SCHEMA;
            const ud_batch *schema = n->ports.inputs[port].schema;
            for (uint32_t key = 0; key < count; key++) {
                uint32_t selected, partitioned;
                UD_TRY(ud_compute_field_index(schema, ud_json_at(keys, key), &selected));
                UD_TRY(ud_compute_field_index(schema, ud_json_at(routes[slot], key), &partitioned));
                if (selected != partitioned)
                    return UD_SCHEMA;
            }
        }
    }
    return UD_OK;
}

/* Canonical cut ordering keeps partition assignment independent of dispatch order. */
static ud_status bind_stage_inputs(const physical_plan *plan, uint32_t members, int gather) {
    const ud_json *flows = ud_member(plan->execution->package->document, "dataFlows");
    uint32_t source_count = 0, sources[2];
    const ud_json *routes[2] = {0};
    for (uint32_t i = 0; i < plan->count; i++) {
        const node *n = plan->nodes + i;
        if (!(n->descriptor.capabilities & UD_COMPONENT_THREAD_SAFE) ||
            (n->descriptor.capabilities & UD_COMPONENT_THREAD_AFFINE))
            return UD_UNSUPPORTED;
        if (n->descriptor.category != UD_COMPONENT_SOURCE)
            continue;
        uint32_t slot = source_count++;
        sources[slot] = i;
        const ud_json *from = ud_member(n->configuration, "fromStage");
        if (from) {
            ud_string producer, address, connection;
            UD_TRY(ud_json_string(from, &producer));
            UD_TRY(ud_json_string(ud_member(n->configuration, "address"), &address));
            UD_TRY(ud_json_string(ud_member(n->configuration, "connection"), &connection));
            uint32_t matches = 0;
            for (uint32_t j = 0; j < ud_json_size(flows); j++) {
                const ud_json *flow = ud_json_at(flows, j);
                ud_string flow_id;
                UD_TRY(ud_json_string(ud_member(flow, "id"), &flow_id));
                if (ud_same(flow_id, plan->id))
                    break;
                if (!ud_same(flow_id, producer))
                    continue;
                const ud_json *nodes = ud_member(flow, "components");
                for (uint32_t k = 0; k < ud_json_size(nodes); k++) {
                    const ud_json *entry = ud_json_at(nodes, k), *config = ud_member(entry, "config");
                    ud_string component, target = {0}, reference = {0};
                    UD_TRY(ud_json_string(ud_member(entry, "component"), &component));
                    if (!ud_named(component, "mq.destination") ||
                        !ud_kafka_stage_enabled(ud_member(config, "publishStage")))
                        continue;
                    UD_TRY(ud_json_string(ud_member(config, "address"), &target));
                    UD_TRY(ud_json_string(ud_member(config, "connection"), &reference));
                    if (ud_same(target, address) && ud_same(reference, connection)) {
                        int fixed = ud_member(config, "partition") != NULL;
                        if (gather != fixed)
                            return UD_SCHEMA;
                        routes[slot] = ud_member(ud_member(config, "repartition"), "keys");
                        matches++;
                    }
                }
            }
            if (matches != 1)
                return UD_SCHEMA;
            continue;
        }
        if (gather)
            return UD_SCHEMA;
        const ud_json *cuts = ud_member(n->configuration, "endOffsets");
        if (ud_json_size(cuts) != members)
            return UD_SCHEMA;
        for (uint32_t partition = 0; partition < members; partition++) {
            const ud_json *cut = ud_json_at(cuts, partition);
            uint64_t id, offset;
            UD_TRY(ud_json_uint(ud_member(cut, "partition"), &id));
            UD_TRY(ud_json_uint(ud_member(cut, "nextOffset"), &offset));
            if (id != partition || offset > INT64_MAX)
                return UD_SCHEMA;
        }
    }
    return validate_stage_keys(plan, sources, routes, source_count);
}

ud_status ud_pipeline_parallel_bind(physical_plan *plan, const ud_json *definition) {
    uint32_t members, lanes, threads;
    UD_TRY(limits(definition, &members, &lanes, &threads));
    (void)threads;
    if (!plan->kafka_stage)
        for (uint32_t i = 0; i < plan->count; i++) {
            const ud_json *config = plan->nodes[i].configuration;
            if (ud_member(config, "fromStage") || ud_kafka_stage_enabled(ud_member(config, "partitionByWorker")) ||
                ud_kafka_stage_enabled(ud_member(config, "publishStage")))
                return UD_SCHEMA;
        }
    if (members == 1 && lanes == 1 && !plan->kafka_stage)
        return UD_OK;
    if ((plan->execution->parallel && !plan->execution->consumer_lane) ||
        plan->execution->transaction_serial ||
        plan->execution->execution->preview_pipeline)
        return UD_UNSUPPORTED;
    if (plan->kafka_stage)
        return members >= 2 && lanes == 1 ? bind_stage_inputs(plan, members,
            ud_kafka_stage_enabled(ud_member(ud_member(definition, "delivery"), "kafkaMerge"))) : UD_SCHEMA;
    uint32_t sources = 0;
    for (uint32_t i = 0; i < plan->count; i++) {
        const node *n = plan->nodes + i;
        if (n->descriptor.category == UD_COMPONENT_SOURCE) {
            sources++;
            if (!(n->binding.effects & UD_EFFECT_SOURCE_ACK) ||
                ud_member(n->configuration, "endOffsets") ||
                ud_member(n->configuration, "partition"))
                return UD_UNSUPPORTED;
        } else if (n->descriptor.capabilities & (UD_COMPONENT_STATEFUL | UD_COMPONENT_BLOCKING))
            return UD_UNSUPPORTED;
        if (!(n->descriptor.capabilities & UD_COMPONENT_THREAD_SAFE) ||
            (n->descriptor.capabilities & UD_COMPONENT_THREAD_AFFINE))
            return UD_UNSUPPORTED;
    }
    if (sources != 1)
        return UD_UNSUPPORTED;
    return UD_OK;
}

ud_status ud_pipeline_parallel_run(ud_activation *activation, const ud_json *definition,
                                    ud_status (*run_one)(ud_activation *, const ud_json *)) {
    uint32_t members, lanes, threads;
    UD_TRY(limits(definition, &members, &lanes, &threads));
    const ud_execution_options *options = &activation->execution->options;
    const ud_json *stage = ud_member(ud_member(definition, "delivery"), "kafkaStage");
    if (activation->execution->assignment_count) {
        if (members != 1 || lanes != 1 || stage) return UD_UNSUPPORTED;
        UD_TRY(ud_pipeline_bind_validate(activation, definition));
        return ud_pipeline_run_lanes(activation, definition, activation->execution->assignment_count, run_one);
    }
    if ((members > 1 && options->worker_members != members) ||
        (options->worker_members > 1 && options->worker_members != members) ||
        (options->consumer_lanes && options->consumer_lanes != lanes) ||
        (stage && !options->worker_run_id.length) ||
        (ud_member(definition, "parallelism") && ud_execution_concurrency(activation) < threads))
        return UD_UNSUPPORTED;
    UD_TRY(ud_pipeline_bind_validate(activation, definition));
    if (options->worker_member && ud_kafka_stage_enabled(ud_member(ud_member(definition, "delivery"), "kafkaMerge")))
        return UD_OK;
    return ud_pipeline_run_lanes(activation, definition, lanes, run_one);
}
