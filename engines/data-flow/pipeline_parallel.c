#include "pipeline_state.h"
#include "pipeline_lanes.h"

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

ud_status ud_pipeline_parallel_bind(physical_plan *plan, const ud_json *definition) {
    uint32_t members, lanes, threads;
    UD_TRY(limits(definition, &members, &lanes, &threads));
    (void)threads;
    if (members == 1 && lanes == 1)
        return UD_OK;
    if ((plan->execution->parallel && !plan->execution->consumer_lane) ||
        plan->execution->transaction_serial ||
        plan->execution->execution->preview_pipeline)
        return UD_UNSUPPORTED;
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
    if ((members > 1 && options->worker_members != members) ||
        (options->worker_members > 1 && options->worker_members != members) ||
        (options->consumer_lanes && options->consumer_lanes != lanes) ||
        (ud_member(definition, "parallelism") && ud_execution_concurrency(activation) < threads))
        return UD_UNSUPPORTED;
    UD_TRY(ud_pipeline_bind_validate(activation, definition));
    return ud_pipeline_run_lanes(activation, definition, lanes, run_one);
}
