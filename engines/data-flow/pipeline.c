#include "../package/internal.h"
#include "ud/engine.h"
#include "ud/partition.h"
#include "../../components/execution/state.h"
ud_status ud_pipeline_field_namespace(const ud_component_context *c, char output[33]) {
    ud_bytes bytes = {.ctx = c->memory};
    ud_string package = {0}, project = {0};
    ud_status s = ud_json_string(ud_member(c->activation->package->document, "id"), &package);
    const ud_json *p = ud_member(c->activation->package->document, "project");
    if (!s && p)
        s = ud_json_string(ud_member(p, "id"), &project);
    ud_string parts[] = {c->execution->options.tenant, project, package, c->pipeline_id,
                         c->component_id};
    for (uint32_t j = 0; !s && j < 5; j++)
        s = ud_put_string(&bytes, parts[j].data, parts[j].length);
    if (!s) {
        uint8_t hash[32];
        ud_hash(bytes.data, bytes.size, hash);
        ud_id id = {0};
        for (uint32_t j = 0; j < 8; j++) {
            id.high = (id.high << 8) | hash[j];
            id.low = (id.low << 8) | hash[j + 8];
        }
        ud_id_format(id, output);
    }
    ud_bytes_clear(&bytes);
    return s;
}
ud_status ud_component_port_identity(const ud_component_context *c, uint32_t direction,
                                     ud_string logical, ud_id *out) {
    /* Ports identify the Package graph, including before deployment binding.
     * Tenant/project scope belongs to physical fields and rows, never to the
     * references stored in that graph. */
    ud_string package = {0};
    UD_TRY(ud_json_string(ud_member(c->activation->package->document, "id"), &package));
    ud_bytes symbol = {.ctx = c->memory};
    ud_string parts[] = {package, c->pipeline_id, c->component_id, logical};
    ud_status s = ud_put_u64(&symbol, direction);
    for (uint32_t i = 0; !s && i < 4; i++)
        s = ud_put_string(&symbol, parts[i].data, parts[i].length);
    uint8_t digest[32];
    if (!s)
        ud_hash(symbol.data, symbol.size, digest);
    ud_bytes_clear(&symbol);
    char hex[65];
    for (uint32_t j = 0; !s && j < 32; j++)
        snprintf(hex + j * 2, 3, "%02x", digest[j]);
    if (!s)
        s = ud_id_make(c->memory, (ud_string){"component.port.v1", 17}, (ud_string){hex, 64}, out);
    return s;
}
typedef struct edge {
    uint32_t from, to, output, input;
    ud_queue *queue;
    int ended;
    atomic_uint occupied;
    atomic_uint_fast64_t produced, consumed, high_water, spill_bytes;
} edge;
typedef struct execution_tree {
    uint32_t offset, count;
    uint64_t ready_ms, first_ms, end_ms, quanta, busy, callback_ms, cpu_ns;
} execution_tree;
typedef struct node {
    ud_string id;
    ud_component_descriptor descriptor;
    ud_json *configuration;
    char *json;
    uint32_t json_length;
    ud_batch **schemas;
    uint32_t *inputs, *outputs, output_edges, ended, next_port, tree;
    uint8_t *emitted;
    void *instance;
    int opened, emit_allowed, pending, control_pending, watermark_refresh;
    uint32_t watermark_refresh_input;
    ud_component_event deferred_control;
    uint64_t idle_mask, idle_started, idle_ms, sampled_rows;
    atomic_uint_fast64_t live_idle_ms;
    atomic_int idle;
    atomic_int_fast64_t live_watermark;
    atomic_int state, result;
    ud_activation activation;
    ud_component_context context;
    ud_component_plan binding, *input_plans;
    ud_component_ports ports;
    uint64_t steps, processed, produced, rejected, wait_input, wait_capacity, callback_ms, cpu_ns,
        first_ms, end_ms, ready_ms, prepare_ms, prepare_cpu_ns, finish_ms, finish_cpu_ns,
        blocked_ms, input_wait_ms, capacity_wait_ms;
    int blocked_kind;
    uint64_t batch_latency[64], batch_samples, batch_max_ns, batch_sum_ns;
    uint64_t barrier_mask, barrier_epoch, acknowledged_epoch, watermark_mask, schema_version;
    atomic_uint_fast64_t staged_epoch;
    atomic_uint_fast64_t live_processed, live_produced, live_rejected, live_callback_ms;
    atomic_uint_fast64_t *port_rows;
    int64_t watermarks[64], last_watermark;
    ud_string source_position;
} node;
static ud_status node_failed(node *n, ud_status result, const char *stage) {
    atomic_store(&n->result, result);
    atomic_store(&n->state, UD_EXEC_FAILED);
    return ud_fail(n->context.memory, result, "component '%.*s' %s failed", (int)n->id.length,
                   n->id.data, stage);
}
static void batch_observe(node *n, uint64_t nanoseconds) {
    uint32_t bucket = 0;
    uint64_t upper = 1;
    while (upper < nanoseconds && bucket < 63) {
        bucket++;
        upper <<= 1;
    }
    n->batch_latency[bucket]++;
    n->batch_samples++;
    if (nanoseconds > n->batch_max_ns)
        n->batch_max_ns = nanoseconds;
    n->batch_sum_ns += nanoseconds;
}
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
typedef struct physical_plan {
    ud_activation *execution;
    node *nodes;
    edge *edges;
    uint32_t count, paths, *order, *tree_order, tree_count, concurrency;
    execution_tree *trees;
    ud_string id;
    ud_work_group *group;
    atomic_uint_fast64_t confirmed_epoch;
    ud_mutex epoch_lock;
    int epoch_lock_ready;
    ud_json *epoch_record;
    struct physical_plan *progress_next;
    int progress_registered;
    uint64_t sampled_ms;
} physical_plan;
static ud_status plan_json(physical_plan *, ud_bytes *, int, ud_status);
static int index_of(const physical_plan *p, ud_string id) {
    for (uint32_t i = 0; i < p->count; i++)
        if (ud_same(p->nodes[i].id, id))
            return (int)i;
    return -1;
}
static ud_status clear(physical_plan *p, ud_status result) {
    ud_status cleanup = UD_OK;
    if (p->progress_registered) {
        ud_execution *e = p->execution->execution;
        ud_mutex_lock(&e->diagnostics_lock);
        physical_plan **at = (physical_plan **)&e->active_progress;
        while (*at && *at != p)
            at = &(*at)->progress_next;
        if (*at == p)
            *at = p->progress_next;
        p->progress_registered = 0;
        ud_mutex_unlock(&e->diagnostics_lock);
    }
    if (p->edges)
        for (uint32_t i = 0; i < p->paths; i++) {
            if (p->edges[i].queue) {
                atomic_fetch_sub(&p->execution->execution->queued,
                                 ud_queue_count(p->edges[i].queue));
                ud_status status = ud_queue_destroy(p->edges[i].queue);
                if (!cleanup && status)
                    cleanup = status;
            }
        }
    if (p->nodes)
        for (uint32_t i = p->count; i; i--) {
            node *n = p->nodes + (p->order ? p->order[i - 1] : i - 1);
            ud_free((void *)n->source_position.data);
            if (n->opened)
                n->descriptor.dispose(n->instance, result);
            if (n->schemas)
                for (uint32_t j = 0; j < n->descriptor.outputs; j++)
                    ud_batch_destroy(n->schemas[j]);
            ud_component_plan_clear(&n->binding);
            ud_free(n->schemas);
            ud_free(n->input_plans);
            for (uint32_t k = 0; k < n->ports.input_count; k++)
                ud_free((void *)n->ports.inputs[k].name.data);
            for (uint32_t k = 0; k < n->ports.output_count; k++)
                ud_free((void *)n->ports.outputs[k].name.data);
            ud_free(n->ports.inputs);
            ud_free(n->ports.outputs);
            ud_free(n->inputs);
            ud_free(n->outputs);
            ud_free(n->emitted);
            ud_free(n->port_rows);
            ud_free(n->json);
            ud_json_destroy(n->configuration);
        }
    ud_free(p->order);
    ud_json_destroy(p->epoch_record);
    if (p->epoch_lock_ready)
        ud_mutex_destroy(&p->epoch_lock);
    ud_free(p->nodes);
    ud_free(p->edges);
    ud_free(p->trees);
    ud_free(p->tree_order);
    return result ? result : cleanup;
}
static ud_status bind_ports(node *n, ud_activation *e, const ud_json *definition,
                            ud_string pipeline) {
    n->activation = (ud_activation){.execution = e->execution,
                                    .parent = e,
                                    .definition = e->definition,
                                    .package = e->package,
                                    .parameters = e->parameters,
                                    .scope = e->scope,
                                    .task_id = e->task_id,
                                    .pipeline_id = pipeline,
                                    .component_id = n->id,
                                    .deadline = e->deadline,
                                    .scope_authority = e,
                                    .depth = e->depth,
                                    .parallel = e->parallel};
    atomic_init(&n->activation.cancelled, 0);
    n->context = (ud_component_context){.struct_size = sizeof(n->context),
                                        .abi_version = UD_ABI_VERSION,
                                        .memory = e->execution->ctx,
                                        .execution = e->execution,
                                        .activation = &n->activation,
                                        .task_id = e->task_id,
                                        .pipeline_id = pipeline,
                                        .component_id = n->id,
                                        .max_batch_rows = e->execution->options.max_batch_rows};
    UD_TRY(ud_bound_config(&n->activation, ud_member(definition, "config"), &n->configuration));
    ud_json *bound = NULL;
    UD_TRY(ud_component_bind_configuration(e->execution->ctx, &n->descriptor, n->configuration,
                                           &bound));
    ud_json_destroy(n->configuration);
    n->configuration = bound;
    UD_TRY(ud_json_serialize(n->configuration, &n->json, &n->json_length));
    ud_component_ports *ports = &n->ports;
    if (n->descriptor.bind_ports)
        UD_TRY(n->descriptor.bind_ports(n->descriptor.user, &n->context,
                                        (ud_string){n->json, n->json_length}, ports));
    else {
        ports->input_count = n->descriptor.inputs;
        ports->output_count = n->descriptor.outputs;
        ports->inputs =
            ud_alloc(e->execution->ctx, (ports->input_count + 1) * sizeof(*ports->inputs));
        ports->outputs =
            ud_alloc(e->execution->ctx, (ports->output_count + 1) * sizeof(*ports->outputs));
        if (!ports->inputs || !ports->outputs)
            return UD_RESOURCE;
        for (uint32_t direction = 0; direction < 2; direction++)
            for (uint32_t i = 0; i < (direction ? ports->output_count : ports->input_count); i++) {
                ud_component_port *port = (direction ? ports->outputs : ports->inputs) + i;
                char name[32];
                int length =
                    snprintf(name, sizeof(name), "%s%u", direction ? "output" : "input", i);
                port->name = (ud_string){ud_strdup_n(e->execution->ctx, name, (size_t)length),
                                         (uint32_t)length};
                if (!port->name.data)
                    return UD_RESOURCE;
                port->flags =
                    ((direction ? n->descriptor.optional_outputs : n->descriptor.optional_inputs) &
                     (UINT64_C(1) << i))
                        ? 0
                        : UD_PORT_REQUIRED;
                if (direction && (int32_t)i == n->descriptor.error_output)
                    port->flags |= UD_PORT_ERROR;
            }
    }
    if (ports->input_count > 64 || ports->output_count > 64)
        return UD_SCHEMA;
    n->descriptor.inputs = ports->input_count;
    n->descriptor.outputs = ports->output_count;
    n->descriptor.optional_inputs = n->descriptor.optional_outputs = 0;
    n->descriptor.error_output = -1;
    for (uint32_t direction = 0; direction < 2; direction++)
        for (uint32_t i = 0; i < (direction ? ports->output_count : ports->input_count); i++) {
            ud_component_port *port = (direction ? ports->outputs : ports->inputs) + i;
            port->direction = direction ? UD_PORT_OUTPUT : UD_PORT_INPUT;
            port->index = i;
            if (!port->name.data || !port->name.length ||
                memchr(port->name.data, 0, port->name.length))
                return UD_SCHEMA;
            for (uint32_t j = 0; j < i; j++) {
                ud_component_port *other = (direction ? ports->outputs : ports->inputs) + j;
                if (ud_same(port->name, other->name))
                    return UD_SCHEMA;
            }
            if (!port->id.high && !port->id.low)
                UD_TRY(ud_component_port_identity(&n->context, direction, port->name, &port->id));
            for (uint32_t j = 0; j < i; j++) {
                ud_component_port *other = (direction ? ports->outputs : ports->inputs) + j;
                if (!ud_id_compare(other->id, port->id))
                    return UD_SCHEMA;
            }
            if (!(port->flags & UD_PORT_REQUIRED)) {
                if (direction)
                    n->descriptor.optional_outputs |= UINT64_C(1) << i;
                else
                    n->descriptor.optional_inputs |= UINT64_C(1) << i;
            }
            if (port->flags & UD_PORT_ERROR) {
                if (!direction || n->descriptor.error_output != -1)
                    return UD_SCHEMA;
                n->descriptor.error_output = (int32_t)i;
            }
        }
    n->context.input_count = ports->input_count;
    n->context.output_count = ports->output_count;
    return UD_OK;
}
ud_status ud_execution_component_ports(ud_execution *execution, ud_string pipeline_id,
                                       ud_string component_id, ud_string kind, ud_string config,
                                       char **out, uint32_t *length) {
    if (!execution || !out || !length || !pipeline_id.length || !component_id.length ||
        !kind.length)
        return UD_INVALID;
    *out = NULL;
    *length = 0;
    int state = atomic_load(&execution->state);
    if (state == UD_EXEC_RUNNING || state == UD_EXEC_DRAINING)
        return UD_BUSY;
    node bound = {.id = component_id};
    ud_json *definition = NULL;
    ud_bytes input = {.ctx = execution->ctx}, result = {.ctx = execution->ctx};
    ud_status status = ud_component_lookup(&execution->root, kind, &bound.descriptor);
    if (!status)
        status = ud_bytes_put(&input, "{\"config\":", 10);
    if (!status)
        status = ud_bytes_put(&input, config.data, config.length);
    if (!status)
        status = ud_bytes_put(&input, "}", 1);
    if (!status)
        status = ud_json_parse(execution->ctx, (const char *)input.data, (uint32_t)input.size,
                               &definition, NULL);
    if (!status)
        status = bind_ports(&bound, &execution->root, definition, pipeline_id);
    if (!status)
        status = ud_bytes_put(&result, "[", 1);
    for (uint32_t direction = 0, emitted = 0; !status && direction < 2; direction++) {
        uint32_t count = direction ? bound.ports.output_count : bound.ports.input_count;
        const ud_component_port *ports = direction ? bound.ports.outputs : bound.ports.inputs;
        for (uint32_t at = 0; !status && at < count; at++) {
            if (emitted++)
                status = ud_bytes_put(&result, ",", 1);
            char id[33], numbers[96];
            ud_id_format(ports[at].id, id);
            if (!status)
                status = ud_bytes_put(&result, "{\"id\":", 6);
            if (!status)
                status = ud_json_quote(&result, id, 32);
            if (!status)
                status = ud_bytes_put(&result, ",\"name\":", 8);
            if (!status)
                status = ud_json_quote(&result, ports[at].name.data, ports[at].name.length);
            int size =
                snprintf(numbers, sizeof(numbers), ",\"direction\":%u,\"index\":%u,\"flags\":%u}",
                         ports[at].direction, ports[at].index, ports[at].flags);
            if (!status)
                status = ud_bytes_put(&result, numbers, (size_t)size);
        }
    }
    if (!status)
        status = ud_bytes_put(&result, "]", 1);
    if (!status)
        status = ud_bytes_put(&result, "", 1);
    if (!status) {
        *out = (char *)result.data;
        *length = (uint32_t)result.size - 1;
        result.data = NULL;
    }
    for (uint32_t at = 0; at < bound.ports.input_count; at++)
        ud_free((void *)bound.ports.inputs[at].name.data);
    for (uint32_t at = 0; at < bound.ports.output_count; at++)
        ud_free((void *)bound.ports.outputs[at].name.data);
    ud_free(bound.ports.inputs);
    ud_free(bound.ports.outputs);
    ud_free(bound.json);
    ud_json_destroy(bound.configuration);
    ud_json_destroy(definition);
    ud_bytes_clear(&input);
    ud_bytes_clear(&result);
    return status;
}

static ud_status port_index(const node *n, int output, const ud_json *json, uint64_t *index) {
    if (ud_json_kind(json) == UD_JSON_NUMBER)
        return ud_json_uint(json, index);
    ud_string name;
    UD_TRY(ud_json_string(json, &name));
    ud_id id = {0};
    int by_id = name.length == 32 && !ud_id_parse(name, &id);
    const ud_component_port *ports = output ? n->ports.outputs : n->ports.inputs;
    uint32_t count = output ? n->ports.output_count : n->ports.input_count;
    for (uint32_t i = 0; i < count; i++)
        if (ud_same(ports[i].name, name) || (by_id && !ud_id_compare(ports[i].id, id))) {
            *index = i;
            return UD_OK;
        }
    return UD_SCHEMA;
}
static ud_status plan(ud_activation *e, const ud_json *definition, physical_plan *p) {
    p->execution = e;
    static const char *keys[] = {"id", "components", "paths"};
    UD_TRY(ud_json_keys(definition, keys, 3));
    UD_TRY(ud_json_string(ud_member(definition, "id"), &p->id));
    const ud_json *nodes = ud_member(definition, "components"),
                  *paths = ud_member(definition, "paths");
    if (ud_json_kind(nodes) != UD_JSON_ARRAY || ud_json_kind(paths) != UD_JSON_ARRAY)
        return UD_SCHEMA;
    p->count = ud_json_size(nodes);
    p->paths = ud_json_size(paths);
    if (!p->count || (!p->paths && !e->execution->preview_pipeline) || p->count > 65536 ||
        p->paths > 65536)
        return UD_SCHEMA;
    p->nodes = ud_alloc(e->execution->ctx, p->count * sizeof(*p->nodes));
    p->edges = ud_alloc(e->execution->ctx, (p->paths + 1) * sizeof(*p->edges));
    if (!p->nodes || !p->edges)
        return UD_RESOURCE;
    for (uint32_t i = 0; i < p->count; i++) {
        node *n = p->nodes + i;
        const ud_json *def = ud_json_at(nodes, i);
        static const char *nk[] = {"id", "component", "config"};
        UD_TRY(ud_json_keys(def, nk, 3));
        UD_TRY(ud_json_string(ud_member(def, "id"), &n->id));
        if (index_of(p, n->id) != (int)i)
            return UD_SCHEMA;
        ud_string component = {0};
        UD_TRY(ud_json_string(ud_member(def, "component"), &component));
        n->descriptor = (ud_component_descriptor){.struct_size = sizeof(n->descriptor),
                                                  .abi_version = UD_ABI_VERSION};
        UD_TRY(ud_component_lookup(e, component, &n->descriptor));
        if (e->execution->preview_pipeline &&
            (!(n->descriptor.capabilities & UD_COMPONENT_PREVIEW_SAFE) ||
             (n->descriptor.effects & UD_EFFECT_EXTERNAL_WRITE)))
            return UD_AUTH;
        if (e->parallel && !(n->descriptor.capabilities & UD_COMPONENT_THREAD_SAFE))
            return UD_UNSUPPORTED;
        if (e->execution->options.state_directory.length &&
            !(n->descriptor.capabilities & UD_COMPONENT_REPLAY_SAFE))
            return UD_UNSUPPORTED;
        if (n->descriptor.category < UD_COMPONENT_SOURCE ||
            n->descriptor.category > UD_COMPONENT_DESTINATION)
            return UD_SCHEMA;
        if (ud_json_kind(ud_member(def, "config")) != UD_JSON_OBJECT)
            return UD_SCHEMA;
        ud_status bound = bind_ports(n, e, def, p->id);
        if (bound)
            return ud_fail(e->execution->ctx, bound, "component '%.*s' configuration failed",
                           (int)n->id.length, n->id.data);
        n->inputs = ud_alloc(e->execution->ctx, (n->descriptor.inputs + 1) * sizeof(*n->inputs));
        n->emitted = ud_alloc(e->execution->ctx, n->descriptor.outputs + 1);
        if (!n->inputs || !n->emitted)
            return UD_RESOURCE;
        for (uint32_t j = 0; j < n->descriptor.inputs; j++)
            n->inputs[j] = UINT32_MAX;
        atomic_init(&n->state, UD_EXEC_READY);
        atomic_init(&n->result, UD_OK);
        atomic_init(&n->live_processed, 0);
        atomic_init(&n->live_produced, 0);
        atomic_init(&n->live_rejected, 0);
        atomic_init(&n->live_callback_ms, 0);
        n->port_rows =
            ud_alloc(e->execution->ctx, (n->descriptor.outputs + 1) * sizeof(*n->port_rows));
        if (!n->port_rows)
            return UD_RESOURCE;
        for (uint32_t port = 0; port < n->descriptor.outputs; port++)
            atomic_init(n->port_rows + port, 0);
    }
    uint32_t *degree = ud_alloc(e->execution->ctx, p->count * sizeof(*degree));
    uint8_t *seen = ud_alloc(e->execution->ctx, p->count);
    if (!degree || !seen) {
        ud_free(degree);
        ud_free(seen);
        return UD_RESOURCE;
    }
    ud_status s = UD_OK;
    for (uint32_t i = 0; !s && i < p->paths; i++) {
        const ud_json *def = ud_json_at(paths, i);
        static const char *ek[] = {"from", "output", "to", "input"};
        s = ud_json_keys(def, ek, 4);
        ud_string from = {0}, to = {0};
        uint64_t output = 0, input = 0;
        if (!s)
            s = ud_json_string(ud_member(def, "from"), &from);
        if (!s)
            s = ud_json_string(ud_member(def, "to"), &to);
        int a = !s ? index_of(p, from) : -1, b = !s ? index_of(p, to) : -1;
        if (!s && (a < 0 || b < 0))
            s = UD_SCHEMA;
        if (!s)
            s = port_index(p->nodes + a, 1, ud_member(def, "output"), &output);
        if (!s)
            s = port_index(p->nodes + b, 0, ud_member(def, "input"), &input);
        if (!s && (a < 0 || b < 0 || a == b || output >= p->nodes[a].descriptor.outputs ||
                   input >= p->nodes[b].descriptor.inputs))
            s = UD_SCHEMA;
        if (!s) {
            edge *v = p->edges + i;
            *v = (edge){.from = (uint32_t)a,
                        .to = (uint32_t)b,
                        .output = (uint32_t)output,
                        .input = (uint32_t)input};
            atomic_init(&v->produced, 0);
            atomic_init(&v->consumed, 0);
            atomic_init(&v->high_water, 0);
            atomic_init(&v->spill_bytes, 0);
            atomic_init(&v->occupied, 0);
            if (p->nodes[b].inputs[input] != UINT32_MAX)
                s = UD_SCHEMA;
            else {
                p->nodes[b].inputs[input] = i;
                p->nodes[a].output_edges++;
                degree[b]++;
            }
        }
    }
    for (uint32_t i = 0; !s && i < p->count; i++) {
        node *n = p->nodes + i;
        n->outputs = ud_alloc(e->execution->ctx, (n->output_edges + 1) * sizeof(*n->outputs));
        if (!n->outputs)
            s = UD_RESOURCE;
        n->output_edges = 0;
    }
    for (uint32_t i = 0; !s && i < p->paths; i++) {
        node *n = p->nodes + p->edges[i].from;
        n->outputs[n->output_edges++] = i;
    }
    for (uint32_t i = 0; !s && i < p->count; i++) {
        node *n = p->nodes + i;
        for (uint32_t j = 0; j < n->descriptor.inputs; j++)
            if (n->inputs[j] == UINT32_MAX) {
                if (!(n->descriptor.optional_inputs & (UINT64_C(1) << j)))
                    s = UD_SCHEMA;
                else
                    n->ended++;
            }
        for (uint32_t j = 0; j < n->descriptor.outputs; j++) {
            int found = 0;
            for (uint32_t k = 0; k < n->output_edges; k++)
                if (p->edges[n->outputs[k]].output == j)
                    found = 1;
            if (!found && !e->execution->preview_pipeline &&
                !(n->descriptor.optional_outputs & (UINT64_C(1) << j)))
                s = UD_SCHEMA;
        }
    }
    for (uint32_t i = 0; !s && i < p->count; i++)
        if (ud_named(p->nodes[i].descriptor.id, "scd")) {
            uint32_t destination = UINT32_MAX, seen_ports = 0;
            for (uint32_t j = 0; j < p->nodes[i].output_edges; j++) {
                edge *v = p->edges + p->nodes[i].outputs[j];
                if (v->output >= 1 && v->output <= 4) {
                    if (!ud_named(p->nodes[v->to].descriptor.id, "scd.destination") ||
                        v->input != v->output - 1 ||
                        (destination != UINT32_MAX && destination != v->to) ||
                        (seen_ports & (1u << v->output))) {
                        s = UD_SCHEMA;
                        break;
                    }
                    destination = v->to;
                    seen_ports |= 1u << v->output;
                }
            }
            if (seen_ports != 30u)
                s = UD_SCHEMA;
        }
    uint32_t *order = ud_alloc(e->execution->ctx, p->count * sizeof(*order));
    if (!order && !s)
        s = UD_RESOURCE;
    uint32_t at = 0;
    while (!s && at < p->count) {
        int progress = 0;
        for (uint32_t i = 0; i < p->count; i++)
            if (!seen[i] && !degree[i]) {
                seen[i] = 1;
                order[at++] = i;
                progress = 1;
                for (uint32_t j = 0; j < p->nodes[i].output_edges; j++)
                    degree[p->edges[p->nodes[i].outputs[j]].to]--;
            }
        if (!progress)
            s = UD_SCHEMA;
    }
    ud_free(degree);
    ud_free(seen);
    if (s)
        ud_free(order);
    else {
        p->order = order;
        p->trees = ud_alloc(e->execution->ctx, p->count * sizeof(*p->trees));
        p->tree_order = ud_alloc(e->execution->ctx, p->count * sizeof(*p->tree_order));
        if (!p->trees || !p->tree_order)
            return UD_RESOURCE;
        p->concurrency =
            ud_transaction_active(e) || e->transaction_serial ? 1 : ud_execution_concurrency(e);
        for (uint32_t i = 0; i < p->count; i++) {
            node *n = p->nodes + order[i];
            n->tree = n->descriptor.synchronous_input >= 0
                          ? p->nodes[p->edges[n->inputs[n->descriptor.synchronous_input]].from].tree
                          : p->tree_count++;
            p->trees[n->tree].count++;
            if (!(n->descriptor.capabilities & UD_COMPONENT_THREAD_SAFE) ||
                (n->descriptor.capabilities & UD_COMPONENT_THREAD_AFFINE))
                p->concurrency = 1;
        }

        const ud_json *maximum = ud_member(ud_member(e->package->document, "policy"), "maxTrees");
        if (maximum) {
            uint64_t value = 0;
            UD_TRY(ud_json_uint(maximum, &value));
            if (!value || value > 64)
                return UD_SCHEMA;
            if (value < p->concurrency)
                p->concurrency = (uint32_t)value;
        }
        uint32_t offset = 0;
        for (uint32_t i = 0; i < p->tree_count; i++) {
            p->trees[i].offset = offset;
            offset += p->trees[i].count;
            p->trees[i].count = 0;
        }
        for (uint32_t i = 0; i < p->count; i++) {
            execution_tree *tree = p->trees + p->nodes[order[i]].tree;
            p->tree_order[tree->offset + tree->count++] = order[i];
        }
    }
    return s;
}
ud_status ud_pipeline_validate(ud_activation *e, const ud_json *definition) {
    physical_plan p = {0};
    ud_status s = plan(e, definition, &p);
    return clear(&p, s);
}
static ud_status emit(void *user, uint32_t port, ud_batch *b) {
    node *n = user;
    ud_activation *e = n->context.activation;
    if (!n->emit_allowed)
        return UD_INVALID;
    if (!b || port >= n->descriptor.outputs || n->emitted[port] || b->ctx != e->execution->ctx ||
        b->rows > e->execution->options.max_batch_rows)
        return UD_SCHEMA;
    ud_batch *schema = n->schemas[port];
    if (b->columns != schema->columns)
        return UD_SCHEMA;
    for (uint32_t j = 0; j < b->columns; j++)
        if (!ud_field_matches(&b->column[j].field, &schema->column[j].field, 1))
            return UD_SCHEMA;
    return UD_OK;
}
typedef struct emit_target {
    physical_plan *plan;
    node *node;
} emit_target;
static uint32_t recommended_rows(void *user) {
    emit_target *target = user;
    node *n = target->node;
    physical_plan *p = target->plan;
    /* Receipt digests bind logical delivery boundaries. Recoverable executions
     * keep their declared chunk size so retries cannot reuse a batch ID with a
     * different row set; ephemeral executions adapt to live pressure. */
    if (p->execution->execution->journal)
        return n->context.max_batch_rows;
    uint64_t rows = n->context.max_batch_rows,
             width = n->binding.row_width ? n->binding.row_width : 64;
    uint32_t capacity = p->execution->execution->options.edge_capacity, free_slots = capacity;
    for (uint32_t i = 0; i < n->output_edges; i++) {
        uint32_t used = atomic_load(&p->edges[n->outputs[i]].occupied),
                 available = used < capacity ? capacity - used : 0;
        if (available < free_slots)
            free_slots = available;
    }
    if (capacity && free_slots < capacity)
        rows = rows * free_slots / capacity;
    ud_buffer_options limits;
    ud_buffer_metrics metrics;
    if (!ud_buffer_manager_limits(p->execution->execution->buffers, &limits) &&
        !ud_buffer_manager_metrics(p->execution->execution->buffers, &metrics)) {
        uint64_t available = limits.working_limit > metrics.working_bytes
                                 ? limits.working_limit - metrics.working_bytes
                                 : 0;
        uint64_t fair = p->concurrency ? available / p->concurrency : available;
        uint64_t bounded = fair / (width + sizeof(ud_row_info));
        if (bounded < rows)
            rows = bounded;
    }
    return rows ? (uint32_t)rows : 1;
}
static ud_status stage_epoch(physical_plan *, node *, uint64_t);
static ud_status epoch_component_key(physical_plan *, node *, char[65]);
/* An edge has one publisher. A delivery retains its slot through the
 * consumer callback, even after the frame leaves the physical queue. */
static ud_status publish(physical_plan *p, edge *v, ud_stream_event *frame, const ud_batch *batch) {
    ud_execution *execution = p->execution->execution;
    if (atomic_load(&v->occupied) >= execution->options.edge_capacity)
        return UD_BUSY;
    if (batch)
        UD_TRY(ud_buffer_store_exchange(execution->buffers, batch, &frame->block));
    uint64_t spill = ud_buffer_block_spill_bytes(frame->block);
    atomic_fetch_add(&v->occupied, 1);
    uint64_t queued = atomic_fetch_add(&execution->queued, 1) + 1;
    ud_status status = ud_queue_push(v->queue, frame);
    if (status) {
        atomic_fetch_sub(&execution->queued, 1);
        ud_status cleanup = ud_buffer_release(frame->block);
        atomic_fetch_sub(&v->occupied, 1);
        return cleanup ? cleanup : status;
    }
    atomic_fetch_add(&v->spill_bytes, spill);
    if (frame->kind == UD_STREAM_DATA)
        atomic_fetch_add(&v->produced, batch->rows);
    uint64_t size = ud_queue_count(v->queue), peak = atomic_load(&v->high_water);
    while (size > peak && !atomic_compare_exchange_weak(&v->high_water, &peak, size)) {
    }
    uint64_t previous = atomic_load(&execution->high_water);
    while (queued > previous &&
           !atomic_compare_exchange_weak(&execution->high_water, &previous, queued)) {
    }
    ud_work_group_ready(p->group, p->nodes[v->to].tree);
    return UD_OK;
}
static ud_status release_delivery(physical_plan *p, edge *v, ud_stream_event *event,
                                  ud_buffer_view *view) {
    ud_buffer_view_release(view);
    ud_status status = ud_stream_event_clear(event);
    if (v && status != UD_BUSY) {
        atomic_fetch_sub(&v->occupied, 1);
        if (!status)
            ud_work_group_ready(p->group, p->nodes[v->from].tree);
    }
    return status;
}

static ud_status route_identified_full(void *user, uint32_t port, ud_batch *b) {
    emit_target *target = user;
    node *n = target->node;
    physical_plan *p = target->plan;
    ud_activation *e = p->execution;
    UD_TRY(ud_activation_check(&n->activation));
    UD_TRY(emit(n, port, b));
    UD_TRY(ud_batch_seal(b));
    int connected = 0;
    for (uint32_t i = 0; i < n->output_edges; i++)
        if (p->edges[n->outputs[i]].output == port)
            connected = 1;
    if (!connected && !p->execution->execution->preview_pipeline)
        return UD_SCHEMA;
    for (uint32_t i = 0; i < n->output_edges; i++) {
        edge *v = p->edges + n->outputs[i];
        if (v->output != port)
            continue;
        ud_stream_event frame = {.struct_size = sizeof(frame),
                                 .abi_version = UD_ABI_VERSION,
                                 .kind = UD_STREAM_DATA,
                                 .epoch = atomic_load(&p->confirmed_epoch) + 1};
        UD_TRY(publish(p, v, &frame, b));
    }
    n->emitted[port] = 1;
    n->produced += b->rows;
    atomic_fetch_add(&n->live_produced, b->rows);
    atomic_fetch_add(n->port_rows + port, b->rows);
    if ((int32_t)port == n->descriptor.error_output) {
        n->rejected += b->rows;
        atomic_fetch_add(&n->live_rejected, b->rows);
        atomic_fetch_add(&e->execution->error_rows, b->rows);
        ud_activation *owner =
            n->activation.scope_authority ? n->activation.scope_authority : &n->activation;
        atomic_fetch_add(&owner->error_rows, b->rows);
    }
    atomic_fetch_add(&e->execution->batches, 1);
    UD_TRY(ud_debug_preview(&n->activation, n->ports.outputs + port, b));
    if (n->descriptor.category == UD_COMPONENT_SOURCE)
        ud_task_rows(e, b->rows, 0, 0);

    return UD_OK;
}
static ud_status route_identified(void *user, uint32_t port, ud_batch *batch) {
    emit_target *target = user;
    node *n = target->node;
    ud_execution *e = target->plan->execution->execution;
    if (e->preview_pipeline && n->descriptor.category == UD_COMPONENT_SOURCE && batch) {
        uint64_t remaining =
            e->preview_source_rows > n->produced ? e->preview_source_rows - n->produced : 0;
        if (!remaining) {
            if (port >= n->descriptor.outputs)
                return UD_SCHEMA;
            n->emitted[port] = 1;
            return UD_OK;
        }
        if (batch->rows > remaining) {
            ud_batch *slice = NULL;
            UD_TRY(ud_batch_seal(batch));
            UD_TRY(ud_batch_slice(e->ctx, batch, 0, (uint32_t)remaining, &slice));
            ud_status s = route_identified_full(user, port, slice);
            ud_batch_destroy(slice);
            return s;
        }
    }
    return route_identified_full(user, port, batch);
}
static ud_status route_event(void *user, uint32_t port, const ud_component_event *event) {
    emit_target *target = user;
    physical_plan *p = target->plan;
    node *n = target->node;
    if (!event || event->struct_size != sizeof(*event) || event->abi_version != UD_ABI_VERSION ||
        port >= n->descriptor.outputs || !n->emit_allowed || n->emitted[port])
        return UD_INVALID;
    if (event->kind == UD_COMPONENT_DATA)
        return route_identified(user, port, (ud_batch *)event->batch);
    if (event->kind != UD_COMPONENT_WATERMARK && event->kind != UD_COMPONENT_CHECKPOINT_BARRIER &&
        event->kind != UD_COMPONENT_SCHEMA_CHANGE && event->kind != UD_COMPONENT_INPUT_END &&
        event->kind != UD_COMPONENT_IDLE && event->kind != UD_COMPONENT_ACTIVE)
        return UD_SCHEMA;
    if (event->kind == UD_COMPONENT_WATERMARK) {
        if (event->watermark < n->last_watermark)
            return UD_FENCED;
        n->last_watermark = event->watermark;
        atomic_store(&n->live_watermark, event->watermark);
    }
    if (event->kind == UD_COMPONENT_SCHEMA_CHANGE ||
        (event->kind == UD_COMPONENT_CHECKPOINT_BARRIER && event->schema_version))
        n->schema_version = event->schema_version;
    if (event->kind == UD_COMPONENT_CHECKPOINT_BARRIER &&
        n->descriptor.category == UD_COMPONENT_SOURCE) {
        if (!n->descriptor.confirm_epoch || !event->epoch ||
            event->epoch != atomic_load(&p->confirmed_epoch) + 1)
            return UD_UNSUPPORTED;
        if (n->barrier_epoch != event->epoch) {
            ud_free((void *)n->source_position.data);
            n->source_position =
                (ud_string){ud_strdup_n(p->execution->execution->ctx, event->source_position.data,
                                        event->source_position.length),
                            event->source_position.length};
            if (!n->source_position.data)
                return UD_RESOURCE;
            n->barrier_epoch = event->epoch;
            UD_TRY(stage_epoch(p, n, event->epoch));
        }
    }
    int connected = 0;
    for (uint32_t i = 0; i < n->output_edges; i++) {
        edge *v = p->edges + n->outputs[i];
        if (v->output != port)
            continue;
        connected = 1;
        ud_stream_event frame = {.struct_size = sizeof(frame),
                                 .abi_version = UD_ABI_VERSION,
                                 .kind = event->kind,
                                 .epoch = event->epoch,
                                 .schema_version = event->schema_version,
                                 .watermark = event->watermark,
                                 .source_position = event->source_position};
        UD_TRY(publish(p, v, &frame, event->batch));
    }
    if (!connected && !p->execution->execution->preview_pipeline)
        return UD_SCHEMA;
    n->emitted[port] = 1;
    return UD_OK;
}
static ud_status route(void *user, uint32_t port, ud_batch *batch) {
    emit_target *target = user;
    node *n = target->node;
    if (!batch || n->descriptor.category != UD_COMPONENT_SOURCE || !batch->rows)
        return route_identified(user, port, batch);
    uint32_t specified = 0;
    for (uint32_t row = 0; row < batch->rows; row++) {
        ud_row_info info;
        UD_TRY(ud_batch_row_info(batch, row, &info));
        specified += info.id.high || info.id.low;
    }
    if (specified == batch->rows)
        return route_identified(user, port, batch);
    if (specified)
        return UD_SCHEMA;
    char scope[33];
    ud_status status = ud_pipeline_field_namespace(&n->context, scope);
    ud_id identity = {0};
    if (!status)
        status =
            ud_id_make(n->context.memory, (ud_string){scope, 32}, n->context.task_id, &identity);
    if (!status)
        status = ud_batch_seal(batch);
    ud_batch *identified = NULL;
    if (!status)
        status = ud_batch_identify(n->context.memory, batch, identity, n->produced, &identified);
    if (!status)
        status = route_identified(user, port, identified);
    ud_batch_destroy(identified);
    return status;
}
static ud_status validate_effects(physical_plan *p) {
    uint8_t *reachable = ud_alloc(p->execution->execution->ctx, p->count);
    if (!reachable)
        return UD_RESOURCE;
    ud_status status = UD_OK;
    int epochs = 0;
    for (uint32_t index = 0; index < p->count; index++)
        epochs |= p->nodes[index].descriptor.category == UD_COMPONENT_SOURCE &&
                  p->nodes[index].descriptor.confirm_epoch != NULL;
    for (uint32_t index = 0; !status && index < p->count; index++) {
        node *n = p->nodes + index;
        if (epochs && (n->binding.effects & UD_EFFECT_EXTERNAL_WRITE) &&
            !(n->binding.effects & (UD_EFFECT_RECEIPTED | UD_EFFECT_IDEMPOTENT)) &&
            !(p->execution->transaction_epoch && (n->binding.effects & UD_EFFECT_TRANSACTIONAL))) {
            status = UD_UNSUPPORTED;
            break;
        }
        if (n->descriptor.category == UD_COMPONENT_SOURCE && n->descriptor.confirm_epoch &&
            p->execution->transaction_serial && !p->execution->transaction_epoch) {
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
static ud_status configure_plan(physical_plan *p, const ud_json *definition, emit_target *targets) {
    ud_activation *e = p->execution;
    (void)definition;
    /* Entire pipeline schema/configuration is validated before any acquisition. */
    for (uint32_t i = 0; i < p->count; i++) {
        node *n = p->nodes + p->order[i];
        uint32_t index = p->order[i];
        targets[index] = (emit_target){p, n};
        n->activation =
            (ud_activation){.execution = e->execution,
                            .parent = e,
                            .definition = e->definition,
                            .package = e->package,
                            .parameters = e->parameters,
                            .scope = e->scope,
                            .task_id = e->task_id,
                            .pipeline_id = p->id,
                            .component_id = n->id,
                            .deadline = e->deadline,
                            .depth = e->depth,
                            .scope_authority = e,
                            .parallel = e->parallel || (p->tree_count > 1 && p->concurrency > 1),
                            .transaction = e->transaction,
                            .transaction_serial = e->transaction_serial,
                            .transaction_epoch = e->transaction_epoch,
                            .transaction_participants = e->transaction_participants};
        atomic_init(&n->activation.cancelled, 0);
        n->context = (ud_component_context){.struct_size = sizeof(n->context),
                                            .abi_version = UD_ABI_VERSION,
                                            .memory = e->execution->ctx,
                                            .execution = e->execution,
                                            .activation = &n->activation,
                                            .task_id = e->task_id,
                                            .pipeline_id = p->id,
                                            .component_id = n->id,
                                            .max_batch_rows = e->execution->options.max_batch_rows,
                                            .recommended_batch_rows = recommended_rows,
                                            .emit = route,
                                            .emit_user = targets + index,
                                            .resolve = ud_resolve_value,
                                            .resolve_user = &n->activation,
                                            .resolve_field = ud_resolve_field};

        char error_scope[33];
        UD_TRY(ud_pipeline_field_namespace(&n->context, error_scope));
        ud_id scope = {0};
        UD_TRY(ud_id_parse((ud_string){error_scope, 32}, &scope));
        n->context.errors = (ud_error_context){.struct_size = sizeof(ud_error_context),
                                               .abi_version = UD_ABI_VERSION,
                                               .memory = n->context.memory,
                                               .execution_id = e->execution->options.execution_id,
                                               .package_id = e->package->id,
                                               .task_id = e->task_id,
                                               .pipeline_id = p->id,
                                               .component_id = n->id,
                                               .field_scope = scope};

        if (n->activation.transaction_serial && ud_member(n->configuration, "connection")) {
            ud_string connection;
            UD_TRY(ud_json_string(ud_member(n->configuration, "connection"), &connection));
            UD_TRY(ud_transaction_resource_validate(&n->activation, connection));
        }

        if (n->descriptor.category == UD_COMPONENT_SOURCE &&
            ud_member(n->configuration, "incremental"))
            n->descriptor.capabilities |= UD_COMPONENT_STATEFUL | UD_COMPONENT_BLOCKING;
        n->schemas = ud_alloc(e->execution->ctx, (n->descriptor.outputs + 1) * sizeof(*n->schemas));
        if (!n->schemas)
            return UD_RESOURCE;
        const ud_batch **inputs =
            ud_alloc(e->execution->ctx, (n->descriptor.inputs + 1) * sizeof(*inputs));
        if (!inputs)
            return UD_RESOURCE;
        n->input_plans =
            ud_alloc(e->execution->ctx, (n->descriptor.inputs + 1) * sizeof(*n->input_plans));
        if (!n->input_plans) {
            ud_free(inputs);
            return UD_RESOURCE;
        }
        for (uint32_t edge_index = 0; edge_index < n->output_edges; edge_index++)
            n->context.connected_outputs |= UINT64_C(1) << p->edges[n->outputs[edge_index]].output;
        if (e->execution->preview_pipeline)
            n->context.connected_outputs = n->descriptor.outputs == 64
                                               ? UINT64_MAX
                                               : (UINT64_C(1) << n->descriptor.outputs) - 1;
        n->context.emit_event = route_event;
        n->context.input_count = n->descriptor.inputs;
        n->last_watermark = INT64_MIN;
        atomic_init(&n->staged_epoch, 0);
        atomic_init(&n->idle, 0);
        atomic_init(&n->live_idle_ms, 0);
        atomic_init(&n->live_watermark, INT64_MIN);
        n->context.output_count = n->descriptor.outputs;
        n->context.input_plans = n->input_plans;
        n->context.output_schemas = (const ud_batch *const *)n->schemas;
        for (uint32_t j = 0; j < n->descriptor.inputs; j++) {
            n->input_plans[j] = (ud_component_plan){.struct_size = sizeof(ud_component_plan),
                                                    .abi_version = UD_ABI_VERSION,
                                                    .partitions = 1,
                                                    .ordering = UD_ORDER_UNORDERED};
            inputs[j] = NULL;
            if (n->inputs[j] != UINT32_MAX) {
                edge *v = p->edges + n->inputs[j];
                inputs[j] = p->nodes[v->from].schemas[v->output];
                n->ports.inputs[j].schema = inputs[j];
                if ((int32_t)v->output != p->nodes[v->from].descriptor.error_output)
                    n->input_plans[j] = p->nodes[v->from].binding;
            }
        }
        ud_status s =
            n->descriptor.configure(n->descriptor.user, &n->context,
                                    (ud_string){n->json, n->json_length}, inputs, n->schemas);
        n->binding = (ud_component_plan){.struct_size = sizeof(n->binding),
                                         .abi_version = UD_ABI_VERSION,
                                         .partitions = 1,
                                         .ordering = UD_ORDER_UNORDERED};
        if (!s && !n->descriptor.describe_plan && n->descriptor.synchronous_input >= 0) {
            s = ud_component_plan_copy(
                e->execution->ctx, n->input_plans + n->descriptor.synchronous_input, &n->binding);
        }
        if (!s && n->descriptor.describe_plan)
            s = n->descriptor.describe_plan(n->descriptor.user, &n->context,
                                            (ud_string){n->json, n->json_length}, inputs,
                                            &n->binding);
        if (!s && n->descriptor.category == UD_COMPONENT_SOURCE && !n->descriptor.describe_plan &&
            (n->descriptor.capabilities & UD_COMPONENT_CONTINUOUS))
            n->binding.boundedness = UD_SOURCE_CONTINUOUS;
        ud_free(inputs);
        if (!s && (n->binding.struct_size != sizeof(n->binding) ||
                   n->binding.abi_version != UD_ABI_VERSION || !n->binding.partitions ||
                   n->binding.partitions > 64 || n->binding.ordering < UD_ORDER_UNORDERED ||
                   n->binding.ordering > UD_ORDER_GLOBAL ||
                   n->binding.boundedness > UD_SOURCE_SNAPSHOT_THEN_STREAM ||
                   (n->descriptor.category == UD_COMPONENT_SOURCE && n->binding.boundedness &&
                    !(n->descriptor.capabilities & UD_COMPONENT_CONTINUOUS))))
            s = UD_SCHEMA;
        if (s)
            return node_failed(n, s, "schema/plan binding");
        if (ud_member(n->configuration, "errorPolicy")) {
            ud_string policy;
            UD_TRY(ud_json_string(ud_member(n->configuration, "errorPolicy"), &policy));
            if (ud_named(policy, "redirect")) {
                int connected = 0;
                for (uint32_t j = 0; j < n->output_edges; j++)
                    if ((int32_t)p->edges[n->outputs[j]].output == n->descriptor.error_output)
                        connected = 1;
                if (!connected && !e->execution->preview_pipeline)
                    return UD_SCHEMA;
            }
        }
        for (uint32_t input = 0; input < n->descriptor.inputs; input++)
            if (n->input_plans[input].boundedness > n->binding.boundedness)
                n->binding.boundedness = n->input_plans[input].boundedness;
        n->binding.effects |= n->descriptor.effects;
        if (n->binding.boundedness && n->descriptor.category != UD_COMPONENT_SOURCE &&
            (n->descriptor.capabilities & (UD_COMPONENT_BLOCKING | UD_COMPONENT_STATEFUL)) &&
            !(n->descriptor.capabilities & UD_COMPONENT_STREAM_SAFE) &&
            !(n->descriptor.category == UD_COMPONENT_DESTINATION &&
              (n->binding.effects & (UD_EFFECT_RECEIPTED | UD_EFFECT_IDEMPOTENT))))
            return node_failed(n, UD_UNSUPPORTED,
                               "unbounded input requires window/TTL or bounded reference state");
        if (ud_member(n->configuration, "latePolicy")) {
            ud_string policy;
            UD_TRY(ud_json_string(ud_member(n->configuration, "latePolicy"), &policy));
            if (ud_named(policy, "redirect") && !(n->context.connected_outputs & UINT64_C(2)) &&
                !e->execution->preview_pipeline)
                return node_failed(n, UD_SCHEMA, "late redirect requires connected output 1");
        }
        if (e->execution->preview_pipeline && (n->binding.effects & UD_EFFECT_EXTERNAL_WRITE))
            return UD_AUTH;
        n->binding.state_version = n->descriptor.state_version;
        for (uint32_t j = 0; j < n->descriptor.outputs; j++) {
            n->ports.outputs[j].schema = n->schemas[j];
            if ((int32_t)j != n->descriptor.error_output) {
                n->ports.outputs[j].sort = n->binding.sort;
                n->ports.outputs[j].partition_count = n->binding.partitions;
                n->ports.outputs[j].partition_key_count = n->binding.partition_key_count;
                memcpy(n->ports.outputs[j].partition_keys, n->binding.partition_keys,
                       sizeof(n->binding.partition_keys));
            }
        }
        for (uint32_t j = 0; j < n->descriptor.outputs; j++)
            if (!n->schemas[j] || n->schemas[j]->ctx != e->execution->ctx || n->schemas[j]->rows)
                return UD_SCHEMA;
    }
    return validate_effects(p);
}
ud_status ud_pipeline_bind_validate(ud_activation *e, const ud_json *definition) {
    physical_plan p = {0};
    ud_status s = plan(e, definition, &p);
    emit_target *targets = NULL;
    if (!s) {
        targets = ud_alloc(e->execution->ctx, p.count * sizeof(*targets));
        if (!targets)
            s = UD_RESOURCE;
    }
    if (!s)
        s = configure_plan(&p, definition, targets);
    if (!s) {
        ud_bytes report = {.ctx = e->execution->ctx};
        s = plan_json(&p, &report, 0, UD_OK);
        if (!s) {
            ud_mutex_lock(&e->execution->diagnostics_lock);
            if (e->execution->bound_plans.size)
                s = ud_bytes_put(&e->execution->bound_plans, ",", 1);
            if (!s)
                s = ud_bytes_put(&e->execution->bound_plans, report.data, report.size);
            ud_mutex_unlock(&e->execution->diagnostics_lock);
        }
        ud_bytes_clear(&report);
    }
    s = clear(&p, s);
    ud_free(targets);
    return s;
}
static ud_status prepare(physical_plan *p, const ud_json *definition, emit_target *targets) {
    ud_activation *e = p->execution;
    UD_TRY(configure_plan(p, definition, targets));
    ud_mutex_init(&p->epoch_lock);
    p->epoch_lock_ready = 1;
    atomic_init(&p->confirmed_epoch, 0);
    char *epoch_json = NULL;
    uint32_t epoch_length = 0;
    ud_status saved = ud_checkpoint_epoch_info(e, p->id, &epoch_json, &epoch_length);
    if (saved != UD_END && saved)
        return saved;
    if (!saved) {
        ud_status status =
            ud_json_parse(e->execution->ctx, epoch_json, epoch_length, &p->epoch_record, NULL);
        ud_free(epoch_json);
        if (status)
            return status;
        uint64_t epoch;
        UD_TRY(ud_json_uint(ud_member(p->epoch_record, "epoch"), &epoch));
        atomic_store(&p->confirmed_epoch, epoch);
    }
    for (uint32_t i = 0; i < p->paths; i++)
        UD_TRY(ud_queue_create(e->execution->ctx, e->execution->options.edge_capacity,
                               &p->edges[i].queue));
    for (uint32_t pass = 0; pass < 2; pass++)
        for (uint32_t i = 0; i < p->count; i++) {
            node *n = p->nodes + p->order[i];
            if ((n->descriptor.category == UD_COMPONENT_SOURCE) != (pass == 0))
                continue;
            n->context.recovery_epoch = atomic_load(&p->confirmed_epoch);
            const ud_json *saved_components = ud_member(p->epoch_record, "components");
            for (uint32_t k = 0; k < ud_json_size(saved_components); k++) {
                const ud_json *entry = ud_json_at(saved_components, k);
                ud_string id;
                UD_TRY(ud_json_string(ud_member(entry, "id"), &id));
                if (ud_same(id, n->id)) {
                    const char *value = NULL;
                    uint32_t length = 0;
                    const ud_json *terminal = ud_member(entry, "terminal");
                    if (ud_json_kind(terminal) != UD_JSON_BOOL)
                        return UD_CORRUPT;
                    UD_TRY(ud_json_text(terminal, &value, &length));
                    n->context.recovery_terminal = length == 4 && !memcmp(value, "true", 4);
                }
            }

            const ud_json *positions = ud_member(p->epoch_record, "sources");
            for (uint32_t k = 0; k < ud_json_size(positions); k++) {
                ud_string id;
                UD_TRY(ud_json_string(ud_member(ud_json_at(positions, k), "id"), &id));
                if (ud_same(id, n->id))
                    UD_TRY(ud_json_string(ud_member(ud_json_at(positions, k), "position"),
                                          &n->context.recovery_position));
            }
            if (n->descriptor.category == UD_COMPONENT_SOURCE &&
                n->context.recovery_position.length) {
                n->source_position =
                    (ud_string){ud_strdup_n(e->execution->ctx, n->context.recovery_position.data,
                                            n->context.recovery_position.length),
                                n->context.recovery_position.length};
                if (!n->source_position.data)
                    return UD_RESOURCE;
            }
            const ud_batch **inputs =
                ud_alloc(e->execution->ctx, (n->descriptor.inputs + 1) * sizeof(*inputs));
            if (!inputs)
                return UD_RESOURCE;
            for (uint32_t j = 0; j < n->descriptor.inputs; j++) {
                inputs[j] = NULL;
                if (n->inputs[j] != UINT32_MAX) {
                    edge *v = p->edges + n->inputs[j];
                    inputs[j] = p->nodes[v->from].schemas[v->output];
                }
            }
            uint64_t started = ud_now_ms(), cpu = ud_thread_cpu_ns();
            ud_status s = n->descriptor.prepare(n->descriptor.user, &n->context,
                                                (ud_string){n->json, n->json_length}, inputs,
                                                (const ud_batch *const *)n->schemas, &n->instance);
            n->prepare_ms = ud_now_ms() - started;
            n->prepare_cpu_ns = ud_thread_cpu_ns() - cpu;
            ud_free(inputs);
            if (s)
                return node_failed(n, s, "prepare");
            n->opened = 1;
            if (atomic_load(&p->confirmed_epoch)) {
                const ud_json *components = ud_member(p->epoch_record, "components"), *entry = NULL;
                for (uint32_t k = 0; k < ud_json_size(components); k++) {
                    ud_string id;
                    const ud_json *item = ud_json_at(components, k);
                    UD_TRY(ud_json_string(ud_member(item, "id"), &id));
                    if (ud_same(id, n->id)) {
                        entry = item;
                        break;
                    }
                }
                if (!entry)
                    return UD_CORRUPT;
                uint64_t version = 0, ended = 0;
                UD_TRY(ud_json_uint(ud_member(entry, "stateVersion"), &version));
                UD_TRY(ud_json_uint(ud_member(entry, "endedInputs"), &ended));
                if (version != n->descriptor.state_version ||
                    (n->descriptor.inputs < 64 && (ended >> n->descriptor.inputs)))
                    return UD_INCOMPATIBLE;
                UD_TRY(ud_json_uint(ud_member(entry, "producedRows"), &n->produced));
                UD_TRY(ud_json_uint(ud_member(entry, "processedRows"), &n->processed));
                if (n->descriptor.restore) {
                    char key[65];
                    UD_TRY(epoch_component_key(p, n, key));
                    ud_batch *state = NULL;
                    ud_status restored =
                        ud_checkpoint_snapshot_read(p->execution, (ud_string){key, 64},
                                                    atomic_load(&p->confirmed_epoch), &state);
                    if (!restored)
                        restored = n->descriptor.restore(n->instance, state);
                    ud_batch_destroy(state);
                    if (restored)
                        return node_failed(n, restored, "restore");
                }
                n->ended = 0;
                for (uint32_t port = 0; port < n->descriptor.inputs; port++)
                    if (ended & (UINT64_C(1) << port)) {
                        n->ended++;
                        if (n->inputs[port] != UINT32_MAX)
                            p->edges[n->inputs[port]].ended = 1;
                    }
                if (n->context.recovery_terminal)
                    atomic_store(&n->state, UD_EXEC_SUCCEEDED);
                atomic_store(&n->staged_epoch, atomic_load(&p->confirmed_epoch));
                n->acknowledged_epoch = atomic_load(&p->confirmed_epoch);
            }
        }
    return UD_OK;
}
static ud_status epoch_component_key(physical_plan *p, node *n, char out[65]) {
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
static ud_status stage_epoch(physical_plan *p, node *n, uint64_t epoch) {
    if (atomic_load(&n->staged_epoch) == epoch)
        return UD_OK;
    if (ud_transaction_active(&n->activation) && !n->activation.transaction_epoch)
        return UD_UNSUPPORTED;
    if ((n->binding.effects & UD_EFFECT_EXTERNAL_WRITE) &&
        !(n->binding.effects & (UD_EFFECT_RECEIPTED | UD_EFFECT_IDEMPOTENT)) &&
        !(n->activation.transaction_epoch && (n->binding.effects & UD_EFFECT_TRANSACTIONAL)))
        return UD_UNSUPPORTED;
    if ((n->descriptor.capabilities & UD_COMPONENT_STATEFUL) && !n->descriptor.checkpoint)
        return UD_UNSUPPORTED;
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
        status = epoch_component_key(p, n, key);
    if (!status)
        status = ud_checkpoint_snapshot_write(p->execution, (ud_string){key, 64}, epoch, state);
    ud_batch_destroy(state);
    if (!status)
        atomic_store(&n->staged_epoch, epoch);
    return status;
}
static ud_status commit_epoch(physical_plan *p, uint64_t epoch) {
    ud_mutex_lock(&p->epoch_lock);
    ud_status status = UD_OK;
    if (epoch <= atomic_load(&p->confirmed_epoch)) {
        ud_mutex_unlock(&p->epoch_lock);
        return UD_OK;
    }
    int active = 0;
    for (uint32_t i = 0; i < p->count; i++)
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
            status = stage_epoch(p, p->nodes + i, epoch);
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
        uint64_t ended = 0;
        for (uint32_t port = 0; port < component->descriptor.inputs; port++)
            if (component->inputs[port] == UINT32_MAX || p->edges[component->inputs[port]].ended)
                ended |= UINT64_C(1) << port;
        int length = snprintf(
            metadata, sizeof(metadata),
            ",\"stateVersion\":%u,\"endedInputs\":%llu,\"terminal\":%s,\"producedRows\":%llu,"
            "\"processedRows\":%llu}",
            component->descriptor.state_version, (unsigned long long)ended,
            atomic_load(&component->state) == UD_EXEC_SUCCEEDED ? "true" : "false",
            (unsigned long long)component->produced, (unsigned long long)component->processed);
        if (!status)
            status = ud_bytes_put(&payload, metadata, (size_t)length);
    }
    if (!status)
        status = ud_bytes_put(&payload, "]}", 2);
    if (!status)
        status = ud_checkpoint_epoch_commit(
            p->execution, p->id, (ud_string){(char *)payload.data, (uint32_t)payload.size});
    if (!status && ud_transaction_active(p->execution))
        status = ud_transaction_commit_epoch(p->execution);
    if (!status) {
        uint64_t previous = atomic_load(&p->confirmed_epoch);
        atomic_store(&p->confirmed_epoch, epoch);
        if (previous)
            for (uint32_t i = 0; !status && i < p->count; i++)
                if (p->nodes[i].descriptor.checkpoint) {
                    char key[65];
                    status = epoch_component_key(p, p->nodes + i, key);
                    if (!status)
                        status = ud_checkpoint_snapshot_forget(p->execution, (ud_string){key, 64},
                                                               previous, 1);
                }
        for (uint32_t i = 0; i < p->tree_count; i++)
            ud_work_group_ready(p->group, i);
    }
    ud_bytes_clear(&payload);
    ud_mutex_unlock(&p->epoch_lock);
    return status;
}
static ud_status forward_control(physical_plan *p, node *n, const ud_component_event *event) {
    emit_target target = {p, n};
    for (uint32_t port = 0; port < n->descriptor.outputs; port++)
        if (n->context.connected_outputs & (UINT64_C(1) << port)) {
            ud_component_event output = *event;
            if (event->kind == UD_COMPONENT_SCHEMA_CHANGE)
                output.batch = n->schemas[port];
            UD_TRY(route_event(&target, port, &output));
        }
    return UD_OK;
}
static ud_status notify_control(node *n, const ud_component_event *event) {
    if (!(n->descriptor.capabilities & UD_COMPONENT_CONTROL_EVENTS))
        return UD_OK;
    int allowed = n->emit_allowed;
    n->emit_allowed =
        event->kind == UD_COMPONENT_WATERMARK || event->kind == UD_COMPONENT_CHECKPOINT_BARRIER;
    ud_status status = n->descriptor.step(n->instance, event);
    n->emit_allowed = allowed;
    return status == UD_END || (status == UD_MORE && event->kind == UD_COMPONENT_SCHEMA_CHANGE)
               ? UD_INVALID
               : status;
}
static ud_status stream_control(physical_plan *p, node *n, const ud_component_event *event) {
    ud_activation *e = p->execution;
    uint64_t bit = UINT64_C(1) << event->input;
    if (event->kind == UD_COMPONENT_CHECKPOINT_BARRIER) {
        uint64_t committed = atomic_load(&p->confirmed_epoch);
        if (event->epoch != committed + 1)
            return UD_FENCED;
        if (n->barrier_mask && n->barrier_epoch != event->epoch)
            return UD_FENCED;
        n->barrier_epoch = event->epoch;
        n->barrier_mask |= bit;
        uint64_t required = 0;
        for (uint32_t i = 0; i < n->descriptor.inputs; i++)
            if (n->inputs[i] != UINT32_MAX && !p->edges[n->inputs[i]].ended)
                required |= UINT64_C(1) << i;
        if ((n->barrier_mask & required) != required)
            return UD_OK;
        n->deferred_control = *event;
        n->deferred_control.batch = NULL;
        n->deferred_control.source_position = (ud_string){0};
        n->control_pending = 1;
        return notify_control(n, event);
    }
    if (event->kind == UD_COMPONENT_IDLE || event->kind == UD_COMPONENT_ACTIVE) {
        if (event->kind == UD_COMPONENT_IDLE)
            n->idle_mask |= bit;
        else
            n->idle_mask &= ~bit;
        uint64_t required =
            n->descriptor.inputs == 64 ? UINT64_MAX : (UINT64_C(1) << n->descriptor.inputs) - 1;
        if (event->kind == UD_COMPONENT_ACTIVE || n->idle_mask == required)
            return forward_control(p, n, event);
    }
    if (event->kind == UD_COMPONENT_WATERMARK || event->kind == UD_COMPONENT_IDLE) {
        if (event->kind == UD_COMPONENT_WATERMARK) {
            n->idle_mask &= ~bit;
            if ((n->watermark_mask & bit) && event->watermark < n->watermarks[event->input])
                return UD_FENCED;
            n->watermarks[event->input] = event->watermark;
            n->watermark_mask |= bit;
            if (n->descriptor.capabilities & UD_COMPONENT_WATERMARK_POLICY)
                return notify_control(n, event);
        }
        uint64_t required = 0;
        int64_t minimum = INT64_MAX;
        for (uint32_t i = 0; i < n->descriptor.inputs; i++)
            if (n->inputs[i] != UINT32_MAX && !p->edges[n->inputs[i]].ended &&
                !(n->idle_mask & (UINT64_C(1) << i))) {
                required |= UINT64_C(1) << i;
                if (n->watermarks[i] < minimum)
                    minimum = n->watermarks[i];
            }
        if (!required || (n->watermark_mask & required) != required || minimum <= n->last_watermark)
            return UD_OK;
        n->last_watermark = minimum;
        ud_component_event output = *event;
        output.kind = UD_COMPONENT_WATERMARK;
        output.watermark = minimum;
        atomic_store(&n->live_watermark, minimum);
        n->deferred_control = output;
        n->deferred_control.batch = NULL;
        n->deferred_control.source_position = (ud_string){0};
        n->control_pending = 1;
        return notify_control(n, &output);
    }
    if (event->kind == UD_COMPONENT_SCHEMA_CHANGE) {
        if (!event->batch)
            return UD_SCHEMA;
        const ud_batch *expected = n->ports.inputs[event->input].schema;
        if (expected->columns != event->batch->columns)
            return UD_INCOMPATIBLE;
        for (uint32_t i = 0; i < expected->columns; i++)
            if (!ud_field_matches(&expected->column[i].field, &event->batch->column[i].field,
                                  UD_FIELD_PORT))
                return UD_INCOMPATIBLE;
        /* Stable projected contracts admit the event. Re-run pure binding for
         * affected nodes; state remains owned by the original instance. */
        const ud_batch **inputs =
            ud_alloc(e->execution->ctx, n->descriptor.inputs * sizeof(*inputs));
        ud_batch **outputs = ud_alloc(e->execution->ctx, n->descriptor.outputs * sizeof(*outputs));
        if ((n->descriptor.inputs && !inputs) || (n->descriptor.outputs && !outputs)) {
            ud_free(inputs);
            ud_free(outputs);
            return UD_RESOURCE;
        }
        for (uint32_t at = 0; at < n->descriptor.inputs; at++)
            inputs[at] = n->ports.inputs[at].schema;
        ud_status status = n->descriptor.configure(
            n->descriptor.user, &n->context, (ud_string){n->json, n->json_length}, inputs, outputs);
        for (uint32_t port = 0; !status && port < n->descriptor.outputs; port++) {
            if (!outputs[port] || outputs[port]->columns != n->schemas[port]->columns) {
                status = UD_INCOMPATIBLE;
                break;
            }
            for (uint32_t field = 0; !status && field < outputs[port]->columns; field++)
                if (!ud_field_matches(&outputs[port]->column[field].field,
                                      &n->schemas[port]->column[field].field, UD_FIELD_PORT))
                    status = UD_INCOMPATIBLE;
        }
        for (uint32_t at = 0; at < n->descriptor.outputs; at++)
            ud_batch_destroy(outputs[at]);
        ud_free(inputs);
        ud_free(outputs);
        if (!status) {
            n->schema_version = event->schema_version;
            status = notify_control(n, event);
            if (!status)
                status = forward_control(p, n, event);
        }
        return status;
    }
    return UD_UNSUPPORTED;
}
static ud_status step(physical_plan *p, node *n, int *progress) {
    uint64_t confirmed = atomic_load(&p->confirmed_epoch);
    if (n->barrier_mask && n->barrier_epoch <= confirmed)
        n->barrier_mask = 0;
    if (n->descriptor.confirm_epoch && n->barrier_epoch) {
        if (n->barrier_epoch > confirmed &&
            (n->descriptor.category == UD_COMPONENT_SOURCE ||
             atomic_load(&n->staged_epoch) == n->barrier_epoch))
            return UD_OK;
        if (n->barrier_epoch <= confirmed && n->acknowledged_epoch < n->barrier_epoch) {
            UD_TRY(n->descriptor.confirm_epoch(n->instance, n->barrier_epoch, n->source_position));
            n->acknowledged_epoch = n->barrier_epoch;
        }
    }
    ud_activation *e = p->execution;
    if (atomic_load(&n->state) == UD_EXEC_SUCCEEDED)
        return UD_OK;
    if (n->descriptor.category == UD_COMPONENT_SOURCE) {
        if (ud_debug_enabled(e->execution))
            for (uint32_t other = 0; other < p->count; other++)
                if (p->nodes[other].descriptor.category != UD_COMPONENT_SOURCE &&
                    p->nodes[other].pending)
                    return UD_OK;
        ud_status gate = ud_debug_boundary(&n->activation, UD_DEBUG_BATCH);
        if (gate == UD_BUSY)
            return UD_OK;
        UD_TRY(gate);
    }
    for (uint32_t i = 0; i < n->output_edges; i++)
        if (atomic_load(&p->edges[n->outputs[i]].occupied) >=
            e->execution->options.edge_capacity) {
            n->wait_capacity++;
            if (!n->blocked_ms) {
                n->blocked_ms = ud_now_ms();
                n->blocked_kind = 2;
            }
            return UD_OK;
        }
    if (n->control_pending && !n->pending) {
        memset(n->emitted, 0, n->descriptor.outputs);
        n->emit_allowed = 1;
        ud_status status = UD_OK;
        if (n->deferred_control.kind == UD_COMPONENT_CHECKPOINT_BARRIER)
            status = stage_epoch(p, n, n->deferred_control.epoch);
        if (!status)
            status = forward_control(p, n, &n->deferred_control);
        n->emit_allowed = 0;
        if (!status && n->deferred_control.kind == UD_COMPONENT_CHECKPOINT_BARRIER)
            status = commit_epoch(p, n->deferred_control.epoch);
        if (!status) {
            n->control_pending = 0;
            *progress = 1;
        }
        return status;
    }
    if (n->watermark_refresh && !n->pending && !n->control_pending) {
        // END removes an input from the frontier even when the other inputs
        // never produce another Watermark. Drain any output from END first.
        n->watermark_refresh = 0;
        memset(n->emitted, 0, n->descriptor.outputs);
        n->emit_allowed = 1;
        ud_component_event idle = {.struct_size = sizeof(idle),
                                   .abi_version = UD_ABI_VERSION,
                                   .kind = UD_COMPONENT_IDLE,
                                   .input = n->watermark_refresh_input};
        ud_status status = stream_control(p, n, &idle);
        n->emit_allowed = 0;
        n->pending = status == UD_MORE;
        *progress = 1;
        return status == UD_MORE ? UD_OK : status;
    }
    if (n->barrier_mask && !n->pending && atomic_load(&n->staged_epoch) != n->barrier_epoch) {
        uint64_t required = 0;
        for (uint32_t port = 0; port < n->descriptor.inputs; port++)
            if (n->inputs[port] != UINT32_MAX && !p->edges[n->inputs[port]].ended)
                required |= UINT64_C(1) << port;
        if ((n->barrier_mask & required) == required) {
            memset(n->emitted, 0, n->descriptor.outputs);
            n->emit_allowed = 1;
            ud_component_event barrier = {.struct_size = sizeof(barrier),
                                          .abi_version = UD_ABI_VERSION,
                                          .kind = UD_COMPONENT_CHECKPOINT_BARRIER,
                                          .epoch = n->barrier_epoch};
            ud_status status = stage_epoch(p, n, n->barrier_epoch);
            if (!status)
                status = forward_control(p, n, &barrier);
            n->emit_allowed = 0;
            if (!status)
                status = commit_epoch(p, n->barrier_epoch);
            if (!status)
                *progress = 1;
            return status;
        }
    }
    ud_component_event event = {sizeof(event), UD_ABI_VERSION, UD_COMPONENT_POLL, 0, NULL};
    const ud_batch *input = NULL;
    edge *delivery = NULL;
    ud_stream_event received = {0};
    ud_buffer_view *view = NULL;
    int selected = n->pending || n->descriptor.category == UD_COMPONENT_SOURCE ||
                   n->ended == n->descriptor.inputs;
    uint64_t demand =
        n->descriptor.input_demand ? n->descriptor.input_demand(n->instance) : UINT64_MAX;
    if (!selected)
        for (uint32_t i = 0; i < n->descriptor.inputs; i++) {
            uint32_t port = (n->next_port + i) % n->descriptor.inputs;
            if ((n->barrier_mask & (UINT64_C(1) << port)) || n->inputs[port] == UINT32_MAX)
                continue;
            edge *v = p->edges + n->inputs[port];
            int32_t head = ud_queue_front_kind(v->queue);
            if (!(demand & (UINT64_C(1) << port)) && (!head || head == UD_STREAM_DATA))
                continue;
            if (ud_queue_count(v->queue)) {
                UD_TRY(ud_queue_pop(v->queue, &received));
                delivery = v;
                atomic_fetch_sub(&e->execution->queued, 1);
                ud_status status = received.block ? ud_buffer_read(received.block, &view) : UD_OK;
                if (status) {
                    ud_status cleanup = release_delivery(p, delivery, &received, view);
                    return cleanup ? cleanup : status;
                }
                input = view ? ud_buffer_view_batch(view) : NULL;
                if (received.kind == UD_STREAM_DATA) {
                    n->idle_mask &= ~(UINT64_C(1) << port);
                    atomic_fetch_add(&v->consumed, input->rows);
                }
                event.kind = received.kind;
                event.input = port;
                event.batch = input;
                event.epoch = received.epoch;
                event.schema_version = received.schema_version;
                event.watermark = received.watermark;
                event.source_position = received.source_position;
                if (event.kind == UD_COMPONENT_INPUT_END) {
                    if (v->ended) {
                        ud_status cleanup = release_delivery(p, delivery, &received, view);
                        return cleanup ? cleanup : UD_SCHEMA;
                    }
                    v->ended = 1;
                    n->ended++;
                }
                selected = 1;
                n->next_port = (port + 1) % n->descriptor.inputs;
                break;
            }
        }
    if (!selected) {
        n->wait_input++;
        if (!n->blocked_ms) {
            n->blocked_ms = ud_now_ms();
            n->blocked_kind = 1;
        }
        return UD_OK;
    }
    if (n->blocked_ms) {
        uint64_t elapsed = ud_now_ms() - n->blocked_ms;
        if (n->blocked_kind == 1)
            n->input_wait_ms += elapsed;
        else
            n->capacity_wait_ms += elapsed;
        n->blocked_ms = 0;
    }
    atomic_store(&n->state, n->ended == n->descriptor.inputs && n->descriptor.inputs
                                ? UD_EXEC_DRAINING
                                : UD_EXEC_RUNNING);
    if (n->steps == 0 || event.kind != UD_COMPONENT_POLL)
        ud_activation_notify(&n->activation, atomic_load(&n->state), UD_OK);
    memset(n->emitted, 0, n->descriptor.outputs);
    n->emit_allowed = 1;
    uint64_t started = ud_now_ms(), cpu = ud_thread_cpu_ns();
    uint64_t started_ns = ud_monotonic_ns(), produced_before = n->produced;
    if (!n->first_ms)
        n->first_ms = started;
    n->steps++;
    if (input) {
        n->processed += input->rows;
        atomic_fetch_add(&n->live_processed, input->rows);
    }
    ud_task_cancel_register(&n->activation, n->instance, n->descriptor.cancel);
    ud_status s = ud_activation_check(&n->activation);
    if (!s)
        s = atomic_load(&e->execution->stop_requested) &&
                    n->descriptor.category == UD_COMPONENT_SOURCE &&
                    !(n->descriptor.capabilities & UD_COMPONENT_CONTINUOUS)
                ? UD_END
            : e->execution->preview_pipeline && n->descriptor.category == UD_COMPONENT_SOURCE &&
                    n->produced >= e->execution->preview_source_rows
                ? UD_END
            : event.kind >= UD_COMPONENT_WATERMARK ? stream_control(p, n, &event)
                                                   : n->descriptor.step(n->instance, &event);
    ud_task_cancel_unregister(&n->activation);
    if (input || n->produced != produced_before)
        batch_observe(n, ud_monotonic_ns() - started_ns);
    n->callback_ms += ud_now_ms() - started;
    atomic_fetch_add(&n->live_callback_ms, ud_now_ms() - started);
    n->cpu_ns += ud_thread_cpu_ns() - cpu;
    n->emit_allowed = 0;
    if (s == UD_BUSY && n->descriptor.category == UD_COMPONENT_SOURCE &&
        (n->descriptor.capabilities & UD_COMPONENT_CONTINUOUS)) {
        uint64_t now = ud_now_ms();
        if (!n->idle_started)
            n->idle_started = now;
        atomic_store(&n->idle, 1);
        atomic_store(&n->live_idle_ms, n->idle_ms + now - n->idle_started);
        ud_work_group_defer(p->group, n->tree, 50);
        return UD_OK;
    }
    if (n->idle_started) {
        n->idle_ms += ud_now_ms() - n->idle_started;
        n->idle_started = 0;
    }
    atomic_store(&n->live_idle_ms, n->idle_ms);
    atomic_store(&n->idle, 0);
    if ((s == UD_OK || s == UD_MORE) && event.kind == UD_COMPONENT_INPUT_END &&
        n->ended < n->descriptor.inputs) {
        n->watermark_refresh = 1;
        n->watermark_refresh_input = event.input;
    }
    int was_pending = n->pending;
    n->pending = s == UD_MORE;
    if (s == UD_MORE)
        s = UD_OK;
    uint32_t emitted = 0;
    for (uint32_t j = 0; j < n->descriptor.outputs; j++)
        emitted += n->emitted[j];
    if (emitted && n->descriptor.category == UD_COMPONENT_SOURCE &&
        !ud_transaction_active(&n->activation))
        ud_debug_advance(&n->activation);
    if (s == UD_BUSY || s < UD_OK || s > UD_TRUNCATED)
        s = UD_INVALID;
    if (s == UD_END) {
        if (emitted ||
            (n->descriptor.category != UD_COMPONENT_SOURCE && n->ended != n->descriptor.inputs))
            s = UD_INVALID;
        else {
            n->end_ms = ud_now_ms();
            n->emit_allowed = 1;
            ud_component_event end = {.struct_size = sizeof(end),
                                      .abi_version = UD_ABI_VERSION,
                                      .kind = UD_COMPONENT_INPUT_END,
                                      .source_position = n->source_position};
            s = forward_control(p, n, &end);
            n->emit_allowed = 0;
            if (!s)
                atomic_store(&n->state, UD_EXEC_SUCCEEDED);
            if (!s)
                s = commit_epoch(p, atomic_load(&p->confirmed_epoch) + 1);
            for (uint32_t i = 0; i < n->output_edges; i++)
                ud_work_group_ready(p->group, p->nodes[p->edges[n->outputs[i]].to].tree);
        }
    } else if (!s && event.kind == UD_COMPONENT_POLL && !emitted && !was_pending && !n->pending)
        s = UD_INVALID;
    if (!s && event.kind == UD_COMPONENT_DATA && n->descriptor.category == UD_COMPONENT_DESTINATION)
        ud_task_rows(e, 0, input->rows, 0);
    ud_status cleanup = release_delivery(p, delivery, &received, view);
    if (cleanup && !s)
        s = cleanup;
    if (s)
        s = node_failed(n, s, "processing");
    else
        *progress = 1;
    return s;
}
static ud_status tree_quantum(void *user, uint32_t index) {
    physical_plan *plan = user;
    ud_execution *execution = plan->execution->execution;
    unsigned active = atomic_fetch_add(&execution->tree_active, 1) + 1;
    unsigned previous = atomic_load(&execution->tree_high_water);
    while (active > previous &&
           !atomic_compare_exchange_weak(&execution->tree_high_water, &previous, active)) {
    }
    execution_tree *tree = plan->trees + index;
    uint64_t started = ud_now_ms(), cpu = ud_thread_cpu_ns();
    if (!tree->first_ms)
        tree->first_ms = started;
    tree->quanta++;
    int progress = 0;
    uint32_t complete = 0;
    ud_status status = UD_OK;
    for (uint32_t i = tree->count; !status && i; i--) {
        node *component = plan->nodes + plan->tree_order[tree->offset + i - 1];
        status = ud_activation_check(&component->activation);
        if (!status)
            status = step(plan, component, &progress);
        if (atomic_load(&component->state) == UD_EXEC_SUCCEEDED)
            complete++;
    }
    tree->cpu_ns += ud_thread_cpu_ns() - cpu;
    tree->callback_ms += ud_now_ms() - started;
    if (!progress && complete != tree->count)
        tree->busy++;
    if (complete == tree->count)
        tree->end_ms = ud_now_ms();
    atomic_fetch_sub(&execution->tree_active, 1);
    return status ? status : complete == tree->count ? UD_OK : progress ? UD_MORE : UD_BUSY;
}
static ud_status json_number(ud_bytes *b, const char *name, uint64_t value) {
    char text[128];
    int n = snprintf(text, sizeof(text), "\"%s\":%llu", name, (unsigned long long)value);
    return ud_bytes_put(b, text, (size_t)n);
}
static ud_status plan_json(physical_plan *p, ud_bytes *b, int runtime, ud_status outcome) {
    UD_TRY(ud_bytes_put(b, "{\"id\":", sizeof("{\"id\":") - 1));
    UD_TRY(ud_json_quote(b, p->id.data, p->id.length));
    UD_TRY(ud_bytes_put(b, ",\"task\":", sizeof(",\"task\":") - 1));
    UD_TRY(ud_json_quote(b, p->execution->task_id.data, p->execution->task_id.length));
    UD_TRY(ud_bytes_put(b, ",", 1));
    UD_TRY(json_number(b, "treeConcurrency", p->concurrency));
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
ud_status ud_pipeline_execute(ud_activation *activation, const ud_json *definition) {
    physical_plan plan_value = {0};
    ud_status status = plan(activation, definition, &plan_value);
    emit_target *targets = NULL;
    if (!status) {
        targets = ud_alloc(activation->execution->ctx, plan_value.count * sizeof(*targets));
        if (!targets)
            status = UD_RESOURCE;
    }
    if (!status)
        status = prepare(&plan_value, definition, targets);
    if (!status) {
        ud_execution *owner = activation->execution;
        ud_mutex_lock(&owner->diagnostics_lock);
        plan_value.progress_next = owner->active_progress;
        owner->active_progress = &plan_value;
        plan_value.progress_registered = 1;
        ud_mutex_unlock(&owner->diagnostics_lock);
        atomic_fetch_add(&activation->execution->pipeline_trees, plan_value.tree_count);
        uint64_t ready = ud_now_ms();
        for (uint32_t i = 0; i < plan_value.tree_count; i++)
            plan_value.trees[i].ready_ms = ready;
        for (uint32_t i = 0; i < plan_value.count; i++) {
            plan_value.nodes[i].ready_ms = ready;
            plan_value.nodes[i].activation.stepping = 1;
        }
        status = ud_work_group_create(activation->execution->executor, plan_value.tree_count,
                                      plan_value.concurrency, tree_quantum, &plan_value,
                                      &plan_value.group);
        if (!status) {
            for (uint32_t i = 0; i < plan_value.tree_count; i++)
                ud_work_group_ready(plan_value.group, i);
            status = ud_work_group_wait(plan_value.group);
        }
        ud_status closed = ud_work_group_destroy(plan_value.group);
        plan_value.group = NULL;
        if (!status)
            status = closed;
    }
    /* All tree jobs have joined. Finish in topological order after full drain;
     * component variable writes now use the owning task's lexical authority. */
    for (uint32_t i = 0; !status && i < plan_value.count; i++) {
        node *component = plan_value.nodes + plan_value.order[i];
        component->activation.stepping = 0;
        component->activation.parallel = activation->parallel;
        status = ud_activation_check(&component->activation);
        if (!status) {
            uint64_t started = ud_now_ms(), cpu = ud_thread_cpu_ns();
            status = component->descriptor.finish(component->instance);
            component->finish_ms = ud_now_ms() - started;
            component->finish_cpu_ns = ud_thread_cpu_ns() - cpu;
        }
        if (status)
            status = node_failed(component, status, "finish");
    }
    for (uint32_t i = 0; !status && i < plan_value.count; i++) {
        node *component = plan_value.nodes + plan_value.order[i];
        if (!activation->execution->preview_pipeline && component->descriptor.confirm)
            status = component->descriptor.confirm(component->instance);
        if (status)
            status = node_failed(component, status, "confirmation");
    }
    if (targets && plan_value.nodes && plan_value.order) {
        int schemas = 1;
        for (uint32_t i = 0; i < plan_value.count; i++)
            if (!plan_value.nodes[i].schemas)
                schemas = 0;
        if (schemas) {
            ud_execution *e = activation->execution;
            for (uint32_t i = 0; i < plan_value.count; i++)
                plan_value.nodes[i].activation.pipeline_result = status;
            ud_bytes report = {.ctx = e->ctx};
            ud_status recorded = plan_json(&plan_value, &report, 1, status);
            ud_mutex_lock(&e->diagnostics_lock);
            e->pipeline_record_count++;
            if (e->pipeline_record_count > 256)
                recorded = UD_RESOURCE;
            uint64_t before = e->pipeline_reports.size;
            if (!recorded && before)
                recorded = ud_bytes_put(&e->pipeline_reports, ",", 1);
            if (!recorded)
                recorded = ud_bytes_put(&e->pipeline_reports, report.data, report.size);
            if (recorded) {
                e->pipeline_reports.size = before;
                e->pipeline_records_dropped++;
            }
            ud_mutex_unlock(&e->diagnostics_lock);
            ud_bytes_clear(&report);
        }
    }
    status = clear(&plan_value, status);
    ud_free(targets);
    return status;
}

ud_status ud_execution_progress(ud_execution *e, char **out, uint32_t *length) {
    if (!e || !out || !length)
        return UD_INVALID;
    *out = NULL;
    *length = 0;
    ud_bytes b = {.ctx = e->ctx, .limit = 4194304};
    ud_status s = ud_bytes_put(&b, "{\"pipelines\":[", sizeof("{\"pipelines\":[") - 1);
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
