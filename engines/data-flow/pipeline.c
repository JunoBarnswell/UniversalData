#include "../package/internal.h"
#include "ud/engine.h"
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
        ud_column_id id = {0};
        for (uint32_t j = 0; j < 8; j++) {
            id.high = (id.high << 8) | hash[j];
            id.low = (id.low << 8) | hash[j + 8];
        }
        ud_column_id_format(id, output);
    }
    ud_bytes_clear(&bytes);
    return s;
}
typedef struct edge {
    uint32_t from, to, output, input;
    ud_queue *queue;
    int ended;
} edge;
typedef struct execution_tree {
    uint32_t offset, count;
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
    int opened, emit_allowed, pending;
    atomic_int state;
    ud_activation activation;
    ud_component_context context;
} node;
typedef struct physical_plan {
    ud_activation *execution;
    node *nodes;
    edge *edges;
    uint32_t count, paths, *order, *tree_order, tree_count, concurrency;
    execution_tree *trees;
    ud_string id;
} physical_plan;
static int index_of(const physical_plan *p, ud_string id) {
    for (uint32_t i = 0; i < p->count; i++)
        if (ud_same(p->nodes[i].id, id))
            return (int)i;
    return -1;
}
static ud_status clear(physical_plan *p, ud_status result) {
    ud_status cleanup = UD_OK;
    if (p->edges)
        for (uint32_t i = 0; i < p->paths; i++) {
            if (p->edges[i].queue) {
                atomic_fetch_sub(&p->execution->execution->queued, ud_queue_count(p->edges[i].queue));
                ud_status status = ud_queue_destroy(p->edges[i].queue);
                if (!cleanup && status)
                    cleanup = status;
            }
        }
    if (p->nodes)
        for (uint32_t i = p->count; i; i--) {
            node *n = p->nodes + (p->order ? p->order[i - 1] : i - 1);
            if (n->opened)
                n->descriptor.dispose(n->instance, result);
            if (n->schemas)
                for (uint32_t j = 0; j < n->descriptor.outputs; j++)
                    ud_batch_destroy(n->schemas[j]);
            ud_free(n->schemas);
            ud_free(n->inputs);
            ud_free(n->outputs);
            ud_free(n->emitted);
            ud_free(n->json);
            ud_json_destroy(n->configuration);
        }
    ud_free(p->order);
    ud_free(p->nodes);
    ud_free(p->edges);
    ud_free(p->trees);
    ud_free(p->tree_order);
    return result ? result : cleanup;
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
    if (!p->count || !p->paths || p->count > 65536 || p->paths > 65536)
        return UD_SCHEMA;
    p->nodes = ud_alloc(e->execution->ctx, p->count * sizeof(*p->nodes));
    p->edges = ud_alloc(e->execution->ctx, p->paths * sizeof(*p->edges));
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
        n->inputs = ud_alloc(e->execution->ctx, (n->descriptor.inputs + 1) * sizeof(*n->inputs));
        n->emitted = ud_alloc(e->execution->ctx, n->descriptor.outputs + 1);
        if (!n->inputs || !n->emitted)
            return UD_RESOURCE;
        for (uint32_t j = 0; j < n->descriptor.inputs; j++)
            n->inputs[j] = UINT32_MAX;
        atomic_init(&n->state, UD_EXEC_READY);
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
        if (!s)
            s = ud_json_uint(ud_member(def, "output"), &output);
        if (!s)
            s = ud_json_uint(ud_member(def, "input"), &input);
        int a = !s ? index_of(p, from) : -1, b = !s ? index_of(p, to) : -1;
        if (!s && (a < 0 || b < 0 || a == b || output >= p->nodes[a].descriptor.outputs ||
                   input >= p->nodes[b].descriptor.inputs))
            s = UD_SCHEMA;
        if (!s) {
            edge *v = p->edges + i;
            *v = (edge){.from = (uint32_t)a,
                        .to = (uint32_t)b,
                        .output = (uint32_t)output,
                        .input = (uint32_t)input};
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
            if (n->inputs[j] == UINT32_MAX)
                s = UD_SCHEMA;
        for (uint32_t j = 0; j < n->descriptor.outputs; j++) {
            int found = 0;
            for (uint32_t k = 0; k < n->output_edges; k++)
                if (p->edges[n->outputs[k]].output == j)
                    found = 1;
            if (!found)
                s = UD_SCHEMA;
        }
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
        p->concurrency = ud_execution_concurrency(e);
        for (uint32_t i = 0; i < p->count; i++) {
            node *n = p->nodes + order[i];
            n->tree = n->descriptor.synchronous_input >= 0 ?
                p->nodes[p->edges[n->inputs[n->descriptor.synchronous_input]].from].tree :
                p->tree_count++;
            p->trees[n->tree].count++;
            if (!(n->descriptor.capabilities & UD_COMPONENT_THREAD_SAFE))
                p->concurrency = 1;
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
static ud_status route(void *user, uint32_t port, ud_batch *b) {
    emit_target *target = user;
    node *n = target->node;
    physical_plan *p = target->plan;
    ud_activation *e = p->execution;
    UD_TRY(emit(n, port, b));
    UD_TRY(ud_batch_seal(b));
    for (uint32_t i = 0; i < n->output_edges; i++) {
        edge *v = p->edges + n->outputs[i];
        if (v->output != port)
            continue;
        if (ud_queue_count(v->queue) == e->execution->options.edge_capacity)
            return UD_BUSY;
        ud_buffer_block *block = NULL;
        UD_TRY(ud_buffer_store(e->execution->buffers, b, &block));
        uint64_t queued = atomic_fetch_add(&e->execution->queued, 1) + 1;
        ud_status s = ud_queue_push(v->queue, block);
        if (s) {
            atomic_fetch_sub(&e->execution->queued, 1);
            ud_status cleanup = ud_buffer_release(block);
            return cleanup ? cleanup : s;
        }
        uint64_t previous = atomic_load(&e->execution->high_water);
        while (queued > previous &&
               !atomic_compare_exchange_weak(&e->execution->high_water, &previous, queued)) {}
    }
    n->emitted[port] = 1;
    atomic_fetch_add(&e->execution->batches, 1);
    if (n->descriptor.category == UD_COMPONENT_SOURCE)
        ud_task_rows(e, b->rows, 0, 0);
    if ((int32_t)port == n->descriptor.error_output)
        ud_task_rows(e, 0, 0, b->rows);
    return UD_OK;
}
static ud_status configure_plan(physical_plan *p, const ud_json *definition, emit_target *targets) {
    ud_activation *e = p->execution;
    const ud_json *defs = ud_member(definition, "components");
    /* Entire pipeline schema/configuration is validated before any acquisition. */
    for (uint32_t i = 0; i < p->count; i++) {
        node *n = p->nodes + p->order[i];
        uint32_t index = p->order[i];
        targets[index] = (emit_target){p, n};
        n->activation = (ud_activation){.execution = e->execution, .parent = e,
            .definition = e->definition, .package = e->package, .parameters = e->parameters,
            .scope = e->scope, .task_id = e->task_id, .pipeline_id = p->id, .component_id = n->id,
            .deadline = e->deadline, .depth = e->depth, .scope_authority = e,
            .parallel = e->parallel || (p->tree_count > 1 && p->concurrency > 1)};
        atomic_init(&n->activation.cancelled, 0);
        n->context = (ud_component_context){sizeof(n->context),
                                            UD_ABI_VERSION,
                                            e->execution->ctx,
                                            e->execution,
                                            &n->activation,
                                            e->task_id,
                                            p->id,
                                            n->id,
                                            e->execution->options.max_batch_rows,
                                            route,
                                            targets + index,
                                            ud_resolve_value,
                                            &n->activation,
                                            ud_resolve_field};
        UD_TRY(ud_bound_config(&n->activation, ud_member(ud_json_at(defs, index), "config"), &n->configuration));
        UD_TRY(ud_json_serialize(n->configuration, &n->json, &n->json_length));
        n->schemas = ud_alloc(e->execution->ctx, (n->descriptor.outputs + 1) * sizeof(*n->schemas));
        if (!n->schemas)
            return UD_RESOURCE;
        const ud_batch **inputs =
            ud_alloc(e->execution->ctx, (n->descriptor.inputs + 1) * sizeof(*inputs));
        if (!inputs)
            return UD_RESOURCE;
        for (uint32_t j = 0; j < n->descriptor.inputs; j++) {
            edge *v = p->edges + n->inputs[j];
            inputs[j] = p->nodes[v->from].schemas[v->output];
        }
        ud_status s =
            n->descriptor.configure(n->descriptor.user, &n->context,
                                    (ud_string){n->json, n->json_length}, inputs, n->schemas);
        ud_free(inputs);
        if (s)
            return s;
        for (uint32_t j = 0; j < n->descriptor.outputs; j++)
            if (!n->schemas[j] || n->schemas[j]->ctx != e->execution->ctx || n->schemas[j]->rows)
                return UD_SCHEMA;
    }
    return UD_OK;
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
    s = clear(&p, s);
    ud_free(targets);
    return s;
}
static ud_status prepare(physical_plan *p, const ud_json *definition, emit_target *targets) {
    ud_activation *e = p->execution;
    UD_TRY(configure_plan(p, definition, targets));
    for (uint32_t i = 0; i < p->paths; i++)
        UD_TRY(ud_queue_create(e->execution->ctx, e->execution->options.edge_capacity,
                               &p->edges[i].queue));
    for (uint32_t i = 0; i < p->count; i++) {
        node *n = p->nodes + p->order[i];
        const ud_batch **inputs =
            ud_alloc(e->execution->ctx, (n->descriptor.inputs + 1) * sizeof(*inputs));
        if (!inputs)
            return UD_RESOURCE;
        for (uint32_t j = 0; j < n->descriptor.inputs; j++) {
            edge *v = p->edges + n->inputs[j];
            inputs[j] = p->nodes[v->from].schemas[v->output];
        }
        ud_status s = n->descriptor.prepare(n->descriptor.user, &n->context,
                                            (ud_string){n->json, n->json_length}, inputs,
                                            (const ud_batch *const *)n->schemas, &n->instance);
        ud_free(inputs);
        if (s)
            return s;
        n->opened = 1;
    }
    return UD_OK;
}
static ud_status step(physical_plan *p, node *n, int *progress) {
    ud_activation *e = p->execution;
    if (atomic_load(&n->state) == UD_EXEC_SUCCEEDED)
        return UD_OK;
    for (uint32_t i = 0; i < n->output_edges; i++)
        if (ud_queue_count(p->edges[n->outputs[i]].queue) == e->execution->options.edge_capacity)
            return UD_OK;
    ud_component_event event = {sizeof(event), UD_ABI_VERSION, UD_COMPONENT_POLL, 0, NULL};
    const ud_batch *input = NULL;
    ud_buffer_block *block = NULL;
    ud_buffer_view *view = NULL;
    int selected = n->pending || n->descriptor.category == UD_COMPONENT_SOURCE ||
                   n->ended == n->descriptor.inputs;
    if (!selected)
        for (uint32_t i = 0; i < n->descriptor.inputs; i++) {
            uint32_t port = (n->next_port + i) % n->descriptor.inputs;
            edge *v = p->edges + n->inputs[port];
            if (ud_queue_count(v->queue)) {
                UD_TRY(ud_queue_pop(v->queue, &block));
                atomic_fetch_sub(&e->execution->queued, 1);
                ud_status status = ud_buffer_read(block, &view);
                if (status) {
                    ud_status cleanup = ud_buffer_release(block);
                    return cleanup ? cleanup : status;
                }
                input = ud_buffer_view_batch(view);
                event.kind = UD_COMPONENT_DATA;
                event.input = port;
                event.batch = input;
                selected = 1;
                n->next_port = (port + 1) % n->descriptor.inputs;
                break;
            }
            if (!v->ended && atomic_load(&p->nodes[v->from].state) == UD_EXEC_SUCCEEDED &&
                !ud_queue_count(v->queue)) {
                v->ended = 1;
                n->ended++;
                event.kind = UD_COMPONENT_INPUT_END;
                event.input = port;
                selected = 1;
                n->next_port = (port + 1) % n->descriptor.inputs;
                break;
            }
        }
    if (!selected)
        return UD_OK;
    atomic_store(&n->state, n->ended == n->descriptor.inputs && n->descriptor.inputs ?
                 UD_EXEC_DRAINING : UD_EXEC_RUNNING);
    ud_activation_notify(&n->activation, atomic_load(&n->state), UD_OK);
    memset(n->emitted, 0, n->descriptor.outputs);
    n->emit_allowed = 1;
    ud_status s = n->descriptor.step(n->instance, &event);
    n->emit_allowed = 0;
    int was_pending = n->pending;
    n->pending = s == UD_MORE;
    if (s == UD_MORE)
        s = UD_OK;
    uint32_t emitted = 0;
    for (uint32_t j = 0; j < n->descriptor.outputs; j++)
        emitted += n->emitted[j];
    if (s == UD_BUSY || s < UD_OK || s > UD_TIMEOUT)
        s = UD_INVALID;
    if (s == UD_END) {
        if (emitted ||
            (n->descriptor.category != UD_COMPONENT_SOURCE && n->ended != n->descriptor.inputs))
            s = UD_INVALID;
        else {
            atomic_store(&n->state, UD_EXEC_SUCCEEDED);
            s = UD_OK;
        }
    } else if (!s && event.kind == UD_COMPONENT_POLL && !emitted && !was_pending && !n->pending)
        s = UD_INVALID;
    if (!s && event.kind == UD_COMPONENT_DATA && n->descriptor.category == UD_COMPONENT_DESTINATION)
        ud_task_rows(e, 0, input->rows, 0);
    ud_buffer_view_release(view);
    ud_status cleanup = ud_buffer_release(block);
    if (cleanup && !s)
        s = cleanup;
    if (s)
        atomic_store(&n->state, UD_EXEC_FAILED);
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
           !atomic_compare_exchange_weak(&execution->tree_high_water, &previous, active)) {}
    execution_tree *tree = plan->trees + index;
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
    atomic_fetch_sub(&execution->tree_active, 1);
    return status ? status : complete == tree->count ? UD_OK : progress ? UD_MORE : UD_BUSY;
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
        atomic_fetch_add(&activation->execution->pipeline_trees, plan_value.tree_count);
        for (uint32_t i = 0; i < plan_value.count; i++)
            plan_value.nodes[i].activation.stepping = 1;
        status = ud_executor_run(activation->execution->executor, plan_value.tree_count,
                                 plan_value.concurrency, tree_quantum, &plan_value);
    }
    /* All tree jobs have joined. Finish in topological order after full drain;
     * component variable writes now use the owning task's lexical authority. */
    for (uint32_t i = 0; !status && i < plan_value.count; i++) {
        node *component = plan_value.nodes + plan_value.order[i];
        component->activation.stepping = 0;
        component->activation.parallel = activation->parallel;
        status = ud_activation_check(&component->activation);
        if (!status)
            status = component->descriptor.finish(component->instance);
    }
    status = clear(&plan_value, status);
    ud_free(targets);
    return status;
}
