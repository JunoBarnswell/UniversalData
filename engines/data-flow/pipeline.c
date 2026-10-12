#include "../package/internal.h"
#include "ud/engine.h"
typedef struct edge {
    uint32_t from, to, output, input, count;
    ud_queue *queue;
    int ended;
} edge;
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
    int opened, state, emit_allowed;
    ud_component_context context;
} node;
typedef struct physical_plan {
    ud_execution *execution;
    node *nodes;
    edge *edges;
    uint32_t count, paths, *order;
    ud_string id;
} physical_plan;
static int index_of(const physical_plan *p, ud_string id) {
    for (uint32_t i = 0; i < p->count; i++)
        if (ud_same(p->nodes[i].id, id))
            return (int)i;
    return -1;
}
static void clear(physical_plan *p, ud_status result) {
    if (p->edges)
        for (uint32_t i = 0; i < p->paths; i++) {
            if (p->edges[i].queue) {
                atomic_fetch_sub(&p->execution->queued, p->edges[i].count);
                ud_queue_destroy(p->edges[i].queue);
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
}
static ud_status plan(ud_execution *e, const ud_json *definition, physical_plan *p) {
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
    p->nodes = ud_alloc(e->ctx, p->count * sizeof(*p->nodes));
    p->edges = ud_alloc(e->ctx, p->paths * sizeof(*p->edges));
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
        ud_string component;
        UD_TRY(ud_json_string(ud_member(def, "component"), &component));
        n->descriptor = (ud_component_descriptor){.struct_size = sizeof(n->descriptor),
                                                  .abi_version = UD_ABI_VERSION};
        UD_TRY(ud_component_lookup(e, component, &n->descriptor));
        if (e->options.state_directory.length &&
            !(n->descriptor.capabilities & UD_COMPONENT_REPLAY_SAFE))
            return UD_UNSUPPORTED;
        if (n->descriptor.category < UD_COMPONENT_SOURCE ||
            n->descriptor.category > UD_COMPONENT_DESTINATION)
            return UD_SCHEMA;
        if (ud_json_kind(ud_member(def, "config")) != UD_JSON_OBJECT)
            return UD_SCHEMA;
        n->inputs = ud_alloc(e->ctx, (n->descriptor.inputs + 1) * sizeof(*n->inputs));
        n->emitted = ud_alloc(e->ctx, n->descriptor.outputs + 1);
        if (!n->inputs || !n->emitted)
            return UD_RESOURCE;
        for (uint32_t j = 0; j < n->descriptor.inputs; j++)
            n->inputs[j] = UINT32_MAX;
        n->state = UD_EXEC_READY;
    }
    uint32_t *degree = ud_alloc(e->ctx, p->count * sizeof(*degree));
    uint8_t *seen = ud_alloc(e->ctx, p->count);
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
        ud_string from, to;
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
        n->outputs = ud_alloc(e->ctx, (n->output_edges + 1) * sizeof(*n->outputs));
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
    uint32_t *order = ud_alloc(e->ctx, p->count * sizeof(*order));
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
    else
        p->order = order;
    return s;
}
ud_status ud_pipeline_validate(ud_execution *e, const ud_json *definition) {
    physical_plan p = {0};
    ud_status s = plan(e, definition, &p);
    clear(&p, s);
    return s;
}
static ud_status emit(void *user, uint32_t port, ud_batch *b) {
    node *n = user;
    ud_execution *e = n->context.execution;
    if (!n->emit_allowed)
        return UD_INVALID;
    if (!b || port >= n->descriptor.outputs || n->emitted[port] || b->ctx != e->ctx ||
        b->rows > e->options.max_batch_rows)
        return UD_SCHEMA;
    ud_batch *schema = n->schemas[port];
    if (b->columns != schema->columns)
        return UD_SCHEMA;
    for (uint32_t j = 0; j < b->columns; j++)
        if (!ud_field_equal(&b->column[j].field, &schema->column[j].field, 1))
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
    ud_execution *e = p->execution;
    UD_TRY(emit(n, port, b));
    UD_TRY(ud_batch_seal(b));
    for (uint32_t i = 0; i < n->output_edges; i++) {
        edge *v = p->edges + n->outputs[i];
        if (v->output != port)
            continue;
        if (v->count == e->options.edge_capacity)
            return UD_BUSY;
        UD_TRY(ud_batch_retain(b));
        ud_status s = ud_queue_push(v->queue, b);
        if (s) {
            ud_batch_destroy(b);
            return s;
        }
        v->count++;
        uint64_t queued = atomic_fetch_add(&e->queued, 1) + 1;
        if (queued > atomic_load(&e->high_water))
            atomic_store(&e->high_water, queued);
    }
    n->emitted[port] = 1;
    atomic_fetch_add(&e->batches, 1);
    if (n->descriptor.category == UD_COMPONENT_SOURCE)
        atomic_fetch_add(&e->input_rows, b->rows);
    if ((int32_t)port == n->descriptor.error_output)
        atomic_fetch_add(&e->rejected_rows, b->rows);
    return UD_OK;
}
static ud_status configure_plan(physical_plan *p, const ud_json *definition, emit_target *targets) {
    ud_execution *e = p->execution;
    const ud_json *defs = ud_member(definition, "components");
    uint32_t tree = 0;
    /* Entire pipeline schema/configuration is validated before any acquisition. */
    for (uint32_t i = 0; i < p->count; i++) {
        node *n = p->nodes + p->order[i];
        uint32_t index = p->order[i];
        targets[index] = (emit_target){p, n};
        n->context = (ud_component_context){sizeof(n->context),
                                            UD_ABI_VERSION,
                                            e->ctx,
                                            e,
                                            e->task_id,
                                            p->id,
                                            n->id,
                                            e->options.max_batch_rows,
                                            route,
                                            targets + index,
                                            ud_resolve_value,
                                            e};
        UD_TRY(ud_bound_config(e, ud_member(ud_json_at(defs, index), "config"), &n->configuration));
        UD_TRY(ud_json_serialize(n->configuration, &n->json, &n->json_length));
        n->schemas = ud_alloc(e->ctx, (n->descriptor.outputs + 1) * sizeof(*n->schemas));
        if (!n->schemas)
            return UD_RESOURCE;
        const ud_batch **inputs = ud_alloc(e->ctx, (n->descriptor.inputs + 1) * sizeof(*inputs));
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
            if (!n->schemas[j] || n->schemas[j]->ctx != e->ctx || n->schemas[j]->rows)
                return UD_SCHEMA;
        if (n->descriptor.synchronous_input >= 0)
            n->tree = p->nodes[p->edges[n->inputs[n->descriptor.synchronous_input]].from].tree;
        else
            n->tree = ++tree;
    }
    return UD_OK;
}
ud_status ud_pipeline_bind_validate(ud_execution *e, const ud_json *definition) {
    physical_plan p = {0};
    ud_status s = plan(e, definition, &p);
    emit_target *targets = NULL;
    if (!s) {
        targets = ud_alloc(e->ctx, p.count * sizeof(*targets));
        if (!targets)
            s = UD_RESOURCE;
    }
    if (!s)
        s = configure_plan(&p, definition, targets);
    clear(&p, s);
    ud_free(targets);
    return s;
}
static ud_status prepare(physical_plan *p, const ud_json *definition, emit_target *targets) {
    ud_execution *e = p->execution;
    UD_TRY(configure_plan(p, definition, targets));
    for (uint32_t i = 0; i < p->paths; i++)
        UD_TRY(ud_queue_create(e->ctx, e->options.edge_capacity, &p->edges[i].queue));
    for (uint32_t i = 0; i < p->count; i++) {
        node *n = p->nodes + p->order[i];
        const ud_batch **inputs = ud_alloc(e->ctx, (n->descriptor.inputs + 1) * sizeof(*inputs));
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
    ud_execution *e = p->execution;
    if (n->state == UD_EXEC_SUCCEEDED)
        return UD_OK;
    for (uint32_t i = 0; i < n->output_edges; i++)
        if (p->edges[n->outputs[i]].count == e->options.edge_capacity)
            return UD_OK;
    ud_component_event event = {sizeof(event), UD_ABI_VERSION, UD_COMPONENT_POLL, 0, NULL};
    ud_batch *input = NULL;
    int selected =
        n->descriptor.category == UD_COMPONENT_SOURCE || n->ended == n->descriptor.inputs;
    if (!selected)
        for (uint32_t i = 0; i < n->descriptor.inputs; i++) {
            uint32_t port = (n->next_port + i) % n->descriptor.inputs;
            edge *v = p->edges + n->inputs[port];
            if (v->count) {
                UD_TRY(ud_queue_pop(v->queue, &input));
                v->count--;
                atomic_fetch_sub(&e->queued, 1);
                event.kind = UD_COMPONENT_DATA;
                event.input = port;
                event.batch = input;
                selected = 1;
                n->next_port = (port + 1) % n->descriptor.inputs;
                break;
            }
            if (!v->ended && p->nodes[v->from].state == UD_EXEC_SUCCEEDED) {
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
    n->state = n->ended == n->descriptor.inputs && n->descriptor.inputs ? UD_EXEC_DRAINING
                                                                        : UD_EXEC_RUNNING;
    e->component_id = n->id;
    ud_execution_notify(e, n->state, UD_OK);
    memset(n->emitted, 0, n->descriptor.outputs);
    n->emit_allowed = 1;
    ud_status s = n->descriptor.step(n->instance, &event);
    n->emit_allowed = 0;
    uint32_t emitted = 0;
    for (uint32_t j = 0; j < n->descriptor.outputs; j++)
        emitted += n->emitted[j];
    if (s == UD_BUSY || s < UD_OK || s > UD_FENCED)
        s = UD_INVALID;
    if (s == UD_END) {
        if (emitted ||
            (n->descriptor.category != UD_COMPONENT_SOURCE && n->ended != n->descriptor.inputs))
            s = UD_INVALID;
        else {
            n->state = UD_EXEC_SUCCEEDED;
            s = UD_OK;
        }
    } else if (!s && event.kind == UD_COMPONENT_POLL && !emitted)
        s = UD_INVALID;
    if (!s && event.kind == UD_COMPONENT_DATA && n->descriptor.category == UD_COMPONENT_DESTINATION)
        atomic_fetch_add(&e->output_rows, input->rows);
    ud_batch_destroy(input);
    if (s)
        n->state = UD_EXEC_FAILED;
    else
        *progress = 1;
    return s;
}
ud_status ud_pipeline_execute(ud_execution *e, const ud_json *definition) {
    physical_plan p = {0};
    ud_status s = plan(e, definition, &p);
    emit_target *targets = NULL;
    ud_string old_pipeline = e->pipeline_id, old_component = e->component_id;
    e->pipeline_id = p.id;
    if (!s) {
        targets = ud_alloc(e->ctx, p.count * sizeof(*targets));
        if (!targets)
            s = UD_RESOURCE;
    }
    if (!s)
        s = prepare(&p, definition, targets);
    while (!s) {
        s = ud_execution_check(e);
        if (s)
            break;
        int progress = 0;
        uint32_t completed = 0;
        for (uint32_t i = p.count; !s && i; i--) {
            node *n = p.nodes + p.order[i - 1];
            s = step(&p, n, &progress);
            if (n->state == UD_EXEC_SUCCEEDED)
                completed++;
            if (!s)
                s = ud_execution_check(e);
        }
        if (s || completed == p.count)
            break;
        if (!progress)
            s = UD_CORRUPT;
    }
    /* No destination commits until every component has successfully drained. */
    for (uint32_t i = 0; !s && i < p.count; i++) {
        node *n = p.nodes + p.order[i];
        s = ud_execution_check(e);
        if (!s)
            s = n->descriptor.finish(n->instance);
    }
    clear(&p, s);
    ud_free(targets);
    e->pipeline_id = old_pipeline;
    e->component_id = old_component;
    return s;
}
