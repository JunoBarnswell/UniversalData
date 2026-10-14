#include "../package/internal.h"
#include "ud/engine.h"
static int task_index(const ud_json *tasks, ud_string id) {
    for (uint32_t i = 0; i < ud_json_size(tasks); i++) {
        ud_string other = {0};
        if (!ud_json_string(ud_member(ud_json_at(tasks, i), "id"), &other) && ud_same(id, other))
            return (int)i;
    }
    return -1;
}
static const ud_json *pipeline(ud_activation *e, ud_string id) {
    const ud_json *flows = ud_member(e->package->document, "dataFlows");
    for (uint32_t i = 0; i < ud_json_size(flows); i++) {
        const ud_json *p = ud_json_at(flows, i);
        ud_string other = {0};
        if (!ud_json_string(ud_member(p, "id"), &other) && ud_same(id, other))
            return p;
    }
    return NULL;
}
static ud_status assignment(ud_activation *e, const ud_json *v, int perform) {
    static const char *keys[] = {"name", "value"};
    UD_TRY(ud_json_keys(v, keys, 2));
    ud_string name = {0};
    UD_TRY(ud_json_string(ud_member(v, "name"), &name));
    ud_value old;
    UD_TRY(ud_activation_variable(e, name, &old));
    ud_variable *target;
    UD_TRY(ud_variable_writable(e, name, &target));
    ud_expression *expr = NULL;
    UD_TRY(ud_expression_json(e, ud_member(v, "value"), NULL, &expr));
    ud_status s = ud_expression_type(expr) == old.type ? UD_OK : UD_SCHEMA;
    if (!s && perform) {
        ud_value value = {0};
        s = ud_expression_evaluate(expr, NULL, 0, &value);
        if (!s)
            s = ud_activation_set_variable(e, name, &value, &target->box->column[0].field);
    }
    ud_expression_destroy(expr);
    return s;
}
static ud_status condition(ud_activation *e, const ud_json *v, int *out) {
    ud_expression *expr = NULL;
    UD_TRY(ud_expression_json(e, v, NULL, &expr));
    ud_value value = {0};
    ud_status s = ud_expression_type(expr) == UD_BOOL ? UD_OK : UD_SCHEMA;
    if (!s)
        s = ud_expression_evaluate(expr, NULL, 0, &value);
    *out = !s && !value.is_null && value.integer;
    ud_expression_destroy(expr);
    return s;
}
typedef struct control_edge {
    uint32_t from, to, next_input, next_output;
    uint8_t on, evaluation, combine;
    const ud_json *expression;
} control_edge;
typedef struct control_plan {
    uint32_t count, edge_count, concurrency;
    int parallel;
    control_edge *edges;
    uint32_t *input_head, *output_head, *remaining;
} control_plan;
static int parallel_task(ud_activation *, const ud_json *);
static void control_clear(control_plan *p) {
    ud_free(p->edges);
    ud_free(p->input_head);
    ud_free(p->output_head);
    ud_free(p->remaining);
    memset(p, 0, sizeof(*p));
}
static ud_status control_bind(ud_activation *a, const ud_json *tasks, const ud_json *constraints,
                              control_plan *p) {
    if (ud_json_kind(tasks) != UD_JSON_ARRAY || ud_json_size(tasks) > 65536 ||
        (constraints && ud_json_kind(constraints) != UD_JSON_ARRAY) ||
        ud_json_size(constraints) > 65536)
        return UD_SCHEMA;
    p->count = ud_json_size(tasks);
    p->edge_count = ud_json_size(constraints);
    p->concurrency = ud_execution_concurrency(a);
    if (!p->concurrency)
        return UD_SCHEMA;
    if (ud_transaction_active(a) || a->transaction_serial)
        p->concurrency = 1;
    ud_context *ctx = a->execution->ctx;
    p->edges = ud_alloc(ctx, (size_t)(p->edge_count + 1) * sizeof(*p->edges));
    p->input_head = ud_alloc(ctx, (size_t)(p->count + 1) * sizeof(uint32_t));
    p->output_head = ud_alloc(ctx, (size_t)(p->count + 1) * sizeof(uint32_t));
    p->remaining = ud_alloc(ctx, (size_t)(p->count + 1) * sizeof(uint32_t));
    if (!p->edges || !p->input_head || !p->output_head || !p->remaining)
        return UD_RESOURCE;
    for (uint32_t i = 0; i < p->count; i++) {
        p->input_head[i] = p->output_head[i] = UINT32_MAX;
        if (!parallel_task(a, ud_json_at(tasks, i)))
            p->concurrency = 1;
    }
    for (uint32_t i = 0; i < p->edge_count; i++) {
        const ud_json *def = ud_json_at(constraints, i);
        static const char *keys[] = {"from", "to", "on", "expression", "combine", "evaluation"};
        UD_TRY(ud_json_keys(def, keys, 6));
        ud_string from, to, on, combine = {"and", 3}, evaluation = {0};
        UD_TRY(ud_json_string(ud_member(def, "from"), &from));
        UD_TRY(ud_json_string(ud_member(def, "to"), &to));
        UD_TRY(ud_json_string(ud_member(def, "on"), &on));
        if (ud_member(def, "combine"))
            UD_TRY(ud_json_string(ud_member(def, "combine"), &combine));
        int source = task_index(tasks, from), target = task_index(tasks, to);
        if (source < 0 || target < 0 || source == target ||
            (!ud_named(on, "success") && !ud_named(on, "failure") && !ud_named(on, "completion")) ||
            (!ud_named(combine, "and") && !ud_named(combine, "or")))
            return UD_SCHEMA;
        control_edge *edge = p->edges + i;
        edge->from = (uint32_t)source;
        edge->to = (uint32_t)target;
        edge->on = ud_named(on, "success") ? 0 : ud_named(on, "failure") ? 1 : 2;
        edge->combine = ud_named(combine, "or");
        edge->expression = ud_member(def, "expression");
        edge->evaluation = edge->expression ? 2 : 0;
        if (ud_member(def, "evaluation")) {
            UD_TRY(ud_json_string(ud_member(def, "evaluation"), &evaluation));
            if (ud_named(evaluation, "constraint"))
                edge->evaluation = 0;
            else if (ud_named(evaluation, "expression"))
                edge->evaluation = 1;
            else if (ud_named(evaluation, "and"))
                edge->evaluation = 2;
            else if (ud_named(evaluation, "or"))
                edge->evaluation = 3;
            else
                return UD_SCHEMA;
        }
        if (!!edge->evaluation != !!edge->expression)
            return UD_SCHEMA;
        if (edge->expression) {
            int ignored;
            UD_TRY(condition(a, edge->expression, &ignored));
        }
        if (p->input_head[target] != UINT32_MAX &&
            p->edges[p->input_head[target]].combine != edge->combine)
            return UD_SCHEMA;
        edge->next_input = p->input_head[target];
        p->input_head[target] = i;
        edge->next_output = p->output_head[source];
        p->output_head[source] = i;
        p->remaining[target]++;
    }
    uint32_t *degrees = ud_alloc(ctx, (size_t)(p->count + 1) * sizeof(uint32_t));
    uint32_t *ready = ud_alloc(ctx, (size_t)(p->count + 1) * sizeof(uint32_t));
    if (!degrees || !ready) {
        ud_free(degrees);
        ud_free(ready);
        return UD_RESOURCE;
    }
    memcpy(degrees, p->remaining, (size_t)p->count * sizeof(uint32_t));
    uint32_t head = 0, tail = 0;
    for (uint32_t i = 0; i < p->count; i++)
        if (!degrees[i])
            ready[tail++] = i;
    while (head < tail) {
        if (tail - head > 1)
            p->parallel = p->concurrency > 1;
        uint32_t index = ready[head++];
        for (uint32_t i = p->output_head[index]; i != UINT32_MAX; i = p->edges[i].next_output)
            if (!--degrees[p->edges[i].to])
                ready[tail++] = p->edges[i].to;
    }
    ud_free(degrees);
    ud_free(ready);
    return head == p->count ? UD_OK : UD_SCHEMA;
}
ud_status ud_control_validate(ud_activation *e, const ud_json *flow, uint32_t depth) {
    if (depth >= e->execution->options.max_depth)
        return UD_RESOURCE;
    static const char *flow_keys[] = {"tasks", "constraints", "variables"};
    UD_TRY(ud_json_keys(flow, flow_keys, 3));
    const ud_json *tasks = ud_member(flow, "tasks"), *constraints = ud_member(flow, "constraints");
    if (ud_json_kind(tasks) != UD_JSON_ARRAY || ud_json_size(tasks) > 65536 ||
        (constraints && ud_json_kind(constraints) != UD_JSON_ARRAY))
        return UD_SCHEMA;
    ud_scope scope = {0};
    ud_status s = ud_scope_enter(e, (ud_string){"flow", 4}, ud_member(flow, "variables"), &scope);
    uint32_t count = ud_json_size(tasks);
    control_plan preview = {0};
    if (!s)
        s = control_bind(e, tasks, constraints, &preview);
    int parallel = preview.parallel;
    control_clear(&preview);
    ud_activation *parent = e;
    for (uint32_t i = 0; !s && i < count; i++) {
        ud_activation validation = {.execution = parent->execution,
                                    .parent = parent,
                                    .definition = parent->definition,
                                    .package = parent->package,
                                    .parameters = parent->parameters,
                                    .scope = parent->scope,
                                    .depth = parent->depth,
                                    .parallel = parent->parallel || parallel,
                                    .transaction = parent->transaction,
                                    .transaction_serial = parent->transaction_serial,
                                    .transaction_epoch = parent->transaction_epoch,
                                    .transaction_participants = parent->transaction_participants};
        atomic_init(&validation.cancelled, 0);
        e = &validation;
        const ud_json *task = ud_json_at(tasks, i);
        e->task_definition = task;
        static const char *keys[] = {"id",        "kind",      "config",        "children",
                                     "variables", "timeoutMs", "eventHandlers", "transaction",
                                     "retry"};
        s = ud_json_keys(task, keys, 9);
        ud_string id = {0}, kind = {0};
        if (!s)
            s = ud_json_string(ud_member(task, "id"), &id);
        if (!s)
            s = ud_json_string(ud_member(task, "kind"), &kind);
        if (!s && task_index(tasks, id) != (int)i)
            s = UD_SCHEMA;
        if (!s && ud_member(task, "timeoutMs")) {
            uint64_t deadline;
            s = ud_json_uint(ud_member(task, "timeoutMs"), &deadline);
            if (!s && !deadline)
                s = UD_SCHEMA;
        }
        const ud_json *config = ud_member(task, "config");
        if (!s && ud_json_kind(config) != UD_JSON_OBJECT)
            s = UD_SCHEMA;
        ud_scope local = {0};
        ud_string task_identity = {0};
        if (!s)
            s = ud_json_string(ud_member(task, "id"), &task_identity);
        if (!s) {
            e->task_id = task_identity;
            s = ud_scope_enter(e, task_identity, ud_member(task, "variables"), &local);
        }
        if (!s)
            s = ud_transaction_enter(e, ud_member(task, "transaction"), 1);
        if (!s && e->transaction_serial && ud_member(config, "connection")) {
            ud_json *bound = NULL;
            s = ud_bound_config(e, config, &bound);
            ud_string connection = {0};
            if (!s)
                s = ud_json_string(ud_member(bound, "connection"), &connection);
            if (!s)
                s = ud_transaction_resource_validate(e, connection);
            ud_json_destroy(bound);
        }
        const ud_json *retry = ud_member(task, "retry");
        if (!s && retry) {
            static const char *rk[] = {"maxAttempts", "backoffMs"};
            s = ud_json_keys(retry, rk, 2);
            uint64_t attempts = 0, backoff = 0;
            if (!s)
                s = ud_json_uint(ud_member(retry, "maxAttempts"), &attempts);
            if (!s && (!attempts || attempts > 100))
                s = UD_SCHEMA;
            if (!s && ud_member(retry, "backoffMs"))
                s = ud_json_uint(ud_member(retry, "backoffMs"), &backoff);
            if (!s && backoff > 60000)
                s = UD_SCHEMA;
            if (!s && attempts > 1) {
                ud_component_descriptor rd = {.struct_size = sizeof(rd),
                                              .abi_version = UD_ABI_VERSION};
                s = ud_component_lookup(e, kind, &rd);
                if (!s && !(rd.capabilities & UD_COMPONENT_REPLAY_SAFE))
                    s = UD_UNSUPPORTED;
            }
        }
        if (!s &&
            (ud_named(kind, "sequence") || ud_named(kind, "for") || ud_named(kind, "foreach"))) {
            if (ud_named(kind, "for")) {
                static const char *fk[] = {"initialize", "condition", "increment", "maxIterations"};
                s = ud_json_keys(config, fk, 4);
                uint64_t max = 0;
                if (!s)
                    s = ud_json_uint(ud_member(config, "maxIterations"), &max);
                if (!s && (!max || max > 1000000))
                    s = UD_SCHEMA;
                if (!s)
                    s = assignment(e, ud_member(config, "initialize"), 0);
                if (!s) {
                    int unused;
                    s = condition(e, ud_member(config, "condition"), &unused);
                }
                if (!s)
                    s = assignment(e, ud_member(config, "increment"), 0);
            } else if (ud_named(kind, "foreach")) {
                s = ud_foreach(e, config, ud_member(task, "children"), 0, depth);
            } else if (ud_json_size(config))
                s = UD_SCHEMA;
            if (!s && !ud_named(kind, "foreach"))
                s = ud_control_validate(e, ud_member(task, "children"), depth + 1);
        } else if (!s && ud_named(kind, "dataFlow")) {
            static const char *dk[] = {"pipeline"};
            s = ud_json_keys(config, dk, 1);
            ud_string name = {0};
            if (!s)
                s = ud_json_string(ud_member(config, "pipeline"), &name);
            if (!s && !pipeline(e, name))
                s = UD_SCHEMA;
            if (!s)
                s = ud_pipeline_bind_validate(e, pipeline(e, name));
        } else if (!s && ud_named(kind, "set"))
            s = assignment(e, config, 0);
        else if (!s && ud_named(kind, "executePackage")) {
            if (ud_member(task, "children"))
                s = UD_SCHEMA;
            if (!s)
                s = ud_child_package(e, config, 0, depth + 1);
        } else if (!s) {
            ud_component_descriptor d = {.struct_size = sizeof(d), .abi_version = UD_ABI_VERSION};
            s = ud_component_lookup(e, kind, &d);
            if (!s && d.category != UD_COMPONENT_TASK)
                s = UD_SCHEMA;
            if (!s && d.configure) {
                ud_json *bound = NULL;
                char *text = NULL;
                uint32_t n = 0;
                s = ud_bound_config(e, config, &bound);
                ud_json *validated = NULL;
                if (!s)
                    s = ud_component_bind_configuration(e->execution->ctx, &d, bound, &validated);
                ud_json_destroy(bound);
                bound = validated;
                if (!s)
                    s = ud_json_serialize(bound, &text, &n);
                ud_component_context context = {.struct_size = sizeof(context),
                                                .abi_version = UD_ABI_VERSION,
                                                .memory = e->execution->ctx,
                                                .execution = e->execution,
                                                .activation = e,
                                                .max_batch_rows =
                                                    e->execution->options.max_batch_rows,
                                                .resolve = ud_resolve_value,
                                                .resolve_user = e,
                                                .resolve_field = ud_resolve_field};
                if (!s)
                    s = d.configure(d.user, &context, (ud_string){text, n}, NULL, NULL);
                ud_free(text);
                ud_json_destroy(bound);
            }
        }
        const ud_json *handlers = ud_member(task, "eventHandlers");
        if (!s && handlers && ud_json_kind(handlers) != UD_JSON_OBJECT)
            s = UD_SCHEMA;
        for (uint32_t j = 0; !s && j < ud_json_size(handlers); j++) {
            const char *key = NULL;
            uint32_t length = 0;
            s = ud_json_key(handlers, j, &key, &length);
            ud_string name = {key, length};
            if (!s && !ud_named(name, "before") && !ud_named(name, "after") &&
                !ud_named(name, "failure") && !ud_named(name, "warning") &&
                !ud_named(name, "error") && !ud_named(name, "taskFailed"))
                s = UD_UNSUPPORTED;
            if (!s)
                s = ud_control_validate(e, ud_json_at(handlers, j), depth + 1);
        }
        if (local.parent)
            ud_scope_leave(e, &local);
        e = parent;
    }
    control_plan graph = {0};
    if (!s)
        s = control_bind(e, tasks, constraints, &graph);
    control_clear(&graph);
    ud_scope_leave(e, &scope);
    return s;
}
ud_status ud_control_iteration(ud_activation *e, const ud_json *children, uint64_t number) {
    ud_string parent = e->task_id;
    size_t size = (size_t)parent.length + 64;
    char *identity = ud_alloc(e->execution->ctx, size);
    if (!identity)
        return UD_RESOURCE;
    int n = snprintf(identity, size, "%.*s@iteration:%llu;", (int)parent.length,
                     parent.data ? parent.data : "", (unsigned long long)number);
    ud_activation iteration = {.execution = e->execution,
                               .parent = e,
                               .definition = e->definition,
                               .package = e->package,
                               .parameters = e->parameters,
                               .scope = e->scope,
                               .task_id = {identity, (uint32_t)n},
                               .depth = e->depth,
                               .parallel = e->parallel,
                               .transaction = e->transaction,
                               .transaction_serial = e->transaction_serial,
                               .transaction_epoch = e->transaction_epoch,
                               .transaction_participants = e->transaction_participants,
                               .event_depth = e->event_depth};
    atomic_init(&iteration.cancelled, 0);
    ud_status result = ud_control_execute(&iteration, children);
    ud_free(identity);
    return result;
}
ud_status ud_control_handler(ud_activation *e, const ud_json *flow, const char *kind) {
    if (!flow)
        return UD_OK;
    ud_string parent = e->task_id;
    size_t size = (size_t)parent.length + strlen(kind) + 24;
    char *identity = ud_alloc(e->execution->ctx, size);
    if (!identity)
        return UD_RESOURCE;
    int length = snprintf(identity, size, "%.*s/handler:%s;", (int)parent.length,
                          parent.data ? parent.data : "", kind);
    if (length < 0 || (size_t)length >= size) {
        ud_free(identity);
        return UD_OVERFLOW;
    }
    ud_activation handler = {.execution = e->execution,
                             .parent = e,
                             .definition = e->definition,
                             .package = e->package,
                             .parameters = e->parameters,
                             .scope = e->scope,
                             .task_id = {identity, (uint32_t)length},
                             .depth = e->depth,
                             .parallel = e->parallel,
                             .transaction = e->transaction,
                             .transaction_serial = e->transaction_serial,
                             .transaction_epoch = e->transaction_epoch,
                             .transaction_participants = e->transaction_participants,
                             .event_depth = e->event_depth + 1};
    atomic_init(&handler.cancelled, 0);
    atomic_init(&handler.input_rows, 0);
    atomic_init(&handler.output_rows, 0);
    atomic_init(&handler.rejected_rows, 0);
    atomic_init(&handler.committed_rows, 0);
    atomic_init(&handler.acknowledged_rows, 0);
    atomic_init(&handler.error_rows, 0);
    atomic_init(&handler.committed_error_rows, 0);
    atomic_init(&handler.acknowledged_error_rows, 0);
    ud_status s = ud_control_execute(&handler, flow);
    if (s)
        ud_activation_notify(&handler, UD_EXEC_FAILED, s);
    ud_rule_facts_destroy(&handler.rule_facts);
    ud_free(identity);
    return s;
}
static ud_status perform_task(ud_activation *e, const ud_json *task) {
    ud_string kind = {0};
    UD_TRY(ud_json_string(ud_member(task, "kind"), &kind));
    const ud_json *config = ud_member(task, "config");
    ud_scope scope = {0};
    ud_string task_identity = {0};
    ud_status s = ud_json_string(ud_member(task, "id"), &task_identity);
    if (!s)
        s = ud_scope_enter(e, task_identity, ud_member(task, "variables"), &scope);
    int entered = 0;
    if (!s) {
        e->depth++;
        entered = 1;
        e->invocation++;
        if (e->depth > e->execution->options.max_depth)
            s = UD_RESOURCE;
    }
    const ud_json *handlers = ud_member(task, "eventHandlers");
    e->event_handlers = handlers;
    if (!s)
        s = ud_debug_boundary(e, UD_DEBUG_TASK);
    if (!s)
        s = ud_control_handler(e, ud_member(handlers, "before"), "before");
    if (!s && ud_named(kind, "sequence"))
        s = ud_control_execute(e, ud_member(task, "children"));
    else if (!s && ud_named(kind, "executePackage"))
        s = ud_child_package(e, config, 1, e->depth);
    else if (!s && ud_named(kind, "for")) {
        s = assignment(e, ud_member(config, "initialize"), 1);
        uint64_t max = 0;
        ud_json_uint(ud_member(config, "maxIterations"), &max);
        uint64_t iteration = 0;
        while (!s) {
            int yes = 0;
            s = ud_activation_check(e);
            if (!s)
                s = condition(e, ud_member(config, "condition"), &yes);
            if (s || !yes)
                break;
            if (iteration++ >= max) {
                s = UD_RESOURCE;
                break;
            }
            s = ud_control_iteration(e, ud_member(task, "children"), iteration - 1);
            if (!s)
                s = assignment(e, ud_member(config, "increment"), 1);
        }
    } else if (!s && ud_named(kind, "foreach")) {
        s = ud_foreach(e, config, ud_member(task, "children"), 1, e->depth);
    } else if (!s && ud_named(kind, "dataFlow")) {
        ud_string name = {0};
        ud_json_string(ud_member(config, "pipeline"), &name);
        const ud_json *p = pipeline(e, name);
        if (!p)
            s = UD_SCHEMA;
        else
            s = ud_pipeline_execute(e, p);
    } else if (!s && ud_named(kind, "set"))
        s = assignment(e, config, 1);
    else if (!s) {
        ud_component_descriptor d = {.struct_size = sizeof(d), .abi_version = UD_ABI_VERSION};
        s = ud_component_lookup(e, kind, &d);
        ud_json *bound = NULL;
        char *json = NULL;
        uint32_t n = 0;
        if (!s)
            s = ud_bound_config(e, config, &bound);
        if (!s)
            s = ud_json_serialize(bound, &json, &n);
        ud_component_context ctx = {.struct_size = sizeof(ctx),
                                    .abi_version = UD_ABI_VERSION,
                                    .memory = e->execution->ctx,
                                    .execution = e->execution,
                                    .activation = e,
                                    .task_id = e->task_id,
                                    .max_batch_rows = e->execution->options.max_batch_rows,
                                    .resolve = ud_resolve_value,
                                    .resolve_user = e,
                                    .resolve_field = ud_resolve_field};
        if (!s) {
            int serial = !(d.capabilities & UD_COMPONENT_THREAD_SAFE);
            if (serial)
                ud_mutex_lock(&e->execution->serial_tasks_lock);
            s = ud_activation_check(e);
            void *instance = d.user;
            int prepared = 0;
            if (!s && d.prepare) {
                s = d.prepare(d.user, &ctx, (ud_string){json, n}, NULL, NULL, &instance);
                prepared = !s;
            }
            if (!s) {
                ud_task_cancel_register(e, instance, d.cancel);
                s = ud_activation_check(e);
                if (!s)
                    s = d.invoke(instance, &ctx, (ud_string){json, n});
                ud_task_cancel_unregister(e);
            }
            if (!s)
                s = ud_activation_check(e);
            if (!s && d.finish)
                s = d.finish(instance);
            if ((s == UD_CANCELLED || s == UD_TIMEOUT) && d.cancel && !e->cancel_sent)
                d.cancel(instance);
            if (prepared)
                d.dispose(instance, s);
            if (!s)
                s = ud_activation_check(e);
            if (serial)
                ud_mutex_unlock(&e->execution->serial_tasks_lock);
        }
        if (s == UD_END || s == UD_MORE || s < UD_OK || s > UD_TRUNCATED)
            s = UD_INVALID; /* Iteration sentinels are not Task outcomes. */
        ud_free(json);
        ud_json_destroy(bound);
    }
    if (s == UD_MORE || s == UD_END)
        s = UD_INVALID;
    if (s == UD_BUSY)
        s = UD_TRANSIENT;
    if (!s)
        s = ud_control_handler(e, ud_member(handlers, "after"), "after");
    else if (s != UD_CANCELLED && s != UD_COMMIT_UNKNOWN) {
        ud_status secondary = ud_control_handler(e, ud_member(handlers, "error"), "error");
        if (secondary == UD_COMMIT_UNKNOWN)
            s = secondary;
        secondary = ud_control_handler(e, ud_member(handlers, "failure"), "failure");
        if (secondary == UD_COMMIT_UNKNOWN)
            s = secondary;
        secondary = ud_control_handler(e, ud_member(handlers, "taskFailed"), "taskFailed");
        if (secondary == UD_COMMIT_UNKNOWN)
            s = secondary;
    }
    if (entered)
        e->depth--;
    ud_scope_leave(e, &scope);
    return s;
}
typedef struct task_run {
    ud_activation activation;
    const ud_json *definition;
    ud_status result;
    uint64_t ready_ms;
} task_run;
static ud_status task_result(task_run *run) {
    ud_activation *a = &run->activation;
    ud_string kind = {0};
    UD_TRY(ud_json_string(ud_member(run->definition, "kind"), &kind));
    int completed = 0;
    ud_status s = ud_checkpoint_task_begin(a, kind, &completed);
    if (!s && !completed) {
        ud_activation_notify(a, UD_EXEC_RUNNING, UD_OK);
        uint64_t attempts = 1, backoff = 0;
        const ud_json *retry = ud_member(run->definition, "retry");
        if (retry) {
            ud_json_uint(ud_member(retry, "maxAttempts"), &attempts);
            if (ud_member(retry, "backoffMs"))
                ud_json_uint(ud_member(retry, "backoffMs"), &backoff);
        }
        for (uint64_t attempt = 0; attempt < attempts; attempt++) {
            s = ud_transaction_enter(a, ud_member(run->definition, "transaction"), 0);
            /* Resolving an interrupted commit can publish the task's completion
             * snapshot. Restore it before any handler or component is invoked. */
            if (!s && ud_transaction_recovered(a)) {
                s = ud_checkpoint_task_begin(a, kind, &completed);
                if (!s && !completed)
                    s = UD_CORRUPT;
            }
            if (!s && !completed)
                s = perform_task(a, run->definition);
            /* Component leases must be closed before a success snapshot or
             * prepare. The manager retains only coordinator-owned leases. */
            ud_status leaked = ud_connections_release_activation(a);
            if (!s && leaked)
                s = leaked;
            int pending = 0;
            if (!s && !completed) {
                ud_task_fact(a, UD_TASK_EXECUTED);
                /* This snapshot joins the transaction's existing Journal facts
                 * before prepare, so its publication and business commit have
                 * the same recovery decision. Local scopes have been unwound. */
                s = ud_checkpoint_task_done(a);
                if (!s)
                    pending = ud_transaction_task_done(a);
            }
            s = ud_transaction_leave(a, s);
            leaked = ud_connections_release_activation(a);
            if (!s && leaked)
                s = leaked;
            if (!s && !completed && a->execution->journal && !pending)
                ud_task_fact(a, UD_TASK_DURABLE);
            if (s != UD_TRANSIENT || attempt + 1 == attempts)
                break;
            ud_status waiting = ud_wait(a->execution->ctx, (uint32_t)backoff);
            if (!waiting)
                waiting = ud_activation_check(a);
            if (waiting) {
                s = waiting;
                break;
            }
        }
    }
    if (completed)
        ud_task_fact(a, UD_TASK_RESUMED);
    if (s == UD_COMMIT_UNKNOWN)
        ud_task_fact(a, UD_TASK_UNCERTAIN);
    ud_rule_facts_destroy(&a->rule_facts);
    int state = s == UD_COMMIT_UNKNOWN ? UD_EXEC_RECOVERY_REQUIRED
                : s == UD_CANCELLED    ? UD_EXEC_CANCELLED
                : s                    ? UD_EXEC_FAILED
                                       : UD_EXEC_SUCCEEDED;
    ud_activation_notify(a, state, s);
    return s;
}
static int parallel_task(ud_activation *e, const ud_json *task) {
    ud_string kind = {0};
    if (ud_json_string(ud_member(task, "kind"), &kind))
        return 0;
    if (ud_named(kind, "dataFlow")) {
        ud_string id = {0};
        if (ud_json_string(ud_member(ud_member(task, "config"), "pipeline"), &id))
            return 0;
        const ud_json *flow = pipeline(e, id), *components = ud_member(flow, "components");
        for (uint32_t i = 0; i < ud_json_size(components); i++) {
            ud_string component = {0};
            if (ud_json_string(ud_member(ud_json_at(components, i), "component"), &component))
                return 0;
            ud_component_descriptor d = {.struct_size = sizeof(d), .abi_version = UD_ABI_VERSION};
            if (ud_component_lookup(e, component, &d) ||
                !(d.capabilities & UD_COMPONENT_THREAD_SAFE))
                return 0;
        }
        return 1;
    }
    if (ud_named(kind, "set") || ud_named(kind, "sequence") || ud_named(kind, "for") ||
        ud_named(kind, "foreach") || ud_named(kind, "executePackage"))
        return 1;
    ud_component_descriptor d = {.struct_size = sizeof(d), .abi_version = UD_ABI_VERSION};
    return !ud_component_lookup(e, kind, &d) && (d.capabilities & UD_COMPONENT_THREAD_SAFE);
}
static ud_status init_task(ud_activation *parent, const ud_json *definition, task_run *run) {
    ud_activation *a = &run->activation;
    a->execution = parent->execution;
    a->parent = parent;
    a->definition = parent->definition;
    a->package = parent->package;
    a->parameters = parent->parameters;
    a->scope = parent->scope;
    a->depth = parent->depth;
    a->event_depth = parent->event_depth;
    a->parallel = parent->parallel;
    a->transaction = parent->transaction;
    a->transaction_serial = parent->transaction_serial;
    a->transaction_epoch = parent->transaction_epoch;
    a->transaction_participants = parent->transaction_participants;
    atomic_init(&a->cancelled, 0);
    atomic_init(&a->input_rows, 0);
    atomic_init(&a->output_rows, 0);
    atomic_init(&a->rejected_rows, 0);
    atomic_init(&a->committed_rows, 0);
    atomic_init(&a->acknowledged_rows, 0);
    atomic_init(&a->error_rows, 0);
    atomic_init(&a->committed_error_rows, 0);
    atomic_init(&a->acknowledged_error_rows, 0);
    run->definition = definition;
    a->task_definition = definition;
    ud_string id = {0};
    UD_TRY(ud_json_string(ud_member(definition, "id"), &id));
    size_t size = (size_t)parent->task_id.length + id.length + 48;
    char *identity = ud_alloc(a->execution->ctx, size);
    if (!identity)
        return UD_RESOURCE;
    int length = snprintf(identity, size, "%.*s/%u:%.*s;", (int)parent->task_id.length,
                          parent->task_id.data ? parent->task_id.data : "", id.length,
                          (int)id.length, id.data);
    if (length < 0 || (size_t)length >= size) {
        ud_free(identity);
        return UD_OVERFLOW;
    }
    a->task_id = (ud_string){identity, (uint32_t)length};
    if (ud_member(definition, "timeoutMs")) {
        uint64_t ms = 0, now = ud_now_ms();
        UD_TRY(ud_json_uint(ud_member(definition, "timeoutMs"), &ms));
        a->deadline = ms > UINT64_MAX - now ? UINT64_MAX : now + ms;
    }
    return UD_OK;
}
typedef struct control_run {
    ud_activation *activation;
    const ud_json *tasks;
    control_plan plan;
    task_run *runs;
    int32_t *states;
    ud_work_group *group;
    ud_mutex mutex;
    ud_status failure;
} control_run;
static ud_status edge_pass(ud_activation *a, const control_edge *edge, int32_t state, int *pass) {
    *pass = 0;
    if (state == UD_EXEC_SKIPPED || state == UD_EXEC_CANCELLED ||
        state == UD_EXEC_RECOVERY_REQUIRED)
        return UD_OK;
    int result = edge->on == 0   ? state == UD_EXEC_SUCCEEDED
                 : edge->on == 1 ? state == UD_EXEC_FAILED
                                 : state == UD_EXEC_SUCCEEDED || state == UD_EXEC_FAILED;
    int expression = 0;
    if (edge->evaluation && (edge->evaluation == 1 || (edge->evaluation == 2 && result) ||
                             (edge->evaluation == 3 && !result)))
        UD_TRY(condition(a, edge->expression, &expression));
    *pass = edge->evaluation == 0   ? result
            : edge->evaluation == 1 ? expression
            : edge->evaluation == 2 ? result && expression
                                    : result || expression;
    return UD_OK;
}
static void record_task(task_run *run, int32_t state, int pass, uint64_t start, uint64_t cpu) {
    ud_execution *e = run->activation.execution;
    /* Diagnostics are optional and bounded; allocation failure cannot rewrite
     * an already committed business outcome. Observers retain the full stream. */
    ud_mutex_lock(&e->diagnostics_lock);
    e->control_record_count++;
    if (e->control_record_count > 1024) {
        e->control_records_dropped++;
        ud_mutex_unlock(&e->diagnostics_lock);
        return;
    }
    ud_bytes item = {.ctx = e->ctx};
    ud_status status = ud_bytes_put(&item, "{\"activation\":", sizeof("{\"activation\":") - 1);
    if (!status)
        status = ud_json_quote(&item, run->activation.task_id.data, run->activation.task_id.length);
    char fields[256];
    int n = snprintf(
        fields, sizeof(fields),
        ",\"state\":%d,\"outcome\":%d,\"precedencePassed\":%s,\"readyWaitMs\":%llu,\"elapsedMs\":%"
        "llu,\"cpuNs\":%llu}",
        state, run->result, pass ? "true" : "false", (unsigned long long)(start - run->ready_ms),
        (unsigned long long)(ud_now_ms() - start), (unsigned long long)(ud_thread_cpu_ns() - cpu));
    if (!status)
        status = ud_bytes_put(&item, fields, (size_t)n);
    uint64_t before = e->control_reports.size;
    if (!status && before)
        status = ud_bytes_put(&e->control_reports, ",", 1);
    if (!status)
        status = ud_bytes_put(&e->control_reports, item.data, item.size);
    if (status) {
        e->control_reports.size = before;
        e->control_records_dropped++;
    }
    ud_bytes_clear(&item);
    ud_mutex_unlock(&e->diagnostics_lock);
}
static ud_status execute_task(void *user, uint32_t index) {
    control_run *flow = user;
    ud_activation *parent = flow->activation;
    UD_TRY(ud_activation_check(parent));
    ud_status stopped = atomic_load(&parent->execution->error_limit_reason);
    if (stopped && !parent->event_depth)
        return stopped;
    uint64_t started = ud_now_ms(), cpu = ud_thread_cpu_ns();
    task_run *run = flow->runs + index;
    UD_TRY(init_task(parent, ud_json_at(flow->tasks, index), run));
    run->activation.parallel = parent->parallel || flow->plan.parallel;
    int pass = 1;
    ud_status status = UD_OK;
    uint32_t input = flow->plan.input_head[index];
    int has = 0;
    for (; !status && input != UINT32_MAX; input = flow->plan.edges[input].next_input) {
        control_edge *edge = flow->plan.edges + input;
        int matched = 0;
        status = edge_pass(parent, edge, flow->states[edge->from], &matched);
        pass = !has ? matched : edge->combine ? pass || matched : pass && matched;
        has = 1;
    }
    int32_t state = UD_EXEC_SKIPPED;
    if (!status && pass) {
        status = task_result(run);
        state = status == UD_COMMIT_UNKNOWN ? UD_EXEC_RECOVERY_REQUIRED
                : status == UD_CANCELLED    ? UD_EXEC_CANCELLED
                : status                    ? UD_EXEC_FAILED
                                            : UD_EXEC_SUCCEEDED;
    } else if (!status)
        ud_activation_notify(&run->activation, UD_EXEC_SKIPPED, UD_OK);
    else
        state = UD_EXEC_FAILED;
    run->result = status;
    int halted = status == UD_COMMIT_UNKNOWN || status == UD_CANCELLED;
    if (status) {
        unsigned errors = atomic_fetch_add(&parent->execution->error_count, 1) + 1;
        const ud_json *maximum =
            ud_member(ud_member(parent->package->document, "policy"), "maxErrorCount");
        uint64_t limit = 0;
        if (maximum)
            ud_json_uint(maximum, &limit);
        if (limit && errors >= limit && !parent->event_depth) {
            int expected = UD_OK;
            atomic_compare_exchange_strong(&parent->execution->error_limit_reason, &expected,
                                           status);
            halted = 1;
        }
    }
    ud_mutex_lock(&flow->mutex);
    flow->states[index] = state;
    if (status == UD_COMMIT_UNKNOWN || (status && !flow->failure))
        flow->failure = status;
    for (uint32_t at = flow->plan.output_head[index]; !halted && at != UINT32_MAX;
         at = flow->plan.edges[at].next_output) {
        uint32_t target = flow->plan.edges[at].to;
        if (!--flow->plan.remaining[target]) {
            flow->runs[target].ready_ms = ud_now_ms();
            ud_work_group_ready(flow->group, target);
        }
    }
    ud_mutex_unlock(&flow->mutex);
    record_task(run, state, pass, started, cpu);
    ud_free((void *)run->activation.task_id.data);
    run->activation.task_id = (ud_string){0};
    if (status == UD_COMMIT_UNKNOWN) {
        ud_execution_cancel(parent->execution);
        return status;
    }
    if (halted)
        return status;
    /* Ordinary failure is a precedence fact and can select a failure branch. */
    return UD_OK;
}
ud_status ud_control_execute(ud_activation *e, const ud_json *definition) {
    ud_scope scope = {0};
    ud_status status =
        ud_scope_enter(e, (ud_string){"flow", 4}, ud_member(definition, "variables"), &scope);
    control_run flow = {.activation = e, .tasks = ud_member(definition, "tasks")};
    ud_mutex_init(&flow.mutex);
    if (!status)
        status = control_bind(e, flow.tasks, ud_member(definition, "constraints"), &flow.plan);
    uint32_t count = flow.plan.count;
    if (!status && count) {
        flow.runs = ud_alloc(e->execution->ctx, (size_t)count * sizeof(*flow.runs));
        flow.states = ud_alloc(e->execution->ctx, (size_t)count * sizeof(*flow.states));
        if (!flow.runs || !flow.states)
            status = UD_RESOURCE;
    }
    if (!status && count)
        status = ud_work_group_create(e->execution->executor, count, flow.plan.concurrency,
                                      execute_task, &flow, &flow.group);
    if (!status && count) {
        for (uint32_t i = 0; i < count; i++) {
            flow.states[i] = UD_EXEC_READY;
            if (!flow.plan.remaining[i]) {
                flow.runs[i].ready_ms = ud_now_ms();
                ud_work_group_ready(flow.group, i);
            }
        }
        status = ud_work_group_wait(flow.group);
    }
    ud_status cleanup = ud_work_group_destroy(flow.group);
    if (!status)
        status = cleanup;
    if (flow.failure == UD_COMMIT_UNKNOWN || (!status && flow.failure))
        status = flow.failure;
    control_clear(&flow.plan);
    ud_free(flow.states);
    ud_free(flow.runs);
    ud_mutex_destroy(&flow.mutex);
    ud_scope_leave(e, &scope);
    return status;
}

ud_status ud_control_explain(ud_activation *a, const ud_json *definition, ud_bytes *out) {
    ud_scope scope = {0};
    ud_status status =
        ud_scope_enter(a, (ud_string){"flow", 4}, ud_member(definition, "variables"), &scope);
    const ud_json *tasks = ud_member(definition, "tasks");
    control_plan p = {0};
    if (!status)
        status = control_bind(a, tasks, ud_member(definition, "constraints"), &p);
    char policy[128];
    int n = snprintf(policy, sizeof(policy),
                     "{\"taskConcurrency\":%u,\"parallelScope\":%s,\"executables\":[",
                     p.concurrency, p.parallel ? "true" : "false");
    if (!status)
        status = ud_bytes_put(out, policy, (size_t)n);
    for (uint32_t i = 0; !status && i < p.count; i++) {
        const ud_json *task = ud_json_at(tasks, i);
        ud_string id = {0}, kind = {0};
        status = ud_json_string(ud_member(task, "id"), &id);
        if (!status)
            status = ud_json_string(ud_member(task, "kind"), &kind);
        if (!status && i)
            status = ud_bytes_put(out, ",", sizeof(",") - 1);
        if (!status)
            status = ud_bytes_put(out, "{\"id\":", sizeof("{\"id\":") - 1);
        if (!status)
            status = ud_json_quote(out, id.data, id.length);
        if (!status)
            status = ud_bytes_put(out, ",\"kind\":", sizeof(",\"kind\":") - 1);
        if (!status)
            status = ud_json_quote(out, kind.data, kind.length);
        if (!status)
            status = ud_bytes_put(out, "}", sizeof("}") - 1);
    }
    if (!status)
        status = ud_bytes_put(out, "],\"constraints\":[", sizeof("],\"constraints\":[") - 1);
    const char *on[] = {"success", "failure", "completion"},
               *evaluation[] = {"constraint", "expression", "and", "or"};
    for (uint32_t i = 0; !status && i < p.edge_count; i++) {
        control_edge *edge = p.edges + i;
        char text[256];
        n = snprintf(
            text, sizeof(text),
            "%s{\"from\":%u,\"to\":%u,\"on\":\"%s\",\"evaluation\":\"%s\",\"combine\":\"%s\"}",
            i ? "," : "", edge->from, edge->to, on[edge->on], evaluation[edge->evaluation],
            edge->combine ? "or" : "and");
        status = ud_bytes_put(out, text, (size_t)n);
    }
    if (!status)
        status = ud_bytes_put(out, "]}", sizeof("]}") - 1);
    control_clear(&p);
    ud_scope_leave(a, &scope);
    return status;
}
ud_status ud_control_catalog(ud_bytes *bytes) {
    /* Structural control tasks are executed by Control Flow itself. Metadata
     * belongs to the same owner; it never registers a second execution callback. */
    static const struct {
        const char *id, *name, *schema;
        int children;
    } kinds[] = {
        {"dataFlow", "Data Flow",
         "{\"type\":\"object\",\"required\":[\"pipeline\"],\"additionalProperties\":false,"
         "\"properties\":{\"pipeline\":{\"type\":\"string\"}}}",
         0},
        {"sequence", "Sequence",
         "{\"type\":\"object\",\"additionalProperties\":false,\"properties\":{}}", 1},
        {"for", "For Loop",
         "{\"type\":\"object\",\"additionalProperties\":false,\"required\":[\"initialize\","
         "\"condition\",\"increment\",\"maxIterations\"],\"properties\":{\"initialize\":{\"type\":"
         "\"object\"},\"condition\":{\"type\":\"object\"},\"increment\":{\"type\":\"object\"},"
         "\"maxIterations\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":1000000}}}",
         1}};
    for (uint32_t at = 0; at < sizeof(kinds) / sizeof(*kinds); at++) {
        UD_TRY(ud_bytes_put(bytes, ",{\"id\":", 7));
        UD_TRY(ud_json_quote(bytes, kinds[at].id, (uint32_t)strlen(kinds[at].id)));
        UD_TRY(ud_bytes_put(bytes, ",\"name\":", 8));
        UD_TRY(ud_json_quote(bytes, kinds[at].name, (uint32_t)strlen(kinds[at].name)));
        UD_TRY(ud_bytes_put(
            bytes,
            ",\"version\":\"" UD_SDK_VERSION
            "\",\"category\":1,\"inputs\":0,\"outputs\":0,\"dynamicPorts\":false,\"errorOutput\":-"
            "1,\"structuralControl\":true,\"children\":",
            sizeof(",\"version\":\"" UD_SDK_VERSION
                   "\",\"category\":1,\"inputs\":0,\"outputs\":0,\"dynamicPorts\":false,"
                   "\"errorOutput\":-1,\"structuralControl\":true,\"children\":") -
                1));
        UD_TRY(
            ud_bytes_put(bytes, kinds[at].children ? "true" : "false", kinds[at].children ? 4 : 5));
        UD_TRY(ud_bytes_put(bytes, ",\"configurationSchema\":", 23));
        UD_TRY(ud_bytes_put(bytes, kinds[at].schema, strlen(kinds[at].schema)));
        UD_TRY(ud_bytes_put(bytes, "}", 1));
    }
    return UD_OK;
}
