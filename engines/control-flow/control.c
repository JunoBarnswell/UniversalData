#include "../package/internal.h"
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
    uint32_t *indegree = ud_alloc(e->execution->ctx, (count + 1) * sizeof(*indegree));
    uint8_t *done = ud_alloc(e->execution->ctx, count + 1);
    if (!indegree || !done)
        s = UD_RESOURCE;
    for (uint32_t i = 0; !s && i < count; i++) {
        const ud_json *task = ud_json_at(tasks, i);
        static const char *keys[] = {"id",        "kind",      "config",       "children",
                                     "variables", "timeoutMs", "eventHandlers"};
        s = ud_json_keys(task, keys, 7);
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
        if (!s)
            s = ud_scope_enter(e, task_identity, ud_member(task, "variables"), &local);
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
                static const char *fk[] = {"variable", "items"};
                s = ud_json_keys(config, fk, 2);
                ud_string name = {0};
                ud_value v = {0};
                if (!s)
                    s = ud_json_string(ud_member(config, "variable"), &name);
                if (!s)
                    s = ud_activation_variable(e, name, &v);
                ud_variable *target;
                if (!s)
                    s = ud_variable_writable(e, name, &target);
                if (!s && ud_json_kind(ud_member(config, "items")) != UD_JSON_ARRAY)
                    s = UD_SCHEMA;
                ud_field field = {0};
                if (!s)
                    s = ud_activation_variable_field(e, name, &field);
                for (uint32_t j = 0; !s && j < ud_json_size(ud_member(config, "items")); j++) {
                    ud_value item = {0};
                    s = ud_json_value_decode(e->execution->ctx, &field,
                                             ud_json_at(ud_member(config, "items"), j),
                                             UD_ROUND_EXACT, &item);
                    ud_clear_value(&item);
                }
            } else if (ud_json_size(config))
                s = UD_SCHEMA;
            if (!s)
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
                !ud_named(name, "failure"))
                s = UD_UNSUPPORTED;
            if (!s)
                s = ud_control_validate(e, ud_json_at(handlers, j), depth + 1);
        }
        if (local.parent)
            ud_scope_leave(e, &local);
    }
    for (uint32_t j = 0; !s && j < ud_json_size(constraints); j++) {
        const ud_json *c = ud_json_at(constraints, j);
        static const char *keys[] = {"from", "to", "on", "expression", "combine"};
        s = ud_json_keys(c, keys, 5);
        ud_string a = {0}, b = {0}, on = {0}, combine = {"and", 3};
        if (!s)
            s = ud_json_string(ud_member(c, "from"), &a);
        if (!s)
            s = ud_json_string(ud_member(c, "to"), &b);
        if (!s)
            s = ud_json_string(ud_member(c, "on"), &on);
        if (!s && ud_member(c, "combine"))
            s = ud_json_string(ud_member(c, "combine"), &combine);
        int from = !s ? task_index(tasks, a) : -1, to = !s ? task_index(tasks, b) : -1;
        if (!s && (from < 0 || to < 0 || from == to ||
                   (!ud_named(on, "success") && !ud_named(on, "failure") &&
                    !ud_named(on, "completion")) ||
                   (!ud_named(combine, "and") && !ud_named(combine, "or"))))
            s = UD_SCHEMA;
        if (!s)
            indegree[to]++;
        if (!s && ud_member(c, "expression")) {
            int unused;
            s = condition(e, ud_member(c, "expression"), &unused);
        }
        for (uint32_t k = 0; !s && k < j; k++) {
            const ud_json *other = ud_json_at(constraints, k);
            ud_string target, mode = {"and", 3};
            s = ud_json_string(ud_member(other, "to"), &target);
            if (!s && ud_same(target, b)) {
                if (ud_member(other, "combine"))
                    s = ud_json_string(ud_member(other, "combine"), &mode);
                if (!s && !ud_same(mode, combine))
                    s = UD_SCHEMA;
            }
        }
    }
    uint32_t complete = 0;
    while (!s && complete < count) {
        int progress = 0;
        for (uint32_t i = 0; i < count; i++)
            if (!done[i] && !indegree[i]) {
                done[i] = 1;
                complete++;
                progress = 1;
                for (uint32_t j = 0; j < ud_json_size(constraints); j++) {
                    ud_string from = {0}, to = {0};
                    ud_json_string(ud_member(ud_json_at(constraints, j), "from"), &from);
                    ud_json_string(ud_member(ud_json_at(constraints, j), "to"), &to);
                    if (task_index(tasks, from) == (int)i)
                        indegree[task_index(tasks, to)]--;
                }
            }
        if (!progress)
            s = UD_SCHEMA;
    }
    ud_free(indegree);
    ud_free(done);
    ud_scope_leave(e, &scope);
    return s;
}
static ud_status execute_iteration(ud_activation *e, const ud_json *children, uint64_t number) {
    ud_string parent = e->task_id;
    size_t size = (size_t)parent.length + 64;
    char *identity = ud_alloc(e->execution->ctx, size);
    if (!identity)
        return UD_RESOURCE;
    int n = snprintf(identity, size, "%.*s@iteration:%llu;", (int)parent.length,
                     parent.data ? parent.data : "", (unsigned long long)number);
    e->task_id = (ud_string){identity, (uint32_t)n};
    ud_status result = ud_control_execute(e, children);
    e->task_id = parent;
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
    e->task_id = (ud_string){identity, (uint32_t)length};
    ud_status s = ud_control_execute(e, flow);
    if (s)
        ud_activation_notify(e, UD_EXEC_FAILED, s);
    e->task_id = parent;
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
            s = execute_iteration(e, ud_member(task, "children"), iteration - 1);
            if (!s)
                s = assignment(e, ud_member(config, "increment"), 1);
        }
    } else if (!s && ud_named(kind, "foreach")) {
        ud_string name = {0};
        ud_json_string(ud_member(config, "variable"), &name);
        ud_field field = {0};
        s = ud_activation_variable_field(e, name, &field);
        const ud_json *items = ud_member(config, "items");
        for (uint32_t i = 0; !s && i < ud_json_size(items); i++) {
            ud_value value = {0};
            s = ud_activation_check(e);
            if (!s)
                s = ud_json_value_decode(e->execution->ctx, &field, ud_json_at(items, i),
                                         UD_ROUND_EXACT, &value);
            if (!s)
                s = ud_activation_set_variable(e, name, &value, &field);
            ud_clear_value(&value);
            if (!s)
                s = execute_iteration(e, ud_member(task, "children"), i);
        }
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
            if (!s)
                s = d.invoke(d.user, &ctx, (ud_string){json, n});
            if (serial)
                ud_mutex_unlock(&e->execution->serial_tasks_lock);
        }
        if (s == UD_END || s < UD_OK || s > UD_TIMEOUT)
            s = UD_INVALID; /* Iteration sentinels are not Task outcomes. */
        ud_free(json);
        ud_json_destroy(bound);
    }
    if (!s)
        s = ud_control_handler(e, ud_member(handlers, "after"), "after");
    else if (s != UD_CANCELLED && s != UD_COMMIT_UNKNOWN) {
        ud_status secondary = ud_control_handler(e, ud_member(handlers, "failure"), "failure");
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
} task_run;
static ud_status task_result(task_run *run) {
    ud_activation *a = &run->activation;
    ud_string kind = {0};
    UD_TRY(ud_json_string(ud_member(run->definition, "kind"), &kind));
    int completed = 0;
    ud_status s = ud_checkpoint_task_begin(a, kind, &completed);
    if (!s && !completed) {
        ud_activation_notify(a, UD_EXEC_RUNNING, UD_OK);
        s = perform_task(a, run->definition);
        if (!s) {
            ud_task_fact(a, UD_TASK_EXECUTED);
            s = ud_checkpoint_task_done(a);
            if (!s && a->execution->journal)
                ud_task_fact(a, UD_TASK_DURABLE);
        }
    }
    if (completed)
        ud_task_fact(a, UD_TASK_RESUMED);
    if (s == UD_COMMIT_UNKNOWN)
        ud_task_fact(a, UD_TASK_UNCERTAIN);
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
static ud_status ready(ud_activation *e, const ud_json *tasks, const ud_json *constraints,
                       const int32_t *states, uint32_t index, int *runnable, int *pass) {
    *runnable = 1;
    *pass = 1;
    int has = 0;
    for (uint32_t j = 0; j < ud_json_size(constraints); j++) {
        const ud_json *c = ud_json_at(constraints, j);
        ud_string from, to, on, mode = {"and", 3};
        UD_TRY(ud_json_string(ud_member(c, "to"), &to));
        if (task_index(tasks, to) != (int)index)
            continue;
        UD_TRY(ud_json_string(ud_member(c, "from"), &from));
        int previous = states[task_index(tasks, from)];
        if (!ud_execution_terminal(previous)) {
            *runnable = 0;
            return UD_OK;
        }
        UD_TRY(ud_json_string(ud_member(c, "on"), &on));
        if (ud_member(c, "combine"))
            UD_TRY(ud_json_string(ud_member(c, "combine"), &mode));
        int matched = ud_named(on, "success") ? previous == UD_EXEC_SUCCEEDED
                      : ud_named(on, "failure")
                          ? previous == UD_EXEC_FAILED
                          : previous == UD_EXEC_SUCCEEDED || previous == UD_EXEC_FAILED;
        if (matched && ud_member(c, "expression"))
            UD_TRY(condition(e, ud_member(c, "expression"), &matched));
        *pass = !has ? matched : ud_named(mode, "or") ? (*pass || matched) : (*pass && matched);
        has = 1;
    }
    return UD_OK;
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
    a->parallel = parent->parallel;
    atomic_init(&a->cancelled, 0);
    atomic_init(&a->input_rows, 0);
    atomic_init(&a->output_rows, 0);
    atomic_init(&a->rejected_rows, 0);
    run->definition = definition;
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
typedef struct task_wave {
    task_run *runs;
    uint32_t *indices;
} task_wave;
static ud_status execute_task(void *user, uint32_t index) {
    task_wave *wave = user;
    task_run *run = wave->runs + wave->indices[index];
    run->result = task_result(run);
    return UD_OK; /* A task failure selects failure constraints; the group still joins. */
}
ud_status ud_control_execute(ud_activation *e, const ud_json *flow) {
    ud_scope scope = {0};
    ud_status status = ud_scope_enter(e, (ud_string){"flow", 4}, ud_member(flow, "variables"), &scope);
    const ud_json *tasks = ud_member(flow, "tasks"), *constraints = ud_member(flow, "constraints");
    uint32_t count = ud_json_size(tasks), complete = 0;
    int32_t *states = ud_alloc(e->execution->ctx, (size_t)(count + 1) * sizeof(*states));
    task_run *runs = ud_alloc(e->execution->ctx, (size_t)(count + 1) * sizeof(*runs));
    uint32_t *indices = ud_alloc(e->execution->ctx, (size_t)(count + 1) * sizeof(*indices));
    if (!states || !runs || !indices)
        status = UD_RESOURCE;
    for (uint32_t i = 0; !status && i < count; i++)
        states[i] = UD_EXEC_READY;
    uint32_t concurrency = ud_execution_concurrency(e);
    if (!concurrency && !status)
        status = UD_SCHEMA;
    ud_status failure = UD_OK;
    while (!status && complete < count) {
        uint32_t scheduled = 0;
        int progress = 0;
        for (uint32_t i = 0; !status && i < count; i++) {
            if (states[i] != UD_EXEC_READY)
                continue;
            status = ud_activation_check(e);
            if (status)
                break;
            int runnable = 0, pass = 0;
            status = ready(e, tasks, constraints, states, i, &runnable, &pass);
            if (status || !runnable)
                continue;
            const ud_json *task = ud_json_at(tasks, i);
            int safe = parallel_task(e, task);
            if (scheduled && !safe)
                continue;
            status = init_task(e, task, runs + i);
            if (status)
                break;
            if (!pass) {
                states[i] = UD_EXEC_SKIPPED;
                ud_activation_notify(&runs[i].activation, states[i], UD_OK);
                ud_free((void *)runs[i].activation.task_id.data);
                runs[i].activation.task_id = (ud_string){0};
                complete++;
                progress = 1;
                continue;
            }
            states[i] = UD_EXEC_RUNNING;
            indices[scheduled++] = i;
            progress = 1;
            if (!safe || concurrency == 1)
                break;
        }
        if (!status && scheduled) {
            for (uint32_t index = 0; index < scheduled; index++)
                runs[indices[index]].activation.parallel = e->parallel ||
                                                          (scheduled > 1 && concurrency > 1);
            task_wave wave = {runs, indices};
            status = ud_executor_run(e->execution->executor, scheduled, concurrency, execute_task, &wave);
        }
        for (uint32_t index = 0; index < scheduled; index++) {
            uint32_t i = indices[index];
            task_run *run = runs + i;
            ud_status result = status ? status : run->result;
            states[i] = result == UD_COMMIT_UNKNOWN ? UD_EXEC_RECOVERY_REQUIRED
                        : result == UD_CANCELLED ? UD_EXEC_CANCELLED
                        : result ? UD_EXEC_FAILED : UD_EXEC_SUCCEEDED;
            if (result == UD_COMMIT_UNKNOWN) {
                failure = result;
                ud_cancel(e->execution->ctx);
            } else if (result && !failure)
                failure = result;
            ud_free((void *)run->activation.task_id.data);
            run->activation.task_id = (ud_string){0};
            complete++;
        }
        if (!status && !progress)
            status = UD_CORRUPT;
    }
    ud_free(indices);
    ud_free(states);
    ud_free(runs);
    ud_scope_leave(e, &scope);
    return failure == UD_COMMIT_UNKNOWN ? failure : status ? status : failure;
}
