#include "../package/internal.h"
static int task_index(const ud_json *tasks, ud_string id) {
    for (uint32_t i = 0; i < ud_json_size(tasks); i++) {
        ud_string other;
        if (!ud_json_string(ud_member(ud_json_at(tasks, i), "id"), &other) && ud_same(id, other))
            return (int)i;
    }
    return -1;
}
static const ud_json *pipeline(ud_execution *e, ud_string id) {
    const ud_json *flows = ud_member(e->package->document, "dataFlows");
    for (uint32_t i = 0; i < ud_json_size(flows); i++) {
        const ud_json *p = ud_json_at(flows, i);
        ud_string other;
        if (!ud_json_string(ud_member(p, "id"), &other) && ud_same(id, other))
            return p;
    }
    return NULL;
}
static ud_status assignment(ud_execution *e, const ud_json *v, int perform) {
    static const char *keys[] = {"name", "value"};
    UD_TRY(ud_json_keys(v, keys, 2));
    ud_string name;
    UD_TRY(ud_json_string(ud_member(v, "name"), &name));
    ud_value old;
    UD_TRY(ud_execution_variable(e, name, &old));
    ud_variable *target;
    UD_TRY(ud_variable_writable(e, name, &target));
    ud_expression *expr = NULL;
    UD_TRY(ud_expression_json(e, ud_member(v, "value"), NULL, &expr));
    ud_status s = ud_expression_type(expr) == old.type ? UD_OK : UD_SCHEMA;
    if (!s && perform) {
        ud_value value;
        s = ud_expression_evaluate(expr, NULL, 0, &value);
        if (!s)
            s = ud_execution_set_variable(e, name, &value);
    }
    ud_expression_destroy(expr);
    return s;
}
static ud_status condition(ud_execution *e, const ud_json *v, int *out) {
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
ud_status ud_control_validate(ud_execution *e, const ud_json *flow, uint32_t depth) {
    if (depth >= e->options.max_depth)
        return UD_RESOURCE;
    static const char *flow_keys[] = {"tasks", "constraints", "variables"};
    UD_TRY(ud_json_keys(flow, flow_keys, 3));
    const ud_json *tasks = ud_member(flow, "tasks"), *constraints = ud_member(flow, "constraints");
    if (ud_json_kind(tasks) != UD_JSON_ARRAY || ud_json_size(tasks) > 65536 ||
        (constraints && ud_json_kind(constraints) != UD_JSON_ARRAY))
        return UD_SCHEMA;
    ud_scope scope = {0};
    ud_status s = ud_scope_enter(e, ud_member(flow, "variables"), &scope);
    uint32_t count = ud_json_size(tasks);
    uint32_t *indegree = ud_alloc(e->ctx, (count + 1) * sizeof(*indegree));
    uint8_t *done = ud_alloc(e->ctx, count + 1);
    if (!indegree || !done)
        s = UD_RESOURCE;
    for (uint32_t i = 0; !s && i < count; i++) {
        const ud_json *task = ud_json_at(tasks, i);
        static const char *keys[] = {"id", "kind", "config", "children", "variables"};
        s = ud_json_keys(task, keys, 5);
        ud_string id = {0}, kind = {0};
        if (!s)
            s = ud_json_string(ud_member(task, "id"), &id);
        if (!s)
            s = ud_json_string(ud_member(task, "kind"), &kind);
        if (!s && task_index(tasks, id) != (int)i)
            s = UD_SCHEMA;
        const ud_json *config = ud_member(task, "config");
        if (!s && ud_json_kind(config) != UD_JSON_OBJECT)
            s = UD_SCHEMA;
        ud_scope local = {0};
        if (!s)
            s = ud_scope_enter(e, ud_member(task, "variables"), &local);
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
                ud_string name;
                ud_value v;
                if (!s)
                    s = ud_json_string(ud_member(config, "variable"), &name);
                if (!s)
                    s = ud_execution_variable(e, name, &v);
                ud_variable *target;
                if (!s)
                    s = ud_variable_writable(e, name, &target);
                if (!s && ud_json_kind(ud_member(config, "items")) != UD_JSON_ARRAY)
                    s = UD_SCHEMA;
                for (uint32_t j = 0; !s && j < ud_json_size(ud_member(config, "items")); j++) {
                    ud_value item;
                    s = ud_scalar_value(ud_json_at(ud_member(config, "items"), j), v.type, &item);
                }
            } else if (ud_json_size(config))
                s = UD_SCHEMA;
            if (!s)
                s = ud_control_validate(e, ud_member(task, "children"), depth + 1);
        } else if (!s && ud_named(kind, "dataFlow")) {
            static const char *dk[] = {"pipeline"};
            s = ud_json_keys(config, dk, 1);
            ud_string name;
            if (!s)
                s = ud_json_string(ud_member(config, "pipeline"), &name);
            if (!s && !pipeline(e, name))
                s = UD_SCHEMA;
            if (!s)
                s = ud_pipeline_bind_validate(e, pipeline(e, name));
        } else if (!s && ud_named(kind, "set"))
            s = assignment(e, config, 0);
        else if (!s && ud_named(kind, "executePackage"))
            s = ud_control_validate(e, ud_member(task, "children"), depth + 1);
        else if (!s) {
            ud_component_descriptor d = {.struct_size = sizeof(d), .abi_version = UD_ABI_VERSION};
            s = ud_component_lookup(e, kind, &d);
            if (!s && d.category != UD_COMPONENT_TASK)
                s = UD_SCHEMA;
        }
        if (local.parent)
            ud_scope_leave(e, &local);
    }
    for (uint32_t j = 0; !s && j < ud_json_size(constraints); j++) {
        const ud_json *c = ud_json_at(constraints, j);
        static const char *keys[] = {"from", "to", "on", "expression", "combine"};
        s = ud_json_keys(c, keys, 5);
        ud_string a, b, on, combine = {"and", 3};
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
                    ud_string from, to;
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
static ud_status execute_iteration(ud_execution *e, const ud_json *children, uint64_t number) {
    ud_string parent = e->task_id;
    size_t size = (size_t)parent.length + 64;
    char *identity = ud_alloc(e->ctx, size);
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
static ud_status perform_task(ud_execution *e, const ud_json *task) {
    ud_string kind;
    UD_TRY(ud_json_string(ud_member(task, "kind"), &kind));
    const ud_json *config = ud_member(task, "config");
    ud_scope scope = {0};
    ud_status s = ud_scope_enter(e, ud_member(task, "variables"), &scope);
    int entered = 0;
    if (!s) {
        e->depth++;
        entered = 1;
        e->invocation++;
        if (e->depth > e->options.max_depth)
            s = UD_RESOURCE;
    }
    if (!s && (ud_named(kind, "sequence") || ud_named(kind, "executePackage")))
        s = ud_control_execute(e, ud_member(task, "children"));
    else if (!s && ud_named(kind, "for")) {
        s = assignment(e, ud_member(config, "initialize"), 1);
        uint64_t max = 0;
        ud_json_uint(ud_member(config, "maxIterations"), &max);
        uint64_t iteration = 0;
        while (!s) {
            int yes = 0;
            s = ud_execution_check(e);
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
        ud_string name;
        ud_json_string(ud_member(config, "variable"), &name);
        ud_value old;
        ud_execution_variable(e, name, &old);
        const ud_json *items = ud_member(config, "items");
        for (uint32_t i = 0; !s && i < ud_json_size(items); i++) {
            ud_value value;
            s = ud_execution_check(e);
            if (!s)
                s = ud_scalar_value(ud_json_at(items, i), old.type, &value);
            if (!s)
                s = ud_execution_set_variable(e, name, &value);
            if (!s)
                s = execute_iteration(e, ud_member(task, "children"), i);
        }
    } else if (!s && ud_named(kind, "dataFlow")) {
        ud_string name;
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
                                    .memory = e->ctx,
                                    .execution = e,
                                    .task_id = e->task_id,
                                    .max_batch_rows = e->options.max_batch_rows,
                                    .resolve = ud_resolve_value,
                                    .resolve_user = e};
        if (!s)
            s = d.invoke(d.user, &ctx, (ud_string){json, n});
        ud_free(json);
        ud_json_destroy(bound);
    }
    if (entered)
        e->depth--;
    ud_scope_leave(e, &scope);
    return s;
}
ud_status ud_control_execute(ud_execution *e, const ud_json *flow) {
    ud_scope scope = {0};
    ud_status s = ud_scope_enter(e, ud_member(flow, "variables"), &scope);
    const ud_json *tasks = ud_member(flow, "tasks"), *constraints = ud_member(flow, "constraints");
    uint32_t count = ud_json_size(tasks);
    int32_t *states = ud_alloc(e->ctx, (count + 1) * sizeof(*states));
    if (!states)
        s = UD_RESOURCE;
    for (uint32_t i = 0; !s && i < count; i++)
        states[i] = UD_EXEC_READY;
    uint32_t complete = 0;
    ud_status failure = UD_OK;
    ud_string parent = e->task_id;
    while (!s && complete < count) {
        int progress = 0;
        for (uint32_t i = 0; !s && i < count; i++) {
            if (states[i] != UD_EXEC_READY)
                continue;
            s = ud_execution_check(e);
            if (s)
                break;
            int ready = 1, pass = 1, has = 0, is_or = 0;
            for (uint32_t j = 0; !s && j < ud_json_size(constraints); j++) {
                const ud_json *c = ud_json_at(constraints, j);
                ud_string from, to, on, mode = {"and", 3};
                ud_json_string(ud_member(c, "to"), &to);
                if (task_index(tasks, to) != (int)i)
                    continue;
                ud_json_string(ud_member(c, "from"), &from);
                int previous = states[task_index(tasks, from)];
                if (!ud_execution_terminal(previous)) {
                    ready = 0;
                    break;
                }
                ud_json_string(ud_member(c, "on"), &on);
                if (ud_member(c, "combine"))
                    ud_json_string(ud_member(c, "combine"), &mode);
                is_or = ud_named(mode, "or");
                int matched = ud_named(on, "success") ? previous == UD_EXEC_SUCCEEDED
                              : ud_named(on, "failure")
                                  ? previous == UD_EXEC_FAILED
                                  : previous == UD_EXEC_SUCCEEDED || previous == UD_EXEC_FAILED;
                if (matched && ud_member(c, "expression"))
                    s = condition(e, ud_member(c, "expression"), &matched);
                if (!has)
                    pass = matched;
                else
                    pass = is_or ? (pass || matched) : (pass && matched);
                has = 1;
            }
            if (s || !ready)
                continue;
            const ud_json *task = ud_json_at(tasks, i);
            ud_string id;
            ud_json_string(ud_member(task, "id"), &id);
            size_t identity_size = (size_t)parent.length + id.length + 48;
            char *identity = ud_alloc(e->ctx, identity_size);
            if (!identity) {
                s = UD_RESOURCE;
                break;
            }
            int identity_length =
                snprintf(identity, identity_size, "%.*s/%u:%.*s;", (int)parent.length,
                         parent.data ? parent.data : "", id.length, (int)id.length, id.data);
            e->task_id = (ud_string){identity, (uint32_t)identity_length};
            if (!pass) {
                states[i] = UD_EXEC_SKIPPED;
                ud_execution_notify(e, states[i], UD_OK);
            } else {
                ud_string kind;
                ud_json_string(ud_member(task, "kind"), &kind);
                int completed = 0;
                ud_status result = ud_checkpoint_task_begin(e, kind, &completed);
                if (!result && !completed) {
                    states[i] = UD_EXEC_RUNNING;
                    ud_execution_notify(e, states[i], UD_OK);
                    result = perform_task(e, task);
                    if (!result) {
                        atomic_fetch_add(&e->completed_tasks, 1);
                        result = ud_checkpoint_task_done(e);
                    }
                }
                states[i] = result == UD_CANCELLED        ? UD_EXEC_CANCELLED
                            : result == UD_COMMIT_UNKNOWN ? UD_EXEC_RECOVERY_REQUIRED
                            : result                      ? UD_EXEC_FAILED
                                                          : UD_EXEC_SUCCEEDED;
                ud_execution_notify(e, states[i], result);
                if (result == UD_CANCELLED || result == UD_COMMIT_UNKNOWN)
                    s = result;
                else if (result && !failure)
                    failure = result;
            }
            e->task_id = parent;
            ud_free(identity);
            progress = 1;
            complete++;
        }
        if (!s && !progress)
            s = UD_CORRUPT;
    }
    e->task_id = parent;
    ud_free(states);
    ud_scope_leave(e, &scope);
    return s ? s : failure;
}
