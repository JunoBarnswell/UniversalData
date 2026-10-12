#include "internal.h"
#ifdef _WIN32
#include <process.h>
#endif
static ud_status copy_string(ud_context *ctx, ud_string source, ud_string *out) {
    if ((!source.data && source.length) ||
        (source.length &&
         (memchr(source.data, 0, source.length) || !ud_utf8_valid(source.data, source.length))))
        return UD_INVALID;
    out->data = ud_strdup_n(ctx, source.data ? source.data : "", source.length);
    out->length = source.length;
    return out->data ? UD_OK : UD_RESOURCE;
}
void ud_execution_notify_kind(ud_execution *e, int32_t kind, ud_execution_state state,
                              ud_status result) {
    if (!e->options.observe)
        return;
    ud_execution_event event = {sizeof(event),
                                UD_ABI_VERSION,
                                e->package->id,
                                e->options.execution_id,
                                e->task_id,
                                e->pipeline_id,
                                e->component_id,
                                state,
                                result,
                                ud_now_ms(),
                                kind};
    event.kind = kind;
    e->options.observe(e->options.observe_user, &event);
}
void ud_execution_notify(ud_execution *e, ud_execution_state state, ud_status result) {
    ud_execution_notify_kind(e, UD_EXEC_EVENT_STATE, state, result);
}
ud_status ud_execution_check(ud_execution *e) {
    if (ud_is_cancelled(e->ctx))
        return UD_CANCELLED;
    if (e->deadline && ud_now_ms() >= e->deadline) {
        ud_cancel(e->ctx);
        return UD_CANCELLED;
    }
    return UD_OK;
}
ud_status ud_component_lookup(ud_execution *e, ud_string id, ud_component_descriptor *out) {
    ud_status s = ud_registry_find(e->registry, id, out);
    if (s == UD_UNSUPPORTED && e->external)
        s = ud_registry_find(e->external, id, out);
    return s;
}
ud_status ud_execution_bind_roots(ud_execution *e) {
    ud_status s = UD_OK;
    if (!s) {
        const ud_json *project = ud_member(e->package->document, "project");
        if (project) {
            s = ud_scope_enter(e, (ud_string){"project", 7}, ud_member(project, "variables"), &e->project_scope);
            e->project_scope.parameters = ud_member(project, "parameters");
            if (!s) s = ud_scope_bind_parameters(e, &e->project_scope);
        }
    }
    if (!s)
        s = ud_scope_enter(e, (ud_string){"package", 7}, ud_member(e->package->document, "variables"), &e->globals);
    if (!s) {
        e->globals.parameters = ud_member(e->package->document, "parameters");
        e->globals.provided = e->parameters;
        s = ud_parameters_check(e->ctx, e->globals.parameters, e->parameters, 1);
        if (!s) s = ud_scope_bind_parameters(e, &e->globals);
    }
    return s;
}
ud_status ud_execution_validate_definition(ud_execution *e, uint32_t depth) {
    const ud_json *flows = ud_member(e->package->document, "dataFlows");
    for (uint32_t i = 0; i < ud_json_size(flows); i++)
        UD_TRY(ud_pipeline_validate(e, ud_json_at(flows, i)));
    UD_TRY(ud_control_validate(e, ud_member(e->package->document, "controlFlow"), depth));
    const ud_json *handlers = ud_member(e->package->document, "eventHandlers");
    for (uint32_t i = 0; i < ud_json_size(handlers); i++) {
        const char *key;
        uint32_t n;
        UD_TRY(ud_json_key(handlers, i, &key, &n));
        if (n != 7 || memcmp(key, "failure", 7))
            return UD_UNSUPPORTED;
        UD_TRY(ud_control_validate(e, ud_json_at(handlers, i), depth));
    }
    return UD_OK;
}
ud_status ud_execution_create_json(ud_string json, const ud_execution_options *options,
                                   ud_execution **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!options)
        return UD_INVALID;
    if (options->struct_size < sizeof(*options) || options->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!options->edge_capacity || options->edge_capacity > 65536 || !options->max_batch_rows ||
        options->max_batch_rows > 65536 || !options->max_depth || options->max_depth > 64 ||
        options->resume < 0 || options->resume > 1 ||
        (options->resume && !options->state_directory.length))
        return UD_INVALID;
    ud_context *ctx = NULL;
    UD_TRY(ud_context_create(&options->context, &ctx));
    ud_execution *e = ud_alloc(ctx, sizeof(*e));
    if (!e) {
        ud_context_destroy(ctx);
        return UD_RESOURCE;
    }
    e->ctx = ctx;
    e->options = *options;
    e->options.parameters = (ud_string){0};
    e->options.tenant = (ud_string){0};
    e->options.execution_id = (ud_string){0};
    e->options.state_directory = (ud_string){0};
    atomic_init(&e->state, UD_EXEC_CREATED);
    atomic_init(&e->result, UD_BUSY);
    atomic_init(&e->input_rows, 0);
    atomic_init(&e->output_rows, 0);
    atomic_init(&e->rejected_rows, 0);
    atomic_init(&e->batches, 0);
    atomic_init(&e->queued, 0);
    atomic_init(&e->high_water, 0);
    atomic_init(&e->completed_tasks, 0);
    atomic_init(&e->spill_bytes, 0);
    ud_status s = copy_string(ctx, options->tenant, &e->options.tenant);
    if (!s)
        s = copy_string(ctx, options->execution_id, &e->options.execution_id);
    if (!s)
        s = copy_string(ctx, options->state_directory, &e->options.state_directory);
    if (!s)
        s = ud_package_create(ctx, json, &e->package);
    if (!s) {
        atomic_store(&e->state, UD_EXEC_VALIDATING);
        ud_execution_notify(e, UD_EXEC_VALIDATING, UD_OK);
    }
    if (!s)
        s = ud_json_parse(ctx, options->parameters.data ? options->parameters.data : "{}",
                          options->parameters.data ? options->parameters.length : 2, &e->parameters,
                          NULL);
    if (!s) {
        char *text = NULL;
        uint32_t n;
        s = ud_json_serialize(e->parameters, &text, &n);
        if (!s)
            ud_hash(text, n, e->parameters_fingerprint);
        ud_free(text);
        const ud_json *deployment = ud_member(e->package->document, "deployment");
        if (!s && deployment) {
            text = NULL;
            s = ud_json_serialize(deployment, &text, &n);
            if (!s)
                e->deployment_metadata = (ud_string){text, n};
        }
    }
    if (!s)
        s = ud_execution_bind_roots(e);
    if (!s)
        s = ud_registry_create(ctx, &e->registry);
    if (!s)
        s = ud_builtin_register(e->registry);
    if (!s && options->registry) {
        s = ud_registry_acquire(options->registry);
        if (!s)
            e->external = options->registry;
        for (uint32_t i = 0; !s && i < ud_registry_count(e->external); i++) {
            ud_component_descriptor d = {.struct_size = sizeof(d), .abi_version = UD_ABI_VERSION};
            s = ud_registry_get(e->external, i, &d);
            ud_component_descriptor found = {.struct_size = sizeof(found),
                                             .abi_version = UD_ABI_VERSION};
            if (!s && ud_registry_find(e->registry, d.id, &found) == UD_OK)
                s = UD_SCHEMA;
        }
    }
    if (!s)
        s = ud_execution_validate_definition(e, 0);
    if (s) {
        atomic_store(&e->state, UD_EXEC_FAILED);
        atomic_store(&e->result, s);
        ud_status closed = ud_execution_destroy(e);
        return closed ? closed : s;
    }
    atomic_store(&e->state, UD_EXEC_READY);
    ud_execution_notify(e, UD_EXEC_READY, UD_OK);
    *out = e;
    return UD_OK;
}
ud_status ud_execution_create(const ud_package *p, const ud_execution_options *o,
                              ud_execution **out) {
    if (!p)
        return UD_INVALID;
    char *json = NULL;
    uint32_t n;
    UD_TRY(ud_package_serialize(p, &json, &n));
    ud_status s = ud_execution_create_json((ud_string){json, n}, o, out);
    ud_free(json);
    return s;
}
ud_status ud_execution_run_definition(ud_execution *e) {
    ud_status s = ud_execution_check(e);
    if (!s)
        s = ud_control_execute(e, ud_member(e->package->document, "controlFlow"));
    if (s && s != UD_CANCELLED && s != UD_COMMIT_UNKNOWN) {
        const ud_json *handler =
            ud_member(ud_member(e->package->document, "eventHandlers"), "failure");
        if (handler)
            (void)ud_control_execute(e, handler);
    }
    return s == UD_BUSY ? UD_TRANSIENT : s;
}
static ud_status execute(ud_execution *e) {
    uint64_t now = ud_now_ms();
    if (e->options.timeout_ms)
        e->deadline =
            e->options.timeout_ms > UINT64_MAX - now ? UINT64_MAX : now + e->options.timeout_ms;
    ud_execution_notify(e, UD_EXEC_RUNNING, UD_OK);
    ud_status s = ud_execution_check(e);
    if (!s)
        s = ud_checkpoint_open(e);
    if (!s)
        s = ud_execution_run_definition(e);
    if (s == UD_BUSY)
        s = UD_TRANSIENT; /* Terminal lock contention must not look like wait. */
    if (!s) {
        atomic_store(&e->state, UD_EXEC_DRAINING);
        ud_execution_notify(e, UD_EXEC_DRAINING, UD_OK);
    }
    ud_checkpoint_close(e);
    int32_t state = s == UD_COMMIT_UNKNOWN ? UD_EXEC_RECOVERY_REQUIRED
                    : s == UD_CANCELLED    ? UD_EXEC_CANCELLED
                    : s                    ? UD_EXEC_FAILED
                                           : UD_EXEC_SUCCEEDED;
    atomic_store(&e->result, s);
    atomic_store(&e->state, state);
    ud_execution_notify(e, state, s);
    return s;
}
ud_status ud_execution_execute(ud_execution *e) {
    if (!e)
        return UD_INVALID;
    int expected = UD_EXEC_READY;
    if (!atomic_compare_exchange_strong(&e->state, &expected, UD_EXEC_RUNNING))
        return UD_BUSY;
    return execute(e);
}
#ifdef _WIN32
static unsigned __stdcall execution_thread(void *p) {
    (void)execute(p);
    return 0;
}
#else
static void *execution_thread(void *p) {
    (void)execute(p);
    return NULL;
}
#endif
ud_status ud_execution_start(ud_execution *e) {
    if (!e)
        return UD_INVALID;
    int expected = UD_EXEC_READY;
    if (!atomic_compare_exchange_strong(&e->state, &expected, UD_EXEC_RUNNING))
        return UD_BUSY;
#ifdef _WIN32
    e->thread = (HANDLE)_beginthreadex(NULL, 0, execution_thread, e, 0, NULL);
    int failed = !e->thread;
#else
    int failed = pthread_create(&e->thread, NULL, execution_thread, e) != 0;
#endif
    if (failed) {
        atomic_store(&e->result, UD_RESOURCE);
        atomic_store(&e->state, UD_EXEC_FAILED);
        return UD_RESOURCE;
    }
    e->threaded = 1;
    return UD_OK;
}
ud_status ud_execution_wait(ud_execution *e, uint32_t timeout) {
    if (!e)
        return UD_INVALID;
    uint64_t start = ud_now_ms();
    while (!ud_execution_terminal(atomic_load(&e->state))) {
        if (atomic_load(&e->state) == UD_EXEC_READY)
            return UD_BUSY;
        if (ud_now_ms() - start >= timeout)
            return UD_BUSY;
        ud_sleep_ms(1);
    }
    return atomic_load(&e->result);
}
void ud_execution_cancel(ud_execution *e) {
    if (e)
        ud_cancel(e->ctx);
}
ud_status ud_execution_get_metrics(const ud_execution *e, ud_execution_metrics *m) {
    if (!e || !m)
        return UD_INVALID;
    if (m->struct_size < sizeof(*m) || m->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    m->state = atomic_load(&e->state);
    m->result = atomic_load(&e->result);
    m->input_rows = atomic_load(&e->input_rows);
    m->output_rows = atomic_load(&e->output_rows);
    m->rejected_rows = atomic_load(&e->rejected_rows);
    m->batches = atomic_load(&e->batches);
    m->queued_batches = atomic_load(&e->queued);
    m->queue_high_water = atomic_load(&e->high_water);
    m->memory_used = ud_memory_used(e->ctx);
    m->completed_tasks = atomic_load(&e->completed_tasks);
    m->spill_bytes = atomic_load(&e->spill_bytes);
    return UD_OK;
}
ud_status ud_execution_destroy(ud_execution *e) {
    if (!e)
        return UD_OK;
    int state = atomic_load(&e->state);
    if (state == UD_EXEC_RUNNING || state == UD_EXEC_DRAINING)
        return UD_BUSY;
    if (e->threaded) {
#ifdef _WIN32
        if (WaitForSingleObject(e->thread, INFINITE) != WAIT_OBJECT_0)
            return UD_IO;
        CloseHandle(e->thread);
#else
        if (pthread_join(e->thread, NULL))
            return UD_IO;
#endif
    }
    ud_context *ctx = e->ctx;
    ud_checkpoint_close(e);
    ud_scope_leave(e, &e->globals);
    ud_scope_leave(e, &e->project_scope);
    ud_registry_release(e->external);
    ud_registry_destroy(e->registry);
    ud_json_destroy(e->parameters);
    ud_free((void *)e->deployment_metadata.data);
    ud_package_destroy(e->package);
    ud_free((void *)e->options.tenant.data);
    ud_free((void *)e->options.execution_id.data);
    ud_free((void *)e->options.state_directory.data);
    ud_free(e);
    return ud_context_destroy(ctx);
}
