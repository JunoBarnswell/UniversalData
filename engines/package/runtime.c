#if defined(__linux__) && !defined(_GNU_SOURCE)
#define _GNU_SOURCE
#endif
#include "internal.h"
#include "ud/connection_manager.h"
#ifdef _WIN32
#include <process.h>
#elif defined(__linux__)
#include <sched.h>
#include <unistd.h>
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
void ud_execution_capture_error(ud_execution *execution, ud_status result) {
    if (!execution || result <= UD_END || result > UD_STATUS_LAST || result == UD_MORE || result == UD_BUSY)
        return;
    ud_error_detail detail = {.struct_size = sizeof(detail), .abi_version = UD_ABI_VERSION};
    if (ud_error_details(execution->ctx, &detail) ||
        (detail.source == UD_ERROR_NATIVE && !detail.native_code) ||
        (detail.cause != result && result != UD_COMMIT_UNKNOWN))
        return;
    ud_mutex_lock(&execution->diagnostics_lock);
    if (!execution->error_details[result].cause)
        execution->error_details[result] = detail;
    ud_mutex_unlock(&execution->diagnostics_lock);
}
ud_status ud_execution_error_details(ud_execution *execution, ud_error_detail *out) {
    if (!execution || !out)
        return UD_INVALID;
    if (out->struct_size != sizeof(*out) || out->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    ud_status result = atomic_load(&execution->result);
    if (result <= UD_END || result > UD_STATUS_LAST)
        return UD_END;
    ud_mutex_lock(&execution->diagnostics_lock);
    if (execution->error_details[result].cause)
        *out = execution->error_details[result];
    else
        *out = (ud_error_detail){.struct_size = sizeof(*out), .abi_version = UD_ABI_VERSION,
                                 .source = UD_ERROR_NATIVE, .cause = result};
    ud_mutex_unlock(&execution->diagnostics_lock);
    return UD_OK;
}
void ud_activation_notify_kind(ud_activation *e, int32_t kind, ud_execution_state state,
                               ud_status result) {
    ud_execution_capture_error(e->execution, result);
    if (!e->execution->options.observe)
        return;
    ud_mutex_lock(&e->execution->events_lock);
    if (e->execution->event_sequence == UINT64_MAX) {
        atomic_store(&e->execution->observer_fault, UD_RESOURCE);
        ud_mutex_unlock(&e->execution->events_lock);
        return;
    }
    ud_execution_event event = {sizeof(event),
                                UD_ABI_VERSION,
                                e->package->id,
                                e->execution->options.execution_id,
                                e->task_id,
                                e->pipeline_id,
                                e->component_id,
                                state,
                                result,
                                ud_wall_ms(),
                                kind,
                                ++e->execution->event_sequence,
                                e,
                                {0}};
    ud_bytes chain = {.ctx = e->execution->ctx, .limit = 65536};
    if (result != UD_OK && result != UD_BUSY && result != UD_MORE && result != UD_END) {
        ud_status encoded = ud_bytes_put(&chain, "[", 1);
        uint32_t depth = 0;
        for (const ud_activation *frame = e; !encoded && frame && depth < 32;
             frame = frame->parent, depth++) {
            if (depth)
                encoded = ud_bytes_put(&chain, ",", 1);
            if (!encoded)
                encoded = ud_bytes_put(&chain, "{\"taskId\":", 10);
            if (!encoded)
                encoded = ud_json_quote(&chain, frame->task_id.data ? frame->task_id.data : "",
                                        frame->task_id.length);
            if (!encoded)
                encoded = ud_bytes_put(&chain, ",\"pipelineId\":", 14);
            if (!encoded)
                encoded =
                    ud_json_quote(&chain, frame->pipeline_id.data ? frame->pipeline_id.data : "",
                                  frame->pipeline_id.length);
            if (!encoded)
                encoded = ud_bytes_put(&chain, ",\"componentId\":", 15);
            if (!encoded)
                encoded =
                    ud_json_quote(&chain, frame->component_id.data ? frame->component_id.data : "",
                                  frame->component_id.length);
            char code[32];
            int length = snprintf(code, sizeof(code), ",\"result\":%d}", result);
            if (!encoded)
                encoded = ud_bytes_put(&chain, code, (size_t)length);
        }
        if (!encoded)
            encoded = ud_bytes_put(&chain, "]", 1);
        if (!encoded)
            event.error_chain = (ud_string){(const char *)chain.data, (uint32_t)chain.size};
        else
            atomic_store(&e->execution->observer_fault, encoded);
    }
    event.kind = kind;
    e->execution->options.observe(e->execution->options.observe_user, &event);
    ud_bytes_clear(&chain);
    ud_mutex_unlock(&e->execution->events_lock);
}
void ud_activation_notify(ud_activation *e, ud_execution_state state, ud_status result) {
    ud_activation_notify_kind(e, UD_EXEC_EVENT_STATE, state, result);
}
void ud_task_rows(ud_activation *activation, uint64_t input, uint64_t output, uint64_t rejected) {
    ud_activation *owner = activation->scope_authority ? activation->scope_authority : activation;
    atomic_fetch_add(&owner->input_rows, input);
    atomic_fetch_add(&owner->output_rows, output);
    atomic_fetch_add(&owner->rejected_rows, rejected);
    atomic_fetch_add(&activation->execution->input_rows, input);
    atomic_fetch_add(&activation->execution->output_rows, output);
    atomic_fetch_add(&activation->execution->rejected_rows, rejected);
}
void ud_task_fact(ud_activation *activation, uint32_t fact) {
    ud_execution *execution = activation->execution;
    if (activation->task_facts & fact)
        return;
    activation->task_facts |= fact;
    int kind = 0;
    if (fact == UD_TASK_EXECUTED) {
        atomic_fetch_add(&execution->executed_tasks, 1);
        kind = UD_EXEC_EVENT_TASK_EXECUTED;
    } else if (fact == UD_TASK_DURABLE) {
        atomic_fetch_add(&execution->durable_tasks, 1);
        if (activation->task_facts & UD_TASK_UNCERTAIN) {
            activation->task_facts &= ~UD_TASK_UNCERTAIN;
            atomic_fetch_sub(&execution->recovery_tasks, 1);
        }
        kind = UD_EXEC_EVENT_TASK_CONFIRMED;
    } else if (fact == UD_TASK_UNCERTAIN) {
        atomic_fetch_add(&execution->recovery_tasks, 1);
        kind = UD_EXEC_EVENT_TASK_UNCERTAIN;
    } else if (fact == UD_TASK_RESUMED)
        atomic_fetch_add(&execution->resumed_tasks, 1);
    if (kind)
        ud_activation_notify_kind(activation, kind,
                                  fact == UD_TASK_UNCERTAIN ? UD_EXEC_RECOVERY_REQUIRED
                                                            : UD_EXEC_RUNNING,
                                  fact == UD_TASK_UNCERTAIN ? UD_COMMIT_UNKNOWN : UD_OK);
}
uint32_t ud_execution_concurrency(const ud_activation *activation) {
    if (ud_debug_enabled(activation->execution))
        return 1;
    uint32_t concurrency = activation->execution->options.max_concurrency;
    for (const ud_activation *a = activation; a; a = a->parent) {
        const ud_json *policy = ud_member(a->package->document, "policy");
        if (ud_member(policy, "maxConcurrency")) {
            uint64_t requested = 0;
            if (ud_json_uint(ud_member(policy, "maxConcurrency"), &requested))
                return 0;
            if (requested < concurrency)
                concurrency = (uint32_t)requested;
        }
    }
    return concurrency;
}
ud_status ud_activation_scope_identity(const ud_activation *activation, char output[65]) {
    if (!activation || !output)
        return UD_INVALID;
    ud_bytes bytes = {.ctx = activation->execution->ctx};
    static const char domain[] = "UD.Activation." UD_PROTOCOL_VERSION_TEXT;
    ud_status status = ud_bytes_put(&bytes, domain, sizeof(domain) - 1);
    if (!status)
        status = ud_bytes_put(&bytes, activation->package->fingerprint, 32);
    if (!status)
        status = ud_put_string(&bytes, activation->task_id.data, activation->task_id.length);
    if (!status) {
        uint8_t digest[32];
        static const char hex[] = "0123456789abcdef";
        ud_hash(bytes.data, bytes.size, digest);
        for (uint32_t at = 0; at < 32; at++) {
            output[at * 2] = hex[digest[at] >> 4];
            output[at * 2 + 1] = hex[digest[at] & 15];
        }
        output[64] = 0;
    }
    ud_bytes_clear(&bytes);
    return status;
}
int ud_activation_stopping(const ud_activation *e) {return e && atomic_load(&e->execution->stop_requested);}
ud_status ud_activation_check(const ud_activation *e) {
    if (!e)
        return UD_INVALID;
    ud_status observer = atomic_load(&e->execution->observer_fault);
    if (observer)
        return observer;
    if (ud_is_cancelled(e->execution->ctx))
        return UD_CANCELLED;
    uint64_t now = ud_now_ms();
    for (const ud_activation *a = e; a; a = a->parent)
        if (atomic_load(&a->cancelled))
            return UD_CANCELLED;
        else if (a->deadline && now >= a->deadline)
            return UD_TIMEOUT;
    return UD_OK;
}
ud_status ud_activation_report_fault(const ud_activation *activation, ud_status status,
                                     ud_string message) {
    if (!activation)
        return UD_INVALID;
    ud_status reported = ud_context_report(activation->execution->ctx, status, message);
    if (reported != status)
        return reported;
    int expected = UD_OK;
    if (atomic_compare_exchange_strong(&activation->execution->observer_fault, &expected, status))
        atomic_fetch_add(&activation->execution->error_count, 1);
    return status;
}
ud_status ud_component_lookup(ud_activation *e, ud_string id, ud_component_descriptor *out) {
    if (!out)
        return UD_INVALID;
    *out = (ud_component_descriptor){.struct_size = sizeof(*out), .abi_version = UD_ABI_VERSION};
    return ud_registry_find(e->execution->registry, id, out);
}
ud_status ud_activation_bind_roots(ud_activation *e) {
    ud_status s = UD_OK;
    if (!s) {
        const ud_json *project = ud_member(e->package->document, "project");
        if (project) {
            s = ud_scope_enter(e, (ud_string){"project", 7}, ud_member(project, "variables"),
                               &e->project_scope);
            e->project_scope.parameters = ud_member(project, "parameters");
            if (!s)
                s = ud_scope_bind_parameters(e, &e->project_scope);
        }
    }
    if (!s)
        s = ud_scope_enter(e, (ud_string){"package", 7},
                           ud_member(e->package->document, "variables"), &e->globals);
    if (!s) {
        e->globals.parameters = ud_member(e->package->document, "parameters");
        e->globals.provided = e->parameters;
        s = ud_parameters_check(e->execution->ctx, e->globals.parameters, e->parameters, 1);
        if (!s)
            s = ud_scope_bind_parameters(e, &e->globals);
    }
    return s;
}
ud_status ud_activation_validate_definition(ud_activation *e, uint32_t depth) {
    UD_TRY(ud_transaction_enter(
        e, ud_member(ud_member(e->package->document, "policy"), "transaction"), 1));
    const ud_json *flows = ud_member(e->package->document, "dataFlows");
    UD_TRY(ud_pipeline_validate_stages(e->package));
    for (uint32_t i = 0; i < ud_json_size(flows); i++)
        UD_TRY(ud_pipeline_validate(e, ud_json_at(flows, i)));
    UD_TRY(ud_control_validate(e, ud_member(e->package->document, "controlFlow"), depth));
    const ud_json *handlers = ud_member(e->package->document, "eventHandlers");
    for (uint32_t i = 0; i < ud_json_size(handlers); i++) {
        const char *key;
        uint32_t n;
        UD_TRY(ud_json_key(handlers, i, &key, &n));
        if (!((n == 7 && !memcmp(key, "failure", 7)) || (n == 6 && !memcmp(key, "before", 6)) ||
              (n == 5 && !memcmp(key, "after", 5)) || (n == 7 && !memcmp(key, "warning", 7)) ||
              (n == 5 && !memcmp(key, "error", 5)) || (n == 10 && !memcmp(key, "taskFailed", 10))))
            return UD_UNSUPPORTED;
        UD_TRY(ud_control_validate(e, ud_json_at(handlers, i), depth));
    }
    return UD_OK;
}
static ud_status create_execution(ud_connection_pool *pool, ud_string json,
    const ud_execution_options *options,
    ud_string partition_run, ud_string partition_topic,
    const ud_kafka_partition_assignment *assignments, uint32_t assignment_count, ud_execution **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!options)
        return UD_INVALID;
    if (options->struct_size != sizeof(*options) || options->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!options->edge_capacity || options->edge_capacity > 65536 || !options->max_batch_rows ||
        options->max_batch_rows > 65536 || !options->max_depth || options->max_depth > 64 ||
        options->resume < 0 || options->resume > 1 ||
        (options->resume && (!options->state_directory.length || !options->execution_id.length)) ||
        options->broker_restore < 0 || options->broker_restore > 1 ||
        (options->broker_restore && (options->resume || !options->execution_id.length)) ||
        options->execution_id.length > 255 ||
        (options->execution_id.length &&
         (!options->execution_id.data ||
          !ud_utf8_valid(options->execution_id.data, options->execution_id.length))) ||
        options->max_concurrency > 64 || options->worker_members > 64 ||
        (options->worker_members && options->worker_member >= options->worker_members) ||
        (!options->worker_members && options->worker_member) ||
        (options->worker_run_id.length && (!options->worker_run_id.data ||
         options->worker_run_id.length > 255 || options->worker_members < 2 ||
         memchr(options->worker_run_id.data, 0, options->worker_run_id.length) ||
         !ud_utf8_valid(options->worker_run_id.data, options->worker_run_id.length))) ||
        options->consumer_lanes > options->max_concurrency)
        return UD_INVALID;
    for (uint32_t i = 0; i < options->execution_id.length; i++)
        if ((unsigned char)options->execution_id.data[i] < 32 ||
            (unsigned char)options->execution_id.data[i] == 127)
            return UD_INVALID;
    ud_context *ctx = NULL;
    UD_TRY(ud_context_create(&options->context, &ctx));
    ud_execution *e = ud_alloc(ctx, sizeof(*e));
    if (!e) {
        ud_context_destroy(ctx);
        return UD_RESOURCE;
    }
    e->ctx = ctx;
    atomic_init(&e->observer_fault, UD_OK);
    e->sql_sessions = 0;
    e->root.execution = e;
    e->root.definition = &e->root;
    atomic_init(&e->root.cancelled, 0);
    atomic_init(&e->stop_requested, 0);
    atomic_init(&e->root.input_rows, 0);
    atomic_init(&e->root.output_rows, 0);
    atomic_init(&e->root.rejected_rows, 0);
    atomic_init(&e->root.committed_rows, 0);
    atomic_init(&e->root.acknowledged_rows, 0);
    atomic_init(&e->root.error_rows, 0);
    atomic_init(&e->root.committed_error_rows, 0);
    atomic_init(&e->root.acknowledged_error_rows, 0);
    ud_mutex_init(&e->events_lock);
    ud_mutex_init(&e->journal_lock);
    ud_mutex_init(&e->serial_tasks_lock);
    ud_condition_init(&e->finished);
    atomic_init(&e->error_count, 0);
    atomic_init(&e->error_limit_reason, UD_OK);
    e->pipeline_reports.ctx = ctx;
    e->bound_plans.ctx = ctx;
    e->control_reports.ctx = ctx;
    ud_mutex_init(&e->diagnostics_lock);
    ud_mutex_init(&e->asset_lock);
    ud_mutex_init(&e->cancellation_lock);
    ud_condition_init(&e->cancellation_changed);
    e->options = *options;
    e->event_sequence = options->event_sequence_start;
    if (!e->options.max_concurrency)
        e->options.max_concurrency = 1;
    e->options.parameters = (ud_string){0};
    e->options.tenant = (ud_string){0};
    e->options.execution_id = (ud_string){0};
    e->options.state_directory = (ud_string){0};
    e->options.worker_run_id = (ud_string){0};
    atomic_init(&e->state, UD_EXEC_CREATED);
    atomic_init(&e->result, UD_BUSY);
    atomic_init(&e->input_rows, 0);
    atomic_init(&e->output_rows, 0);
    atomic_init(&e->rejected_rows, 0);
    atomic_init(&e->batches, 0);
    atomic_init(&e->queued, 0);
    atomic_init(&e->high_water, 0);
    atomic_init(&e->executed_tasks, 0);
    atomic_init(&e->durable_tasks, 0);
    atomic_init(&e->recovery_tasks, 0);
    atomic_init(&e->resumed_tasks, 0);
    atomic_init(&e->pipeline_trees, 0);
    atomic_init(&e->tree_active, 0);
    atomic_init(&e->tree_high_water, 0);
    atomic_init(&e->spill_bytes, 0);
    atomic_init(&e->committed_rows, 0);
    atomic_init(&e->acknowledged_rows, 0);
    atomic_init(&e->error_rows, 0);
    atomic_init(&e->committed_error_rows, 0);
    atomic_init(&e->acknowledged_error_rows, 0);
    ud_status s = ud_executor_create(ctx, e->options.max_concurrency, &e->executor);
    if (!s)
        s = copy_string(ctx, options->tenant, &e->options.tenant);
    if (!s && options->execution_id.length)
        s = copy_string(ctx, options->execution_id, &e->options.execution_id);
    else if (!s) {
        uint8_t random[16];
        s = ud_random_bytes(ctx, random, sizeof(random));
        if (!s) {
            random[6] = (uint8_t)((random[6] & 15) | 64);
            random[8] = (uint8_t)((random[8] & 63) | 128);
            static const char digits[] = "0123456789abcdef";
            char id[37];
            uint32_t at = 0;
            for (uint32_t i = 0; i < 16; i++) {
                if (i == 4 || i == 6 || i == 8 || i == 10)
                    id[at++] = '-';
                id[at++] = digits[random[i] >> 4];
                id[at++] = digits[random[i] & 15];
            }
            id[at] = 0;
            s = copy_string(ctx, (ud_string){id, at}, &e->options.execution_id);
        }
    }
    if (!s)
        s = copy_string(ctx, options->state_directory, &e->options.state_directory);
    if (!s && options->worker_run_id.length)
        s = copy_string(ctx, options->worker_run_id, &e->options.worker_run_id);
    if (!s && assignment_count) {
        e->assignments = ud_alloc(ctx, assignment_count * sizeof(*assignments));
        if (!e->assignments) s = UD_RESOURCE;
        else {
            memcpy(e->assignments, assignments, assignment_count * sizeof(*assignments));
            e->assignment_count = assignment_count;
            s = copy_string(ctx, partition_run, &e->partition_run);
            if (!s) s = copy_string(ctx, partition_topic, &e->partition_topic);
        }
    }
    if (!s)
        s = ud_package_create(ctx, json, &e->package);
    e->root.package = e->package;
    if (!s) {
        atomic_store(&e->state, UD_EXEC_VALIDATING);
        ud_activation_notify(&e->root, UD_EXEC_VALIDATING, UD_OK);
    }
    if (!s)
        s = ud_json_parse(ctx, options->parameters.data ? options->parameters.data : "{}",
                          options->parameters.data ? options->parameters.length : 2, &e->parameters,
                          NULL);
    e->root.parameters = e->parameters;
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
        s = ud_activation_bind_roots(&e->root);
    if (!s)
        s = ud_registry_create(ctx, &e->registry);
    if (!s)
        s = ud_expression_register_functions(e->registry);
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
            if (!s)
                s = ud_registry_register(e->registry, &d);
        }
    }
    if (!s)
        s = ud_connections_create(e, pool);
    if (!s)
        s = ud_assets_create(e);
    if (!s)
        s = ud_activation_validate_definition(&e->root, 0);
    if (s) {
        atomic_store(&e->state, UD_EXEC_FAILED);
        atomic_store(&e->result, s);
        if (e->package)
            ud_activation_notify(&e->root, UD_EXEC_FAILED, s);
        ud_status closed = ud_execution_destroy(e);
        return closed ? closed : s;
    }
    atomic_store(&e->state, UD_EXEC_READY);
    ud_activation_notify(&e->root, UD_EXEC_READY, UD_OK);
    *out = e;
    return UD_OK;
}
ud_status ud_execution_create_json(ud_string json, const ud_execution_options *options, ud_execution **out) {
    return create_execution(NULL, json, options, (ud_string){0}, (ud_string){0}, NULL, 0, out);
}

ud_status ud_execution_create_json_in_pool(ud_connection_pool *pool, ud_string json,
                                           const ud_execution_options *options, ud_execution **out) {
    if (!pool)
        return UD_INVALID;
    return create_execution(pool, json, options, (ud_string){0}, (ud_string){0}, NULL, 0, out);
}

static ud_status create_partitioned(ud_connection_pool *pool, ud_string json,
    const ud_execution_options *options,
    ud_string run, ud_string topic, const ud_kafka_partition_assignment *parts, uint32_t count, ud_execution **out) {
    if (!out) return UD_INVALID;
    *out = NULL;
    if (!options || !parts || !count || count > 64 || count > options->max_concurrency ||
        options->worker_members || options->broker_restore || options->resume ||
        !run.data || !run.length || run.length > 255 || memchr(run.data, 0, run.length) ||
        !topic.data || !topic.length || topic.length > 249 || memchr(topic.data, 0, topic.length))
        return UD_INVALID;
    for (uint32_t i = 0; i < count; i++)
        if (parts[i].partition < 0 || parts[i].partition >= 65536 || parts[i].start_offset < 0 ||
            (i && parts[i - 1].partition >= parts[i].partition) ||
            parts[i].require_checkpoint < 0 || parts[i].require_checkpoint > 1)
            return UD_INVALID;
    return create_execution(pool, json, options, run, topic, parts, count, out);
}

ud_status ud_execution_create_partitioned(ud_string json, const ud_execution_options *options,
    ud_string run, ud_string topic, const ud_kafka_partition_assignment *parts, uint32_t count, ud_execution **out) {
    return create_partitioned(NULL, json, options, run, topic, parts, count, out);
}

ud_status ud_execution_create_partitioned_in_pool(ud_connection_pool *pool, ud_string json,
    const ud_execution_options *options, ud_string run, ud_string topic,
    const ud_kafka_partition_assignment *parts, uint32_t count, ud_execution **out) {
    if (!pool)
        return UD_INVALID;
    return create_partitioned(pool, json, options, run, topic, parts, count, out);
}

ud_status ud_execution_message_partition_positions(ud_execution *e, ud_string connection,
    ud_string topic, ud_string group, ud_message_partition_position *positions, uint32_t capacity, uint32_t *count) {
    if (!e || atomic_load(&e->state) != UD_EXEC_READY) return UD_BUSY;
    ud_connection_lease *lease = NULL;
    ud_status status = ud_activation_acquire_connection(&e->root, connection,
        UD_CONNECT_MESSAGE_SUBSCRIBE | UD_CONNECT_MESSAGE_TRANSACTION, &lease);
    if (!status) status = ud_connection_lease_message_partition_positions(lease,topic,group,positions,capacity,count);
    ud_status closed = ud_connection_lease_release(lease);
    return status ? status : closed;
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
ud_status ud_execution_id(const ud_execution *e, ud_string *out) {
    if (!e || !out)
        return UD_INVALID;
    *out = e->options.execution_id;
    return UD_OK;
}
ud_status ud_activation_run_definition(ud_activation *e) {
    int completed = 0;
    UD_TRY(ud_checkpoint_package_resume(e, &completed));
    if (completed)
        return UD_OK;
    const ud_json *handlers = ud_member(e->package->document, "eventHandlers");
    e->event_handlers = handlers;
    ud_status s = ud_activation_check(e);
    if (!s)
        s = ud_transaction_enter(
            e, ud_member(ud_member(e->package->document, "policy"), "transaction"), 0);
    if (!s && ud_transaction_recovered(e)) {
        s = ud_checkpoint_package_resume(e, &completed);
        if (!s && !completed)
            s = UD_CORRUPT;
        return ud_transaction_leave(e, s);
    }
    if (!s)
        s = ud_control_handler(e, ud_member(handlers, "before"), "before");
    if (!s)
        s = ud_control_execute(e, ud_member(e->package->document, "controlFlow"));
    if (!s)
        s = ud_control_handler(e, ud_member(handlers, "after"), "after");
    else if (s != UD_CANCELLED && s != UD_COMMIT_UNKNOWN) {
        ud_status secondary = ud_control_handler(e, ud_member(handlers, "failure"), "failure");
        if (secondary == UD_COMMIT_UNKNOWN)
            s = secondary;
    }
    if (!s && ud_transaction_owned(e))
        s = ud_checkpoint_package_done(e);
    s = ud_transaction_leave(e, s);
    return s == UD_BUSY ? UD_TRANSIENT : s;
}
static ud_status execute(ud_execution *e) {
    ud_context_clear_error(e->ctx);
    ud_status pool = ud_executor_start(e->executor);
    uint64_t now = ud_now_ms();
    if (e->options.timeout_ms)
        e->root.deadline =
            e->options.timeout_ms > UINT64_MAX - now ? UINT64_MAX : now + e->options.timeout_ms;
    ud_activation_notify(&e->root, UD_EXEC_RUNNING, UD_OK);
    ud_status s = pool ? pool : ud_activation_check(&e->root);
    if (!s)
        s = ud_checkpoint_open(e);
    if (!s)
        s = ud_execution_prepare_buffers(e);
    if (!s) {
        if (e->transaction_recovery_only)
            s = ud_control_execute(&e->root, ud_member(e->package->document, "controlFlow"));
        else if (e->preview_schema_only)
            s = ud_pipeline_bind_validate(&e->root, e->preview_pipeline);
        else
            s = e->preview_pipeline ? ud_pipeline_execute(&e->root, e->preview_pipeline)
                                    : ud_activation_run_definition(&e->root);
    }
    if (s == UD_BUSY)
        s = UD_TRANSIENT; /* Terminal lock contention must not look like wait. */
    if (!s) {
        atomic_store(&e->state, UD_EXEC_DRAINING);
        ud_activation_notify(&e->root, UD_EXEC_DRAINING, UD_OK);
    }
    ud_checkpoint_close(e);
    ud_status assets = ud_assets_release(e);
    if (!s && assets)
        s = assets;
    ud_status released = ud_execution_release_buffers(e);
    if (!s && released)
        s = released;
    int32_t state = s == UD_COMMIT_UNKNOWN ? UD_EXEC_RECOVERY_REQUIRED
                    : s == UD_CANCELLED    ? UD_EXEC_CANCELLED
                    : s                    ? UD_EXEC_FAILED
                    : atomic_load(&e->stop_requested) ? UD_EXEC_STOPPED : UD_EXEC_SUCCEEDED;

    ud_execution_capture_error(e, s);
    atomic_store(&e->result, s);
    atomic_store(&e->state, state);
    ud_execution_wake_results(e);
    ud_mutex_lock(&e->events_lock);
    ud_condition_wake(&e->finished);
    ud_mutex_unlock(&e->events_lock);
    ud_activation_notify(&e->root, state, s);
    return s;
}
static int enter_execution(ud_execution *e, int recover_transaction) {
    int expected = UD_EXEC_READY;
    ud_mutex_lock(&e->events_lock);
    int started = !e->sql_sessions &&
                  atomic_compare_exchange_strong(&e->state, &expected, UD_EXEC_RUNNING);
    if (started) {
        e->transaction_recovery_only = recover_transaction;
        e->transaction_recovery_decision = -1;
    }
    ud_mutex_unlock(&e->events_lock);
    return started;
}
ud_status ud_execution_execute(ud_execution *e) {
    if (!e)
        return UD_INVALID;
    if (!enter_execution(e, 0))
        return UD_BUSY;
    return execute(e);
}
ud_status ud_execution_recover_transaction(ud_execution *e, int32_t *decision) {
    if (!e || !decision || !e->options.resume || !e->options.state_directory.length)
        return UD_INVALID;
    *decision = 0;
    const ud_json *package = e->package->document;
    const ud_json *flow = ud_member(package, "controlFlow");
    const ud_json *tasks = ud_member(flow, "tasks");
    const ud_json *task = ud_json_at(tasks, 0), *policy = ud_member(task, "transaction");
    ud_string mode = {0}, scope = {"task", 4};
    if (e->preview_pipeline || ud_json_size(tasks) != 1 ||
        ud_json_size(ud_member(flow, "constraints")) || ud_member(task, "children") ||
        ud_member(ud_member(package, "policy"), "transaction") ||
        ud_json_string(ud_member(policy, "mode"), &mode) || !ud_named(mode, "required") ||
        (ud_member(policy, "scope") && ud_json_string(ud_member(policy, "scope"), &scope)) ||
        !ud_named(scope, "task"))
        return UD_UNSUPPORTED;
    if (!enter_execution(e, 1))
        return UD_BUSY;
    ud_status status = execute(e);
    if (!status && e->transaction_recovery_decision < 0)
        return UD_FENCED;
    if (!status)
        *decision = e->transaction_recovery_decision;
    return status;
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
static ud_status start_execution(ud_execution *e, int cpu) {
    if (!e)
        return UD_INVALID;
#ifdef __linux__
    pthread_attr_t attributes;
    pthread_attr_t *launch = NULL;
    if (cpu >= 0) {
        long configured_cpus = sysconf(_SC_NPROCESSORS_CONF);
        size_t count = configured_cpus > CPU_SETSIZE ? (size_t)configured_cpus : CPU_SETSIZE;
        if ((size_t)cpu >= count)
            return UD_INVALID;
        size_t bytes = CPU_ALLOC_SIZE(count);
        cpu_set_t *mask = CPU_ALLOC(count);
        if (!mask)
            return UD_RESOURCE;
        if (pthread_getaffinity_np(pthread_self(), bytes, mask) || !CPU_ISSET_S(cpu, bytes, mask)) {
            CPU_FREE(mask);
            return UD_AUTH;
        }
        CPU_ZERO_S(bytes, mask);
        CPU_SET_S(cpu, bytes, mask);
        int configured = pthread_attr_init(&attributes);
        if (!configured) {
            launch = &attributes;
            configured = pthread_attr_setaffinity_np(launch, bytes, mask);
        }
        CPU_FREE(mask);
        if (configured) {
            if (launch)
                pthread_attr_destroy(launch);
            return UD_RESOURCE;
        }
    }
#else
    if (cpu >= 0)
        return UD_UNSUPPORTED;
#endif
    if (!enter_execution(e, 0)) {
#ifdef __linux__
        if (launch)
            pthread_attr_destroy(launch);
#endif
        return UD_BUSY;
    }
#ifdef _WIN32
    e->thread = (HANDLE)_beginthreadex(NULL, 0, execution_thread, e, 0, NULL);
    int failed = !e->thread;
#elif defined(__linux__)
    int failed = pthread_create(&e->thread, launch, execution_thread, e) != 0;
    if (launch)
        pthread_attr_destroy(launch);
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
ud_status ud_execution_start(ud_execution *e) {
    return start_execution(e, -1);
}
ud_status ud_execution_start_on_cpu(ud_execution *e, uint32_t cpu) {
    return cpu > INT32_MAX ? UD_INVALID : start_execution(e, (int)cpu);
}
ud_status ud_execution_wait(ud_execution *e, uint32_t timeout) {
    if (!e)
        return UD_INVALID;
    uint64_t start = ud_now_ms();
    ud_mutex_lock(&e->events_lock);
    while (!ud_execution_terminal(atomic_load(&e->state))) {
        uint64_t elapsed = ud_now_ms() - start;
        if (atomic_load(&e->state) == UD_EXEC_READY || elapsed >= timeout) {
            ud_mutex_unlock(&e->events_lock);
            return UD_BUSY;
        }
        ud_condition_wait_ms(&e->finished, &e->events_lock, timeout - (uint32_t)elapsed);
    }
    ud_mutex_unlock(&e->events_lock);
    return atomic_load(&e->result);
}
void ud_execution_stop(ud_execution *e) {
    if (!e) return;
    atomic_store(&e->stop_requested, 1);
    ud_executor_cancel(e->executor); /* Wake parked work; does not cancel Context. */
}
void ud_execution_cancel(ud_execution *e) {
    if (e) {
        ud_cancel(e->ctx);
        ud_executor_cancel(e->executor);
        ud_connections_cancel(e);
        ud_mutex_lock(&e->cancellation_lock);
        for (;;) {
            ud_activation *a = e->cancellable;
            while (a && a->cancel_sent)
                a = a->next_cancel;
            if (!a)
                break;
            a->cancel_sent = 1;
            a->cancel_pins++;
            ud_mutex_unlock(&e->cancellation_lock);
            a->cancel_callback(a->cancel_instance);
            ud_mutex_lock(&e->cancellation_lock);
            a->cancel_pins--;
            ud_condition_wake(&e->cancellation_changed);
        }
        ud_mutex_unlock(&e->cancellation_lock);
    }
}
ud_status ud_execution_get_metrics(const ud_execution *e, ud_execution_metrics *m) {
    if (!e || !m)
        return UD_INVALID;
    if (m->struct_size != sizeof(*m) || m->abi_version != UD_ABI_VERSION)
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
    m->executed_tasks = atomic_load(&e->executed_tasks);
    m->durable_tasks = atomic_load(&e->durable_tasks);
    m->recovery_tasks = atomic_load(&e->recovery_tasks);
    m->resumed_tasks = atomic_load(&e->resumed_tasks);
    m->pipeline_trees = atomic_load(&e->pipeline_trees);
    m->tree_high_water = atomic_load(&e->tree_high_water);
    m->worker_high_water = ud_executor_high_water(e->executor);
    m->spill_bytes = atomic_load(&e->spill_bytes);
    m->error_rows = atomic_load(&e->error_rows);
    m->committed_error_rows = atomic_load(&e->committed_error_rows);
    m->acknowledged_error_rows = atomic_load(&e->acknowledged_error_rows);
    m->committed_rows = atomic_load(&e->committed_rows);
    m->acknowledged_rows = atomic_load(&e->acknowledged_rows);
    m->error_count = atomic_load(&e->error_count);
    return UD_OK;
}
ud_status ud_execution_destroy(ud_execution *e) {
    if (!e)
        return UD_OK;
    ud_mutex_lock(&e->events_lock);
    int state = atomic_load(&e->state);
    int busy = e->sql_sessions || state == UD_EXEC_RUNNING || state == UD_EXEC_DRAINING;
    ud_mutex_unlock(&e->events_lock);
    if (busy)
        return UD_BUSY;
    ud_context *ctx = e->ctx;
    if (e->threaded) {
#ifdef _WIN32
        if (WaitForSingleObject(e->thread, INFINITE) != WAIT_OBJECT_0)
            return UD_IO;
        if (!CloseHandle(e->thread))
            return UD_IO;
#else
        if (pthread_join(e->thread, NULL))
            return UD_IO;
#endif
        e->threaded = 0;
    }
    if (!e->disposed) {
        ud_execution_destroy_results(e);
        ud_checkpoint_close(e);
        ud_assets_destroy(e);
        ud_source_versions_destroy(e);
        ud_rule_facts_destroy(&e->rule_facts);
        ud_rule_facts_destroy(&e->root.rule_facts);
        ud_status cleanup = ud_execution_release_buffers(e);
        if (cleanup)
            return ud_fail(ctx, UD_RESOURCE, "execution retains buffer resources: %s",
                           ud_status_name(cleanup));
        cleanup = ud_connections_destroy(e);
        if (cleanup)
            return cleanup;
        ud_execution_destroy_connectors(e);
        ud_scope_leave(&e->root, &e->root.globals);
        ud_scope_leave(&e->root, &e->root.project_scope);
        cleanup = ud_registry_destroy(e->registry);
        if (cleanup)
            return ud_fail(ctx, UD_RESOURCE, "execution retains registry leases");
        e->registry = NULL;
        ud_registry_release(e->external);
        e->external = NULL;
        ud_json_destroy(e->parameters);
        e->parameters = NULL;
        ud_free((void *)e->deployment_metadata.data);
        e->deployment_metadata = (ud_string){0};
        ud_package_destroy(e->package);
        e->package = NULL;
        e->root.package = NULL;
        cleanup = ud_executor_destroy(e->executor);
        if (cleanup)
            return cleanup;
        e->executor = NULL;
        ud_free((void *)e->options.tenant.data);
        e->options.tenant = (ud_string){0};
        ud_free((void *)e->options.execution_id.data);
        e->options.execution_id = (ud_string){0};
        ud_free((void *)e->options.state_directory.data);
        e->options.state_directory = (ud_string){0};
        ud_free(e->assignments);
        ud_free((void *)e->partition_run.data);
        ud_free((void *)e->partition_topic.data);
        ud_free((void *)e->options.worker_run_id.data);
        e->options.worker_run_id = (ud_string){0};
        ud_bytes_clear(&e->pipeline_reports);
        ud_bytes_clear(&e->schema_reports);
        ud_bytes_clear(&e->bound_plans);
        ud_bytes_clear(&e->control_reports);
        ud_debug_destroy(e);
        ud_json_destroy(e->preview_pipeline);
        e->preview_pipeline = NULL;
        e->disposed = 1;
    }
    if (ud_memory_used(ctx) != ud_allocation_bytes(e)) {
        atomic_store(&e->state, UD_EXEC_FAILED);
        atomic_store(&e->result, UD_RESOURCE);
        return ud_fail(ctx, UD_RESOURCE, "execution contains retained native allocations");
    }
    ud_mutex_destroy(&e->events_lock);
    ud_mutex_destroy(&e->journal_lock);
    ud_mutex_destroy(&e->serial_tasks_lock);
    ud_condition_destroy(&e->finished);
    ud_mutex_destroy(&e->asset_lock);
    ud_mutex_destroy(&e->diagnostics_lock);
    ud_mutex_destroy(&e->cancellation_lock);
    ud_condition_destroy(&e->cancellation_changed);
    ud_free(e);
    return ud_context_destroy(ctx);
}

static ud_status owned_json(ud_bytes *bytes, char **out, uint32_t *length) {
    if (bytes->size > UINT32_MAX)
        return UD_RESOURCE;
    *out = ud_strdup_n(bytes->ctx, (const char *)bytes->data, bytes->size);
    if (!*out)
        return UD_RESOURCE;
    *length = (uint32_t)bytes->size;
    return UD_OK;
}
ud_status ud_execution_explain(ud_execution *e, char **out, uint32_t *length) {
    if (!e || !out || !length)
        return UD_INVALID;
    *out = NULL;
    *length = 0;
    int state = atomic_load(&e->state);
    if (state == UD_EXEC_RUNNING || state == UD_EXEC_DRAINING)
        return UD_BUSY;
    ud_bytes bytes = {.ctx = e->ctx};
    ud_status status = ud_bytes_put(&bytes, "{\"version\":", sizeof("{\"version\":") - 1);
    char version[128];
    int n =
        snprintf(version, sizeof(version), "%u,\"workerLimit\":%u,\"control\":", UD_PLAN_VERSION,
                 e->options.max_concurrency);
    if (!status)
        status = ud_bytes_put(&bytes, version, (size_t)n);
    if (!status)
        status =
            ud_control_explain(&e->root, ud_member(e->package->document, "controlFlow"), &bytes);
    if (!status)
        status = ud_bytes_put(&bytes, ",\"pipelines\":[", sizeof(",\"pipelines\":[") - 1);
    if (!status)
        status = ud_bytes_put(&bytes, e->bound_plans.data, e->bound_plans.size);
    if (!status)
        status = ud_bytes_put(&bytes, "]}", sizeof("]}") - 1);
    if (!status)
        status = owned_json(&bytes, out, length);
    ud_bytes_clear(&bytes);
    return status;
}
ud_status ud_execution_diagnostics(ud_execution *e, char **out, uint32_t *length) {
    if (!e || !out || !length)
        return UD_INVALID;
    *out = NULL;
    *length = 0;
    int state = atomic_load(&e->state);
    if (state == UD_EXEC_RUNNING || state == UD_EXEC_DRAINING)
        return UD_BUSY;
    ud_bytes bytes = {.ctx = e->ctx};
    ud_status status = ud_bytes_put(&bytes, "{\"connections\":", sizeof("{\"connections\":") - 1);
    if (!status)
        status = ud_connections_diagnostics(e, &bytes);
    if (!status)
        status = ud_bytes_put(&bytes, ",\"ruleDeliveries\":", 18);
    if (!status)
        status = ud_rule_facts_json(e->rule_facts, &bytes);
    if (!status)
        status = ud_bytes_put(&bytes, ",\"schemas\":[", 12);
    if (!status)
        status = ud_bytes_put(&bytes, e->schema_reports.data, e->schema_reports.size);
    if (!status)
        status = ud_bytes_put(&bytes, "]", 1);
    if (!status)
        status = ud_bytes_put(&bytes, ",\"caches\":", sizeof(",\"caches\":") - 1);
    if (!status)
        status = ud_assets_diagnostics(e, &bytes);
    if (!status) {
        ud_buffer_metrics m = e->buffer_metrics;
        if (e->buffers)
            status = ud_buffer_manager_metrics(e->buffers, &m);
        char buffer[768];
        int n = snprintf(
            buffer, sizeof(buffer),
            ",\"buffers\":{\"residentPeak\":%llu,\"workingPeak\":%llu,\"spillPeak\":%llu,"
            "\"totalSpill\":%llu,\"encodes\":%llu,\"decodes\":%llu,\"encodedBytes\":%llu,"
            "\"readBytes\":%llu,\"writeBytes\":%llu,\"encodeCpuNs\":%llu,\"decodeCpuNs\":%llu,"
            "\"readMs\":%llu,\"writeMs\":%llu}",
            (unsigned long long)m.resident_high_water, (unsigned long long)m.working_high_water,
            (unsigned long long)m.spill_high_water, (unsigned long long)m.total_spill_bytes,
            (unsigned long long)m.wire_encodes, (unsigned long long)m.wire_decodes,
            (unsigned long long)m.encoded_bytes, (unsigned long long)m.read_bytes,
            (unsigned long long)m.write_bytes, (unsigned long long)m.encode_cpu_ns,
            (unsigned long long)m.decode_cpu_ns, (unsigned long long)m.read_ms,
            (unsigned long long)m.write_ms);
        if (!status)
            status = ud_bytes_put(&bytes, buffer, (size_t)n);
    }
    if (!status)
        status = ud_bytes_put(&bytes, ",\"scheduler\":", sizeof(",\"scheduler\":") - 1);
    if (!status)
        status = ud_executor_diagnostics(e->executor, &bytes);
    if (!status)
        status = ud_bytes_put(&bytes, ",\"tasks\":[", sizeof(",\"tasks\":[") - 1);
    if (!status)
        status = ud_bytes_put(&bytes, e->control_reports.data, e->control_reports.size);
    char count[128];
    int count_size = snprintf(count, sizeof(count),
                              "],\"taskRecords\":%llu,\"taskRecordsDropped\":%llu,\"pipelines\":[",
                              (unsigned long long)e->control_record_count,
                              (unsigned long long)e->control_records_dropped);
    if (!status)
        status = ud_bytes_put(&bytes, count, (size_t)count_size);
    ud_mutex_lock(&e->diagnostics_lock);
    if (!status)
        status = ud_bytes_put(&bytes, e->pipeline_reports.data, e->pipeline_reports.size);
    ud_mutex_unlock(&e->diagnostics_lock);
    char tail[384];
    int n = snprintf(tail, sizeof(tail),
                     "],\"pipelineRecords\":%llu,\"pipelineRecordsDropped\":%llu,\"errorCount\":%u,"
                     "\"workerHighWater\":%u,\"spillBytes\":%llu,\"state\":%d,\"outcome\":%d}",
                     (unsigned long long)e->pipeline_record_count,
                     (unsigned long long)e->pipeline_records_dropped, atomic_load(&e->error_count),
                     ud_executor_high_water(e->executor),
                     (unsigned long long)atomic_load(&e->spill_bytes), atomic_load(&e->state),
                     atomic_load(&e->result));
    if (!status)
        status = ud_bytes_put(&bytes, tail, (size_t)n);
    if (!status)
        status = owned_json(&bytes, out, length);
    ud_bytes_clear(&bytes);
    return status;
}
ud_status ud_activation_raise(ud_activation *a, int32_t kind, ud_status result) {
    if (!a || (kind != UD_EXEC_EVENT_WARNING && kind != UD_EXEC_EVENT_ERROR))
        return UD_INVALID;
    if (a->event_depth >= a->execution->options.max_depth)
        return UD_RESOURCE;
    ud_activation_notify_kind(a, kind, UD_EXEC_RUNNING, result);
    const char *handler = kind == UD_EXEC_EVENT_WARNING ? "warning" : "error";
    a->event_depth++;
    ud_status status = UD_OK;
    for (ud_activation *at = a; !status && at; at = at->parent)
        status = ud_control_handler(at, ud_member(at->event_handlers, handler), handler);
    a->event_depth--;
    return status;
}

void ud_task_cancel_register(ud_activation *a, void *instance, void (*cancel)(void *)) {
    if (!cancel)
        return;
    ud_execution *e = a->execution;
    ud_mutex_lock(&e->cancellation_lock);
    a->cancel_instance = instance;
    a->cancel_callback = cancel;
    a->cancel_sent = 0;
    a->next_cancel = e->cancellable;
    e->cancellable = a;
    ud_mutex_unlock(&e->cancellation_lock);
    if (ud_is_cancelled(e->ctx))
        ud_execution_cancel(e);
}
void ud_task_cancel_unregister(ud_activation *a) {
    if (!a->cancel_callback)
        return;
    ud_execution *e = a->execution;
    ud_mutex_lock(&e->cancellation_lock);
    while (a->cancel_pins)
        ud_condition_wait(&e->cancellation_changed, &e->cancellation_lock);
    ud_activation **at = &e->cancellable;
    while (*at && *at != a)
        at = &(*at)->next_cancel;
    if (*at)
        *at = a->next_cancel;
    a->next_cancel = NULL;
    a->cancel_callback = NULL;
    ud_mutex_unlock(&e->cancellation_lock);
}

void ud_target_rows(ud_activation *a, uint64_t rows, uint64_t errors, int acknowledged) {
    ud_activation *owner = a->scope_authority ? a->scope_authority : a;
    if (acknowledged) {
        atomic_fetch_add(&a->execution->acknowledged_rows, rows);
        atomic_fetch_add(&owner->acknowledged_rows, rows);
        atomic_fetch_add(&a->execution->acknowledged_error_rows, errors);
        atomic_fetch_add(&owner->acknowledged_error_rows, errors);
    } else {
        atomic_fetch_add(&a->execution->committed_rows, rows);
        atomic_fetch_add(&owner->committed_rows, rows);
        atomic_fetch_add(&a->execution->committed_error_rows, errors);
        atomic_fetch_add(&owner->committed_error_rows, errors);
        ud_activation_notify_kind(a, UD_EXEC_EVENT_TARGET_COMMITTED, UD_EXEC_RUNNING, UD_OK);
    }
}

ud_status ud_connection_field_namespace(ud_activation *a, ud_string connection, ud_string table,
                                        char out[33]) {
    ud_bytes data = {.ctx = a->execution->ctx};
    ud_string project = {0};
    const ud_json *p = ud_member(a->package->document, "project");
    ud_status status = p ? ud_json_string(ud_member(p, "id"), &project) : UD_OK;
    const ud_json *deployment = ud_member(a->package->document, "deployment");
    if (!status && p && deployment && !ud_json_size(ud_member(p, "parameters")) &&
        !ud_json_size(ud_member(p, "variables")) && !ud_json_size(ud_member(p, "connections"))) {
        /* A Studio publication adds an empty catalog project around the same package. */
        static const char prefix[] = "deployment-project/";
        size_t prefix_length = sizeof(prefix) - 1;
        ud_string id = {0};
        status = ud_json_string(ud_member(deployment, "id"), &id);
        if (!status && project.length == prefix_length + id.length &&
            !memcmp(project.data, prefix, prefix_length) &&
            !memcmp(project.data + prefix_length, id.data, id.length))
            project = (ud_string){0};
    }
    ud_string parts[] = {a->execution->options.tenant, project, a->package->id, connection, table};
    for (uint32_t i = 0; !status && i < 5; i++)
        status = ud_put_string(&data, parts[i].data, parts[i].length);
    if (!status) {
        uint8_t hash[32];
        ud_hash(data.data, data.size, hash);
        ud_id id = {0};
        for (uint32_t i = 0; i < 8; i++) {
            id.high = (id.high << 8) | hash[i];
            id.low = (id.low << 8) | hash[i + 8];
        }
        ud_id_format(id, out);
    }
    ud_bytes_clear(&data);
    return status;
}
ud_status ud_execution_discover_schema(ud_execution *e, ud_string connection, ud_string table,
                                       char **out, uint32_t *length) {
    if (!e || !out || !length)
        return UD_INVALID;
    *out = NULL;
    *length = 0;
    if (atomic_load(&e->state) == UD_EXEC_RUNNING || atomic_load(&e->state) == UD_EXEC_DRAINING)
        return UD_BUSY;
    ud_connection_lease *lease = NULL;
    ud_batch *schema = NULL;
    char scope[33];
    ud_status status = ud_connection_field_namespace(&e->root, connection, table, scope);
    if (!status)
        status = ud_activation_acquire_connection(&e->root, connection,
                                                  UD_CONNECT_READ | UD_CONNECT_METADATA, &lease);
    if (!status)
        status = ud_connection_lease_schema(lease, table, (ud_string){scope, 32}, &schema);
    if (!status)
        status = ud_schema_to_json(schema, out, length);
    ud_batch_destroy(schema);
    ud_status closed = ud_connection_lease_release(lease);
    if (closed && !status) {
        ud_free(*out);
        *out = NULL;
        *length = 0;
        status = closed;
    }
    return status;
}

ud_status ud_schema_report(ud_activation *a, ud_string diff, ud_status result) {
    ud_execution *e = a->execution;
    ud_bytes record = {.ctx = e->ctx, .limit = 65536};
    ud_status status = ud_bytes_put(&record, "{\"task\":", 8);
    if (!status)
        status = ud_json_quote(&record, a->task_id.data, a->task_id.length);
    if (!status)
        status = ud_bytes_put(&record, ",\"component\":", 13);
    if (!status)
        status = ud_json_quote(&record, a->component_id.data, a->component_id.length);
    char code[64];
    int n = snprintf(code, sizeof(code), ",\"result\":%d,\"diff\":", result);
    if (!status)
        status = ud_bytes_put(&record, code, (size_t)n);
    if (!status)
        status = ud_bytes_put(&record, diff.data, diff.length);
    if (!status)
        status = ud_bytes_put(&record, "}", 1);
    ud_mutex_lock(&e->diagnostics_lock);
    e->schema_reports.ctx = e->ctx;
    e->schema_reports.limit = 1048576;
    uint64_t before = e->schema_reports.size;
    if (!status && before)
        status = ud_bytes_put(&e->schema_reports, ",", 1);
    if (!status)
        status = ud_bytes_put(&e->schema_reports, record.data, record.size);
    if (status)
        e->schema_reports.size = before;
    ud_mutex_unlock(&e->diagnostics_lock);
    ud_bytes_clear(&record);
    return status;
}

ud_status ud_execution_component_catalog(ud_execution *e, char **out, uint32_t *length) {
    if (!e)
        return UD_INVALID;
    if (!out || !length)
        return UD_INVALID;
    *out = NULL;
    *length = 0;
    char *registered = NULL;
    uint32_t size = 0;
    ud_status status = ud_registry_describe(e->registry, &registered, &size);
    ud_bytes catalog = {.ctx = e->ctx};
    if (!status && (!size || registered[size - 1] != ']'))
        status = UD_CORRUPT;
    if (!status)
        status = ud_bytes_put(&catalog, registered, size - 1);
    if (!status)
        status = ud_control_catalog(&catalog);
    if (!status)
        status = ud_bytes_put(&catalog, "]", 1);
    if (!status)
        status = ud_bytes_put(&catalog, "", 1);
    if (!status) {
        *out = (char *)catalog.data;
        *length = (uint32_t)catalog.size - 1;
        catalog.data = NULL;
    }
    ud_free(registered);
    ud_bytes_clear(&catalog);
    return status;
}

ud_status ud_component_schema_namespace(const ud_component_context *context, char **out,
                                        uint32_t *length) {
    if (!context || !out || !length || context->struct_size != sizeof(*context) ||
        context->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    *out = NULL;
    *length = 0;
    char scope[33];
    UD_TRY(ud_pipeline_field_namespace(context, scope));
    *out = ud_strdup_n(context->memory, scope, 32);
    if (!*out)
        return UD_RESOURCE;
    *length = 32;
    return UD_OK;
}

ud_status ud_execution_compile_expression(ud_execution *e, ud_string schema_json, ud_string source,
                                          char **out, uint32_t *length) {
    if (!e || !out || !length)
        return UD_INVALID;
    *out = NULL;
    *length = 0;
    if (atomic_load(&e->state) != UD_EXEC_READY)
        return UD_BUSY;
    ud_batch *schema = NULL;
    ud_status status = UD_OK;
    if (schema_json.length)
        status = ud_schema_from_json(e->ctx, schema_json.data, schema_json.length, (ud_string){0},
                                     &schema);
    ud_expression *expression = NULL;
    ud_expression_diagnostic diagnostic = {0};
    ud_expression_options options = {
        sizeof(options), UD_ABI_VERSION,   schema,      ud_resolve_value,
        &e->root,        ud_resolve_field, e->registry, 0};
    if (!status)
        status = ud_expression_compile(e->ctx, source, &options, &diagnostic, &expression);
    ud_bytes json = {.ctx = e->ctx};
    char head[160];
    int n = snprintf(head, sizeof(head),
                     "{\"result\":%d,\"start\":%u,\"length\":%u,\"message\":", status,
                     diagnostic.start, diagnostic.length);
    ud_status result = ud_bytes_put(&json, head, (size_t)n);
    if (!result)
        result = ud_json_quote(&json, diagnostic.message, (uint32_t)strlen(diagnostic.message));
    if (!result && expression) {
        char *text = NULL;
        uint32_t bytes = 0;
        result = ud_expression_describe(expression, &text, &bytes);
        if (!result)
            result = ud_bytes_put(&json, ",\"schema\":", 10);
        if (!result)
            result = ud_bytes_put(&json, text, bytes);
        ud_free(text);
    }
    if (!result)
        result = ud_bytes_put(&json, "}", 1);
    if (!result) {
        *length = (uint32_t)json.size;
        result = ud_bytes_put(&json, "", 1);
    }
    if (!result) {
        *out = (char *)json.data;
        json.data = NULL;
    }
    ud_bytes_clear(&json);
    ud_expression_destroy(expression);
    ud_batch_destroy(schema);
    return result;
}

ud_status ud_component_source_identity(const ud_component_context *c, ud_batch *batch, uint32_t row,
                                       uint64_t position, ud_string source) {
    if (!c || !batch || c->memory != batch->ctx)
        return UD_INVALID;
    char scope[33];
    ud_id_format(c->errors.field_scope, scope);
    ud_bytes symbol = {.ctx = c->memory};
    ud_status status = ud_bytes_put(&symbol, scope, 32);
    if (!status)
        status = ud_bytes_put(&symbol, "/", 1);
    if (!status)
        status = ud_bytes_put(&symbol, source.data, source.length);
    ud_id identity;
    ud_row_info info;
    if (!status)
        status =
            ud_id_make(c->memory, c->errors.execution_id,
                       (ud_string){(const char *)symbol.data, (uint32_t)symbol.size}, &identity);
    if (!status)
        status = ud_row_info_source(identity, position, &info);
    if (!status)
        status = ud_batch_set_row_info(batch, row, &info);
    ud_bytes_clear(&symbol);
    return status;
}
