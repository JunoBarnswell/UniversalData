#include "incremental.h"

static ud_status document_part(ud_bytes *bytes, const ud_json *value) {
    if (!value)
        return ud_put_string(bytes, "", 0);
    char *text = NULL;
    uint32_t length = 0;
    ud_status status = ud_json_serialize(value, &text, &length);
    if (!status)
        status = ud_put_string(bytes, text, length);
    ud_free(text);
    return status;
}

static ud_status member_parts(ud_bytes *bytes, const ud_json *value, const char *const fields[],
                              uint32_t count) {
    for (uint32_t i = 0; i < count; i++)
        UD_TRY(document_part(bytes, ud_member(value, fields[i])));
    return UD_OK;
}

static int field_is(const ud_json *value, const char *field, const char *expected) {
    ud_string actual = {0};
    return !ud_json_string(ud_member(value, field), &actual) && ud_named(actual, expected);
}

static ud_status fingerprint(ud_context *ctx, const ud_json *package, const ud_json *flow,
                             const ud_json *source, const ud_json *parameters, ud_incremental_plan *plan) {
    const ud_json *config = ud_member(source, "config");
    const ud_json *connections = ud_member(package, "connections");
    const ud_json *components = ud_member(flow, "components");
    ud_bytes signature = {.ctx = ctx};
    static const char *const sql_source[] = {
        "connection", "table", "schema", "schemaPolicy", "where", "errorPolicy"};
    static const char *const cursor_fields[] = {"mode", "keys", "unique", "monotonic"};
    static const char *const connector[] = {"id", "driver", "resource", "revision", "credentialReference"};
    ud_status status = ud_put_string(&signature, plan->task.data, plan->task.length);
    if (!status)
        status = ud_put_string(&signature, plan->pipeline.data, plan->pipeline.length);
    if (!status)
        status = ud_put_string(&signature, plan->component.data, plan->component.length);
    if (!status)
        status = member_parts(&signature, config, sql_source, sizeof(sql_source) / sizeof(*sql_source));
    if (!status)
        status = member_parts(&signature, ud_member(config, "incremental"), cursor_fields,
                              sizeof(cursor_fields) / sizeof(*cursor_fields));
    for (uint32_t i = 0; !status && i < ud_json_size(connections); i++)
        status = member_parts(&signature, ud_json_at(connections, i), connector,
                              sizeof(connector) / sizeof(*connector));
    for (uint32_t i = 0; !status && i < ud_json_size(components); i++)
        if (ud_json_at(components, i) != source)
            status = document_part(&signature, ud_json_at(components, i));
    if (!status)
        status = document_part(&signature, ud_member(flow, "paths"));
    static const char *const package_scope[] = {"parameters", "variables", "project"};
    if (!status)
        status = member_parts(&signature, package, package_scope, sizeof(package_scope) / sizeof(*package_scope));
    if (!status)
        status = document_part(&signature, parameters);
    if (!status) {
        uint8_t hash[32];
        ud_hash(signature.data, signature.size, hash);
        for (uint32_t i = 0; i < sizeof(hash); i++)
            snprintf(plan->fingerprint + i * 2, 3, "%02x", hash[i]);
    }
    ud_bytes_clear(&signature);
    return status;
}

ud_status ud_incremental_describe(ud_context *ctx, const ud_json *package, const ud_json *parameters,
                                  ud_incremental_plan *plan) {
    *plan = (ud_incremental_plan){0};
    const ud_json *flows = ud_member(package, "dataFlows"), *source = NULL, *flow = NULL;
    for (uint32_t i = 0; i < ud_json_size(flows); i++) {
        const ud_json *current = ud_json_at(flows, i);
        const ud_json *components = ud_member(current, "components");
        for (uint32_t j = 0; j < ud_json_size(components); j++) {
            const ud_json *node = ud_json_at(components, j), *config = ud_member(node, "config");
            const ud_json *cursor = ud_member(config, "incremental");
            if (!field_is(node, "component", "sql.source") || !cursor ||
                field_is(cursor, "mode", "snapshot"))
                continue;
            if (source || !ud_member(config, "table") || ud_member(config, "sql") ||
                ud_member(cursor, "overlap") || ud_member(current, "parallelism") ||
                field_is(config, "readMode", "stream"))
                return UD_UNSUPPORTED;
            source = node;
            flow = current;
            plan->initial = ud_member(cursor, "after");
        }
    }
    if (!source)
        return UD_END;
    UD_TRY(ud_json_string(ud_member(flow, "id"), &plan->pipeline));
    UD_TRY(ud_json_string(ud_member(source, "id"), &plan->component));
    const ud_json *tasks = ud_member(ud_member(package, "controlFlow"), "tasks");
    for (uint32_t k = 0; k < ud_json_size(tasks); k++) {
        const ud_json *task = ud_json_at(tasks, k);
        ud_string pipeline = {0};
        if (!field_is(task, "kind", "dataFlow") ||
            ud_json_string(ud_member(ud_member(task, "config"), "pipeline"), &pipeline) ||
            !ud_same(pipeline, plan->pipeline))
            continue;
        if (plan->task.length)
            return UD_UNSUPPORTED;
        UD_TRY(ud_json_string(ud_member(task, "id"), &plan->task));
    }
    if (!plan->task.length || ud_member(ud_member(package, "policy"), "transaction"))
        return UD_UNSUPPORTED;
    return fingerprint(ctx, package, flow, source, parameters, plan);
}

static ud_status string_member(ud_bytes *bytes, const char *label, ud_string value, int first) {
    if (!first)
        UD_TRY(ud_bytes_put(bytes, ",", 1));
    UD_TRY(ud_json_quote(bytes, label, (uint32_t)strlen(label)));
    UD_TRY(ud_bytes_put(bytes, ":", 1));
    return ud_json_quote(bytes, value.data, value.length);
}

static ud_status value_json(ud_bytes *bytes, const ud_json *value) {
    if (!value)
        return ud_bytes_put(bytes, "null", 4);
    char *text = NULL;
    uint32_t length = 0;
    ud_status status = ud_json_serialize(value, &text, &length);
    if (!status)
        status = ud_bytes_put(bytes, text, length);
    ud_free(text);
    return status;
}

ud_status ud_incremental_binding_json(ud_bytes *bytes, const ud_incremental_plan *plan,
                                       const ud_json *lower) {
    UD_TRY(ud_bytes_put(bytes, "{", 1));
    UD_TRY(string_member(bytes, "task", plan->task, 1));
    UD_TRY(string_member(bytes, "pipeline", plan->pipeline, 0));
    UD_TRY(string_member(bytes, "component", plan->component, 0));
    UD_TRY(string_member(bytes, "fingerprint", (ud_string){plan->fingerprint, 64}, 0));
    UD_TRY(ud_bytes_put(bytes, ",\"after\":", sizeof(",\"after\":") - 1));
    UD_TRY(value_json(bytes, lower));
    return ud_bytes_put(bytes, "}", 1);
}

static int same_member(const ud_json *object, const char *key, ud_string expected) {
    ud_string actual;
    return !ud_json_string(ud_member(object, key), &actual) && ud_same(actual, expected);
}

ud_status ud_execution_set_incremental(ud_execution *e, ud_string binding) {
    if (!e || !binding.length)
        return UD_INVALID;
    if (atomic_load(&e->state) != UD_EXEC_READY || e->incremental_binding)
        return UD_BUSY;
    if (!e->options.state_directory.length || e->options.worker_members > 1)
        return UD_UNSUPPORTED;
    ud_incremental_plan plan;
    UD_TRY(ud_incremental_describe(e->ctx, e->package->document, e->parameters, &plan));
    ud_json *doc = NULL;
    UD_TRY(ud_json_parse(e->ctx, binding.data, binding.length, &doc, NULL));
    const char *const keys[] = {"task", "pipeline", "component", "fingerprint", "after"};
    ud_status status = ud_json_keys(doc, keys, 5);
    int type = ud_json_kind(ud_member(doc, "after"));
    if (!status && (!same_member(doc, "task", plan.task) || !same_member(doc, "pipeline", plan.pipeline) ||
                    !same_member(doc, "component", plan.component) ||
                    !same_member(doc, "fingerprint", (ud_string){plan.fingerprint, 64}) ||
                    (type != UD_JSON_ARRAY && type != UD_JSON_NULL)))
        status = UD_INCOMPATIBLE;
    if (!status)
        status = ud_control_task_identity(e->ctx, e->root.task_id, plan.task, &e->incremental_task_id);
    if (!status)
        e->incremental_binding = doc;
    else
        ud_json_destroy(doc);
    return status;
}

int ud_incremental_source(const ud_activation *a, ud_string component) {
    const ud_json *doc = a->execution->incremental_binding;
    return ud_incremental_task(a) && same_member(doc, "component", component) &&
           same_member(doc, "pipeline", a->pipeline_id);
}

int ud_incremental_task(const ud_activation *a) {
    return a->execution->incremental_binding && ud_same(a->execution->incremental_task_id, a->task_id);
}

ud_status ud_incremental_cursor_policy(const ud_activation *a, ud_string component,
                                       const ud_json *original, char **out, uint32_t *length) {
    if (!ud_incremental_source(a, component))
        return ud_json_serialize(original, out, length);
    const ud_json *after = ud_member(a->execution->incremental_binding, "after");
    ud_bytes bytes = {.ctx = a->execution->ctx};
    ud_status status = ud_bytes_put(&bytes, "{", 1);
    uint32_t written = 0;
    for (uint32_t i = 0; !status && i < ud_json_size(original); i++) {
        ud_string name;
        status = ud_json_key(original, i, &name.data, &name.length);
        if (status || ud_named(name, "after"))
            continue;
        if (written++)
            status = ud_bytes_put(&bytes, ",", 1);
        if (!status)
            status = ud_json_quote(&bytes, name.data, name.length);
        if (!status)
            status = ud_bytes_put(&bytes, ":", 1);
        if (!status)
            status = value_json(&bytes, ud_json_find(original, name.data, name.length));
    }
    if (!status && ud_json_kind(after) == UD_JSON_ARRAY) {
        status = ud_bytes_put(&bytes, ",\"after\":", sizeof(",\"after\":") - 1);
        if (!status)
            status = value_json(&bytes, after);
    }
    if (!status)
        status = ud_bytes_put(&bytes, "}", 1);
    if (!status) {
        *out = ud_strdup_n(bytes.ctx, (const char *)bytes.data, bytes.size);
        *length = (uint32_t)bytes.size;
        if (!*out)
            status = UD_RESOURCE;
    }
    ud_bytes_clear(&bytes);
    return status;
}

ud_status ud_incremental_capture(ud_execution *e) {
    if (!e->incremental_binding || atomic_load(&e->stop_requested))
        return UD_OK;
    const ud_json *bound = e->incremental_binding;
    ud_activation source = {.execution = e, .task_id = e->incremental_task_id};
    ud_string component, fingerprint_value;
    UD_TRY(ud_json_string(ud_member(bound, "pipeline"), &source.pipeline_id));
    UD_TRY(ud_json_string(ud_member(bound, "component"), &component));
    UD_TRY(ud_json_string(ud_member(bound, "fingerprint"), &fingerprint_value));
    char *record = NULL;
    uint32_t length = 0;
    ud_status status = ud_checkpoint_cursor_read(&source, component, &record, &length);
    if (status == UD_END)
        return UD_COMMIT_UNKNOWN;
    ud_json *cursor = NULL;
    if (!status)
        status = ud_json_parse(e->ctx, record, length, &cursor, NULL);
    ud_free(record);
    uint64_t rows = 0;
    if (!status)
        status = ud_json_uint(ud_member(cursor, "lastConfirmed"), &rows);
    const ud_json *values = ud_member(cursor, "values");
    if (!status && rows && (ud_json_kind(values) != UD_JSON_ARRAY || !ud_json_size(values)))
        status = UD_CORRUPT;
    ud_bytes proof = {.ctx = e->ctx};
    if (!status)
        status = ud_bytes_put(&proof, "{", 1);
    if (!status)
        status = string_member(&proof, "fingerprint", fingerprint_value, 1);
    if (!status)
        status = string_member(&proof, "executionId", e->options.execution_id, 0);
    if (!status)
        status = ud_bytes_put(&proof, ",\"after\":", sizeof(",\"after\":") - 1);
    if (!status)
        status = value_json(&proof, rows ? values : NULL);
    if (!status)
        status = ud_bytes_put(&proof, "}", 1);
    if (!status)
        status = ud_json_parse(e->ctx, (const char *)proof.data, (uint32_t)proof.size,
                               &e->incremental_confirmation, NULL);
    ud_bytes_clear(&proof);
    ud_json_destroy(cursor);
    return status ? UD_COMMIT_UNKNOWN : UD_OK;
}

ud_status ud_execution_incremental_confirmation(ud_execution *e, char **out, uint32_t *length) {
    if (!e || !out || !length)
        return UD_INVALID;
    *out = NULL;
    *length = 0;
    if (atomic_load(&e->state) != UD_EXEC_SUCCEEDED || !e->incremental_confirmation)
        return UD_END;
    return ud_json_serialize(e->incremental_confirmation, out, length);
}
