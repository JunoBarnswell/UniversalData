#include "internal.h"
/* A nested definition frame on the existing Execution/context/journal. It has
 * no separate scheduler, thread, registry, deadline, transaction or recovery
 * store. Only immutable definitions/parameters and lexical roots are swapped. */
static ud_status mapped_parameters(ud_activation *e, const ud_json *mapping, ud_json **out) {
    if (mapping && ud_json_kind(mapping) != UD_JSON_OBJECT)
        return UD_SCHEMA;
    ud_bytes b = {.ctx = e->execution->ctx};
    ud_status s = ud_bytes_put(&b, "{", 1);
    for (uint32_t i = 0; !s && i < ud_json_size(mapping); i++) {
        const char *key;
        uint32_t n;
        s = ud_json_key(mapping, i, &key, &n);
        ud_expression *expr = NULL;
        ud_value value;
        if (!s)
            s = ud_expression_json(e, ud_json_at(mapping, i), NULL, &expr);
        if (!s)
            s = ud_expression_evaluate(expr, NULL, 0, &value);
        if (!s && i)
            s = ud_bytes_put(&b, ",", 1);
        if (!s)
            s = ud_json_quote(&b, key, n);
        if (!s)
            s = ud_bytes_put(&b, ":", 1);
        ud_field field = {0};
        if (!s) s = ud_expression_field(expr, &field);
        if (!s) s = ud_scalar_json(&b, &field, &value);
        ud_expression_destroy(expr);
    }
    if (!s)
        s = ud_bytes_put(&b, "}", 1);
    if (!s && b.size > UINT32_MAX)
        s = UD_OVERFLOW;
    if (!s)
        s = ud_json_parse(e->execution->ctx, (char *)b.data, (uint32_t)b.size, out, NULL);
    ud_bytes_clear(&b);
    return s;
}
ud_status ud_child_package(ud_activation *e, const ud_json *config, int execute, uint32_t depth) {
    if (depth >= e->execution->options.max_depth)
        return UD_RESOURCE;
    static const char *keys[] = {"package", "parameters"};
    UD_TRY(ud_json_keys(config, keys, 2));
    const ud_json *definition = ud_member(config, "package");
    if (ud_json_kind(definition) != UD_JSON_OBJECT)
        return UD_SCHEMA;
    ud_json *parameters = NULL;
    ud_package *package = NULL;
    ud_status s = mapped_parameters(e, ud_member(config, "parameters"), &parameters);
    char *text = NULL;
    uint32_t n;
    if (!s)
        s = ud_json_serialize(definition, &text, &n);
    if (!s)
        s = ud_package_create(e->execution->ctx, (ud_string){text, n}, &package);
    ud_free(text);
    if (s) {
        ud_json_destroy(parameters);
        return s;
    }
    ud_activation child = {0};
    child.execution = e->execution;
    child.parent = e;
    child.definition = &child;
    child.package = package;
    child.parameters = parameters;
    child.depth = depth;
    child.parallel = e->parallel;
    atomic_init(&child.cancelled, 0);
    size_t size = (size_t)e->task_id.length + package->id.length + 64;
    char *identity = ud_alloc(e->execution->ctx, size);
    if (!identity) s = UD_RESOURCE;
    if (!s) {
        int count = snprintf(identity, size, "%.*spackage:%u:%.*s;", (int)e->task_id.length,
                             e->task_id.data ? e->task_id.data : "", package->id.length,
                             (int)package->id.length, package->id.data);
        if (count < 0 || (size_t)count >= size) s = UD_OVERFLOW;
        else child.task_id = (ud_string){identity, (uint32_t)count};
    }
    if (!s) s = ud_activation_bind_roots(&child);
    if (!s) s = ud_activation_validate_definition(&child, depth);
    if (!s && execute) {
        ud_activation_notify(&child, UD_EXEC_RUNNING, UD_OK);
        s = ud_activation_run_definition(&child);
        if (s == UD_BUSY) s = UD_TRANSIENT;
        if (!s) ud_activation_notify(&child, UD_EXEC_DRAINING, UD_OK);
        ud_execution_state state = s == UD_COMMIT_UNKNOWN ? UD_EXEC_RECOVERY_REQUIRED
                                 : s == UD_CANCELLED ? UD_EXEC_CANCELLED
                                 : s ? UD_EXEC_FAILED : UD_EXEC_SUCCEEDED;
        ud_activation_notify(&child, state, s);
    }
    ud_scope_leave(&child, &child.globals);
    ud_scope_leave(&child, &child.project_scope);
    ud_free(identity);
    ud_json_destroy(parameters);
    ud_package_destroy(package);
    return s;
}
