#include "internal.h"

/* This composes a subset of the existing pipeline. It does not interpret data
 * or component semantics; binding and scheduling remain owned by pipeline.c. */
static ud_status write_json(ud_bytes *b, const ud_json *value) {
    char *text = NULL;
    uint32_t size = 0;
    ud_status s = ud_json_serialize(value, &text, &size);
    if (!s)
        s = ud_bytes_put(b, text, size);
    ud_free(text);
    return s;
}
static int node_index(const ud_json *nodes, ud_string id) {
    for (uint32_t i = 0; i < ud_json_size(nodes); i++) {
        ud_string name = {0};
        if (!ud_json_string(ud_member(ud_json_at(nodes, i), "id"), &name) && ud_same(name, id))
            return (int)i;
    }
    return -1;
}
ud_status ud_preview_build(ud_execution *e, ud_string text, ud_string selected, ud_json **out) {
    *out = NULL;
    ud_json *definition = NULL;
    UD_TRY(ud_json_parse(e->ctx, text.data, text.length, &definition, NULL));
    const ud_json *nodes = ud_member(definition, "components"),
                  *paths = ud_member(definition, "paths");
    ud_string id = {0};
    uint32_t count = ud_json_size(nodes);
    ud_status s = ud_json_kind(nodes) != UD_JSON_ARRAY || ud_json_kind(paths) != UD_JSON_ARRAY ||
                          !count || count > 128 || ud_json_size(paths) > 1024
                      ? UD_RESOURCE
                      : UD_OK;
    if (!s)
        s = ud_json_string(ud_member(definition, "id"), &id);
    int target = !s ? node_index(nodes, selected) : -1;
    if (!s && target < 0)
        s = UD_SCHEMA;
    uint8_t keep[128] = {0};
    if (!s)
        keep[target] = 1;
    for (uint32_t pass = 0; !s && pass < count; pass++) {
        int changed = 0;
        for (uint32_t i = 0; !s && i < ud_json_size(paths); i++) {
            const ud_json *path = ud_json_at(paths, i);
            ud_string from = {0}, to = {0};
            s = ud_json_string(ud_member(path, "from"), &from);
            if (!s)
                s = ud_json_string(ud_member(path, "to"), &to);
            int a = node_index(nodes, from), b = node_index(nodes, to);
            if (!s && (a < 0 || b < 0))
                s = UD_SCHEMA;
            if (!s && keep[b] && !keep[a]) {
                keep[a] = 1;
                changed = 1;
            }
        }
        if (!changed)
            break;
    }
    int schema_only = 0;
    for (uint32_t i = 0; !s && i < count; i++)
        if (keep[i]) {
            const ud_json *entry = ud_json_at(nodes, i), *config = ud_member(entry, "config");
            ud_string kind = {0};
            s = ud_json_string(ud_member(entry, "component"), &kind);
            ud_component_descriptor d = {.struct_size = sizeof(d), .abi_version = UD_ABI_VERSION};
            if (!s)
                s = ud_component_lookup(&e->root, kind, &d);
            if (!s && i == (uint32_t)target && d.category == UD_COMPONENT_DESTINATION) {
                keep[i] = 0;
                continue;
            }
            if (!s &&
                (!(d.capabilities & (UD_COMPONENT_PREVIEW_SAFE | UD_COMPONENT_PREVIEW_SCHEMA)) ||
                 d.effects & UD_EFFECT_EXTERNAL_WRITE || d.category == UD_COMPONENT_DESTINATION))
                s = UD_UNSUPPORTED;
            if (!s && (d.capabilities & UD_COMPONENT_PREVIEW_SCHEMA))
                schema_only = 1;
            /* CDC and cursor confirmation are never a preview. Raw SQL is excluded
             * even for a read-only session: only Native quoted table scans enter. */
            if (!s && (ud_member(config, "incremental") ||
                       (ud_named(kind, "sql.source") && ud_member(config, "sql"))))
                s = UD_UNSUPPORTED;
        }
    ud_bytes b = {.ctx = e->ctx, .limit = 8388608};
    if (!s)
        s = ud_bytes_put(&b, "{\"id\":", sizeof("{\"id\":") - 1);
    if (!s)
        s = ud_json_quote(&b, id.data, id.length);
    if (!s)
        s = ud_bytes_put(&b, ",\"components\":[", sizeof(",\"components\":[") - 1);
    uint32_t written = 0;
    for (uint32_t i = 0; !s && i < count; i++)
        if (keep[i]) {
            if (written++)
                s = ud_bytes_put(&b, ",", sizeof(",") - 1);
            if (!s)
                s = write_json(&b, ud_json_at(nodes, i));
        }
    if (!s && !written)
        s = UD_SCHEMA;
    if (!s)
        s = ud_bytes_put(&b, "],\"paths\":[", sizeof("],\"paths\":[") - 1);
    written = 0;
    for (uint32_t i = 0; !s && i < ud_json_size(paths); i++) {
        const ud_json *p = ud_json_at(paths, i);
        ud_string from = {0}, to = {0};
        s = ud_json_string(ud_member(p, "from"), &from);
        if (!s)
            s = ud_json_string(ud_member(p, "to"), &to);
        int a = node_index(nodes, from), c = node_index(nodes, to);
        if (!s && keep[a] && keep[c]) {
            if (written++)
                s = ud_bytes_put(&b, ",", sizeof(",") - 1);
            if (!s)
                s = write_json(&b, p);
        }
    }
    if (!s)
        s = ud_bytes_put(&b, "]}", sizeof("]}") - 1);
    if (!s)
        s = ud_json_parse(e->ctx, (char *)b.data, (uint32_t)b.size, out, NULL);
    if (!s)
        e->preview_schema_only = schema_only;
    ud_bytes_clear(&b);
    ud_json_destroy(definition);
    return s;
}
static ud_status readonly_authorize(void *user, ud_string tenant, ud_string resource,
                                    int32_t access) {
    ud_execution *e = user;
    if (access != UD_ACCESS_OPEN && access != UD_ACCESS_READ && access != UD_ACCESS_DISCOVER)
        return UD_AUTH;
    return e->preview_authorize
               ? e->preview_authorize(e->preview_authorize_user, tenant, resource, access)
               : UD_AUTH;
}
ud_status ud_execution_preview(ud_execution *e, ud_string pipeline, ud_string component,
                               uint32_t source_rows, uint32_t samples, char **out,
                               uint32_t *length) {
    if (!e || !out || !length || !source_rows || source_rows > 10000 || !samples || samples > 32)
        return UD_INVALID;
    *out = NULL;
    *length = 0;
    if (atomic_load(&e->state) != UD_EXEC_READY || e->preview_pipeline || e->debug)
        return UD_BUSY;
    if (e->options.state_directory.length || e->external ||
        ud_member(e->package->document, "extensions"))
        return UD_UNSUPPORTED;
    UD_TRY(ud_preview_build(e, pipeline, component, &e->preview_pipeline));
    e->preview_source_rows = e->preview_schema_only ? 0 : source_rows;
    e->preview_authorize = e->options.authorize;
    e->preview_authorize_user = e->options.authorize_user;
    e->options.authorize = readonly_authorize;
    e->options.authorize_user = e;
    e->options.max_concurrency = 1;
    if (!e->options.timeout_ms || e->options.timeout_ms > 30000)
        e->options.timeout_ms = 30000;
    char config[80];
    int n = snprintf(config, sizeof(config), "{\"previewRows\":%u}", samples);
    UD_TRY(ud_execution_debug_configure(e, (ud_string){config, (uint32_t)n}));
    ud_status result = ud_execution_execute(e);
    /* Business failure is represented beside samples and diagnostics so the
     * designer can locate the actual failing component without fake rows. */
    char *snapshot = NULL, *diagnostics = NULL;
    uint32_t a = 0, b = 0;
    ud_status s = ud_execution_debug_snapshot(e, &snapshot, &a);
    if (!s)
        s = ud_execution_diagnostics(e, &diagnostics, &b);
    ud_bytes json = {.ctx = e->ctx, .limit = 16777216};
    n = snprintf(config, sizeof(config),
                 "{\"result\":%d,\"sourceLimit\":%u,\"sampled\":%s,\"snapshot\":", result,
                 e->preview_source_rows, e->preview_schema_only ? "false" : "true");
    if (!s)
        s = ud_bytes_put(&json, config, (size_t)n);
    if (!s)
        s = ud_bytes_put(&json, snapshot, a);
    if (!s)
        s = ud_bytes_put(&json, ",\"diagnostics\":", sizeof(",\"diagnostics\":") - 1);
    if (!s)
        s = ud_bytes_put(&json, diagnostics, b);
    if (!s)
        s = ud_bytes_put(&json, "}", sizeof("}") - 1);
    if (!s) {
        *out = ud_strdup_n(e->ctx, (char *)json.data, json.size);
        if (!*out)
            s = UD_RESOURCE;
        else
            *length = (uint32_t)json.size;
    }
    ud_free(snapshot);
    ud_free(diagnostics);
    ud_bytes_clear(&json);
    return s;
}
