#include "ud/connection_manager.h"
#include "ud/plugin.h"
#include "../engines/package/internal.h"
#include <sys/stat.h>
struct ud_connection_lease {
    ud_activation *execution;
    ud_json *definition;
    ud_string id, resource, driver;
    ud_plugin *plugin;
    ud_connection *connection;
    uint64_t capabilities;
};
ud_status ud_connection_lease_authorize(ud_connection_lease *l, int32_t action) {
    if (!l)
        return UD_INVALID;
    ud_activation *e = l->execution;
    UD_TRY(ud_activation_check(e));
    return e->execution->options.authorize && e->execution->options.tenant.length &&
                   e->execution->options.authorize(e->execution->options.authorize_user, e->execution->options.tenant, l->resource,
                                        action) == UD_OK
               ? UD_OK
               : UD_AUTH;
}
ud_status ud_connection_definition(ud_activation *e, ud_string id, const ud_json **out,
                                   int *project_scope) {
    const ud_json *defs = ud_member(e->package->document, "connections"), *def = NULL;
    *out = NULL;
    *project_scope = 0;
    for (int level = 0; !def && level < 2; level++) {
        if (level)
            defs = ud_member(ud_member(e->package->document, "project"), "connections");
        for (uint32_t i = 0; i < ud_json_size(defs); i++) {
            ud_string name;
            UD_TRY(ud_json_string(ud_member(ud_json_at(defs, i), "id"), &name));
            if (ud_same(name, id)) {
                def = ud_json_at(defs, i);
                *project_scope = level;
                break;
            }
        }
    }
    *out = def;
    return def ? UD_OK : UD_SCHEMA;
}
static ud_status acquire(ud_activation *e, ud_string id, uint64_t caps, int endpoint_kind,
                         ud_connection_lease **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!e)
        return UD_INVALID;
    const ud_json *def = NULL;
    int project_definition = 0;
    UD_TRY(ud_connection_definition(e, id, &def, &project_definition));
    ud_connection_lease *l = ud_alloc(e->execution->ctx, sizeof(*l));
    if (!l)
        return UD_RESOURCE;
    l->execution = e;
    l->id = id;
    ud_scope *saved_scope = e->scope;
    if (project_definition)
        e->scope = &e->definition->project_scope;
    ud_status s = ud_bound_config(e, def, &l->definition);
    e->scope = saved_scope;
    static const char *keys[] = {"id", "driver", "resource", "credentialReference"};
    if (!s)
        s = ud_json_keys(l->definition, keys, 4);
    if (!s)
        s = ud_json_string(ud_member(l->definition, "driver"), &l->driver);
    if (!s)
        s = ud_json_string(ud_member(l->definition, "resource"), &l->resource);
    if (!s)
        s = ud_connection_lease_authorize(l, UD_ACCESS_OPEN);
    if (!s && ((endpoint_kind == 1 && !ud_named(l->driver, "file")) ||
               (endpoint_kind == 2 && !ud_named(l->driver, "process")) ||
               (endpoint_kind == 3 && !ud_named(l->driver, "http"))))
        s = UD_UNSUPPORTED;
    if (!s && ud_named(l->driver, "http")) {
#ifdef UD_WITH_HTTP
        l->capabilities = UD_CONNECT_HTTP;
#else
        s = UD_UNSUPPORTED;
#endif
    } else if (!s && (ud_named(l->driver, "file") || ud_named(l->driver, "process"))) {
        int process = ud_named(l->driver, "process");
        l->capabilities =
            process ? UD_CONNECT_EXECUTE_PROCESS : UD_CONNECT_READ | UD_CONNECT_APPEND;
        if (process && ud_member(l->definition, "credentialReference"))
            s = UD_SCHEMA;
#ifdef _WIN32
        if (l->resource.length < 3 || l->resource.data[1] != ':')
            s = UD_INVALID;
#else
        if (l->resource.data[0] != '/')
            s = UD_INVALID;
#endif
    } else if (!s) {
        if (!e->execution->options.authorize ||
            e->execution->options.authorize(e->execution->options.authorize_user, e->execution->options.tenant, l->driver,
                                 UD_ACCESS_OPEN) != UD_OK)
            s = UD_AUTH;
        char *path = !s ? ud_strdup_n(e->execution->ctx, l->driver.data, l->driver.length) : NULL;
        if (!s && !path)
            s = UD_RESOURCE;
        if (!s)
            s = ud_plugin_load(e->execution->ctx, path, 0, &l->plugin);
        ud_free(path);
        ud_string credential = {0};
        if (!s)
            s = ud_json_string(ud_member(l->definition, "credentialReference"), &credential);
        ud_connection_options options = {sizeof(options),      UD_ABI_VERSION,
                                         e->execution->options.tenant,    l->resource,
                                         credential,           e->execution->options.authorize_user,
                                         e->execution->options.authorize, e->execution->options.resolve_user,
                                         e->execution->options.resolve};
        if (!s)
            s = ud_connection_open(l->plugin, &options, &l->connection);
        if (!s)
            l->capabilities = ud_connection_capabilities(l->connection);
    }
    if (!s && (l->capabilities & caps) != caps)
        s = UD_UNSUPPORTED;
    if (s) {
        ud_connection_lease_release(l);
        return s;
    }
    *out = l;
    return UD_OK;
}
ud_status ud_activation_acquire_connection(ud_activation *e, ud_string id, uint64_t caps,
                                          ud_connection_lease **out) {
    return acquire(e, id, caps, 0, out);
}
ud_status ud_acquire_file_connection(ud_activation *e, ud_string id, ud_connection_lease **out) {
    return acquire(e, id, 0, 1, out);
}
ud_status ud_acquire_process_connection(ud_activation *e, ud_string id, ud_connection_lease **out) {
    return acquire(e, id, UD_CONNECT_EXECUTE_PROCESS, 2, out);
}
ud_status ud_acquire_http_connection(ud_activation *e, ud_string id, ud_connection_lease **out) {
    return acquire(e, id, UD_CONNECT_HTTP, 3, out);
}
ud_status ud_http_connection_resource(ud_connection_lease *l, ud_string *out) {
    if (!l || !out)
        return UD_INVALID;
    if (!ud_named(l->driver, "http"))
        return UD_UNSUPPORTED;
    *out = l->resource;
    return UD_OK;
}
ud_status ud_http_connection_credentials(ud_connection_lease *l, ud_string *out) {
    if (!l || !out)
        return UD_INVALID;
    *out = (ud_string){0};
    if (!ud_named(l->driver, "http"))
        return UD_UNSUPPORTED;
    UD_TRY(ud_connection_lease_authorize(l, UD_ACCESS_NETWORK));
    const ud_json *ref = ud_member(l->definition, "credentialReference");
    if (!ref)
        return UD_OK;
    ud_string reference;
    UD_TRY(ud_json_string(ref, &reference));
    ud_activation *e = l->execution;
    if (!e->execution->options.resolve)
        return UD_AUTH;
    ud_status s = e->execution->options.resolve(e->execution->options.resolve_user, e->execution->ctx, e->execution->options.tenant,
                                     l->resource, reference, out);
    if (s || !out->data || !out->length || out->length > 32768) {
        ud_free((void *)out->data);
        *out = (ud_string){0};
        return s ? s : UD_SCHEMA;
    }
    return UD_OK;
}
ud_status ud_process_connection_path(ud_connection_lease *l, ud_string *out) {
    if (!l || !out)
        return UD_INVALID;
    if (!ud_named(l->driver, "process"))
        return UD_UNSUPPORTED;
    *out = l->resource;
    return UD_OK;
}
ud_status ud_connection_lease_path(const ud_connection_lease *l, ud_string *out) {
    if (!l || !out)
        return UD_INVALID;
    if (!ud_named(l->driver, "file"))
        return UD_UNSUPPORTED;
    *out = l->resource;
    return UD_OK;
}
ud_status ud_connection_lease_read_open(ud_connection_lease *l, const ud_read_options *o,
                                        const ud_batch *schema) {
    if (!l || !l->connection)
        return UD_UNSUPPORTED;
    UD_TRY(ud_connection_lease_authorize(l, UD_ACCESS_READ));
    return ud_connection_read_open(l->connection, o, schema);
}
ud_status ud_connection_lease_read_next(ud_connection_lease *l, ud_batch **out) {
    if (!l || !l->connection)
        return UD_UNSUPPORTED;
    UD_TRY(ud_connection_lease_authorize(l, UD_ACCESS_READ));
    return ud_connection_read_next(l->connection, out);
}
ud_status ud_connection_lease_read_close(ud_connection_lease *l) {
    return l && l->connection ? ud_connection_read_close(l->connection) : UD_UNSUPPORTED;
}
ud_status ud_connection_lease_write(ud_connection_lease *l, const ud_write_options *o,
                                    const ud_batch *b) {
    if (!l || !l->connection)
        return UD_UNSUPPORTED;
    UD_TRY(ud_connection_lease_authorize(l, UD_ACCESS_WRITE));
    return ud_connection_write(l->connection, o, b);
}
ud_status ud_connection_lease_receipt(ud_connection_lease *l, ud_string job, ud_string batch,
                                      ud_commit_receipt *out) {
    if (!l || !l->connection)
        return UD_UNSUPPORTED;
    UD_TRY(ud_connection_lease_authorize(l, UD_ACCESS_RECEIPT));
    return ud_connection_receipt(l->connection, job, batch, out);
}
ud_status ud_connection_lease_execute_sql(ud_connection_lease *l, ud_string sql) {
    if (!l || !l->connection)
        return UD_UNSUPPORTED;
    UD_TRY(ud_connection_lease_authorize(l, UD_ACCESS_WRITE));
    return ud_connection_execute_sql(l->connection, sql);
}
ud_status ud_connection_lease_release(ud_connection_lease *l) {
    if (!l)
        return UD_OK;
    ud_status s = ud_connection_close(l->connection);
    if (s)
        return s;
    s = ud_plugin_unload(l->plugin);
    if (s)
        return s;
    ud_json_destroy(l->definition);
    ud_free(l);
    return UD_OK;
}

ud_status ud_connection_lease_digest(ud_connection_lease *l, const ud_write_options *options,
                                     const ud_batch *batch, uint8_t digest[32]) {
    if (!l || !l->connection)
        return UD_UNSUPPORTED;
    UD_TRY(ud_connection_lease_authorize(l, UD_ACCESS_WRITE));
    return ud_connection_digest(l->connection, options, batch, digest);
}
