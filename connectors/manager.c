#include "ud/connection_manager.h"
#include "ud/plugin.h"
#include "../engines/package/internal.h"
#include <sys/stat.h>
struct ud_connection_lease {
    ud_execution *execution;
    ud_json *definition;
    ud_string id, resource, driver;
    ud_plugin *plugin;
    ud_connection *connection;
    uint64_t capabilities;
};
ud_status ud_connection_lease_authorize(ud_connection_lease *l, int32_t action) {
    if (!l)
        return UD_INVALID;
    ud_execution *e = l->execution;
    UD_TRY(ud_execution_check(e));
    return e->options.authorize && e->options.tenant.length &&
                   e->options.authorize(e->options.authorize_user, e->options.tenant, l->resource,
                                        action) == UD_OK
               ? UD_OK
               : UD_AUTH;
}
ud_status ud_execution_acquire_connection(ud_execution *e, ud_string id, uint64_t caps,
                                          ud_connection_lease **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!e)
        return UD_INVALID;
    const ud_json *defs = ud_member(e->package->document, "connections"), *def = NULL;
    for (uint32_t i = 0; i < ud_json_size(defs); i++) {
        ud_string name;
        UD_TRY(ud_json_string(ud_member(ud_json_at(defs, i), "id"), &name));
        if (ud_same(name, id)) {
            def = ud_json_at(defs, i);
            break;
        }
    }
    if (!def)
        return UD_SCHEMA;
    ud_connection_lease *l = ud_alloc(e->ctx, sizeof(*l));
    if (!l)
        return UD_RESOURCE;
    l->execution = e;
    l->id = id;
    ud_status s = ud_bound_config(e, def, &l->definition);
    static const char *keys[] = {"id", "driver", "resource", "credentialReference"};
    if (!s)
        s = ud_json_keys(l->definition, keys, 4);
    if (!s)
        s = ud_json_string(ud_member(l->definition, "driver"), &l->driver);
    if (!s)
        s = ud_json_string(ud_member(l->definition, "resource"), &l->resource);
    if (!s)
        s = ud_connection_lease_authorize(l, UD_ACCESS_OPEN);
    if (!s && ud_named(l->driver, "file")) {
        l->capabilities = UD_CONNECT_READ | UD_CONNECT_APPEND;
#ifdef _WIN32
        if (l->resource.length < 3 || l->resource.data[1] != ':')
            s = UD_INVALID;
#else
        if (l->resource.data[0] != '/')
            s = UD_INVALID;
#endif
    } else if (!s) {
        if (!e->options.authorize ||
            e->options.authorize(e->options.authorize_user, e->options.tenant, l->driver,
                                 UD_ACCESS_OPEN) != UD_OK)
            s = UD_AUTH;
        char *path = !s ? ud_strdup_n(e->ctx, l->driver.data, l->driver.length) : NULL;
        if (!s && !path)
            s = UD_RESOURCE;
        if (!s)
            s = ud_plugin_load(e->ctx, path, 0, &l->plugin);
        ud_free(path);
        ud_string credential = {0};
        if (!s)
            s = ud_json_string(ud_member(l->definition, "credentialReference"), &credential);
        ud_connection_options options = {sizeof(options),      UD_ABI_VERSION,
                                         e->options.tenant,    l->resource,
                                         credential,           e->options.authorize_user,
                                         e->options.authorize, e->options.resolve_user,
                                         e->options.resolve};
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
