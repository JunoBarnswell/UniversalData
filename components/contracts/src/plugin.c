#include "ud/plugin.h"
#include "../../data/internal.h"
#ifdef _WIN32
#include <windows.h>
#else
#include <dlfcn.h>
#endif
struct ud_plugin {
    ud_context *ctx;
    void *library;
    ud_plugin_api api;
    ud_host_api host;
    uint32_t live, active;
};
struct ud_plugin_instance {
    ud_plugin *plugin;
    void *state;
    int busy;
};
static ud_status report(ud_context *ctx, ud_status s, const char *message) {
    return ud_fail(ctx, s, "%s", message ? message : "plugin failure");
}
static void close_library(void *lib) {
#ifdef _WIN32
    if (lib)
        FreeLibrary((HMODULE)lib);
#else
    if (lib)
        dlclose(lib);
#endif
}
ud_status ud_plugin_load(ud_context *ctx, const char *path, uint64_t caps, ud_plugin **out) {
    if (!path || !out)
        return UD_INVALID;
    *out = NULL;
#ifdef _WIN32
    if (strlen(path) < 3 || !((path[1] == ':' && (path[2] == '/' || path[2] == '\\')) ||
                              (path[0] == '\\' && path[1] == '\\')))
        return UD_INVALID;
#else
    if (path[0] != '/')
        return UD_INVALID;
#endif
    ud_plugin *p = ud_alloc(ctx, sizeof(*p));
    if (!p)
        return UD_RESOURCE;
    p->ctx = ctx;
    ud_plugin_entry_fn entry = NULL;
    unsigned long loader_error = 0;
#ifdef _WIN32
    int len = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path, -1, NULL, 0);
    wchar_t *wide = len ? ud_alloc(ctx, (size_t)len * sizeof(wchar_t)) : NULL;
    if (wide) {
        MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path, -1, wide, len);
        /* LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR requires a native absolute path;
         * CMake and external consumers commonly supply forward slashes. */
        for (int i = 0; i < len; i++)
            if (wide[i] == L'/')
                wide[i] = L'\\';
        p->library = LoadLibraryExW(
            wide, NULL, LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR | LOAD_LIBRARY_SEARCH_DEFAULT_DIRS);
        ud_free(wide);
    }
    if (p->library) {
        FARPROC symbol = GetProcAddress((HMODULE)p->library, "ud_plugin_get_api");
        memcpy(&entry, &symbol, sizeof(entry));
    }
    if (!entry)
        loader_error = GetLastError();
#else
    p->library = dlopen(path, RTLD_NOW | RTLD_LOCAL);
    if (p->library) {
        void *symbol = dlsym(p->library, "ud_plugin_get_api");
        memcpy(&entry, &symbol, sizeof(entry));
    }
#endif
    if (!entry) {
        close_library(p->library);
        ud_free(p);
        return ud_fail(ctx, UD_IO, "plugin library or entry point missing (OS error %lu): %s",
                       loader_error, path);
    }
    p->api.struct_size = sizeof(p->api);
    p->api.abi_version = UD_PLUGIN_ABI;
    ud_status s = entry(UD_PLUGIN_ABI, &p->api);
    if (!s && (p->api.struct_size < sizeof(p->api) || p->api.abi_version != UD_PLUGIN_ABI ||
               !p->api.id || !p->api.version || !p->api.initialize || !p->api.execute ||
               !p->api.destroy || !p->api.shutdown))
        s = UD_INCOMPATIBLE;
    if (!s && (p->api.capabilities & caps) != caps)
        s = UD_UNSUPPORTED;
    if (s) {
        close_library(p->library);
        ud_free(p);
        return ud_fail(ctx, s, "plugin ABI/capabilities incompatible: %s", path);
    }
    p->host = (ud_host_api){sizeof(ud_host_api),
                            UD_PLUGIN_ABI,
                            ctx,
                            ud_alloc,
                            ud_free,
                            ud_batch_rows,
                            ud_batch_columns,
                            ud_batch_field,
                            ud_batch_get,
                            ud_batch_create,
                            ud_batch_append,
                            ud_batch_destroy,
                            ud_csv_open,
                            ud_csv_next,
                            ud_csv_close,
                            ud_csv_write,
                            ud_is_cancelled,
                            report,
                            ud_value_parse_field,
                            ud_value_format};
    *out = p;
    return UD_OK;
}
ud_status ud_plugin_info(const ud_plugin *p, ud_plugin_api *out) {
    if (!p || !out || out->struct_size < sizeof(*out))
        return UD_INVALID;
    *out = p->api;
    return UD_OK;
}
ud_status ud_plugin_create(ud_plugin *p, const char *config, uint32_t n, ud_plugin_instance **out) {
    if (!p || !out || (!config && n) || p->active)
        return UD_INVALID;
    *out = NULL;
    ud_plugin_instance *instance = ud_alloc(p->ctx, sizeof(*instance));
    if (!instance)
        return UD_RESOURCE;
    p->active++;
    ud_status s = p->api.initialize(&p->host, config ? config : "", n, &instance->state);
    p->active--;
    if (s) {
        if (instance->state)
            p->api.destroy(instance->state);
        ud_free(instance);
        return s;
    }
    instance->plugin = p;
    p->live++;
    *out = instance;
    return UD_OK;
}
static uint64_t operation_cap(int32_t op) {
    switch (op) {
    case UD_OP_READ:
        return UD_CAP_READ;
    case UD_OP_WRITE:
        return UD_CAP_WRITE;
    case UD_OP_SCALAR:
        return UD_CAP_SCALAR;
    case UD_OP_SQL:
        return UD_CAP_SQL;
    case UD_OP_PARQUET_READ:
    case UD_OP_PARQUET_WRITE:
        return UD_CAP_PARQUET;
    default:
        return 0;
    }
}
ud_status ud_plugin_execute(ud_plugin_instance *i, int32_t op, const ud_batch *in, const char *arg,
                            uint32_t n, ud_batch **out) {
    if (!i || !out || (!arg && n))
        return UD_INVALID;
    *out = NULL;
    ud_plugin *p = i->plugin;
    if (i->busy || p->active)
        return UD_BUSY;
    uint64_t cap = operation_cap(op);
    if (!cap || !(p->api.capabilities & cap))
        return ud_fail(p->ctx, UD_UNSUPPORTED, "plugin does not support operation %d", op);
    if (ud_is_cancelled(p->ctx))
        return UD_CANCELLED;
    i->busy = 1;
    p->active++;
    ud_status s = p->api.execute(i->state, op, in, arg ? arg : "", n, out);
    p->active--;
    i->busy = 0;
    if (s && *out) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}
ud_status ud_plugin_instance_destroy(ud_plugin_instance *i) {
    if (!i)
        return UD_OK;
    if (i->busy || i->plugin->active)
        return UD_BUSY;
    ud_plugin *p = i->plugin;
    p->active++;
    p->api.destroy(i->state);
    p->active--;
    p->live--;
    ud_free(i);
    return UD_OK;
}
ud_status ud_plugin_unload(ud_plugin *p) {
    if (!p)
        return UD_OK;
    if (p->live || p->active)
        return ud_fail(p->ctx, UD_BUSY, "plugin has live instances or callbacks");
    p->active++;
    p->api.shutdown();
    close_library(p->library);
    ud_free(p);
    return UD_OK;
}

struct ud_connection {
    ud_plugin_instance *instance;
    ud_connection_options options;
    const ud_connector_api *api;
    int cursor;
};
static int valid_string(ud_string s, int required) {
    return (!required || s.length) && (!s.length || (s.data && !memchr(s.data, 0, s.length) &&
                                                     ud_utf8_valid(s.data, s.length)));
}
static ud_status authorize_connection(ud_connection *c, int32_t action) {
    if (!c || !c->options.authorize)
        return UD_AUTH;
    return c->options.authorize(c->options.authorize_user, c->options.tenant, c->options.resource,
                                action) == UD_OK
               ? UD_OK
               : ud_fail(c->instance->plugin->ctx, UD_AUTH,
                         "connector operation denied by authorization callback");
}
static ud_status connection_enter(ud_connection *c, int32_t action) {
    if (!c)
        return UD_INVALID;
    UD_TRY(authorize_connection(c, action));
    if (c->instance->busy || c->instance->plugin->active)
        return UD_BUSY;
    if (ud_is_cancelled(c->instance->plugin->ctx))
        return UD_CANCELLED;
    c->instance->busy = 1;
    c->instance->plugin->active++;
    return UD_OK;
}
static void connection_leave(ud_connection *c) {
    c->instance->plugin->active--;
    c->instance->busy = 0;
}
ud_status ud_connection_open(ud_plugin *p, const ud_connection_options *options,
                             ud_connection **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!p || !options || options->struct_size < sizeof(*options) ||
        options->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!valid_string(options->tenant, 1) || !valid_string(options->resource, 1) ||
        !valid_string(options->credential_reference, 1))
        return UD_INVALID;
    if (!options->authorize || !options->resolve ||
        options->authorize(options->authorize_user, options->tenant, options->resource,
                           UD_ACCESS_OPEN) != UD_OK)
        return UD_AUTH;
    const ud_connector_api *api = p->api.connector;
    if (!api)
        return UD_UNSUPPORTED;
    if (api->struct_size < sizeof(*api) || api->abi_version != UD_PLUGIN_ABI || !api->test ||
        !api->discover || !api->cancel)
        return UD_INCOMPATIBLE;
    if ((api->capabilities & UD_CONNECT_READ) &&
        (!api->read_open || !api->read_next || !api->read_close))
        return UD_INCOMPATIBLE;
    if ((api->capabilities & (UD_CONNECT_APPEND | UD_CONNECT_UPSERT | UD_CONNECT_DELETE)) &&
        (!api->write || !api->receipt || !(api->capabilities & UD_CONNECT_RECEIPTS)))
        return UD_INCOMPATIBLE;
    ud_connection *c = ud_alloc(p->ctx, sizeof(*c));
    if (!c)
        return UD_RESOURCE;
    c->options = *options;
    c->options.tenant.data = ud_strdup_n(p->ctx, options->tenant.data, options->tenant.length);
    c->options.resource.data =
        ud_strdup_n(p->ctx, options->resource.data, options->resource.length);
    c->options.credential_reference = (ud_string){0};
    c->api = api;
    ud_string configuration = {0};
    ud_status s =
        !c->options.tenant.data || !c->options.resource.data
            ? UD_RESOURCE
            : options->resolve(options->resolve_user, p->ctx, options->tenant, options->resource,
                               options->credential_reference, &configuration);
    if (!s && !valid_string(configuration, 1))
        s = UD_AUTH;
    if (!s)
        s = ud_plugin_create(p, configuration.data, configuration.length, &c->instance);
    ud_free((void *)configuration.data);
    if (s) {
        ud_free((void *)c->options.tenant.data);
        ud_free((void *)c->options.resource.data);
        ud_free(c);
        return s;
    }
    *out = c;
    return UD_OK;
}
uint64_t ud_connection_capabilities(const ud_connection *c) {
    return c ? c->api->capabilities : 0;
}
ud_status ud_connection_test(ud_connection *c) {
    UD_TRY(connection_enter(c, UD_ACCESS_OPEN));
    ud_status s = c->api->test(c->instance->state);
    connection_leave(c);
    return s;
}
ud_status ud_connection_discover(ud_connection *c, ud_string table, ud_batch **out) {
    if (!out || !valid_string(table, 1))
        return UD_INVALID;
    *out = NULL;
    UD_TRY(connection_enter(c, UD_ACCESS_DISCOVER));
    ud_status s = c->api->discover(c->instance->state, table, out);
    connection_leave(c);
    if (s) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}
ud_status ud_connection_read_open(ud_connection *c, const ud_read_options *options,
                                  const ud_batch *schema) {
    if (!c || !options || options->struct_size < sizeof(*options) ||
        options->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!schema || !valid_string(options->sql, 1) || !options->batch_size ||
        options->batch_size > INT32_MAX || c->cursor)
        return UD_INVALID;
    if (!(c->api->capabilities & UD_CONNECT_READ))
        return UD_UNSUPPORTED;
    UD_TRY(connection_enter(c, UD_ACCESS_READ));
    ud_status s = c->api->read_open(c->instance->state, options, schema);
    connection_leave(c);
    if (!s)
        c->cursor = 1;
    return s;
}
ud_status ud_connection_read_next(ud_connection *c, ud_batch **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!c || !c->cursor)
        return UD_INVALID;
    UD_TRY(connection_enter(c, UD_ACCESS_READ));
    ud_status s = c->api->read_next(c->instance->state, out);
    connection_leave(c);
    if (s) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return s;
}
ud_status ud_connection_read_close(ud_connection *c) {
    if (!c)
        return UD_INVALID;
    if (!c->cursor)
        return UD_OK;
    /* Closing resources is allowed after authorization revocation / cancellation. */
    if (c->instance->busy || c->instance->plugin->active)
        return UD_BUSY;
    c->instance->busy = 1;
    c->instance->plugin->active++;
    ud_status s = c->api->read_close(c->instance->state);
    connection_leave(c);
    if (!s)
        c->cursor = 0;
    return s;
}
ud_status ud_connection_write(ud_connection *c, const ud_write_options *options,
                              const ud_batch *batch) {
    if (!c || !options || options->struct_size < sizeof(*options) ||
        options->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!batch || c->cursor || !valid_string(options->table, 1) || !valid_string(options->job, 1) ||
        !valid_string(options->batch_id, 1) || !valid_string(options->source_position, 0) ||
        (options->key_count && !options->keys))
        return UD_INVALID;
    if (options->key_count > ud_batch_columns(batch))
        return UD_SCHEMA;
    uint64_t cap = options->mode == UD_WRITE_APPEND   ? UD_CONNECT_APPEND
                   : options->mode == UD_WRITE_UPSERT ? UD_CONNECT_UPSERT
                   : options->mode == UD_WRITE_DELETE ? UD_CONNECT_DELETE
                                                      : 0;
    if (!cap || !(c->api->capabilities & cap))
        return UD_UNSUPPORTED;
    if (options->mode != UD_WRITE_APPEND && !options->key_count)
        return UD_INVALID;
    for (uint32_t j = 0; j < options->key_count; j++) {
        if (options->keys[j] >= ud_batch_columns(batch))
            return UD_SCHEMA;
        for (uint32_t k = 0; k < j; k++)
            if (options->keys[j] == options->keys[k])
                return UD_SCHEMA;
    }
    UD_TRY(connection_enter(c, UD_ACCESS_WRITE));
    uint8_t digest[32];
    ud_status s = ud_connection_digest(c, options, batch, digest);
    if (!s)
        s = c->api->write(c->instance->state, c->options.tenant, options, batch, digest);
    connection_leave(c);
    return s;
}
ud_status ud_connection_digest(ud_connection *c, const ud_write_options *options,
                               const ud_batch *batch, uint8_t out[32]) {
    if (!c || !options || !batch || !out)
        return UD_INVALID;
    if (options->struct_size < sizeof(*options) || options->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!valid_string(options->table, 1) || !valid_string(options->job, 1) ||
        !valid_string(options->batch_id, 1) || !valid_string(options->source_position, 0) ||
        options->key_count > batch->columns || (options->key_count && !options->keys))
        return UD_INVALID;
    ud_bytes wire = {.ctx = c->instance->plugin->ctx};
    ud_status s = ud_put_u64(&wire, UD_CONFIG_VERSION);
    ud_string strings[] = {c->options.tenant, c->options.resource, options->table,
                           options->job,      options->batch_id,   options->source_position};
    for (uint32_t j = 0; !s && j < 6; j++)
        s = ud_put_string(&wire, strings[j].data, strings[j].length);
    if (!s)
        s = ud_put_u64(&wire, (uint64_t)options->mode);
    if (!s)
        s = ud_put_u64(&wire, options->key_count);
    for (uint32_t j = 0; !s && j < options->key_count; j++)
        s = ud_put_u64(&wire, options->keys[j]);
    if (!s)
        s = ud_encode_batch(&wire, batch);
    if (!s)
        ud_hash(wire.data, wire.size, out);
    ud_bytes_clear(&wire);
    return s;
}

ud_status ud_connection_receipt(ud_connection *c, ud_string job, ud_string batch_id,
                                ud_commit_receipt *out) {
    if (!out)
        return UD_INVALID;
    memset(out, 0, sizeof(*out));
    if (!c || !valid_string(job, 1) || !valid_string(batch_id, 1))
        return UD_INVALID;
    if (!(c->api->capabilities & UD_CONNECT_RECEIPTS) || !c->api->receipt)
        return UD_UNSUPPORTED;
    UD_TRY(connection_enter(c, UD_ACCESS_RECEIPT));
    ud_status s = c->api->receipt(c->instance->state, c->options.tenant, job, batch_id, out);
    connection_leave(c);
    if (s) {
        ud_free((void *)out->source_position.data);
        memset(out, 0, sizeof(*out));
    }
    return s;
}
void ud_connection_cancel(ud_connection *c) {
    if (c)
        c->api->cancel(c->instance->state);
}
ud_status ud_connection_close(ud_connection *c) {
    if (!c)
        return UD_OK;
    UD_TRY(ud_connection_read_close(c));
    UD_TRY(ud_plugin_instance_destroy(c->instance));
    ud_free((void *)c->options.tenant.data);
    ud_free((void *)c->options.resource.data);
    ud_free(c);
    return UD_OK;
}

ud_status ud_connection_execute_sql(ud_connection *c, ud_string statement) {
    if (!c || !valid_string(statement, 1) || c->cursor)
        return UD_INVALID;
    if (!(c->api->capabilities & UD_CONNECT_EXECUTE_SQL) || !c->api->execute_sql)
        return UD_UNSUPPORTED;
    UD_TRY(connection_enter(c, UD_ACCESS_WRITE));
    ud_status s = c->api->execute_sql(c->instance->state, statement);
    connection_leave(c);
    return s;
}
