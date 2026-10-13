#include "../connection_internal.h"
#include "../../data/internal.h"
#include "../../foundation/thread.h"
#ifdef _WIN32
#include <windows.h>
#else
#include <dlfcn.h>
#endif
typedef struct module_record {
    void *library;
    ud_plugin_api api;
    uint32_t references;
    struct module_record *next;
} module_record;
static ud_mutex modules_lock = UD_MUTEX_INITIALIZER;
static module_record *modules;
struct ud_plugin {
    ud_context *ctx;
    void *library;
    module_record *module;
    ud_plugin_api api;
    ud_host_api host;
    atomic_uint live, active, registrations;
};
typedef struct ud_plugin_instance ud_plugin_instance;
struct ud_plugin_instance {
    ud_plugin *plugin;
    void *state;
    atomic_int busy;
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
    atomic_init(&p->live, 0);
    atomic_init(&p->active, 0);
    atomic_init(&p->registrations, 0);
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
    ud_mutex_lock(&modules_lock);
    module_record *shared = modules;
    while (shared && shared->library != p->library)
        shared = shared->next;
    p->api.struct_size = sizeof(p->api);
    p->api.abi_version = UD_PLUGIN_ABI;
    ud_status s = shared ? UD_OK : entry(UD_PLUGIN_ABI, &p->api);
    if (shared)
        p->api = shared->api;
    int component_only = p->api.roles == UD_PLUGIN_COMPONENTS && p->api.components &&
                         !p->api.initialize && !p->api.destroy;
    if (!s && (p->api.struct_size != sizeof(p->api) || p->api.abi_version != UD_PLUGIN_ABI ||
               !p->api.id || !p->api.version || !p->api.roles ||
               (p->api.roles & ~(UD_PLUGIN_CONNECTOR | UD_PLUGIN_COMPONENTS)) ||
               ((p->api.roles & UD_PLUGIN_COMPONENTS) && !p->api.components) ||
               (!component_only && (!(p->api.roles & UD_PLUGIN_CONNECTOR) || !p->api.connector ||
                                    !p->api.initialize || !p->api.destroy))))
        s = UD_INCOMPATIBLE;
    if (!s && (p->api.capabilities & caps) != caps)
        s = UD_UNSUPPORTED;
    if (!s && !shared) {
        shared = malloc(sizeof(*shared));
        if (!shared)
            s = UD_RESOURCE;
        else {
            *shared = (module_record){.library = p->library, .api = p->api, .next = modules};
            modules = shared;
        }
    }
    if (!s) {
        shared->references++;
        p->module = shared;
    }
    ud_mutex_unlock(&modules_lock);
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
                            ud_is_cancelled,
                            report,
                            ud_value_parse_field,
                            ud_value_format,
                            NULL,
                            NULL, ud_json_parse, ud_json_destroy, ud_json_find, ud_json_text, ud_json_kind,
                            ud_json_size, ud_json_at, ud_json_keys, ud_json_bind_schema,
                            ud_field_from_sql,ud_sql_compile_write,ud_sql_statement_clear};
    *out = p;
    return UD_OK;
}
ud_status ud_plugin_info(const ud_plugin *p, ud_plugin_api *out) {
    if (!p || !out)
        return UD_INVALID;
    if (out->struct_size != sizeof(*out) || out->abi_version != UD_PLUGIN_ABI)
        return UD_INCOMPATIBLE;
    *out = p->api;
    return UD_OK;
}
static ud_status retain_module(void *owner) {
    ud_plugin *p = owner;
    unsigned count = atomic_load(&p->registrations);
    do {
        if (count == UINT32_MAX)
            return UD_RESOURCE;
    } while (!atomic_compare_exchange_weak(&p->registrations, &count, count + 1));
    return UD_OK;
}
static void release_module(void *owner) {
    ud_plugin *p = owner;
    atomic_fetch_sub(&p->registrations, 1);
}
ud_status ud_plugin_register_components(ud_plugin *p, ud_registry *registry) {
    if (!p || !registry)
        return UD_INVALID;
    const ud_component_plugin_api *components = p->api.components;
    if (!components)
        return UD_UNSUPPORTED;
    if (!(p->api.roles & UD_PLUGIN_COMPONENTS))
        return UD_SCHEMA;
    if (components->struct_size != sizeof(*components) || components->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    return ud_registry_register_owned(registry, components->descriptors, components->count, p,
                                      retain_module, release_module);
}
static ud_status connection_instance_create(ud_plugin *p, const char *config, uint32_t n,
                                            ud_plugin_instance **out, void *control_user,
                                            ud_status (*check)(void *)) {
    if (!p || !out || (!config && n))
        return UD_INVALID;
    *out = NULL;
    if (!p->api.initialize)
        return UD_UNSUPPORTED; /* Component-only tables need no dummy factory. */
    ud_plugin_instance *instance = ud_alloc(p->ctx, sizeof(*instance));
    if (!instance)
        return UD_RESOURCE;
    atomic_fetch_add(&p->active, 1);
    ud_host_api host = p->host;
    host.control_user = control_user;
    host.check_io = check;
    ud_status s = p->api.initialize(&host, config ? config : "", n, &instance->state);
    atomic_fetch_sub(&p->active, 1);
    if (s) {
        if (instance->state)
            p->api.destroy(instance->state);
        ud_free(instance);
        return s;
    }
    atomic_init(&instance->busy, 0);
    instance->plugin = p;
    atomic_fetch_add(&p->live, 1);
    *out = instance;
    return UD_OK;
}
static ud_status connection_instance_destroy(ud_plugin_instance *i) {
    if (!i)
        return UD_OK;
    int expected = 0;
    if (!atomic_compare_exchange_strong(&i->busy, &expected, 1))
        return UD_BUSY;
    ud_plugin *p = i->plugin;
    atomic_fetch_add(&p->active, 1);
    p->api.destroy(i->state);
    atomic_fetch_sub(&p->active, 1);
    atomic_fetch_sub(&p->live, 1);
    ud_free(i);
    return UD_OK;
}
ud_status ud_plugin_unload(ud_plugin *p) {
    if (!p)
        return UD_OK;
    if (atomic_load(&p->live) || atomic_load(&p->active) || atomic_load(&p->registrations))
        return ud_fail(p->ctx, UD_BUSY, "plugin has live instances or callbacks");
    ud_mutex_lock(&modules_lock);
    module_record *module = p->module;
    if (!--module->references) {
        module_record **at = &modules;
        while (*at && *at != module)
            at = &(*at)->next;
        if (*at)
            *at = module->next;
        /* Entry/shutdown are metadata lifecycle callbacks: non-reentrant with
         * load/unload. This is never the scheduler, Journal or event lock. */
        if (module->api.shutdown)
            module->api.shutdown();
        close_library(p->library);
        free(module);
    } else
        close_library(p->library);
    ud_mutex_unlock(&modules_lock);
    ud_free(p);
    return UD_OK;
}

struct ud_connection {
    ud_plugin_instance *instance;
    ud_connection_options options;
    const ud_connector_api *api;
    uint64_t capabilities;
    int cursor, transaction;
};
static int valid_string(ud_string s, int required) {
    return (!required || s.length) && (!s.length || (s.data && !memchr(s.data, 0, s.length) &&
                                                     ud_utf8_valid(s.data, s.length)));
}
static ud_status authorize_connection(ud_connection *c, int32_t action) {
    if (!c || !c->options.authorize)
        return UD_AUTH;
    if (c->options.check)
        UD_TRY(c->options.check(c->options.authorize_user));
    ud_status grant = c->options.authorize(c->options.authorize_user, c->options.tenant,
                                           c->options.resource, action);
    if (grant == UD_OK)
        return UD_OK;
    if (c->options.check)
        UD_TRY(c->options.check(c->options.authorize_user));
    return ud_fail(c->instance->plugin->ctx, UD_AUTH,
                   "connector operation denied by authorization callback");
}
static ud_status connection_enter(ud_connection *c, int32_t action) {
    if (!c)
        return UD_INVALID;
    UD_TRY(authorize_connection(c, action));
    int expected = 0;
    if (!atomic_compare_exchange_strong(&c->instance->busy, &expected, 1))
        return UD_BUSY;
    if (ud_is_cancelled(c->instance->plugin->ctx)) {
        atomic_store(&c->instance->busy, 0);
        return UD_CANCELLED;
    }
    atomic_fetch_add(&c->instance->plugin->active, 1);
    return UD_OK;
}
static void connection_leave(ud_connection *c) {
    atomic_fetch_sub(&c->instance->plugin->active, 1);
    atomic_store(&c->instance->busy, 0);
}
static ud_status validate_driver(ud_plugin *p) {
    const ud_connector_api *api = p->api.connector;
    if (!api)
        return UD_UNSUPPORTED;
    if (api->struct_size != sizeof(*api) || api->abi_version != UD_PLUGIN_ABI || !api->test ||
        !api->discover || !api->cancel || !api->set_control)
        return UD_INCOMPATIBLE;
    if ((api->capabilities & UD_CONNECT_READ) &&
        (!api->read_open || !api->read_next || !api->read_close))
        return UD_INCOMPATIBLE;
    if ((api->capabilities & (UD_CONNECT_APPEND | UD_CONNECT_UPSERT | UD_CONNECT_DELETE)) &&
        (!api->write || !api->receipt || !(api->capabilities & UD_CONNECT_RECEIPTS)))
        return UD_INCOMPATIBLE;
    if ((api->capabilities & UD_CONNECT_TRANSACTION) &&
        (!api->begin || !api->commit || !api->rollback))
        return UD_INCOMPATIBLE;
    if ((api->capabilities & UD_CONNECT_METADATA) && !api->metadata)
        return UD_INCOMPATIBLE;
    if ((api->capabilities & UD_CONNECT_POOL) && !api->reset_session)
        return UD_INCOMPATIBLE;
    return UD_OK;
}
ud_status ud_connection_open_resolved(ud_plugin *p, const ud_connection_options *options,
                                      ud_string configuration, ud_connection **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!p || !options || options->struct_size != sizeof(*options) ||
        options->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!valid_string(options->tenant, 1) || !valid_string(options->resource, 1) ||
        !valid_string(configuration, 1))
        return UD_INVALID;
    if (options->check)
        UD_TRY(options->check(options->authorize_user));
    if (!options->authorize || options->authorize(options->authorize_user, options->tenant,
                                                  options->resource, UD_ACCESS_OPEN) != UD_OK) {
        if (options->check)
            UD_TRY(options->check(options->authorize_user));
        return UD_AUTH;
    }
    UD_TRY(validate_driver(p));
    ud_connection *c = ud_alloc(p->ctx, sizeof(*c));
    if (!c)
        return UD_RESOURCE;
    c->options = *options;
    c->options.tenant.data = ud_strdup_n(p->ctx, options->tenant.data, options->tenant.length);
    c->options.resource.data =
        ud_strdup_n(p->ctx, options->resource.data, options->resource.length);
    c->options.credential_reference = (ud_string){0};
    c->api = p->api.connector;
    ud_status status =
        !c->options.tenant.data || !c->options.resource.data
            ? UD_RESOURCE
            : connection_instance_create(p, configuration.data, configuration.length, &c->instance,
                                         options->authorize_user, options->check);
    if (status) {
        ud_free((void *)c->options.tenant.data);
        ud_free((void *)c->options.resource.data);
        ud_free(c);
        return status;
    }
    c->capabilities=c->api->session_capabilities?c->api->session_capabilities(c->instance->state):c->api->capabilities;
    *out = c;
    return UD_OK;
}
ud_status ud_connection_open(ud_plugin *p, const ud_connection_options *options,
                             ud_connection **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!p || !options || options->struct_size != sizeof(*options) ||
        options->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!valid_string(options->credential_reference, 1))
        return UD_INVALID;
    if (options->check)
        UD_TRY(options->check(options->authorize_user));
    if (!options->authorize || !options->resolve ||
        options->authorize(options->authorize_user, options->tenant, options->resource,
                           UD_ACCESS_OPEN) != UD_OK) {
        if (options->check)
            UD_TRY(options->check(options->authorize_user));
        return UD_AUTH;
    }
    UD_TRY(validate_driver(p));
    ud_string configuration = {0};
    ud_status status =
        options->resolve(options->resolve_user, p->ctx, options->tenant, options->resource,
                         options->credential_reference, &configuration);
    if (!status)
        status = ud_connection_open_resolved(p, options, configuration, out);
    ud_free((void *)configuration.data);
    return status;
}
int ud_connection_reusable(const ud_connection *c) {
    return c && (c->capabilities & UD_CONNECT_POOL) && !c->cursor && !c->transaction &&
           !atomic_load(&c->instance->busy);
}
ud_status ud_connection_reset_session(ud_connection *c) {
    if (!ud_connection_reusable(c))
        return UD_UNSUPPORTED;
    int expected = 0;
    if (!atomic_compare_exchange_strong(&c->instance->busy, &expected, 1))
        return UD_BUSY;
    atomic_fetch_add(&c->instance->plugin->active, 1);
    ud_status status = c->api->reset_session(c->instance->state);
    connection_leave(c);
    return status;
}
ud_status ud_connection_bind_lease(ud_connection *c, void *user, ud_authorize_fn authorize,
                                   ud_status (*check)(void *)) {
    if (!c || atomic_load(&c->instance->busy))
        return UD_BUSY;
    c->options.authorize_user = user;
    c->options.authorize = authorize;
    c->api->set_control(c->instance->state, user, check);
    return UD_OK;
}
ud_status ud_connection_begin(ud_connection *c, int32_t isolation) {
    if (!c || c->transaction || c->cursor || isolation < UD_ISOLATION_READ_COMMITTED ||
        isolation > UD_ISOLATION_SERIALIZABLE)
        return UD_INVALID;
    if (!(c->capabilities & UD_CONNECT_TRANSACTION))
        return UD_UNSUPPORTED;
    UD_TRY(connection_enter(c, UD_ACCESS_WRITE));
    ud_status status = c->api->begin(c->instance->state, isolation);
    if (!status)
        c->transaction = 1;
    connection_leave(c);
    return status;
}
ud_status ud_connection_commit(ud_connection *c) {
    if (!c || !c->transaction || c->cursor)
        return UD_INVALID;
    UD_TRY(connection_enter(c, UD_ACCESS_WRITE));
    ud_status status = c->api->commit(c->instance->state);
    if (!status)
        c->transaction = 0;
    connection_leave(c);
    return status ? UD_COMMIT_UNKNOWN : UD_OK;
}
ud_status ud_connection_rollback(ud_connection *c) {
    if (!c || !c->transaction)
        return UD_INVALID;
    UD_TRY(ud_connection_read_close(c));
    int expected = 0;
    if (!atomic_compare_exchange_strong(&c->instance->busy, &expected, 1))
        return UD_BUSY;
    atomic_fetch_add(&c->instance->plugin->active, 1);
    ud_status status = c->api->rollback(c->instance->state);
    if (!status)
        c->transaction = 0;
    connection_leave(c);
    return status ? UD_COMMIT_UNKNOWN : UD_OK;
}
ud_status ud_connection_metadata(ud_connection *c, const ud_metadata_options *options,
                                 ud_batch **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!c || !options || options->struct_size != sizeof(*options) ||
        options->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!(c->capabilities & UD_CONNECT_METADATA))
        return UD_UNSUPPORTED;
    if (!options->limit || options->limit > 65536 || options->kind < UD_METADATA_CATALOGS ||
        options->kind > UD_METADATA_INDEXES || !valid_string(options->parent, 0))
        return UD_INVALID;
    UD_TRY(connection_enter(c, UD_ACCESS_DISCOVER));
    ud_status status = c->api->metadata(c->instance->state, options, out);
    connection_leave(c);
    if (status) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    return status;
}
uint64_t ud_connection_capabilities(const ud_connection *c) {
    return c ? c->capabilities : 0;
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
ud_status ud_connection_schema(ud_connection *connection, ud_string table, ud_string scope,
                               ud_batch **out) {
    if (!connection || !out)
        return UD_INVALID;
    *out = NULL;
    ud_batch *metadata = NULL;
    ud_status status = ud_connection_discover(connection, table, &metadata);
    if (!status)
        status = ud_schema_from_metadata(connection->instance->plugin->ctx, metadata, scope,
                                         connection->api->metadata_dialect, out);
    ud_batch_destroy(metadata);
    return status;
}
ud_status ud_connection_read_open(ud_connection *c, const ud_read_options *options,
                                  const ud_batch *schema) {
    if (!c || !options || options->struct_size != sizeof(*options) ||
        options->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!schema || !valid_string(options->sql, 1) || !options->batch_size ||
        options->batch_size > INT32_MAX || c->cursor)
        return UD_INVALID;
    if (!(c->capabilities & UD_CONNECT_READ))
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
    int expected = 0;
    if (!atomic_compare_exchange_strong(&c->instance->busy, &expected, 1))
        return UD_BUSY;
    atomic_fetch_add(&c->instance->plugin->active, 1);
    ud_status s = c->api->read_close(c->instance->state);
    connection_leave(c);
    if (!s)
        c->cursor = 0;
    return s;
}
ud_status ud_connection_write(ud_connection *c, const ud_write_options *options,
                              const ud_batch *batch) {
    if (!c || !options || options->struct_size != sizeof(*options) ||
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
                   : options->mode == UD_WRITE_CHANGESET ? UD_CONNECT_CHANGESET
                                                      : 0;
    if (!cap || !(c->capabilities & cap))
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
    if (options->mode == UD_WRITE_CHANGESET) {
        if(c->transaction) return UD_UNSUPPORTED;
        if(!options->operations || !batch->sealed) return UD_SCHEMA;
        if(options->before && (!options->before->sealed || options->before->columns!=batch->columns || options->before->rows!=batch->rows)) return UD_SCHEMA;
        if(options->current_column!=UINT32_MAX && (options->current_column>=batch->columns || batch->column[options->current_column].field.type!=UD_BOOL)) return UD_SCHEMA;
        for(uint32_t j=0;options->before && j<batch->columns;j++) if(!ud_field_matches(&batch->column[j].field,&options->before->column[j].field,UD_FIELD_VALUE_TYPE)) return UD_SCHEMA;
        for(uint32_t row=0;row<batch->rows;row++) if(options->operations[row]<UD_CHANGE_INSERT || options->operations[row]>UD_CHANGE_UPSERT) return UD_SCHEMA;
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
    if (options->struct_size != sizeof(*options) || options->abi_version != UD_ABI_VERSION)
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
    if(!s&&options->mode==UD_WRITE_CHANGESET) {
        if(!options->operations || (options->before && options->before->rows!=batch->rows)) s=UD_SCHEMA;
        if(!s)s=ud_put_u64(&wire,options->current_column);
        for(uint32_t row=0;!s&&row<batch->rows;row++)s=ud_put_u64(&wire,(uint64_t)options->operations[row]);
        if(!s)s=ud_put_u64(&wire, options->before ? 1 : 0);
        if(!s && options->before)s=ud_encode_batch(&wire,options->before);
    }
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
    if (!(c->capabilities & UD_CONNECT_RECEIPTS) || !c->api->receipt)
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
    if (c->transaction)
        UD_TRY(ud_connection_rollback(c));
    UD_TRY(connection_instance_destroy(c->instance));
    ud_free((void *)c->options.tenant.data);
    ud_free((void *)c->options.resource.data);
    ud_free(c);
    return UD_OK;
}

ud_status ud_connection_execute_command(ud_connection *c,const ud_read_options *options,uint64_t *processed) {
    if(processed)*processed=0;
    if(!options||options->struct_size!=sizeof(*options)||options->abi_version!=UD_ABI_VERSION)return UD_INCOMPATIBLE;
    ud_string statement=options->sql;
    if(options->parameters&&!options->parameters->sealed)return UD_INVALID;
    if (!c || !valid_string(statement, 1) || c->cursor)
        return UD_INVALID;
    if (!(c->capabilities & UD_CONNECT_EXECUTE_SQL) || !c->api->execute)
        return UD_UNSUPPORTED;
    UD_TRY(connection_enter(c, UD_ACCESS_WRITE));
    ud_status s = c->api->execute(c->instance->state, options, processed);
    connection_leave(c);
    return s;
}
ud_status ud_connection_provision(ud_connection *c){
    if(!c||!(c->capabilities&UD_CONNECT_PROVISION)||!c->api->provision)return UD_UNSUPPORTED;
    UD_TRY(connection_enter(c,UD_ACCESS_WRITE));ud_status status=c->api->provision(c->instance->state);connection_leave(c);return status;
}
