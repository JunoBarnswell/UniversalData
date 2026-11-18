#include "ud/format.h"
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
    ud_host_api host;
    atomic_int busy;
    /* One physical connection owns one operation at a time. Composite record
     * commands retain this gate across their complete transaction protocol. */
    ud_mutex operation_lock;
    ud_condition operation_changed;
    atomic_uintptr_t operation_owner;
};
static _Thread_local unsigned char operation_thread;
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
               p->api.field_size!=sizeof(ud_field) || p->api.value_size!=sizeof(ud_value) ||
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
                            NULL,
                            ud_json_parse,
                            ud_json_destroy,
                            ud_json_find,
                            ud_json_text,
                            ud_json_kind,
                            ud_json_size,
                            ud_json_at,
                            ud_json_keys,
                            ud_json_bind_schema,
                            ud_field_from_sql,
                            ud_sql_parameter_limit,
                            ud_sql_compile_write,
                            ud_sql_statement_clear,
                            ud_value_compare,
                            ud_sql_compile_preimage,
                            ud_schema_from_metadata,
                            ud_sql_bulk_open,
                            ud_sql_bulk_read,
                            ud_sql_bulk_close,
                            ud_sql_compile_bulk,
                            ud_sql_table_identifier,
                            ud_mysql_json_decode,
                            ud_field_matches,
                            ud_sql_compile_scan,
                            ud_sql_compile_bulk_release,
                            ud_sql_compile_stage,
                            ud_random_bytes,
                            ud_schema_compare,
                            ud_schema_map,
                            ud_schema_bind_identity,
                            ud_sql_record_target,{0}};
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
                                            ud_status (*check)(void *),ud_string resource_namespace,
                                            ud_string resource,
                                            ud_status (*authorize_endpoint)(void *, ud_string)) {
    if (!p || !out || (!config && n))
        return UD_INVALID;
    *out = NULL;
    if (!p->api.initialize)
        return UD_UNSUPPORTED; /* Component-only tables need no dummy factory. */
    ud_plugin_instance *instance = ud_alloc(p->ctx, sizeof(*instance));
    if (!instance)
        return UD_RESOURCE;
    atomic_fetch_add(&p->active, 1);
    instance->host = p->host;
    instance->host.control_user = control_user;
    instance->host.check_io = check;
    instance->host.resource_namespace = resource_namespace;
    instance->host.connection_resource = resource;
    instance->host.authorize_endpoint = authorize_endpoint;
    ud_status s = p->api.initialize(&instance->host, config ? config : "", n, &instance->state);
    atomic_fetch_sub(&p->active, 1);
    if (s) {
        if (instance->state)
            p->api.destroy(instance->state);
        ud_free(instance);
        return s;
    }
    atomic_init(&instance->busy, 0);
    atomic_init(&instance->operation_owner,0);
    ud_mutex_init(&instance->operation_lock);
    ud_condition_init(&instance->operation_changed);
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
    ud_condition_destroy(&i->operation_changed);
    ud_mutex_destroy(&i->operation_lock);
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
    int cursor, transaction, cdc, cdc_transaction, io_action, cleanup;
    ud_string message_source, message_confirmation;
    void *endpoint_user;
    ud_authorize_fn endpoint_grant;
    char resource_namespace[33];
    char transaction_branch[65];
    uint8_t transaction_digest[32];
    uint32_t cdc_batch_rows;
    uint64_t cdc_version;
    ud_batch *cdc_schema;
    ud_context *ctx;
    int32_t dialect;
    uint32_t operation_depth;
    int lease_operation;
    void *saved_control_user;
    ud_authorize_fn saved_authorize;
    ud_status (*saved_check)(void *);
};
static int valid_string(ud_string s, int required) {
    return (!required || s.length) && (!s.length || (s.data && !memchr(s.data, 0, s.length) &&
                                                     ud_utf8_valid(s.data, s.length)));
}
static ud_status authorize_resource(ud_connection *c, ud_string resource, int32_t action) {
    if (!c || !c->options.authorize)
        return UD_AUTH;
    if (!c->cleanup && c->options.check)
        UD_TRY(c->options.check(c->options.authorize_user));
    ud_status grant = c->options.authorize(c->options.authorize_user, c->options.tenant, resource,
                                           action);
    if (grant == UD_OK)
        return UD_OK;
    if (!c->cleanup && c->options.check)
        UD_TRY(c->options.check(c->options.authorize_user));
    return ud_fail(c->ctx, UD_AUTH, "connector operation denied by authorization callback");
}
static ud_status authorize_connection(ud_connection *c, int32_t action) {
    return c ? authorize_resource(c, c->options.resource, action) : UD_AUTH;
}
static ud_status authorize_endpoint(void *user, ud_string endpoint) {
    ud_connection *c = user;
    if (!c->endpoint_grant || !valid_string(endpoint, 1))
        return UD_AUTH;
    return c->endpoint_grant(c->endpoint_user, c->options.tenant, endpoint, UD_ACCESS_NETWORK) ==
                   UD_OK
               ? UD_OK
               : UD_AUTH;
}
static ud_status connection_check(void *user) {
    ud_connection *c = user;
    if (c->cleanup)
        return UD_OK;
    if (c->options.check)
        UD_TRY(c->options.check(c->options.authorize_user));
    return c->io_action ? authorize_connection(c, c->io_action) : UD_OK;
}
static ud_status operation_acquire(ud_connection *connection,void *user,ud_status (*check)(void *),int cleanup) {
    ud_plugin_instance *instance = connection->instance;
    uintptr_t owner = (uintptr_t)&operation_thread;
    if (atomic_load(&instance->operation_owner) == owner) return UD_BUSY;
    ud_mutex_lock(&instance->operation_lock);
    for (;;) {
        ud_status status = !cleanup && ud_is_cancelled(instance->plugin->ctx) ? UD_CANCELLED : UD_OK;
        if (!status && check) {
            /* Execution observers can run in this check; never call the host
             * while holding the physical-session mutex. */
            ud_mutex_unlock(&instance->operation_lock);
            status = check(user);
            ud_mutex_lock(&instance->operation_lock);
        }
        if (status) { ud_mutex_unlock(&instance->operation_lock); return status; }
        int expected = 0;
        if (atomic_compare_exchange_strong(&instance->busy,&expected,1)) break;
        ud_condition_wait_ms(&instance->operation_changed,&instance->operation_lock,25);
    }
    atomic_store(&instance->operation_owner,owner);
    connection->operation_depth = 1;
    ud_mutex_unlock(&instance->operation_lock);
    return UD_OK;
}
static void operation_release(ud_connection *connection) {
    if (connection->operation_depth > 1) { connection->operation_depth--; return; }
    ud_plugin_instance *instance = connection->instance;
    ud_mutex_lock(&instance->operation_lock);
    connection->operation_depth = 0;
    atomic_store(&instance->operation_owner,0); atomic_store(&instance->busy,0);
    ud_condition_wake(&instance->operation_changed); ud_mutex_unlock(&instance->operation_lock);
}
static ud_status connection_enter(ud_connection *connection,int32_t action) {
    if (!connection) return UD_INVALID;
    if (atomic_load(&connection->instance->operation_owner) == (uintptr_t)&operation_thread &&
        connection->lease_operation && connection->operation_depth == 1) connection->operation_depth++;
    else UD_TRY(operation_acquire(connection,connection->cleanup ? NULL : connection->options.authorize_user,
                                  connection->cleanup ? NULL : connection->options.check,connection->cleanup));
    ud_status status = action == UD_ACCESS_WRITE &&
                               connection->transaction == UD_ISOLATION_READ_SNAPSHOT
                           ? UD_UNSUPPORTED
                           : authorize_connection(connection, action);
    if (status) { operation_release(connection); return status; }
    connection->io_action = action;
    connection->api->set_control(connection->instance->state,connection,connection_check);
    atomic_fetch_add(&connection->instance->plugin->active,1);
    return UD_OK;
}
static void connection_leave(ud_connection *connection) {
    atomic_fetch_sub(&connection->instance->plugin->active,1);
    operation_release(connection);
}
static ud_status connection_cleanup_enter(ud_connection *connection) {
    if (atomic_load(&connection->instance->operation_owner) == (uintptr_t)&operation_thread &&
        connection->lease_operation && connection->operation_depth == 1) connection->operation_depth++;
    else UD_TRY(operation_acquire(connection,NULL,NULL,1));
    atomic_fetch_add(&connection->instance->plugin->active,1); return UD_OK;
}
ud_status ud_connection_operation_begin(ud_connection *connection,void *user,ud_authorize_fn authorize,
                                         ud_status (*check)(void *)) {
    if (!connection || !authorize) return UD_INVALID;
    UD_TRY(operation_acquire(connection,user,check,check == NULL));
    connection->saved_control_user = connection->options.authorize_user;
    connection->saved_authorize = connection->options.authorize;
    connection->saved_check = connection->options.check;
    connection->options.authorize_user = user; connection->options.authorize = authorize;
    connection->options.check = check; connection->lease_operation = 1;
    atomic_fetch_add(&connection->instance->plugin->active,1);
    return UD_OK;
}
void ud_connection_operation_end(ud_connection *connection) {
    connection->options.authorize_user = connection->saved_control_user;
    connection->options.authorize = connection->saved_authorize;
    connection->options.check = connection->saved_check;
    connection->lease_operation = 0; connection->io_action = 0;
    connection->api->set_control(connection->instance->state,connection,connection_check);
    connection_leave(connection);
}
static ud_status validate_driver(ud_plugin *p) {
    const ud_connector_api *api = p->api.connector;
    if (!api)
        return UD_UNSUPPORTED;
    if (api->struct_size != sizeof(*api) || api->abi_version != UD_PLUGIN_ABI || !api->test ||
        !api->cancel || !api->set_control)
        return UD_INCOMPATIBLE;
    if ((api->capabilities & UD_CONNECT_READ) &&
        (!api->discover || !api->read_open || !api->read_next || !api->read_close))
        return UD_INCOMPATIBLE;
    if ((api->capabilities & (UD_CONNECT_APPEND | UD_CONNECT_UPSERT | UD_CONNECT_DELETE)) &&
        (!api->write || !api->receipt || !(api->capabilities & UD_CONNECT_RECEIPTS)))
        return UD_INCOMPATIBLE;
    if ((api->capabilities & UD_CONNECT_TRANSACTION) &&
        (!api->begin || !api->commit || !api->rollback))
        return UD_INCOMPATIBLE;
    if ((api->capabilities & UD_CONNECT_READ_SNAPSHOT) &&
        (!(api->capabilities & UD_CONNECT_READ) || !api->begin || !api->rollback))
        return UD_INCOMPATIBLE;
    if ((api->capabilities & UD_CONNECT_TRANSACTIONAL_DML) &&
        (!(api->capabilities & UD_CONNECT_TRANSACTION) || !api->savepoint)) return UD_INCOMPATIBLE;
    if ((api->capabilities & UD_CONNECT_TRANSACTION_PARTICIPANT) &&
        (!(api->capabilities & UD_CONNECT_TRANSACTION) || !api->transaction_begin ||
         !api->transaction_prepare || !api->transaction_resolve || !api->transaction_recover))
        return UD_INCOMPATIBLE;
    if ((api->capabilities & UD_CONNECT_METADATA) && !api->metadata)
        return UD_INCOMPATIBLE;
    if ((api->capabilities & UD_CONNECT_CDC) &&
        (!api->cdc_open || !api->cdc_next || !api->cdc_confirm || !api->cdc_close))
        return UD_INCOMPATIBLE;
    if ((api->capabilities & UD_CONNECT_POOL) && !api->reset_session)
        return UD_INCOMPATIBLE;
    uint64_t messaging = api->capabilities & (UD_CONNECT_MESSAGE_SUBSCRIBE | UD_CONNECT_MESSAGE_PUBLISH);
    if (messaging) {
        const ud_message_api *message = api->message;
        if (!message || message->struct_size != sizeof(*message) ||
            message->abi_version != UD_PLUGIN_ABI || (api->capabilities & UD_CONNECT_POOL))
            return UD_INCOMPATIBLE;
        if ((messaging & UD_CONNECT_MESSAGE_SUBSCRIBE) &&
            (!message->subscribe || !message->receive || !message->acknowledge || !message->unsubscribe))
            return UD_INCOMPATIBLE;
        if ((messaging & UD_CONNECT_MESSAGE_PUBLISH) && !message->publish)
            return UD_INCOMPATIBLE;
        if ((api->capabilities & UD_CONNECT_MESSAGE_TRANSACTION) &&
            (messaging != (UD_CONNECT_MESSAGE_SUBSCRIBE | UD_CONNECT_MESSAGE_PUBLISH) ||
             !message->capture_epoch || !message->begin_epoch || !message->commit_epoch ||
             !message->abort_epoch || !message->release_epoch || !message->visit_committed ||
             !message->verify_epoch))
            return UD_INCOMPATIBLE;
    }
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
    c->ctx = p->ctx;
    c->endpoint_user = options->authorize_user;
    c->endpoint_grant = options->authorize;
    c->io_action = UD_ACCESS_OPEN;
    c->options.tenant.data = ud_strdup_n(p->ctx, options->tenant.data, options->tenant.length);
    c->options.resource.data =
        ud_strdup_n(p->ctx, options->resource.data, options->resource.length);
    c->options.credential_reference = (ud_string){0};
    c->api = p->api.connector;
    ud_id physical={0};
    ud_status status=!c->options.tenant.data || !c->options.resource.data ? UD_RESOURCE :
        ud_id_make(p->ctx,c->options.tenant,c->options.resource,&physical);
    if (!status) {
        ud_id_format(physical,c->resource_namespace);
        status=connection_instance_create(p,configuration.data,configuration.length,&c->instance,c,connection_check,
            (ud_string){c->resource_namespace,32}, c->options.resource, authorize_endpoint);
    }
    if (status) {
        ud_free((void *)c->options.tenant.data);
        ud_free((void *)c->options.resource.data);
        ud_free(c);
        return status;
    }
    c->capabilities = c->api->session_capabilities
                          ? c->api->session_capabilities(c->instance->state)
                          : c->api->capabilities;
    c->dialect = c->api->session_dialect ? c->api->session_dialect(c->instance->state)
                                         : c->api->metadata_dialect;
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
    return c && (c->capabilities & UD_CONNECT_POOL) && !c->cursor && !c->transaction && !c->cdc &&
           !c->transaction_branch[0] && !atomic_load(&c->instance->busy);
}
ud_status ud_connection_reset_session(ud_connection *c) {
    if (!ud_connection_reusable(c))
        return UD_UNSUPPORTED;
    UD_TRY(connection_cleanup_enter(c));
    ud_status status = c->api->reset_session(c->instance->state);
    connection_leave(c);
    return status;
}
ud_status ud_connection_bind_lease(ud_connection *c, void *user, ud_authorize_fn authorize,
                                   ud_status (*check)(void *)) {
    if (!c) return UD_INVALID;
    ud_mutex_lock(&c->instance->operation_lock);
    if (atomic_load(&c->instance->busy)) { ud_mutex_unlock(&c->instance->operation_lock); return UD_BUSY; }
    c->options.authorize_user = user;
    c->options.authorize = authorize;
    c->options.check = check;
    c->io_action = 0;
    c->api->set_control(c->instance->state, c, connection_check);
    ud_mutex_unlock(&c->instance->operation_lock);
    return UD_OK;
}
ud_status ud_connection_begin(ud_connection *c, int32_t isolation) {
    if (!c || c->transaction || c->cursor || c->cdc || isolation < UD_ISOLATION_READ_COMMITTED ||
        isolation > UD_ISOLATION_READ_SNAPSHOT)
        return UD_INVALID;
    int snapshot = isolation == UD_ISOLATION_READ_SNAPSHOT;
    if (!(c->capabilities & (snapshot ? UD_CONNECT_READ_SNAPSHOT : UD_CONNECT_TRANSACTION)))
        return UD_UNSUPPORTED;
    UD_TRY(connection_enter(c, snapshot ? UD_ACCESS_READ : UD_ACCESS_WRITE));
    ud_status status = c->api->begin(c->instance->state, isolation);
    if (!status)
        c->transaction = snapshot ? UD_ISOLATION_READ_SNAPSHOT : 1;
    connection_leave(c);
    return status;
}
ud_status ud_connection_commit(ud_connection *c) {
    if (c && c->transaction_branch[0])
        return UD_UNSUPPORTED;
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
    if (c && c->transaction_branch[0])
        return UD_UNSUPPORTED;
    if (!c || !c->transaction)
        return UD_INVALID;
    UD_TRY(ud_connection_read_close(c));
    UD_TRY(connection_cleanup_enter(c));
    ud_status status = c->api->rollback(c->instance->state);
    if (!status)
        c->transaction = 0;
    connection_leave(c);
    return status ? UD_COMMIT_UNKNOWN : UD_OK;
}
static ud_status transaction_branch_check(const ud_transaction_branch *branch) {
    if (!branch || branch->struct_size != sizeof(*branch) || branch->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!branch->id.data || branch->id.length != 64 ||
        branch->isolation < UD_ISOLATION_READ_COMMITTED ||
        branch->isolation > UD_ISOLATION_SERIALIZABLE)
        return UD_INVALID;
    for (uint32_t index = 0; index < 64; index++) {
        char ch = branch->id.data[index];
        if (!(ch >= '0' && ch <= '9') && !(ch >= 'a' && ch <= 'f'))
            return UD_INVALID;
    }
    return UD_OK;
}
static ud_status transaction_binding_check(ud_connection *connection,
                                           const ud_transaction_branch *branch) {
    UD_TRY(transaction_branch_check(branch));
    if (!connection || !(connection->capabilities & UD_CONNECT_TRANSACTION_PARTICIPANT))
        return UD_UNSUPPORTED;
    if (connection->transaction_branch[0] &&
        (memcmp(connection->transaction_branch, branch->id.data, 64) ||
         memcmp(connection->transaction_digest, branch->digest, 32)))
        return UD_FENCED;
    if (connection->cursor || connection->cdc)
        return UD_BUSY;
    return UD_OK;
}
ud_status ud_connection_transaction_begin(ud_connection *connection,
                                          const ud_transaction_branch *branch) {
    UD_TRY(transaction_binding_check(connection, branch));
    if (connection->transaction || connection->transaction_branch[0])
        return UD_BUSY;
    UD_TRY(connection_enter(connection, UD_ACCESS_WRITE));
    ud_status status = connection->api->transaction_begin(connection->instance->state, branch);
    if (!status) {
        memcpy(connection->transaction_branch, branch->id.data, 64);
        memcpy(connection->transaction_digest, branch->digest, 32);
        connection->transaction = 1;
    }
    connection_leave(connection);
    return status;
}
ud_status ud_connection_transaction_prepare(ud_connection *connection,
                                            const ud_transaction_branch *branch) {
    UD_TRY(transaction_binding_check(connection, branch));
    if (!connection->transaction || !connection->transaction_branch[0])
        return UD_INVALID;
    UD_TRY(connection_enter(connection, UD_ACCESS_WRITE));
    ud_status status = connection->api->transaction_prepare(connection->instance->state, branch);
    if (!status)
        connection->transaction = 0; /* Prepared effects survive transport destruction. */
    connection_leave(connection);
    return status;
}
ud_status ud_connection_transaction_recover(ud_connection *connection,
                                            const ud_transaction_branch *branch, int32_t *state) {
    if (!state)
        return UD_INVALID;
    *state = UD_TX_UNKNOWN;
    UD_TRY(transaction_binding_check(connection, branch));
    UD_TRY(connection_enter(connection, UD_ACCESS_READ));
    ud_status status =
        connection->api->transaction_recover(connection->instance->state, branch, state);
    if (!status && (*state < UD_TX_UNKNOWN || *state > UD_TX_NOT_FOUND))
        status = UD_CORRUPT;
    connection_leave(connection);
    return status;
}
ud_status ud_connection_transaction_resolve(ud_connection *connection,
                                            const ud_transaction_branch *branch, int32_t decision) {
    if (decision != UD_TX_COMMIT && decision != UD_TX_ROLLBACK)
        return UD_INVALID;
    UD_TRY(transaction_binding_check(connection, branch));
    connection->cleanup = 1;
    ud_status entered = connection_enter(connection, UD_ACCESS_WRITE);
    if (entered) {
        connection->cleanup = 0;
        return entered;
    }
    int32_t state = UD_TX_UNKNOWN;
    ud_status status =
        connection->api->transaction_recover(connection->instance->state, branch, &state);
    if (!status && (state < UD_TX_UNKNOWN || state > UD_TX_NOT_FOUND))
        status = UD_CORRUPT;
    if (!status &&
        (state == UD_TX_UNKNOWN || (decision == UD_TX_COMMIT && state == UD_TX_NOT_FOUND)))
        status = UD_COMMIT_UNKNOWN;
    if (!status && decision == UD_TX_COMMIT && state == UD_TX_ACTIVE)
        status = UD_FENCED;
    if (!status && ((decision == UD_TX_COMMIT && state == UD_TX_ROLLED_BACK) ||
                    (decision == UD_TX_ROLLBACK && state == UD_TX_COMMITTED)))
        status = UD_FENCED;
    if (!status &&
        ((decision == UD_TX_COMMIT && state != UD_TX_COMMITTED) ||
         (decision == UD_TX_ROLLBACK && state != UD_TX_ROLLED_BACK && state != UD_TX_NOT_FOUND))) {
        status =
            connection->api->transaction_resolve(connection->instance->state, branch, decision);
        if (!status)
            status =
                connection->api->transaction_recover(connection->instance->state, branch, &state);
    }
    if (!status && (state < UD_TX_UNKNOWN || state > UD_TX_NOT_FOUND))
        status = UD_CORRUPT;
    if (!status &&
        ((decision == UD_TX_COMMIT && state != UD_TX_COMMITTED) ||
         (decision == UD_TX_ROLLBACK && state != UD_TX_ROLLED_BACK && state != UD_TX_NOT_FOUND)))
        status = UD_COMMIT_UNKNOWN;
    if (!status) {
        connection->transaction = 0;
        memset(connection->transaction_branch, 0, sizeof(connection->transaction_branch));
        memset(connection->transaction_digest, 0, sizeof(connection->transaction_digest));
    }
    connection_leave(connection);
    connection->cleanup = 0;
    return status;
}
ud_status ud_connection_metadata(ud_connection *c, const ud_metadata_options *options,
                                 ud_batch **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!c || !options || options->struct_size != sizeof(*options) ||
        options->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (c->cdc)
        return UD_BUSY;
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
    if (c && (c->cdc || c->message_source.data))
        return UD_BUSY;
    UD_TRY(connection_enter(c, UD_ACCESS_OPEN));
    ud_status s = c->api->test(c->instance->state);
    connection_leave(c);
    return s;
}
ud_status ud_connection_discover(ud_connection *c, ud_string table, ud_batch **out) {
    if (c && c->cdc)
        return UD_BUSY;
    if (!out || !valid_string(table, 1))
        return UD_INVALID;
    *out = NULL;
    if (!c || !c->api->discover)
        return UD_UNSUPPORTED;
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
                                         (ud_string){connection->resource_namespace,32},connection->dialect, out);
    ud_batch_destroy(metadata);
    return status;
}
ud_status ud_connection_read_open(ud_connection *c, const ud_read_options *options,
                                  const ud_batch *schema) {
    if (!c || !options || options->struct_size != sizeof(*options) ||
        options->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (c->cdc)
        return UD_BUSY;
    if (!schema || !valid_string(options->sql, 1) || !options->batch_size ||
        options->batch_size > INT32_MAX || c->cursor)
        return UD_INVALID;
    if (!(c->capabilities & UD_CONNECT_READ) ||
        (options->record && !(c->capabilities & UD_CONNECT_RECORD_ERRORS)))
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
    if (!s)
        s = ud_batch_seal(*out);
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
    UD_TRY(connection_cleanup_enter(c));
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
    if (c->cursor || c->cdc)
        return UD_BUSY;
    UD_TRY(ud_write_validate(options, batch));
    uint64_t cap = options->mode == UD_WRITE_APPEND   ? UD_CONNECT_APPEND
                   : options->mode == UD_WRITE_UPSERT ? UD_CONNECT_UPSERT
                   : options->mode == UD_WRITE_DELETE ? UD_CONNECT_DELETE
                                                      : UD_CONNECT_CHANGESET;
    if (!(c->capabilities & cap) || !c->api->write)
        return UD_UNSUPPORTED;
    if (options->load_mode < UD_LOAD_AUTO || options->load_mode > UD_LOAD_NATIVE)
        return UD_INVALID;
    if (options->load_mode == UD_LOAD_NATIVE &&
        (!(c->capabilities & UD_CONNECT_NATIVE_BULK) || options->mode != UD_WRITE_APPEND))
        return UD_UNSUPPORTED;
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
    return c ? ud_write_digest(c->instance->plugin->ctx, c->options.tenant, c->options.resource,
                               options, batch, out)
             : UD_INVALID;
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
    UD_TRY(ud_connection_message_unsubscribe(c));
    if (c->transaction)
        UD_TRY(ud_connection_rollback(c));
    c->cleanup = 1;
    ud_status destroyed = connection_instance_destroy(c->instance);
    if (destroyed) {
        c->cleanup = 0;
        return destroyed;
    }
    ud_free((void *)c->options.tenant.data);
    ud_free((void *)c->options.resource.data);
    ud_batch_destroy(c->cdc_schema);
    ud_free(c);
    return UD_OK;
}

void ud_message_clear(ud_message *message) {
    if (!message)
        return;
    ud_free((void *)message->payload.data);
    ud_free((void *)message->key.data);
    ud_free((void *)message->id.data);
    ud_free((void *)message->address.data);
    for (uint32_t i = 0; i < message->header_count; i++) {
        ud_free((void *)message->headers[i].name.data);
        ud_free((void *)message->headers[i].value.data);
    }
    ud_free(message->headers);
    memset(message, 0, sizeof(*message));
}

void ud_message_epoch_clear(ud_message_epoch *epoch) {
    if (!epoch)
        return;
    ud_free((void *)epoch->cluster_id.data);
    ud_free((void *)epoch->group_metadata.data);
    for (uint32_t i = 0; i < epoch->offset_count; i++)
        ud_free((void *)epoch->offsets[i].topic.data);
    ud_free(epoch->offsets);
    memset(epoch, 0, sizeof(*epoch));
}

static int message_target_valid(const ud_message_target *target) {
    return target && target->kind >= UD_MESSAGE_QUEUE && target->kind <= UD_MESSAGE_EXCHANGE &&
           valid_string(target->name, target->kind != UD_MESSAGE_EXCHANGE) &&
           valid_string(target->routing_key, 0);
}

static ud_status message_resource(ud_connection *c, const char *kind, ud_string name,
                                  ud_string *out) {
    ud_bytes resource = {.ctx = c->ctx};
    ud_string base = c->options.resource;
    ud_status status = ud_bytes_put(&resource, base.data, base.length);
    if (!status)
        status = ud_bytes_put(&resource, kind, strlen(kind));
    if (!status)
        status = ud_bytes_put(&resource, name.data, name.length);
    if (status || resource.size > UINT32_MAX) {
        ud_bytes_clear(&resource);
        return status ? status : UD_OVERFLOW;
    }
    *out = (ud_string){(const char *)resource.data, (uint32_t)resource.size};
    return UD_OK;
}

static const char *message_target_path(const ud_message_target *target) {
    return target->kind == UD_MESSAGE_QUEUE ? "/queue/" :
           target->kind == UD_MESSAGE_TOPIC ? "/topic/" : "/exchange/";
}

ud_status ud_connection_message_subscribe(ud_connection *c, const ud_message_subscription *options) {
    if (!c || !options || options->struct_size != sizeof(*options) ||
        options->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!(c->capabilities & UD_CONNECT_MESSAGE_SUBSCRIBE))
        return UD_UNSUPPORTED;
    if (c->message_source.data || c->cursor || c->transaction || c->cdc)
        return UD_BUSY;
    if (!message_target_valid(&options->source) || !options->prefetch ||
        !options->max_message_bytes || options->max_in_flight_bytes < options->max_message_bytes ||
        options->initial_position < UD_MESSAGE_POSITION_REQUIRE ||
        options->initial_position > UD_MESSAGE_POSITION_LATEST ||
        (options->end_offset_count && (!options->end_offsets ||
                                       !(c->capabilities & UD_CONNECT_MESSAGE_TRANSACTION))) ||
        (options->fixed_partition && (!(c->capabilities & UD_CONNECT_MESSAGE_TRANSACTION) ||
                                      options->source.kind != UD_MESSAGE_TOPIC || !options->group.length)) ||
        !valid_string(options->group, 0) || !valid_string(options->subscription, 0) ||
        !valid_string(options->filter, 0))
        return UD_INVALID;
    UD_TRY(connection_enter(c, UD_ACCESS_READ));
    ud_string source = {0}, confirmation = {0};
    ud_status status = message_resource(c, message_target_path(&options->source),
                                        options->source.name, &source);
    if (!status)
        status = authorize_resource(c, source, UD_ACCESS_READ);
    if (!status)
        status = options->group.length
                     ? message_resource(c, "/group/", options->group, &confirmation)
                     : message_resource(c, message_target_path(&options->source),
                                        options->source.name, &confirmation);
    if (!status)
        status = authorize_resource(c, confirmation, UD_ACCESS_WRITE);
    if (!status)
        status = c->api->message->subscribe(c->instance->state, options);
    if (!status) {
        c->message_source = source;
        c->message_confirmation = confirmation;
    } else {
        ud_free((void *)source.data);
        ud_free((void *)confirmation.data);
    }
    connection_leave(c);
    return status;
}

ud_status ud_connection_message_receive(ud_connection *c, uint32_t timeout_ms, ud_message *out) {
    if (!c || !c->message_source.data || !out)
        return UD_INVALID;
    *out = (ud_message){.partition = -1, .offset = -1, .timestamp_ms = -1};
    UD_TRY(connection_enter(c, UD_ACCESS_READ));
    ud_status status = authorize_resource(c, c->message_source, UD_ACCESS_READ);
    if (!status)
        status = c->api->message->receive(c->instance->state, timeout_ms, out);
    if (status)
        ud_message_clear(out);
    connection_leave(c);
    return status;
}

ud_status ud_connection_message_acknowledge(ud_connection *c, const uint64_t *deliveries,
                                            uint32_t count) {
    if (!c || !c->message_source.data || !count || !deliveries)
        return UD_INVALID;
    UD_TRY(connection_enter(c, UD_ACCESS_READ));
    ud_status status = authorize_resource(c, c->message_confirmation, UD_ACCESS_WRITE);
    if (!status)
        status = c->api->message->acknowledge(c->instance->state, deliveries, count);
    connection_leave(c);
    return status;
}

ud_status ud_connection_message_unsubscribe(ud_connection *c) {
    if (!c)
        return UD_INVALID;
    if (!c->message_source.data)
        return UD_OK;
    UD_TRY(connection_cleanup_enter(c));
    ud_status status = c->api->message->unsubscribe(c->instance->state);
    if (!status) {
        ud_free((void *)c->message_source.data);
        ud_free((void *)c->message_confirmation.data);
        c->message_source = (ud_string){0};
        c->message_confirmation = (ud_string){0};
    }
    connection_leave(c);
    return status;
}

ud_status ud_connection_message_publish(ud_connection *c, const ud_message_target *target,
                                        const ud_message *messages, uint32_t count,
                                        ud_message_result *results) {
    if (!c || !message_target_valid(target) || !messages || !count || !results)
        return UD_INVALID;
    if (!(c->capabilities & UD_CONNECT_MESSAGE_PUBLISH))
        return UD_UNSUPPORTED;
    if (c->message_source.data || c->cursor || c->transaction || c->cdc)
        return UD_BUSY;
    for (uint32_t i = 0; i < count; i++)
        results[i] = (ud_message_result){UD_MESSAGE_NOT_SENT, UD_OK};
    UD_TRY(connection_enter(c, UD_ACCESS_WRITE));
    ud_string resource = {0};
    ud_status status = message_resource(c, message_target_path(target), target->name, &resource);
    if (!status)
        status = authorize_resource(c, resource, UD_ACCESS_WRITE);
    if (!status)
        status = c->api->message->publish(c->instance->state, target, messages, count, results);
    ud_free((void *)resource.data);
    connection_leave(c);
    return status;
}

ud_status ud_connection_message_capture_epoch(ud_connection *c, const uint64_t *ids,
                                              uint32_t count, ud_message_epoch *out) {
    if (!c || !c->message_source.data || (count && !ids) || !out)
        return UD_INVALID;
    *out = (ud_message_epoch){0};
    if (!(c->capabilities & UD_CONNECT_MESSAGE_TRANSACTION))
        return UD_UNSUPPORTED;
    UD_TRY(connection_enter(c, UD_ACCESS_READ));
    ud_status status = authorize_resource(c, c->message_confirmation, UD_ACCESS_WRITE);
    if (!status)
        status = c->api->message->capture_epoch(c->instance->state, ids, count, out);
    if (status)
        ud_message_epoch_clear(out);
    connection_leave(c);
    return status;
}

ud_status ud_connection_message_begin_epoch(ud_connection *c, ud_string id, ud_string cluster) {
    if (!c || !valid_string(id, 1) || !valid_string(cluster, 0))
        return UD_INVALID;
    if (!(c->capabilities & UD_CONNECT_MESSAGE_TRANSACTION))
        return UD_UNSUPPORTED;
    UD_TRY(connection_enter(c, UD_ACCESS_WRITE));
    ud_status status = c->api->message->begin_epoch(c->instance->state, id, cluster);
    connection_leave(c);
    return status;
}

ud_status ud_connection_message_commit_epoch(ud_connection *c, const ud_message_epoch *epoch) {
    if (!c || !epoch || !epoch->cluster_id.length || !epoch->group_metadata.length ||
        (epoch->offset_count && !epoch->offsets))
        return UD_INVALID;
    if (!(c->capabilities & UD_CONNECT_MESSAGE_TRANSACTION))
        return UD_UNSUPPORTED;
    UD_TRY(connection_enter(c, UD_ACCESS_WRITE));
    ud_status status = c->api->message->commit_epoch(c->instance->state, epoch);
    connection_leave(c);
    return status;
}

ud_status ud_connection_message_abort_epoch(ud_connection *c) {
    if (!c || !(c->capabilities & UD_CONNECT_MESSAGE_TRANSACTION))
        return UD_UNSUPPORTED;
    UD_TRY(connection_cleanup_enter(c));
    ud_status status = c->api->message->abort_epoch(c->instance->state);
    connection_leave(c);
    return status;
}

ud_status ud_connection_message_release_epoch(ud_connection *c, const uint64_t *ids,
                                              uint32_t count) {
    if (!c || !ids || !count || !c->message_source.data)
        return UD_INVALID;
    if (!(c->capabilities & UD_CONNECT_MESSAGE_TRANSACTION))
        return UD_UNSUPPORTED;
    UD_TRY(connection_cleanup_enter(c));
    ud_status status = c->api->message->release_epoch(c->instance->state, ids, count);
    connection_leave(c);
    return status;
}

ud_status ud_connection_message_visit_committed(ud_connection *c, ud_string topic,
                                                int32_t partition, void *user,
                                                ud_message_visit visit) {
    if (!c || !valid_string(topic, 1) || partition < 0 || !visit)
        return UD_INVALID;
    if (!(c->capabilities & UD_CONNECT_MESSAGE_TRANSACTION))
        return UD_UNSUPPORTED;
    UD_TRY(connection_enter(c, UD_ACCESS_READ));
    ud_string resource = {0};
    ud_status status = message_resource(c, "/topic/", topic, &resource);
    if (!status)
        status = authorize_resource(c, resource, UD_ACCESS_READ);
    if (!status)
        status = c->api->message->visit_committed(c->instance->state, topic, partition, user, visit);
    ud_free((void *)resource.data);
    connection_leave(c);
    return status;
}

ud_status ud_connection_message_verify_epoch(ud_connection *c, const ud_message_epoch *epoch) {
    if (!c || !c->message_source.data || !epoch || !epoch->cluster_id.length ||
        (epoch->offset_count && !epoch->offsets))
        return UD_INVALID;
    if (!(c->capabilities & UD_CONNECT_MESSAGE_TRANSACTION))
        return UD_UNSUPPORTED;
    UD_TRY(connection_enter(c, UD_ACCESS_READ));
    ud_status status = authorize_resource(c, c->message_source, UD_ACCESS_READ);
    if (!status)
        status = c->api->message->verify_epoch(c->instance->state, epoch);
    connection_leave(c);
    return status;
}

ud_status ud_connection_execute_command(ud_connection *c, const ud_read_options *options,
                                        uint64_t *processed) {
    if (processed)
        *processed = 0;
    if (!options || options->struct_size != sizeof(*options) ||
        options->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    ud_string statement = options->sql;
    if (options->parameters && !options->parameters->sealed)
        return UD_INVALID;
    if (!c || !valid_string(statement, 1) || c->cursor || c->cdc)
        return UD_INVALID;
    if (!(c->capabilities & UD_CONNECT_EXECUTE_SQL) || !c->api->execute)
        return UD_UNSUPPORTED;
    UD_TRY(connection_enter(c, UD_ACCESS_WRITE));
    ud_status s = c->api->execute(c->instance->state, options, processed);
    connection_leave(c);
    return s;
}
void ud_command_result_clear(ud_command_result *result) {
    if (!result || result->struct_size != sizeof(*result) || result->abi_version != UD_ABI_VERSION) return;
    ud_free((void *)result->provider_code.data);
    *result = (ud_command_result){.struct_size = sizeof(*result),.abi_version = UD_ABI_VERSION};
}
static ud_status record_savepoint(ud_connection *connection,int32_t phase) {
    ud_string sql = {0};
    ud_status status = ud_sql_compile_savepoint(connection->instance->plugin->ctx,
        ud_connection_dialect(connection),(ud_string){"ud_record_command",17},phase,&sql);
    if (!status && sql.length) {
        status = connection->api->savepoint(connection->instance->state,sql);
    }
    ud_free((void *)sql.data); return status;
}
ud_status ud_connection_execute_record(ud_connection *connection,const ud_read_options *options,
                                       ud_command_result *out) {
    if (!out || out->struct_size != sizeof(*out) || out->abi_version != UD_ABI_VERSION) return UD_INCOMPATIBLE;
    if (out->provider_code.data) return UD_INVALID;
    out->state = UD_COMMAND_NOT_EXECUTED; out->result = UD_OK; out->affected_rows = 0;
    ud_status status = !connection || !options ? UD_INVALID :
        options->struct_size != sizeof(*options) || options->abi_version != UD_ABI_VERSION ? UD_INCOMPATIBLE :
        connection->cursor || connection->cdc || options->record ||
        (options->parameters && (!options->parameters->sealed || options->parameters->rows != 1)) ? UD_INVALID : UD_OK;
    if (!status && (connection->capabilities & (UD_CONNECT_EXECUTE_SQL | UD_CONNECT_TRANSACTION | UD_CONNECT_TRANSACTIONAL_DML)) !=
                   (UD_CONNECT_EXECUTE_SQL | UD_CONNECT_TRANSACTION | UD_CONNECT_TRANSACTIONAL_DML)) status = UD_UNSUPPORTED;
    if (!status) status = ud_sql_validate_record_command(options->sql);
    if (!status) status = connection_enter(connection,UD_ACCESS_WRITE);
    if (status) { out->result = status; return status; }
    /* Keep the physical operation gate until commit/rollback is resolved. A
     * sibling command cannot interleave another savepoint in this protocol. */
    if (connection->cursor || connection->cdc) { out->result = UD_BUSY; connection_leave(connection); return UD_BUSY; }
    int borrowed = connection->transaction,started = 0;
    if (!status && connection->api->record_diagnostic) {
        ud_string previous = {0}; status = connection->api->record_diagnostic(connection->instance->state,&previous);
        ud_free((void *)previous.data);
    }
    if (!status) status = borrowed ? record_savepoint(connection,UD_SAVEPOINT_BEGIN)
        : connection->api->begin(connection->instance->state,UD_ISOLATION_SERIALIZABLE);
    if (!status) {
        started = 1; if (!borrowed) connection->transaction = 1;
        if (connection->api->record_validate) status = connection->api->record_validate(connection->instance->state,options);
        if (!status) status = connection->api->execute(connection->instance->state,options,&out->affected_rows);
        if (status && connection->api->record_diagnostic) {
            ud_status diagnostic = connection->api->record_diagnostic(connection->instance->state,&out->provider_code);
            if (diagnostic) { ud_free((void *)out->provider_code.data); out->provider_code = (ud_string){0}; }
        }
    }
    if (!status && borrowed) {
        status = record_savepoint(connection,UD_SAVEPOINT_RELEASE);
        if (!status) out->state = UD_COMMAND_APPLIED;
    } else if (!status) {
        status = connection->api->commit(connection->instance->state);
        if (!status) { connection->transaction = 0; out->state = UD_COMMAND_COMMITTED; }
        else { status = UD_COMMIT_UNKNOWN; out->state = UD_COMMAND_UNKNOWN; }
    }
    if (status && started && out->state != UD_COMMAND_UNKNOWN) {
        int cleaning = connection->cleanup; connection->cleanup = 1;
        ud_status rolled = borrowed ? record_savepoint(connection,UD_SAVEPOINT_ROLLBACK)
            : connection->api->rollback(connection->instance->state);
        if (!rolled && borrowed) rolled = record_savepoint(connection,UD_SAVEPOINT_RELEASE);
        connection->cleanup = cleaning;
        if (rolled) { status = UD_COMMIT_UNKNOWN; out->state = UD_COMMAND_UNKNOWN; }
        else { if (!borrowed) connection->transaction = 0; out->state = UD_COMMAND_REJECTED; }
    }
    out->result = status; connection_leave(connection); return status;
}
ud_status ud_connection_savepoint(ud_connection *connection, ud_string name, int32_t phase) {
    if (!connection) return UD_INVALID;
    ud_string sql = {0};
    ud_status status = ud_sql_compile_savepoint(connection->ctx,ud_connection_dialect(connection),name,phase,&sql);
    if (!status) status = connection_enter(connection,UD_ACCESS_WRITE);
    if (!status) {
        if (!connection->transaction) status = UD_UNSUPPORTED;
        else if (!connection->api->savepoint) status = UD_UNSUPPORTED;
        else if (sql.length) status = connection->api->savepoint(connection->instance->state,sql);
        connection_leave(connection);
    }
    ud_free((void *)sql.data); return status;
}
ud_status ud_connection_record_diagnostic(ud_connection *connection, ud_string *out) {
    if (!connection || !out)
        return UD_INVALID;
    *out = (ud_string){0};
    if (!connection->api->record_diagnostic)
        return UD_OK;
    UD_TRY(connection_enter(connection, UD_ACCESS_WRITE));
    ud_status status = connection->api->record_diagnostic(connection->instance->state, out);
    connection_leave(connection);
    return status;
}
ud_status ud_connection_provision(ud_connection *c) {
    if (!c || !(c->capabilities & UD_CONNECT_PROVISION) || !c->api->provision)
        return UD_UNSUPPORTED;
    UD_TRY(connection_enter(c, UD_ACCESS_WRITE));
    ud_status status = c->api->provision(c->instance->state);
    connection_leave(c);
    return status;
}

ud_status ud_connection_cdc_open(ud_connection *c, const ud_cdc_options *options) {
    if (!c || !options || options->struct_size != sizeof(*options) ||
        options->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (c->cursor || c->transaction || c->cdc)
        return UD_BUSY;
    if (!(c->capabilities & UD_CONNECT_CDC) || !c->api->cdc_open)
        return UD_UNSUPPORTED;
    if (!options->schema || !options->table.length || !options->batch_rows ||
        options->mode < UD_CDC_STREAM || options->mode > UD_CDC_SNAPSHOT_STREAM)
        return UD_SCHEMA;
    ud_batch *discovered = NULL, *actual = NULL, *projected = NULL;
    UD_TRY(ud_connection_schema(c, options->table,
                                options->field_namespace.data ? options->field_namespace
                                                              : (ud_string){"cdc.preflight", 13},
                                &discovered));
    ud_status s = ud_schema_bind_identity(c->ctx, options->schema, discovered, &actual);
    ud_batch_destroy(discovered);
    if (!s)
        s = ud_schema_map(c->ctx, actual, options->schema, options->schema_policy, &projected);
    ud_batch_destroy(projected);
    if (s) {
        ud_batch_destroy(actual);
        return s == UD_SCHEMA ? UD_INCOMPATIBLE : s;
    }
    ud_cdc_options current = *options;
    current.schema = actual;
    options = &current;
    if (c->dialect == UD_SQL_SQLSERVER && options->mode == UD_CDC_SNAPSHOT_STREAM)
        UD_TRY(authorize_connection(c, UD_ACCESS_WRITE));
    s = ud_batch_create(c->ctx, options->schema->schema->fields, options->schema->columns, 0,
                        &c->cdc_schema);
    if (s) {
        ud_batch_destroy(actual);
        return s;
    }
    c->cdc_batch_rows = options->batch_rows;
    c->cdc_version = 0;
    c->cdc_transaction = 0;
    s = connection_enter(c,
                         c->dialect == UD_SQL_SQLSERVER && options->mode == UD_CDC_SNAPSHOT_STREAM
                             ? UD_ACCESS_WRITE
                             : UD_ACCESS_READ);
    if (s) {
        ud_batch_destroy(c->cdc_schema);
        c->cdc_schema = NULL;
        ud_batch_destroy(actual);
        return s;
    }
    s = c->api->cdc_open(c->instance->state, options);
    ud_batch_destroy(actual);
    if (!s)
        c->cdc = 1;
    else {
        ud_batch_destroy(c->cdc_schema);
        c->cdc_schema = NULL;
    }
    connection_leave(c);
    return s;
}
static ud_status cdc_shape(ud_connection *c, ud_batch *b) {
    if (!b || b->ctx != c->ctx || b->columns != c->cdc_schema->columns ||
        b->rows > c->cdc_batch_rows)
        return UD_INCOMPATIBLE;
    for (uint32_t i = 0; i < b->columns; i++) {
        const ud_field *a = &b->column[i].field, *expected = &c->cdc_schema->column[i].field;
        if (!ud_field_matches(a, expected, UD_FIELD_VALUE_TYPE) ||
            a->name_length != expected->name_length ||
            memcmp(a->name, expected->name, a->name_length))
            return UD_INCOMPATIBLE;
    }
    return ud_batch_seal(b);
}
ud_status ud_connection_cdc_schema(ud_connection *c, ud_batch **out) {
    if (!c || !out || !c->cdc_schema)
        return UD_INVALID;
    return ud_batch_create(c->ctx, c->cdc_schema->schema->fields, c->cdc_schema->columns, 0, out);
}
ud_status ud_connection_cdc_rebind(ud_connection *c, const ud_batch *schema, uint64_t version) {
    if (!c || !c->cdc || !schema || schema->ctx != c->ctx || !version || version < c->cdc_version)
        return UD_INVALID;
    if (!c->api->cdc_rebind)
        return UD_UNSUPPORTED;
    ud_batch *owned = NULL;
    UD_TRY(ud_batch_create(c->ctx, schema->schema->fields, schema->columns, 0, &owned));
    ud_status status = connection_enter(c, UD_ACCESS_READ);
    if (!status) {
        status = c->api->cdc_rebind(c->instance->state, owned, version);
        connection_leave(c);
    }
    if (!status) {
        ud_batch_destroy(c->cdc_schema);
        c->cdc_schema = owned;
        c->cdc_version = version;
    } else
        ud_batch_destroy(owned);
    return status;
}
ud_status ud_connection_cdc_next(ud_connection *c, ud_cdc_event *out) {
    if (!c || !out || !c->cdc)
        return UD_INVALID;
    memset(out, 0, sizeof(*out));
    UD_TRY(connection_enter(c, UD_ACCESS_READ));
    ud_status status = c->api->cdc_next(c->instance->state, out);
    connection_leave(c);
    if (!status && (out->struct_size != sizeof(*out) || out->abi_version != UD_ABI_VERSION))
        status = UD_INCOMPATIBLE;
    if (!status && (out->kind < UD_CDC_BEGIN || out->kind > UD_CDC_TRUNCATE ||
                    !out->schema_version || out->schema_version < c->cdc_version))
        status = UD_CORRUPT;
    if (!status) {
        switch (out->kind) {
        case UD_CDC_BEGIN:
            if (c->cdc_transaction || !valid_string(out->transaction_id, 1))
                status = UD_CORRUPT;
            else
                c->cdc_transaction = 1;
            break;
        case UD_CDC_ROWS:
            if (!c->cdc_transaction || !out->operations || !valid_string(out->position, 1))
                status = UD_CORRUPT;
            if (!status)
                status = cdc_shape(c, out->after);
            if (!status && out->before) {
                status = cdc_shape(c, out->before);
                if (!status && out->before->rows != out->after->rows)
                    status = UD_CORRUPT;
            }
            for (uint32_t i = 0; !status && i < out->after->rows; i++)
                if (out->operations[i] < UD_CHANGE_INSERT ||
                    out->operations[i] > UD_CHANGE_DELETE ||
                    (out->operations[i] != UD_CHANGE_INSERT && !out->before))
                    status = UD_CORRUPT;
            break;
        case UD_CDC_COMMIT:
            if (!c->cdc_transaction || !valid_string(out->position, 1))
                status = UD_CORRUPT;
            else
                c->cdc_transaction = 0;
            break;
        case UD_CDC_SCHEMA_CHANGE:
            if (!out->after || out->schema_version <= c->cdc_version)
                status = UD_INCOMPATIBLE;
            else
                status = ud_batch_seal(out->after);
            break;
        case UD_CDC_HEARTBEAT:
            break;
        case UD_CDC_TRUNCATE:
            if (!c->cdc_transaction || !valid_string(out->position, 1) || out->after ||
                out->before || out->operations)
                status = UD_CORRUPT;
            break;
        }
    }
    if (!status)
        c->cdc_version = out->schema_version;
    else {
        char safe[128];
        snprintf(safe, sizeof(safe),
                 "CDC event contract rejected (kind=%d, version=%llu, status=%d)", out->kind,
                 (unsigned long long)out->schema_version, status);
        if (status != UD_BUSY && status != UD_END && !*ud_error(c->ctx))
            ud_fail(c->ctx, status, "%s", safe);
        ud_cdc_event_clear(out);
    }
    return status;
}
ud_status ud_connection_cdc_confirm(ud_connection *c, ud_string position) {
    if (!c || !c->cdc || !valid_string(position, 1))
        return UD_INVALID;
    UD_TRY(connection_enter(c, UD_ACCESS_READ));
    ud_status s = c->api->cdc_confirm(c->instance->state, position);
    connection_leave(c);
    return s;
}
ud_status ud_connection_cdc_close(ud_connection *c) {
    if (!c)
        return UD_INVALID;
    if (!c->cdc)
        return UD_OK;
    UD_TRY(connection_cleanup_enter(c));
    c->cleanup = 1;
    c->api->set_control(c->instance->state, c, connection_check);
    ud_status status = c->api->cdc_close(c->instance->state);
    c->cleanup = 0;
    if (!status) {
        c->cdc = 0;
        c->cdc_transaction = 0;
        ud_batch_destroy(c->cdc_schema);
        c->cdc_schema = NULL;
    }
    connection_leave(c);
    return status;
}

int32_t ud_connection_dialect(const ud_connection *c) {
    return c ? c->dialect : UD_SQL_CANONICAL;
}
int32_t ud_connection_parameter_style(const ud_connection *c) {
    return c ? c->api->parameter_style : 0;
}
