#include "ud/connection_manager.h"
#include "ud/avro.h"
#include "../components/contracts/connection_internal.h"
#include "../engines/package/internal.h"
#include "rest.h"
#include "http_internal.h"
#include "manager_pool.h"
#include "manager_binding.h"
#include "ud/csv.h"
#include "ud/xlsx.h"

struct ud_connection_manager {
    ud_connection_pool *pool;
    ud_connection_lease *leases;
    uint32_t capacity, physical, active;
    uint64_t acquisitions, hits, failures, wait_ms, commits, rollbacks, uncertain;
};
typedef struct transaction_participant {
    ud_connection_lease *lease;
    ud_string connection;
    ud_transaction_branch branch;
    char branch_id[65];
    int begun, local;
} transaction_participant;
struct ud_transaction {
    ud_activation *owner;
    transaction_participant *participants;
    uint32_t count;
    char identity[65];
    uint64_t attempt, fact_order;
    int completed;
    ud_status sql_failure;
    char scope_identity[65];
    uint64_t pending_rows, pending_errors;
    uint64_t pending_tasks;
    struct ud_rule_delivery *delivery;
};
static int transaction_is_local(const struct ud_transaction *transaction) {
    return transaction && transaction->count == 1 && transaction->participants &&
           transaction->participants[0].local;
}
struct ud_connection_lease {
    ud_activation *activation;
    ud_json *definition;
    ud_string id, resource, driver;
    session *session;
    ud_rest_target *rest;
    ud_http_oauth_cache oauth;
    ud_connection *connection;
    uint64_t capabilities;
    uint32_t pins, aliases;
    atomic_int cancelled;
    int shared, exclusive_sql, sql_borrowed, sql_transaction, read_only, cleanup, messaging, releasing;
    struct ud_connection_lease *shared_owner;
    void *sql_session;
    void (*sql_session_ended)(void *);
    struct ud_connection_lease *next;
};
ud_status ud_execution_set_connection_limits(ud_execution *execution, uint32_t limit,
                                             uint32_t wait_ms) {
    if (!execution || !limit || limit > 65536 || wait_ms > 3600000)
        return UD_INVALID;
    if (atomic_load(&execution->state) != UD_EXEC_READY)
        return UD_BUSY;
    execution->max_connections_per_source = limit;
    execution->connection_wait_ms = wait_ms;
    return UD_OK;
}

static int network_driver(ud_string driver) {
    return ud_named(driver, "http") || ud_named(driver, "ftp") || ud_named(driver, "sftp") ||
           ud_named(driver, "smtp") || ud_named(driver, "azureBlob") ||
           ud_named(driver, "adlsGen2") || ud_named(driver, "s3");
}
uint64_t ud_connection_lease_capabilities(const ud_connection_lease *lease) {
    return lease ? lease->capabilities : 0;
}
static ud_status check_lease(void *user) {
    ud_connection_lease *l = user;
    if (!l || l->cleanup)
        return UD_OK;
    if (atomic_load(&l->cancelled))
        return UD_CANCELLED;
    return ud_activation_check(l->activation);
}
ud_status ud_connection_lease_authorize(ud_connection_lease *l, int32_t action) {
    if (!l)
        return UD_INVALID;
    if (l->read_only &&
        (action == UD_ACCESS_WRITE || action == UD_ACCESS_DELETE || action == UD_ACCESS_EXECUTE))
        return UD_UNSUPPORTED;
    UD_TRY(check_lease(l));
    ud_execution *e = l->activation->execution;
    return e->options.authorize && e->options.tenant.length &&
                   e->options.authorize(e->options.authorize_user, e->options.tenant, l->resource,
                                        action) == UD_OK
               ? UD_OK
               : UD_AUTH;
}
ud_status ud_connection_lease_authorize_uri(ud_connection_lease *l, ud_string uri) {
    if (!l || !uri.data || !uri.length)
        return UD_INVALID;
    UD_TRY(ud_connection_lease_authorize(l, UD_ACCESS_NETWORK));
    ud_execution *e = l->activation->execution;
    return e->options.authorize &&
                   e->options.authorize(e->options.authorize_user, e->options.tenant, uri,
                                        UD_ACCESS_NETWORK) == UD_OK
               ? UD_OK
               : UD_AUTH;
}
static ud_status session_grant(void *user, ud_string tenant, ud_string resource, int32_t action) {
    ud_connection_lease *l = user;
    if (!l || !ud_same(tenant, l->activation->execution->options.tenant))
        return UD_AUTH;
    if (ud_same(resource, l->resource))
        return ud_connection_lease_authorize(l, action);
    if (action == UD_ACCESS_NETWORK)
        return (network_driver(l->driver) || l->messaging)
                   ? ud_connection_lease_authorize_uri(l, resource)
                   : UD_AUTH;
    if (!l->messaging || l->read_only)
        return UD_AUTH;
    if ((action != UD_ACCESS_READ && action != UD_ACCESS_WRITE) ||
        resource.length <= l->resource.length || resource.data[l->resource.length] != '/' ||
        memcmp(resource.data, l->resource.data, l->resource.length))
        return UD_AUTH;
    UD_TRY(check_lease(l));
    ud_execution *e = l->activation->execution;
    return e->options.authorize &&
                   e->options.authorize(e->options.authorize_user, tenant, resource, action) == UD_OK
               ? UD_OK
               : UD_AUTH;
}
ud_status ud_connections_create(ud_execution *e, ud_connection_pool *pool) {
    uint64_t capacity = pool ? pool->capacity : 64;
    const ud_json *policy = ud_member(ud_member(e->package->document, "policy"), "connections");
    if (policy) {
        static const char *keys[] = {"maxSessions"};
        UD_TRY(ud_json_keys(policy, keys, 1));
        UD_TRY(ud_json_uint(ud_member(policy, "maxSessions"), &capacity));
        if (!capacity || capacity > 65536)
            return UD_SCHEMA;
    }
    struct ud_connection_manager *m = ud_alloc(e->ctx, sizeof(*m));
    if (!m)
        return UD_RESOURCE;
    if (pool) {
        ud_mutex_lock(&pool->mutex);
        if (pool->closing || pool->users == UINT32_MAX) {
            ud_mutex_unlock(&pool->mutex);
            ud_free(m);
            return UD_BUSY;
        }
        pool->users++;
        ud_mutex_unlock(&pool->mutex);
    } else {
        pool = ud_manager_pool_create(e->ctx, (uint32_t)capacity, 0);
        if (!pool) {
            ud_free(m);
            return UD_RESOURCE;
        }
    }
    m->pool = pool;
    m->capacity = capacity < pool->capacity ? (uint32_t)capacity : pool->capacity;
    e->connections = m;
    return UD_OK;
}
ud_status ud_connection_definition(ud_activation *a, ud_string id, const ud_json **out,
                                   int *project_scope) {
    if (!a || !out || !project_scope)
        return UD_INVALID;
    const ud_json *defs = ud_member(a->package->document, "connections");
    *out = NULL;
    *project_scope = 0;
    for (int level = 0; level < 2; level++) {
        if (level)
            defs = ud_member(ud_member(a->package->document, "project"), "connections");
        for (uint32_t i = 0; i < ud_json_size(defs); i++) {
            ud_string name;
            UD_TRY(ud_json_string(ud_member(ud_json_at(defs, i), "id"), &name));
            if (ud_same(name, id)) {
                *out = ud_json_at(defs, i);
                *project_scope = level;
                return UD_OK;
            }
        }
    }
    return UD_SCHEMA;
}
static ud_status definition_limit(const ud_json *d, const char *name, uint64_t fallback,
                                  uint64_t maximum, uint64_t *out) {
    *out = fallback;
    if (ud_member(d, name))
        UD_TRY(ud_json_uint(ud_member(d, name), out));
    return *out <= maximum ? UD_OK : UD_SCHEMA;
}
static void track(ud_connection_lease *l) {
    struct ud_connection_manager *m = l->activation->execution->connections;
    ud_mutex_lock(&m->pool->mutex);
    l->next = m->leases;
    m->leases = l;
    m->active++;
    m->acquisitions++;
    ud_mutex_unlock(&m->pool->mutex);
}
static void clear_configuration(ud_string configuration) {
    volatile char *secret = (volatile char *)configuration.data;
    if (secret)
        for (uint32_t index = 0; index < configuration.length; index++)
            secret[index] = 0;
    ud_free((void *)configuration.data);
}

static ud_status relay_driver_error(ud_connection_lease *lease, ud_status status) {
    ud_execution *execution = lease->activation->execution;
    ud_context *driver = execution->connections->pool->ctx;
    if (driver != execution->ctx && status > UD_END && status != UD_MORE && status != UD_BUSY) {
        ud_error_detail detail = {.struct_size = sizeof(detail), .abi_version = UD_ABI_VERSION};
        if (!ud_error_details(driver, &detail) && detail.cause) {
            char message[1024];
            int length = snprintf(message, sizeof(message), "%s", ud_error(driver));
            if (length > 0 && (size_t)length < sizeof(message))
                return ud_context_report_error(execution->ctx, status,
                                               (ud_string){message, (uint32_t)length}, &detail);
        }
    }
    return status;
}

static ud_status acquire_session(ud_connection_lease *l, uint64_t caps) {
    ud_execution *e = l->activation->execution;
    struct ud_connection_manager *m = e->connections;
    ud_connection_pool *pool = m->pool;
    if (!e->options.resolve || !e->options.authorize ||
        e->options.authorize(e->options.authorize_user, e->options.tenant, l->driver,
                             UD_ACCESS_OPEN) != UD_OK)
        return UD_AUTH;
    ud_string reference;
    UD_TRY(ud_json_string(ud_member(l->definition, "credentialReference"), &reference));
    /* Resolve again for every lease. The SDK reference includes the source
     * revision. The secret hash detects rotation even within one revision. */
    ud_string configuration = {0};
    ud_status status = e->options.resolve(e->options.resolve_user, e->ctx, e->options.tenant,
                                          l->resource, reference, &configuration);
    if (!status && (!configuration.data || !configuration.length ||
                    memchr(configuration.data, 0, configuration.length)))
        status = UD_AUTH;
    ud_bytes identity = {.ctx = e->ctx};
    uint8_t family[32], key[32], credential[32];
    /* A Package connection id is logical scope, not physical identity. Two
     * executions may borrow the same endpoint when tenant, driver, resource,
     * credential reference and resolved credential agree. */
    ud_string fields[] = {e->options.tenant, l->driver, l->resource};
    for (uint32_t index = 0; !status && index < sizeof(fields) / sizeof(fields[0]); index++)
        status = ud_put_string(&identity, fields[index].data, fields[index].length);
    if (!status) {
        ud_hash(identity.data, identity.size, family);
        ud_hash(configuration.data, configuration.length, credential);
        status = ud_put_string(&identity, reference.data, reference.length);
    }
    if (!status)
        status = ud_bytes_put(&identity, credential, sizeof(credential));
    if (!status)
        ud_hash(identity.data, identity.size, key);
    ud_bytes_clear(&identity);
    uint64_t limit = 0, timeout = 0, idle = 0;
    if (!status)
        status = definition_limit(l->definition, "maxLeases", 8, 65536, &limit);
    const ud_json *job_execution = ud_member(ud_member(e->package->document, "policy"), "execution");
    const ud_json *per_source = ud_member(job_execution, "maxConnectionsPerSource");
    if (!status && per_source) {
        uint64_t maximum;
        status = ud_json_uint(per_source, &maximum);
        if (!status && (!ud_member(l->definition, "maxLeases") || limit > maximum))
            limit = maximum;
    }
    if (e->max_connections_per_source && limit > e->max_connections_per_source)
        limit = e->max_connections_per_source;
    if (!status && !limit)
        status = UD_SCHEMA;
    if (!status)
        status = definition_limit(l->definition, "acquireTimeoutMs", e->connection_wait_ms, 3600000, &timeout);
    if (!status)
        status = definition_limit(l->definition, "idleTimeoutMs", 30000, 3600000, &idle);
    uint64_t started = ud_now_ms();
    session *chosen = NULL;
    while (!status) {
        status = check_lease(l);
        if (status)
            break;
        ud_mutex_lock(&pool->mutex);
        session **at = &pool->sessions, **oldest = NULL, *available = NULL, *retired = NULL;
        uint32_t matching = 0;
        uint64_t now = ud_now_ms();
        while (*at) {
            session *s = *at;
            int compatible = !memcmp(s->key, key, sizeof(key));
            if (!s->borrower && (s->unhealthy || now - s->idle_since >= s->idle_timeout ||
                               (!compatible && !memcmp(s->family, family, sizeof(family))))) {
                *at = s->next;
                s->next = retired;
                retired = s;
                continue;
            }
            /* maxLeases belongs to this Package connection declaration. */
            if (s->borrower && s->borrower->activation->execution == e &&
                ud_same(s->borrower->id, l->id))
                matching++;
            if (!s->borrower) {
                if (compatible && !available)
                    available = s;
                if (!oldest || s->idle_since < (*oldest)->idle_since)
                    oldest = at;
            }
            at = &s->next;
        }
        if (matching < limit && m->physical < m->capacity) {
            if (available) {
                chosen = available;
                m->hits++;
            } else if (pool->count < pool->capacity) {
                chosen = ud_alloc(pool->ctx, sizeof(*chosen));
                if (!chosen)
                    status = UD_RESOURCE;
                else {
                    memcpy(chosen->key, key, sizeof(key));
                    memcpy(chosen->family, family, sizeof(family));
                    chosen->next = pool->sessions;
                    pool->sessions = chosen;
                    pool->count++;
                }
            } else if (!retired && oldest) {
                retired = *oldest;
                *oldest = retired->next;
                retired->next = NULL;
            }
        }
        if (chosen) {
            chosen->borrower = l;
            chosen->idle_timeout = idle;
            m->physical++;
        } else if (!status && !retired) {
            uint64_t elapsed = ud_now_ms() - started;
            if (!timeout || elapsed >= timeout)
                status = timeout ? UD_TIMEOUT : UD_BUSY;
            else
                ud_condition_wait_ms(&pool->changed, &pool->mutex,
                                     (uint32_t)(timeout - elapsed < 25 ? timeout - elapsed : 25));
        }
        ud_mutex_unlock(&pool->mutex);
        ud_status closed = ud_manager_pool_dispose(pool, retired);
        if (!status && !chosen && closed)
            status = relay_driver_error(l, closed);
        if (chosen || status)
            break;
    }
    l->session = chosen;
    if (chosen) {
        ud_mutex_lock(&pool->mutex);
        m->wait_ms += ud_now_ms() - started;
        ud_mutex_unlock(&pool->mutex);
        ud_context_clear_error(pool->ctx);
    }
    if (!status && !chosen->plugin) {
        char *path = ud_strdup_n(e->ctx, l->driver.data, l->driver.length);
        if (!path)
            status = UD_RESOURCE;
        if (!status)
            status = ud_plugin_load(pool->ctx, path, 0, &chosen->plugin);
        ud_free(path);
        ud_plugin_api info = {.struct_size = sizeof(info), .abi_version = UD_PLUGIN_ABI};
        if (!status)
            status = ud_plugin_info(chosen->plugin, &info);
        if (!status)
            chosen->plugin_capabilities = info.capabilities;
        if (!status && (!info.connector || (info.connector->capabilities & caps) != caps))
            status = UD_UNSUPPORTED;
        if (!status)
            l->messaging = (info.connector->capabilities &
                            (UD_CONNECT_MESSAGE_SUBSCRIBE | UD_CONNECT_MESSAGE_PUBLISH)) != 0;
        if (!status && l->activation->parallel &&
            (!(info.capabilities & UD_CAP_THREAD_SAFE) || (info.capabilities & UD_CAP_THREAD_AFFINE)))
            status = UD_UNSUPPORTED;
        if (!status && (info.connector->capabilities & UD_CONNECT_EXECUTE_SQL))
            status = ud_manager_pool_reserve(pool, e->ctx, e->options.tenant, l->resource,
                                             (uint32_t)timeout, check_lease, l, &chosen->node_slot);
        ud_connection_options options = {.struct_size = sizeof(options),
                                         .abi_version = UD_ABI_VERSION,
                                         .tenant = e->options.tenant,
                                         .resource = l->resource,
                                         .authorize_user = l,
                                         .authorize = session_grant,
                                         .check = check_lease};
        if (!status)
            status = ud_connection_open_resolved(chosen->plugin, &options, configuration,
                                                 &chosen->connection);
        if (!status)
            chosen->capabilities = ud_connection_capabilities(chosen->connection);
    }
    clear_configuration(configuration);
    if (!status && l->activation->parallel &&
        (!(chosen->plugin_capabilities & UD_CAP_THREAD_SAFE) ||
         (chosen->plugin_capabilities & UD_CAP_THREAD_AFFINE)))
        status = UD_UNSUPPORTED;
    if (!status) {
        l->connection = chosen->connection;
        l->capabilities = chosen->capabilities;
        l->messaging = (l->capabilities &
                        (UD_CONNECT_MESSAGE_SUBSCRIBE | UD_CONNECT_MESSAGE_PUBLISH)) != 0;
        status = ud_connection_bind_lease(l->connection, l, session_grant, check_lease);
    }
    if (!status && (l->capabilities & caps) != caps)
        status = UD_UNSUPPORTED;
    /* Initialize verifies the physical endpoint. Subsequent clean borrows
     * keep authorization fresh without issuing a redundant round trip. */
    if (!status && !chosen->idle_since)
        status = ud_connection_test(l->connection);
    if (status && chosen)
        chosen->unhealthy = 1;
    return relay_driver_error(l, status);
}
static ud_status relative_file_definition(ud_context *memory, ud_json **definition,
                                          ud_string relative) {
    if (!relative.length || relative.length > 32768 || memchr(relative.data, 0, relative.length) ||
        relative.data[0] == '/' || relative.data[0] == '\\' ||
        memchr(relative.data, ':', relative.length))
        return UD_SCHEMA;
    uint32_t begin = 0;
    for (uint32_t at = 0; at <= relative.length; at++) {
        if (at < relative.length && (uint8_t)relative.data[at] < 32)
            return UD_SCHEMA;
        if (at == relative.length || relative.data[at] == '/' || relative.data[at] == '\\') {
            uint32_t count = at - begin;
            if (!count || (count == 1 && relative.data[begin] == '.') ||
                (count == 2 && relative.data[begin] == '.' && relative.data[begin + 1] == '.'))
                return UD_SCHEMA;
            begin = at + 1;
        }
    }
    ud_string driver, base;
    UD_TRY(ud_json_string(ud_member(*definition, "driver"), &driver));
    UD_TRY(ud_json_string(ud_member(*definition, "resource"), &base));
    if (!ud_named(driver, "file"))
        return UD_UNSUPPORTED;
    ud_bytes path = {.ctx = memory, .limit = 65536}, json = {.ctx = memory, .limit = 1048576};
    ud_status status = ud_bytes_put(&path, base.data, base.length);
    if (!status && base.length && base.data[base.length - 1] != '/' &&
        base.data[base.length - 1] != '\\')
        status = ud_bytes_put(&path, "/", 1);
    for (uint32_t at = 0; !status && at < relative.length; at++) {
        char byte = relative.data[at] == '\\' ? '/' : relative.data[at];
        status = ud_bytes_put(&path, &byte, 1);
    }
    if (!status)
        status = ud_bytes_put(&json, "{", 1);
    for (uint32_t at = 0; !status && at < ud_json_size(*definition); at++) {
        ud_string key;
        status = ud_json_key(*definition, at, &key.data, &key.length);
        if (!status && at)
            status = ud_bytes_put(&json, ",", 1);
        if (!status)
            status = ud_json_quote(&json, key.data, key.length);
        if (!status)
            status = ud_bytes_put(&json, ":", 1);
        if (!status && ud_named(key, "resource"))
            status = ud_json_quote(&json, (const char *)path.data, (uint32_t)path.size);
        else if (!status) {
            char *value = NULL;
            uint32_t length = 0;
            status = ud_json_serialize(ud_json_at(*definition, at), &value, &length);
            if (!status)
                status = ud_bytes_put(&json, value, length);
            ud_free(value);
        }
    }
    if (!status)
        status = ud_bytes_put(&json, "}", 1);
    ud_json *bound = NULL;
    if (!status)
        status = ud_json_parse(memory, (const char *)json.data, (uint32_t)json.size, &bound, NULL);
    if (!status) {
        ud_json_destroy(*definition);
        *definition = bound;
    }
    ud_bytes_clear(&path);
    ud_bytes_clear(&json);
    return status;
}
enum { CONNECTION_ATOMIC_SQL = 5 };

static ud_status acquire_resource(ud_activation *a, ud_string id, uint64_t caps, int kind,
                                  ud_string relative, ud_connection_lease **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!a || !a->execution->connections)
        return UD_INVALID;
    const ud_json *def = NULL;
    int project = 0;
    UD_TRY(ud_connection_definition(a, id, &def, &project));
    ud_connection_lease *l = ud_alloc(a->execution->ctx, sizeof(*l));
    if (!l)
        return UD_RESOURCE;
    l->activation = a;
    atomic_init(&l->cancelled, 0);
    l->id = id;
    /* Project binding gets its own frame; acquisition never swaps a live scope. */
    ud_activation binding = {.execution = a->execution,
                             .parent = a,
                             .definition = a->definition,
                             .package = a->package,
                             .parameters = a->parameters,
                             .scope = project ? &a->definition->project_scope : a->scope,
                             .parallel = a->parallel};
    atomic_init(&binding.cancelled, 0);
    ud_status status = ud_bound_config(&binding, def, &l->definition);
    if (!status && relative.length)
        status = relative_file_definition(a->execution->ctx, &l->definition, relative);
    if (!status)
        status = ud_json_string(ud_member(l->definition, "driver"), &l->driver);
    if (!status)
        status = ud_json_string(ud_member(l->definition, "resource"), &l->resource);
    if (!status)
        status = ud_connection_lease_authorize(l, UD_ACCESS_OPEN);
    if (!status)
        status = ud_execution_resolve_driver(a->execution, &l->driver);
    if (!status && ((kind == 1 && !ud_named(l->driver, "file") && !ud_named(l->driver, "excel")) ||
                    (kind == 2 && !ud_named(l->driver, "process")) ||
                    (kind == 3 && !ud_named(l->driver, "http"))))
        status = UD_UNSUPPORTED;
    if (!status && kind == CONNECTION_ATOMIC_SQL && !ud_transaction_active(a)) {
        status = UD_UNSUPPORTED;
    } else if (!status && kind == 4) {
        l->capabilities = 0;
    } else if (!status && a->transaction) {
        ud_connection_lease *owned = NULL;
        for (uint32_t index = 0; index < a->transaction->count; index++)
            if (ud_same(a->transaction->participants[index].connection, id))
                owned = a->transaction->participants[index].lease;
        if (caps & UD_CONNECT_NATIVE_CDC)
            owned = NULL; /* Replication cursors own a dedicated read session. */
        if (kind != CONNECTION_ATOMIC_SQL && !owned && (caps & UD_CONNECT_READ) &&
            !(caps & (UD_CONNECT_APPEND | UD_CONNECT_UPSERT | UD_CONNECT_DELETE |
                      UD_CONNECT_EXECUTE_SQL | UD_CONNECT_CHANGESET))) {
            l->read_only = 1;
            if (ud_named(l->driver, "file") || ud_named(l->driver, "excel"))
                l->capabilities = UD_CONNECT_READ;
            else if (ud_named(l->driver, "http"))
                l->capabilities = UD_CONNECT_HTTP | UD_CONNECT_READ;
            else
                status = acquire_session(l, caps);
        } else if (!owned || a->transaction->completed || !ud_same(owned->resource, l->resource) ||
                   !ud_same(owned->driver, l->driver) || (owned->capabilities & caps) != caps)
            status = UD_UNSUPPORTED;
        else {
            struct ud_connection_manager *manager = a->execution->connections;
            ud_mutex_lock(&manager->pool->mutex);
            if (atomic_load(&owned->cancelled))
                status = UD_CANCELLED;
            else if (owned->sql_borrowed || owned->aliases == UINT32_MAX ||
                     (kind == CONNECTION_ATOMIC_SQL && owned->aliases))
                status = UD_BUSY;
            else {
                owned->aliases++;
                l->shared = 1;
                l->exclusive_sql = kind == CONNECTION_ATOMIC_SQL;
                owned->sql_borrowed = l->exclusive_sql;
                l->shared_owner = owned;
                l->connection = owned->connection;
                l->capabilities = owned->capabilities;
            }
            ud_mutex_unlock(&manager->pool->mutex);
        }
    } else if (!status && ud_named(l->driver, "http")) {
#ifdef UD_WITH_HTTP
        l->capabilities = UD_CONNECT_HTTP;
#else
        status = UD_UNSUPPORTED;
#endif
    } else if (!status && (ud_named(l->driver, "file") || ud_named(l->driver, "excel") ||
                           ud_named(l->driver, "process"))) {
        int process = ud_named(l->driver, "process");
        l->capabilities =
            process ? UD_CONNECT_EXECUTE_PROCESS
                    : UD_CONNECT_READ | UD_CONNECT_APPEND | UD_CONNECT_DELETE | UD_CONNECT_METADATA;
#ifdef _WIN32
        if (l->resource.length < 3 || l->resource.data[1] != ':')
            status = UD_INVALID;
#else
        if (!l->resource.length || l->resource.data[0] != '/')
            status = UD_INVALID;
#endif
        if (process && ud_member(l->definition, "credentialReference"))
            status = UD_SCHEMA;
    } else if (!status)
        status = acquire_session(l, caps);
    if (!status && (l->capabilities & caps) != caps)
        status = UD_UNSUPPORTED;
    track(l); /* Failed partial resources have the same cleanup owner. */
    if (status) {
        struct ud_connection_manager *m = a->execution->connections;
        ud_mutex_lock(&m->pool->mutex);
        m->failures++;
        ud_mutex_unlock(&m->pool->mutex);
        ud_status closed = ud_connection_lease_release(l);
        return closed ? closed : status;
    }
    *out = l;
    return UD_OK;
}
static ud_status acquire(ud_activation *activation, ud_string id, uint64_t capabilities, int kind,
                         ud_connection_lease **out) {
    return acquire_resource(activation, id, capabilities, kind, (ud_string){0}, out);
}
ud_status ud_activation_acquire_file_resource(ud_activation *activation, ud_string connection,
                                              ud_string relative, uint64_t capabilities,
                                              ud_connection_lease **out) {
    if (!relative.length)
        return UD_SCHEMA;
    return acquire_resource(activation, connection, capabilities, 1, relative, out);
}
ud_status ud_activation_acquire_connection(ud_activation *a, ud_string id, uint64_t caps,
                                           ud_connection_lease **out) {
    return acquire(a, id, caps, 0, out);
}
ud_status ud_transaction_sql_lease(ud_activation *activation, ud_string connection,
                                   void *session, void (*ended)(void *), ud_connection_lease **out) {
    UD_TRY(acquire(activation, connection, UD_CONNECT_READ, CONNECTION_ATOMIC_SQL, out));
    (*out)->sql_session = session;
    (*out)->sql_session_ended = ended;
    return UD_OK;
}
ud_status ud_activation_acquire_grant(ud_activation *a, ud_string id, ud_connection_lease **out) {
    return acquire(a, id, 0, 4, out);
}
ud_status ud_acquire_file_connection(ud_activation *a, ud_string id, uint64_t caps,
                                     ud_connection_lease **out) {
    return acquire(a, id, caps, 1, out);
}
ud_status ud_acquire_process_connection(ud_activation *a, ud_string id, ud_connection_lease **out) {
    return acquire(a, id, UD_CONNECT_EXECUTE_PROCESS, 2, out);
}
ud_status ud_acquire_http_connection(ud_activation *a, ud_string id, ud_connection_lease **out) {
    return acquire(a, id, UD_CONNECT_HTTP, 3, out);
}
ud_status ud_connection_lease_path(const ud_connection_lease *l, ud_string *out) {
    if (!l || !out)
        return UD_INVALID;
    if (!ud_named(l->driver, "file") && !ud_named(l->driver, "excel"))
        return UD_UNSUPPORTED;
    *out = l->resource;
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
    if (!network_driver(l->driver))
        return UD_UNSUPPORTED;
    UD_TRY(ud_connection_lease_authorize(l, UD_ACCESS_NETWORK));
    const ud_json *ref = ud_member(l->definition, "credentialReference");
    if (!ref)
        return UD_OK;
    ud_string reference;
    UD_TRY(ud_json_string(ref, &reference));
    return ud_http_resolve_reference(l, l->resource, reference, out);
}
ud_status ud_http_resolve_reference(ud_connection_lease *l, ud_string resource,
                                    ud_string reference, ud_string *out) {
    if (!l || !out || !reference.length)
        return UD_INVALID;
    *out = (ud_string){0};
    UD_TRY(ud_connection_lease_authorize_uri(l, resource));
    ud_execution *e = l->activation->execution;
    if (!e->options.resolve)
        return UD_AUTH;
    ud_status status = e->options.resolve(e->options.resolve_user, e->ctx, e->options.tenant,
                                          resource, reference, out);
    if (status || !out->data || !out->length || out->length > 32768) {
        ud_free((void *)out->data);
        *out = (ud_string){0};
        return status ? status : UD_SCHEMA;
    }
    return UD_OK;
}
const ud_json *ud_http_connection_oauth(ud_connection_lease *l, ud_http_oauth_cache **cache) {
    *cache = l ? &l->oauth : NULL;
    return l ? ud_member(l->definition, "oauth2") : NULL;
}
ud_status ud_network_connection_resource(ud_connection_lease *lease, ud_string *out) {
    if (!lease || !out)
        return UD_INVALID;
    if (!network_driver(lease->driver))
        return UD_UNSUPPORTED;
    *out = lease->resource;
    return UD_OK;
}
ud_status ud_connection_lease_bind_rest(ud_connection_lease *l, const ud_component_context *c,
                                        const ud_json *config) {
    if (!l || !c || l->rest || !ud_named(l->driver, "http"))
        return UD_INVALID;
    UD_TRY(ud_connection_lease_authorize(l, UD_ACCESS_WRITE));
    ud_status status = ud_rest_target_open(c, l, config, &l->rest);
    if (!status)
        l->capabilities |=
            UD_CONNECT_APPEND | UD_CONNECT_UPSERT | UD_CONNECT_DELETE | UD_CONNECT_CHANGESET;
    if (!status && ud_rest_target_delivery(l->rest) == UD_DELIVERY_RECEIPT)
        l->capabilities |= UD_CONNECT_RECEIPTS;
    return status;
}
int32_t ud_connection_lease_delivery(const ud_connection_lease *lease) {
    return !lease                                        ? 0
           : lease->rest                                 ? ud_rest_target_delivery(lease->rest)
           : (lease->capabilities & UD_CONNECT_RECEIPTS) ? UD_DELIVERY_RECEIPT
                                                         : UD_DELIVERY_RESPONSE;
}
static ud_status lease_operation_begin(ud_connection_lease *lease, int32_t action) {
    UD_TRY(ud_connection_lease_authorize(lease, action));
    ud_context_clear_error(lease->activation->execution->connections->pool->ctx);
    return ud_connection_operation_begin(lease->connection, lease, session_grant, check_lease);
}
static ud_status lease_operation_end(ud_connection_lease *lease, ud_status status) {
    ud_connection_operation_end(lease->connection);
    return relay_driver_error(lease, status);
}
ud_status ud_connection_lease_savepoint(ud_connection_lease *lease, ud_string name, int32_t phase) {
    if (!lease || !lease->connection)
        return UD_UNSUPPORTED;
    UD_TRY(lease_operation_begin(lease, UD_ACCESS_WRITE));
    return lease_operation_end(lease, ud_connection_savepoint(lease->connection, name, phase));
}
ud_status ud_connection_lease_record_diagnostic(ud_connection_lease *lease, ud_string *out) {
    if (!lease || !lease->connection)
        return UD_UNSUPPORTED;
    UD_TRY(lease_operation_begin(lease, UD_ACCESS_WRITE));
    return lease_operation_end(lease, ud_connection_record_diagnostic(lease->connection, out));
}
ud_status ud_connection_lease_read_open(ud_connection_lease *l, const ud_read_options *o,
                                        const ud_batch *b) {
    if (!l || !l->connection)
        return UD_UNSUPPORTED;
    UD_TRY(lease_operation_begin(l, UD_ACCESS_READ));
    return lease_operation_end(l, ud_connection_read_open(l->connection, o, b));
}
ud_status ud_connection_lease_read_next(ud_connection_lease *l, ud_batch **out) {
    if (!l || !l->connection)
        return UD_UNSUPPORTED;
    UD_TRY(lease_operation_begin(l, UD_ACCESS_READ));
    ud_status status = ud_connection_read_next(l->connection, out);
    if (!status)
        status = ud_batch_rehome(l->activation->execution->ctx, out);
    return lease_operation_end(l, status);
}
ud_status ud_connection_lease_message_subscribe(ud_connection_lease *l,
                                                const ud_message_subscription *options) {
    if (!l || !l->connection || !l->messaging || l->read_only)
        return UD_UNSUPPORTED;
    UD_TRY(lease_operation_begin(l, UD_ACCESS_READ));
    return lease_operation_end(l, ud_connection_message_subscribe(l->connection, options));
}
ud_status ud_connection_lease_message_subscribe_partition(ud_connection_lease *l,
    const ud_message_subscription *options, int64_t start) {
    if (!l || !l->connection || !l->messaging || l->read_only)
        return UD_UNSUPPORTED;
    UD_TRY(lease_operation_begin(l, UD_ACCESS_READ));
    return lease_operation_end(l, ud_connection_message_subscribe_partition(l->connection, options, start));
}
ud_status ud_connection_lease_message_receive(ud_connection_lease *l, uint32_t timeout_ms,
                                              ud_message *out) {
    if (!l || !l->connection || !l->messaging || l->read_only)
        return UD_UNSUPPORTED;
    UD_TRY(lease_operation_begin(l, UD_ACCESS_READ));
    return lease_operation_end(l, ud_connection_message_receive(l->connection, timeout_ms, out));
}
ud_status ud_connection_lease_message_acknowledge(ud_connection_lease *l, const uint64_t *deliveries,
                                                  uint32_t count) {
    if (!l || !l->connection || !l->messaging || l->read_only)
        return UD_UNSUPPORTED;
    UD_TRY(lease_operation_begin(l, UD_ACCESS_READ));
    return lease_operation_end(l, ud_connection_message_acknowledge(l->connection, deliveries, count));
}
ud_status ud_connection_lease_message_publish(ud_connection_lease *l, const ud_message_target *target,
                                              const ud_message *messages, uint32_t count,
                                              ud_message_result *results) {
    if (!l || !l->connection || !l->messaging || l->read_only)
        return UD_UNSUPPORTED;
    UD_TRY(lease_operation_begin(l, UD_ACCESS_WRITE));
    return lease_operation_end(l, ud_connection_message_publish(l->connection, target, messages,
                                                                 count, results));
}
ud_status ud_connection_lease_message_capture_epoch(ud_connection_lease *l, const uint64_t *ids,
                                                      uint32_t count, ud_message_epoch *out) {
    if (!l || !(l->capabilities & UD_CONNECT_MESSAGE_TRANSACTION) || l->read_only)
        return UD_UNSUPPORTED;
    UD_TRY(lease_operation_begin(l, UD_ACCESS_READ));
    return lease_operation_end(l, ud_connection_message_capture_epoch(l->connection, ids, count, out));
}
ud_status ud_connection_lease_message_begin_epoch(ud_connection_lease *l, ud_string id,
                                                   ud_string cluster) {
    if (!l || !(l->capabilities & UD_CONNECT_MESSAGE_TRANSACTION) || l->read_only)
        return UD_UNSUPPORTED;
    UD_TRY(lease_operation_begin(l, UD_ACCESS_WRITE));
    return lease_operation_end(l, ud_connection_message_begin_epoch(l->connection, id, cluster));
}
ud_status ud_connection_lease_message_commit_epoch(ud_connection_lease *l,
                                                    const ud_message_epoch *epoch) {
    if (!l || !(l->capabilities & UD_CONNECT_MESSAGE_TRANSACTION) || l->read_only)
        return UD_UNSUPPORTED;
    UD_TRY(lease_operation_begin(l, UD_ACCESS_WRITE));
    return lease_operation_end(l, ud_connection_message_commit_epoch(l->connection, epoch));
}
ud_status ud_connection_lease_message_abort_epoch(ud_connection_lease *l) {
    if (!l || !(l->capabilities & UD_CONNECT_MESSAGE_TRANSACTION))
        return UD_UNSUPPORTED;
    UD_TRY(ud_connection_operation_begin(l->connection, l, session_grant, NULL));
    return lease_operation_end(l, ud_connection_message_abort_epoch(l->connection));
}
ud_status ud_connection_lease_message_release_epoch(ud_connection_lease *l, const uint64_t *ids,
                                                     uint32_t count) {
    if (!l || !(l->capabilities & UD_CONNECT_MESSAGE_TRANSACTION))
        return UD_UNSUPPORTED;
    UD_TRY(ud_connection_operation_begin(l->connection, l, session_grant, NULL));
    return lease_operation_end(l, ud_connection_message_release_epoch(l->connection, ids, count));
}
ud_status ud_connection_lease_message_visit_committed(ud_connection_lease *l, ud_string topic,
                                                       int32_t partition, void *user,
                                                       ud_message_visit visit) {
    if (!l || !(l->capabilities & UD_CONNECT_MESSAGE_TRANSACTION) || l->read_only)
        return UD_UNSUPPORTED;
    UD_TRY(lease_operation_begin(l, UD_ACCESS_READ));
    return lease_operation_end(l, ud_connection_message_visit_committed(l->connection, topic,
                                                                         partition, user, visit));
}
ud_status ud_connection_lease_message_partition_positions(ud_connection_lease *l, ud_string topic, ud_string group,
    ud_message_partition_position *positions, uint32_t capacity, uint32_t *count) {
    if (!l || !l->connection)
        return UD_UNSUPPORTED;
    UD_TRY(lease_operation_begin(l, UD_ACCESS_DISCOVER));
    return lease_operation_end(l, ud_connection_message_partition_positions(l->connection,topic,group,positions,capacity,count));
}
ud_status ud_connection_lease_message_verify_epoch(ud_connection_lease *l,
                                                    const ud_message_epoch *epoch) {
    if (!l || !(l->capabilities & UD_CONNECT_MESSAGE_TRANSACTION) || l->read_only)
        return UD_UNSUPPORTED;
    UD_TRY(lease_operation_begin(l, UD_ACCESS_READ));
    return lease_operation_end(l, ud_connection_message_verify_epoch(l->connection, epoch));
}
ud_status ud_connection_lease_read_close(ud_connection_lease *l) {
    if (!l || !l->connection)
        return UD_UNSUPPORTED;
    UD_TRY(ud_connection_operation_begin(l->connection, l, session_grant, NULL));
    return lease_operation_end(l, ud_connection_read_close(l->connection));
}
ud_status ud_connection_lease_read_snapshot(ud_connection_lease *l, int32_t begin) {
    if (!l || !l->connection || !(l->capabilities & UD_CONNECT_READ_SNAPSHOT) || l->shared)
        return UD_UNSUPPORTED;
    if (begin)
        UD_TRY(lease_operation_begin(l, UD_ACCESS_READ));
    else
        UD_TRY(ud_connection_operation_begin(l->connection, l, session_grant, NULL));
    ud_status status = begin ? ud_connection_begin(l->connection, UD_ISOLATION_READ_SNAPSHOT)
                             : ud_connection_rollback(l->connection);
    return lease_operation_end(l, status);
}
ud_status ud_connection_lease_write(ud_connection_lease *l, const ud_write_options *o,
                                    const ud_batch *b) {
    if (!l || (!l->connection && !l->rest))
        return UD_UNSUPPORTED;
    UD_TRY(ud_connection_lease_authorize(l, UD_ACCESS_WRITE));
    if (l->rest) {
        uint8_t digest[32];
        UD_TRY(ud_write_digest(l->activation->execution->ctx,
                               l->activation->execution->options.tenant, l->resource, o, b,
                               digest));
        return ud_rest_target_write(l->rest, o, b, digest);
    }
    UD_TRY(lease_operation_begin(l, UD_ACCESS_WRITE));
    return lease_operation_end(l, ud_connection_write(l->connection, o, b));
}
ud_status ud_connection_lease_receipt(ud_connection_lease *l, ud_string job, ud_string batch,
                                      ud_commit_receipt *out) {
    if (!l || (!l->connection && !l->rest))
        return UD_UNSUPPORTED;
    UD_TRY(ud_connection_lease_authorize(l, UD_ACCESS_RECEIPT));
    if (l->rest)
        return ud_rest_target_receipt(l->rest, job, batch, out);
    UD_TRY(lease_operation_begin(l, UD_ACCESS_RECEIPT));
    return lease_operation_end(l, ud_connection_receipt(l->connection, job, batch, out));
}
ud_status ud_connection_lease_execute_command(ud_connection_lease *l,
                                              const ud_read_options *options, uint64_t *processed) {
    if (!l || !l->connection)
        return UD_UNSUPPORTED;
    UD_TRY(lease_operation_begin(l, UD_ACCESS_WRITE));
    ud_status status = l->exclusive_sql || l->sql_transaction
                           ? ud_connection_execute_transactional_command(l->connection, options, processed)
                           : ud_connection_execute_command(l->connection, options, processed);
    return lease_operation_end(l, status);
}
ud_status ud_connection_lease_execute_record(ud_connection_lease *lease,
                                             const ud_read_options *options,
                                             ud_command_result *out) {
    if (!lease || !lease->connection)
        return UD_UNSUPPORTED;
    UD_TRY(lease_operation_begin(lease, UD_ACCESS_WRITE));
    return lease_operation_end(lease,
                               ud_connection_execute_record(lease->connection, options, out));
}
ud_status ud_connection_lease_digest(ud_connection_lease *l, const ud_write_options *o,
                                     const ud_batch *b, uint8_t digest[32]) {
    if (!l || (!l->connection && !l->rest))
        return UD_UNSUPPORTED;
    UD_TRY(ud_connection_lease_authorize(l, UD_ACCESS_WRITE));
    return l->rest ? ud_write_digest(l->activation->execution->ctx,
                                     l->activation->execution->options.tenant, l->resource, o, b,
                                     digest)
                   : ud_connection_digest(l->connection, o, b, digest);
}
ud_status ud_connection_lease_metadata(ud_connection_lease *l, const ud_metadata_options *o,
                                       ud_batch **out) {
    if (!l || !l->connection)
        return UD_UNSUPPORTED;
    UD_TRY(lease_operation_begin(l, UD_ACCESS_DISCOVER));
    return lease_operation_end(l, ud_connection_metadata(l->connection, o, out));
}
ud_status ud_connection_lease_schema(ud_connection_lease *lease, ud_string table, ud_string scope,
                                     ud_batch **out) {
    if (!lease || !out)
        return UD_INVALID;
    UD_TRY(ud_connection_lease_authorize(lease, UD_ACCESS_DISCOVER));
    if (lease->connection) {
        UD_TRY(lease_operation_begin(lease, UD_ACCESS_DISCOVER));
        ud_status status = ud_connection_schema(lease->connection, table, scope, out);
        if (!status)
            status = ud_batch_rehome(lease->activation->execution->ctx, out);
        return lease_operation_end(lease, status);
    }
    if (!ud_named(lease->driver, "file"))
        return UD_UNSUPPORTED;
    UD_TRY(ud_connection_lease_authorize(lease, UD_ACCESS_READ));
    char *path = ud_strdup_n(lease->activation->execution->ctx, lease->resource.data,
                             lease->resource.length);
    if (!path)
        return UD_RESOURCE;
    ud_context *ctx = lease->activation->execution->ctx;
    ud_status status = ud_path_no_symlinks(ctx, path);
    ud_string format = table;
    ud_json *configuration = NULL;
    char *file_options = NULL;
    uint32_t option_length = 0;
    if (!status && table.length && table.data[0] == '{') {
        static const char *keys[] = {"format", "options"};
        status = ud_json_parse(ctx, table.data, table.length, &configuration, NULL);
        if (!status)
            status = ud_json_keys(configuration, keys, 2);
        if (!status)
            status = ud_json_string(ud_member(configuration, "format"), &format);
        if (!status && ud_member(configuration, "options"))
            status = ud_json_serialize(ud_member(configuration, "options"), &file_options,
                                       &option_length);
    }
    if (!format.length) {
        const char *extension = strrchr(path, '.');
        if (extension && !strcmp(extension, ".csv"))
            format = (ud_string){"csv", 3};
        else if (extension && !strcmp(extension, ".ndjson"))
            format = (ud_string){"ndjson", 6};
        else if (extension && !strcmp(extension, ".xlsx"))
            format = (ud_string){"xlsx", 4};
        else if (extension && !strcmp(extension, ".avro"))
            format = (ud_string){"avro", 4};
        else if (extension && !strcmp(extension, ".json"))
            format = (ud_string){"jsonArray", 9};
    }
    if (!status && ud_named(format, "csv")) {
        ud_csv_options options = {.struct_size = sizeof(options),
                                  .abi_version = UD_ABI_VERSION,
                                  .max_record_bytes = 16777216,
                                  .format = {file_options, option_length}};
        status = ud_csv_discover(ctx, path, scope, &options, 256, out);
    } else if (!status && (ud_named(format, "ndjson") || ud_named(format, "jsonArray"))) {
        ud_json_options options = {.struct_size = sizeof(options),
                                   .version = UD_CONFIG_VERSION,
                                   .batch_size = 256,
                                   .max_record_bytes = 16777216,
                                   .framing = ud_named(format, "jsonArray")
                                                  ? UD_JSON_FRAMING_ARRAY
                                                  : UD_JSON_FRAMING_LINES};
        status = ud_json_discover(ctx, path, scope, &options, 256, out);
    } else if (!status && ud_named(format, "xlsx")) {
        char *metadata = NULL;
        uint32_t size = 0;
        status = ud_xlsx_discover(ctx, path, scope, (ud_string){file_options, option_length}, out,
                                  &metadata, &size);
        ud_free(metadata);
    } else if (!status && ud_named(format, "avro")) {
        status = ud_avro_discover(ctx, path, scope, out);
    } else if (!status)
        status = UD_UNSUPPORTED;
    ud_free(path);
    ud_free(file_options);
    ud_json_destroy(configuration);
    return status;
}
ud_status ud_connection_lease_discover(ud_connection_lease *l, ud_string table, ud_batch **out) {
    if (!l || !l->connection)
        return UD_UNSUPPORTED;
    UD_TRY(lease_operation_begin(l, UD_ACCESS_DISCOVER));
    return lease_operation_end(l, ud_connection_discover(l->connection, table, out));
}
ud_status ud_connection_lease_release(ud_connection_lease *l) {
    if (!l)
        return UD_OK;
    struct ud_connection_manager *m = l->activation->execution->connections;
    ud_connection_pool *pool = m->pool;
    ud_mutex_lock(&pool->mutex);
    if (l->aliases || l->releasing) {
        ud_mutex_unlock(&pool->mutex);
        return UD_BUSY;
    }
    l->releasing = 1;
    while (l->pins)
        ud_condition_wait(&pool->changed, &pool->mutex);
    ud_mutex_unlock(&pool->mutex);
    session *s = l->session;
    int dirty = check_lease(l) != UD_OK;
    ud_status status = l->exclusive_sql ? ud_connection_lease_read_close(l) : UD_OK;
    if (s && s->connection) {
        ud_status closed = ud_connection_read_close(s->connection);
        if (!status)
            status = closed;
        if (!status && !dirty && !s->unhealthy) {
            if (!ud_connection_reusable(s->connection))
                dirty = 1;
            else
                status = ud_connection_reset_session(s->connection);
        }
        if (status != UD_BUSY) {
            ud_status detached = ud_connection_bind_lease(s->connection, NULL, NULL, NULL);
            if (detached)
                status = detached;
        }
    }
    ud_mutex_lock(&pool->mutex);
    if (status == UD_BUSY) {
        l->releasing = 0;
        ud_mutex_unlock(&pool->mutex);
        return status;
    }
    ud_connection_lease **at = &m->leases;
    while (*at && *at != l)
        at = &(*at)->next;
    if (*at) {
        *at = l->next;
        m->active--;
    }
    if (l->shared_owner) {
        l->shared_owner->aliases--;
        if (l->exclusive_sql)
            l->shared_owner->sql_borrowed = 0;
    }
    int retiring = 0;
    if (s) {
        s->idle_since = ud_now_ms();
        if (s->node_slot && s->idle_timeout > 1000)
            s->idle_timeout = 1000;
        if (status || dirty)
            s->unhealthy = 1;
        retiring = s->unhealthy || (s->plugin_capabilities & UD_CAP_THREAD_AFFINE);
        if (retiring) {
            session **entry = &pool->sessions;
            while (*entry && *entry != s)
                entry = &(*entry)->next;
            if (*entry) {
                *entry = s->next;
                s->next = NULL;
            }
        }
        s->borrower = NULL;
        m->physical--;
    }
    ud_condition_wake(&pool->changed);
    ud_mutex_unlock(&pool->mutex);
    if (retiring) {
        ud_status disposed = ud_manager_pool_dispose(pool, s);
        if (!status)
            status = disposed;
    }
    status = relay_driver_error(l, status);
    if (l->sql_session_ended)
        l->sql_session_ended(l->sql_session);
    ud_rest_target_close(l->rest);
    ud_http_clear_secret(&l->oauth.token);
    ud_json_destroy(l->definition);
    ud_free(l);
    return status;
}

ud_status ud_connection_lease_sql_transaction(ud_connection_lease *lease, int32_t operation,
                                              int32_t isolation) {
    if (!lease || !lease->connection || lease->shared)
        return UD_UNSUPPORTED;
    if (operation == UD_SQL_SESSION_ROLLBACK) {
        UD_TRY(ud_connection_operation_begin(lease->connection, lease, session_grant, NULL));
        ud_status status = ud_connection_rollback(lease->connection);
        if (!status)
            lease->sql_transaction = 0;
        return lease_operation_end(lease, status);
    }
    UD_TRY(lease_operation_begin(lease, UD_ACCESS_WRITE));
    ud_status status = operation == UD_SQL_SESSION_BEGIN
                       ? ud_connection_begin(lease->connection, isolation)
                       : ud_connection_commit(lease->connection);
    if (!status)
        lease->sql_transaction = operation == UD_SQL_SESSION_BEGIN;
    return lease_operation_end(lease, status);
}

void ud_connection_lease_cancel(ud_connection_lease *lease) {
    if (lease && lease->exclusive_sql)
        lease = lease->shared_owner;
    if (!lease || !lease->connection || lease->shared)
        return;
    struct ud_connection_manager *manager = lease->activation->execution->connections;
    ud_mutex_lock(&manager->pool->mutex);
    if (atomic_load(&lease->cancelled) || lease->releasing) {
        ud_mutex_unlock(&manager->pool->mutex);
        return;
    }
    atomic_store(&lease->cancelled, 1);
    lease->pins++;
    ud_mutex_unlock(&manager->pool->mutex);
    ud_connection_cancel(lease->connection);
    ud_mutex_lock(&manager->pool->mutex);
    lease->pins--;
    ud_condition_wake(&manager->pool->changed);
    ud_mutex_unlock(&manager->pool->mutex);
}
void ud_connections_cancel(ud_execution *e) {
    if (!e || !e->connections)
        return;
    struct ud_connection_manager *m = e->connections;
    ud_mutex_lock(&m->pool->mutex);
    for (;;) {
        ud_connection_lease *l = m->leases;
        while (l && (atomic_load(&l->cancelled) || !l->connection || l->shared || l->releasing))
            l = l->next;
        if (!l)
            break;
        atomic_store(&l->cancelled, 1);
        l->pins++;
        ud_mutex_unlock(&m->pool->mutex);
        ud_connection_cancel(l->connection);
        ud_mutex_lock(&m->pool->mutex);
        l->pins--;
        ud_condition_wake(&m->pool->changed);
    }
    ud_condition_wake(&m->pool->changed);
    ud_mutex_unlock(&m->pool->mutex);
}
ud_status ud_connections_release_activation(ud_activation *a) {
    struct ud_connection_manager *m = a->execution->connections;
    if (!m)
        return UD_OK;
    int leaked = 0;
    for (;;) {
        ud_mutex_lock(&m->pool->mutex);
        ud_connection_lease *l = m->leases;
        while (l) {
            int retained = 0;
            if (a->transaction)
                for (uint32_t at = 0; at < a->transaction->count; at++)
                    retained |= a->transaction->participants[at].lease == l;
            if (l->activation == a && !retained)
                break;
            l = l->next;
        }
        ud_mutex_unlock(&m->pool->mutex);
        if (!l)
            break;
        leaked = 1;
        UD_TRY(ud_connection_lease_release(l));
    }
    return leaked ? UD_RESOURCE : UD_OK;
}
ud_status ud_connections_destroy(ud_execution *e) {
    struct ud_connection_manager *m = e->connections;
    if (!m)
        return UD_OK;
    ud_connection_pool *pool = m->pool;
    int external = pool->owns_context;
    ud_mutex_lock(&pool->mutex);
    int busy = m->leases || m->active || m->physical;
    if (!busy && external)
        pool->users--;
    ud_mutex_unlock(&pool->mutex);
    if (busy)
        return UD_BUSY;
    if (!external)
        UD_TRY(ud_connection_pool_destroy(pool));
    ud_free(m);
    e->connections = NULL;
    return UD_OK;
}
ud_status ud_connections_diagnostics(ud_execution *e, ud_bytes *b) {
    struct ud_connection_manager *m = e->connections;
    if (!m)
        return ud_bytes_put(b, "{}", 2);
    char text[512];
    ud_mutex_lock(&m->pool->mutex);
    int n = snprintf(text, sizeof(text),
                     "{\"sessions\":%u,\"activeLeases\":%u,\"capacity\":%u,\"acquisitions\":%llu,"
                     "\"poolHits\":%llu,\"failures\":%llu,\"waitMs\":%llu,\"commits\":%llu,"
                     "\"rollbacks\":%llu,\"commitUnknown\":%llu}",
                     m->pool->count, m->active, m->pool->capacity, (unsigned long long)m->acquisitions,
                     (unsigned long long)m->hits, (unsigned long long)m->failures,
                     (unsigned long long)m->wait_ms, (unsigned long long)m->commits,
                     (unsigned long long)m->rollbacks, (unsigned long long)m->uncertain);
    ud_mutex_unlock(&m->pool->mutex);
    return ud_bytes_put(b, text, (size_t)n);
}
int ud_transaction_active(const ud_activation *activation) {
    return activation && activation->transaction && !activation->transaction->completed;
}
void ud_transaction_sql_failed(ud_activation *activation, ud_status status) {
    if (!ud_transaction_active(activation) || !status || status == UD_END || status == UD_BUSY)
        return;
    struct ud_connection_manager *manager = activation->execution->connections;
    ud_mutex_lock(&manager->pool->mutex);
    if (!activation->transaction->sql_failure)
        activation->transaction->sql_failure = status;
    ud_mutex_unlock(&manager->pool->mutex);
}
ud_status ud_transaction_resource_validate(const ud_activation *activation, ud_string connection) {
    if (!activation || !activation->transaction_serial)
        return UD_OK;
    const ud_json *participants = activation->transaction_participants;
    for (uint32_t index = 0; index < ud_json_size(participants); index++) {
        ud_string id;
        UD_TRY(ud_json_string(ud_json_at(participants, index), &id));
        if (ud_same(id, connection))
            return UD_OK;
    }
    const ud_json *definition;
    int project;
    return ud_connection_definition((ud_activation *)activation, connection, &definition, &project);
}
ud_status ud_transaction_fact_position(ud_activation *activation, ud_string *identity,
                                       uint64_t *order) {
    if (!activation || !identity || !order)
        return UD_INVALID;
    struct ud_transaction *transaction = activation->transaction;
    if (!transaction || transaction->completed)
        return UD_END;
    if (transaction->fact_order == UINT64_MAX)
        return UD_RESOURCE;
    *identity = (ud_string){transaction->identity, 64};
    *order = ++transaction->fact_order;
    return UD_OK;
}
int ud_transaction_task_done(ud_activation *activation) {
    /* The owner records its own durable fact after leave. Children remain
     * tentative until the enclosing transaction publishes their facts. */
    if (!ud_transaction_active(activation) || activation->transaction->owner == activation)
        return 0;
    activation->transaction->pending_tasks++;
    return 1;
}
int ud_transaction_recovered(const ud_activation *activation) {
    return activation && activation->transaction && activation->transaction->owner == activation &&
           activation->transaction->completed;
}
int ud_transaction_owned(const ud_activation *activation) {
    return ud_transaction_active(activation) && activation->transaction->owner == activation;
}
void ud_transaction_pending(const ud_activation *activation, uint64_t *rows, uint64_t *errors) {
    *rows = *errors = 0;
    if (ud_transaction_owned(activation)) {
        *rows = activation->transaction->pending_rows;
        *errors = activation->transaction->pending_errors;
    }
}
static void transaction_hex(const uint8_t digest[32], char output[65]) {
    static const char alphabet[] = "0123456789abcdef";
    for (uint32_t index = 0; index < 32; index++) {
        output[2 * index] = alphabet[digest[index] >> 4];
        output[2 * index + 1] = alphabet[digest[index] & 15];
    }
    output[64] = 0;
}
static ud_status transaction_identity(struct ud_transaction *transaction) {
    ud_activation *activation = transaction->owner;
    ud_bytes bytes = {.ctx = activation->execution->ctx};
    ud_status status = ud_put_string(&bytes, activation->execution->options.execution_id.data,
                                     activation->execution->options.execution_id.length);
    if (!status)
        status = ud_put_string(&bytes, activation->task_id.data, activation->task_id.length);
    if (!status)
        status = ud_put_u64(&bytes, transaction->attempt);
    if (!status)
        status = ud_bytes_put(&bytes, activation->package->fingerprint, 32);
    uint8_t digest[32];
    if (!status) {
        ud_hash(bytes.data, bytes.size, digest);
        transaction_hex(digest, transaction->identity);
    }
    ud_bytes_clear(&bytes);
    for (uint32_t index = 0; !status && index < transaction->count; index++) {
        transaction_participant *participant = transaction->participants + index;
        bytes = (ud_bytes){.ctx = activation->execution->ctx};
        status = ud_put_string(&bytes, transaction->identity, 64);
        if (!status)
            status =
                ud_put_string(&bytes, participant->connection.data, participant->connection.length);
        if (!status)
            status = ud_put_string(&bytes, participant->lease->resource.data,
                                   participant->lease->resource.length);
        if (!status)
            status = ud_put_string(&bytes, participant->lease->driver.data,
                                   participant->lease->driver.length);
        if (!status) {
            ud_hash(bytes.data, bytes.size, digest);
            transaction_hex(digest, participant->branch_id);
        }
        char *definition = NULL;
        uint32_t length = 0;
        if (!status)
            status = ud_json_serialize(participant->lease->definition, &definition, &length);
        if (!status)
            status = ud_put_string(&bytes, definition, length);
        ud_free(definition);
        if (!status)
            ud_hash(bytes.data, bytes.size, participant->branch.digest);
        participant->branch.id = (ud_string){participant->branch_id, 64};
        ud_bytes_clear(&bytes);
    }
    return status;
}
static ud_status transaction_manifest(struct ud_transaction *transaction, char **output,
                                      uint32_t *length) {
    ud_bytes bytes = {.ctx = transaction->owner->execution->ctx};
    char head[144];
    int size = snprintf(
        head, sizeof(head), "{\"version\":%u,\"attempt\":%llu,\"id\":\"%s\",\"participants\":[",
        UD_CHECKPOINT_VERSION, (unsigned long long)transaction->attempt, transaction->identity);
    ud_status status = ud_bytes_put(&bytes, head, (size_t)size);
    for (uint32_t index = 0; !status && index < transaction->count; index++) {
        transaction_participant *participant = transaction->participants + index;
        if (index)
            status = ud_bytes_put(&bytes, ",", 1);
        if (!status)
            status = ud_bytes_put(&bytes, "{\"connection\":", 14);
        if (!status)
            status =
                ud_json_quote(&bytes, participant->connection.data, participant->connection.length);
        char digest[65], details[192];
        transaction_hex(participant->branch.digest, digest);
        size = snprintf(details, sizeof(details),
                        ",\"branch\":\"%s\",\"digest\":\"%s\",\"isolation\":%d}",
                        participant->branch_id, digest, participant->branch.isolation);
        if (!status)
            status = ud_bytes_put(&bytes, details, (size_t)size);
    }
    if (!status)
        status = ud_bytes_put(&bytes, "]}", 2);
    if (!status && bytes.size > UINT32_MAX)
        status = UD_RESOURCE;
    if (!status) {
        *output = ud_strdup_n(bytes.ctx, (const char *)bytes.data, bytes.size);
        *length = (uint32_t)bytes.size;
        if (!*output)
            status = UD_RESOURCE;
    }
    ud_bytes_clear(&bytes);
    return status;
}
static ud_status transaction_record_decision(struct ud_transaction *transaction, int decision) {
    const char *payload = decision == UD_TX_COMMIT ? "\"commit\"" : "\"rollback\"";
    return ud_checkpoint_transaction_record(transaction->owner, UD_TX_JOURNAL_DECISION,
                                            (ud_string){transaction->identity, 64},
                                            (ud_string){payload, (uint32_t)strlen(payload)});
}
static ud_status transaction_resolve_all(struct ud_transaction *transaction, int decision) {
    ud_status status = UD_OK;
    /* Every participant is attempted even when another resource is unavailable. */
    for (uint32_t index = 0; index < transaction->count; index++) {
        transaction_participant *participant = transaction->participants + index;
        if (decision == UD_TX_ROLLBACK && !participant->begun)
            continue;
        participant->lease->cleanup = 1;
        ud_status resolved = ud_connection_transaction_resolve(participant->lease->connection,
                                                               &participant->branch, decision);
        participant->lease->cleanup = 0;
        if (resolved && !status)
            status = resolved;
    }
    if (status)
        return UD_COMMIT_UNKNOWN;
    ud_activation *activation = transaction->owner;
    ud_string identity = {transaction->identity, 64};
    status = ud_checkpoint_transaction_facts(activation, identity, decision == UD_TX_COMMIT);
    const char *payload = decision == UD_TX_COMMIT ? "\"commit\"" : "\"rollback\"";
    if (!status)
        status = ud_checkpoint_transaction_record(activation, UD_TX_JOURNAL_COMPLETE, identity,
                                                  (ud_string){payload, (uint32_t)strlen(payload)});
    if (!status && decision == UD_TX_COMMIT)
        status = ud_checkpoint_transaction_facts(activation, identity, 0);
    if (status)
        return UD_COMMIT_UNKNOWN;
    transaction->completed = 1;
    return UD_OK;
}
static ud_status transaction_release(struct ud_transaction *transaction) {
    ud_status status = UD_OK;
    if (!transaction->participants) {
        ud_free(transaction);
        return UD_OK;
    }
    for (uint32_t index = 0; index < transaction->count; index++) {
        ud_status released = ud_connection_lease_release(transaction->participants[index].lease);
        if (!status && released)
            status = released;
    }
    ud_rule_delivery_destroy(transaction->delivery);
    ud_free(transaction->participants);
    ud_free(transaction);
    return status;
}
ud_status ud_transaction_enter(ud_activation *activation, const ud_json *policy, int validate) {
    if (!policy)
        return UD_OK;
    static const char *keys[] = {"mode", "participants", "isolation", "provisionReceipts", "scope"};
    UD_TRY(ud_json_keys(policy, keys, 5));
    ud_string mode;
    UD_TRY(ud_json_string(ud_member(policy, "mode"), &mode));
    if (ud_named(mode, "notSupported")) {
        if (activation->transaction)
            return UD_UNSUPPORTED;
        activation->transaction = NULL;
        activation->transaction_serial = 0;
        activation->transaction_epoch = 0;
        activation->transaction_participants = NULL;
        return UD_OK;
    }
    if (ud_named(mode, "supported"))
        return UD_OK;
    if (!ud_named(mode, "required"))
        return UD_SCHEMA;
    ud_string scope = {"task", 4};
    if (ud_member(policy, "scope"))
        UD_TRY(ud_json_string(ud_member(policy, "scope"), &scope));
    if (!ud_named(scope, "task") && !ud_named(scope, "epoch"))
        return UD_SCHEMA;
    if (ud_named(scope, "epoch")) {
        ud_string kind = {0};
        UD_TRY(ud_json_string(ud_member(activation->task_definition, "kind"), &kind));
        if (!ud_named(kind, "dataFlow") || activation->transaction ||
            activation->transaction_serial)
            return UD_UNSUPPORTED;
        activation->transaction_epoch = 1;
    }
    const ud_json *participants = ud_member(policy, "participants");
    uint32_t count = ud_json_size(participants);
    if (ud_json_kind(participants) != UD_JSON_ARRAY || !count || count > 64)
        return UD_SCHEMA;
    if (!activation->execution->options.state_directory.length)
        return UD_UNSUPPORTED;
    int32_t isolation = UD_ISOLATION_SERIALIZABLE;
    if (ud_member(policy, "isolation")) {
        ud_string value;
        UD_TRY(ud_json_string(ud_member(policy, "isolation"), &value));
        isolation = ud_named(value, "serializable")     ? UD_ISOLATION_SERIALIZABLE
                    : ud_named(value, "repeatableRead") ? UD_ISOLATION_REPEATABLE_READ
                    : ud_named(value, "readCommitted")  ? UD_ISOLATION_READ_COMMITTED
                                                        : 0;
        if (!isolation)
            return UD_SCHEMA;
    }
    int provision = 0;
    if (ud_member(policy, "provisionReceipts")) {
        const char *value;
        uint32_t length;
        if (ud_json_kind(ud_member(policy, "provisionReceipts")) != UD_JSON_BOOL)
            return UD_SCHEMA;
        UD_TRY(ud_json_text(ud_member(policy, "provisionReceipts"), &value, &length));
        provision = length == 4;
    }
    for (uint32_t index = 0; index < count; index++) {
        ud_string id;
        UD_TRY(ud_json_string(ud_json_at(participants, index), &id));
        for (uint32_t before = 0; before < index; before++) {
            ud_string previous;
            UD_TRY(ud_json_string(ud_json_at(participants, before), &previous));
            if (ud_same(previous, id))
                return UD_SCHEMA;
        }
        const ud_json *definition;
        int project;
        UD_TRY(ud_connection_definition(activation, id, &definition, &project));
        ud_string driver;
        UD_TRY(ud_json_string(ud_member(definition, "driver"), &driver));
        if (ud_named(driver, "file") || ud_named(driver, "excel") || network_driver(driver) ||
            ud_named(driver, "process"))
            return UD_UNSUPPORTED;
        if (activation->transaction) {
            int member = 0;
            for (uint32_t existing = 0; existing < activation->transaction->count; existing++)
                member |= ud_same(activation->transaction->participants[existing].connection, id);
            if (!member)
                return UD_UNSUPPORTED;
        }
    }
    if (activation->transaction)
        return UD_OK;
    activation->transaction_serial = 1;
    activation->transaction_participants = participants;
    if (validate)
        return UD_OK;
    if (!activation->execution->journal)
        return UD_UNSUPPORTED;
    struct ud_transaction *transaction = ud_alloc(activation->execution->ctx, sizeof(*transaction));
    if (!transaction)
        return UD_RESOURCE;
    transaction->owner = activation;
    transaction->count = count;
    transaction->attempt = 1;
    transaction->participants =
        ud_alloc(activation->execution->ctx, (size_t)count * sizeof(*transaction->participants));
    ud_status status = transaction->participants
                           ? ud_activation_scope_identity(activation, transaction->scope_identity)
                           : UD_RESOURCE;
    char *saved = NULL;
    uint32_t saved_length = 0;
    ud_status found =
        !status ? ud_checkpoint_transaction_read(activation, UD_TX_JOURNAL_MANIFEST,
                                                 (ud_string){transaction->scope_identity, 64},
                                                 &saved, &saved_length)
                : UD_END;
    if (!status && found != UD_END && found)
        status = found;
    if (activation->execution->transaction_recovery_only && !status && found == UD_END) {
        status = transaction_release(transaction);
        if (!status)
            activation->execution->transaction_recovery_decision = 0;
        return status;
    }
    for (uint32_t index = 0; !status && index < count; index++) {
        transaction_participant *participant = transaction->participants + index;
        status = ud_json_string(ud_json_at(participants, index), &participant->connection);
        participant->branch = (ud_transaction_branch){.struct_size = sizeof(ud_transaction_branch),
                                                      .abi_version = UD_ABI_VERSION,
                                                      .isolation = isolation};
        uint64_t required = UD_CONNECT_TRANSACTION;
        if (count > 1 || activation->transaction_epoch)
            required |= UD_CONNECT_TRANSACTION_PARTICIPANT;
        if (!status)
            status = ud_activation_acquire_connection(
                activation, participant->connection, required, &participant->lease);
        if (!status) {
            uint64_t capabilities = ud_connection_lease_capabilities(participant->lease);
            participant->local = (capabilities & UD_CONNECT_TRANSACTION_LOCAL) != 0;
            if (participant->local &&
                ((capabilities & UD_CONNECT_TRANSACTION_PARTICIPANT) || count != 1 ||
                 activation->transaction_epoch))
                status = UD_UNSUPPORTED;
            else if (!participant->local &&
                     !(capabilities & UD_CONNECT_TRANSACTION_PARTICIPANT))
                status = UD_UNSUPPORTED;
        }
    }
    /* Capability-check the complete set before provisioning or starting a branch. */
    if (provision && !activation->execution->transaction_recovery_only)
        for (uint32_t index = 0; !status && index < count; index++)
            status = ud_connection_lease_provision(transaction->participants[index].lease);
    if (!status && !found) {
        ud_json *manifest = NULL;
        status = ud_json_parse(activation->execution->ctx, saved, saved_length, &manifest, NULL);
        if (!status)
            status = ud_json_uint(ud_member(manifest, "attempt"), &transaction->attempt);
        if (!status)
            status = transaction_identity(transaction);
        for (uint32_t index = 0; !status && index < count; index++)
            transaction->participants[index].begun = 1; /* Manifest may precede a crashed begin. */
        char *current = NULL;
        uint32_t current_length = 0;
        if (!status)
            status = transaction_manifest(transaction, &current, &current_length);
        if (!status && (current_length != saved_length || memcmp(current, saved, current_length)))
            status = UD_FENCED;
        ud_free(current);
        ud_json_destroy(manifest);
        char *decision_json = NULL;
        uint32_t decision_length = 0;
        ud_status decided =
            !status ? ud_checkpoint_transaction_read(activation, UD_TX_JOURNAL_DECISION,
                                                     (ud_string){transaction->identity, 64},
                                                     &decision_json, &decision_length)
                    : UD_END;
        int decision = UD_TX_ROLLBACK;
        if (!status && decided != UD_END && decided)
            status = decided;
        if (!status && !decided) {
            if (decision_length == 8 && !memcmp(decision_json, "\"commit\"", 8))
                decision = UD_TX_COMMIT;
            else if (decision_length != 10 || memcmp(decision_json, "\"rollback\"", 10))
                status = UD_CORRUPT;
        } else if (!status) {
            if (transaction_is_local(transaction)) {
                /* A local participant commits its database before the Native
                 * decision record. On restart, the provider receipt is the
                 * only authoritative commit proof; absence under its writer
                 * lock proves that the open SQLite transaction rolled back. */
                int32_t state = UD_TX_UNKNOWN;
                status = ud_connection_transaction_recover(
                    transaction->participants[0].lease->connection,
                    &transaction->participants[0].branch, &state);
                if (!status && state == UD_TX_COMMITTED)
                    decision = UD_TX_COMMIT;
                else if (!status && (state == UD_TX_ACTIVE || state == UD_TX_NOT_FOUND))
                    decision = UD_TX_ROLLBACK;
                else if (!status)
                    status = UD_CORRUPT;
            }
            if (!status)
                status = transaction_record_decision(transaction, decision);
        }
        ud_free(decision_json);
        char *completion = NULL;
        uint32_t completion_length = 0;
        ud_status finished =
            !status ? ud_checkpoint_transaction_read(activation, UD_TX_JOURNAL_COMPLETE,
                                                     (ud_string){transaction->identity, 64},
                                                     &completion, &completion_length)
                    : UD_END;
        if (!status && finished != UD_END && finished)
            status = finished;
        if (!status && !finished &&
            ((decision == UD_TX_COMMIT &&
              (completion_length != 8 || memcmp(completion, "\"commit\"", 8))) ||
             (decision == UD_TX_ROLLBACK &&
              (completion_length != 10 || memcmp(completion, "\"rollback\"", 10)))))
            status = UD_CORRUPT;
        ud_free(completion);
        if (!status && finished == UD_END)
            status = transaction_resolve_all(transaction, decision);
        else if (!status)
            transaction->completed = 1;
        if (status || activation->execution->transaction_recovery_only) {
            /* An existing decision always dominates a failed rebind or scan. */
            ud_status released = transaction_release(transaction);
            ud_free(saved);
            if (!status && !released)
                activation->execution->transaction_recovery_decision = decision;
            return status ? status : released;
        }
        if (!status && decision == UD_TX_COMMIT && !activation->transaction_epoch) {
            activation->transaction = transaction;
            ud_free(saved);
            return UD_OK;
        }
        if (transaction->attempt == UINT64_MAX) {
            (void)transaction_release(transaction);
            ud_free(saved);
            return UD_RESOURCE;
        }
        if (!status) {
            transaction->attempt++;
            transaction->completed = 0;
            for (uint32_t index = 0; index < count; index++)
                transaction->participants[index].begun = 0;
        }
    }
    ud_free(saved);
    if (!status)
        status = transaction_identity(transaction);
    /* Prove branch recovery and the installed receipts before journaling an attempt. */
    for (uint32_t index = 0; !status && index < count; index++) {
        transaction_participant *participant = transaction->participants + index;
        int32_t state = UD_TX_UNKNOWN;
        status = ud_connection_transaction_recover(participant->lease->connection,
                                                    &participant->branch, &state);
        if (!status && state != UD_TX_NOT_FOUND)
            status = UD_FENCED;
    }
    char *manifest = NULL;
    uint32_t length = 0;
    if (!status)
        status = transaction_manifest(transaction, &manifest, &length);
    if (!status)
        status = ud_checkpoint_transaction_record(activation, UD_TX_JOURNAL_MANIFEST,
                                                  (ud_string){transaction->scope_identity, 64},
                                                  (ud_string){manifest, length});
    ud_free(manifest);
    for (uint32_t index = 0; !status && index < count; index++) {
        transaction_participant *participant = transaction->participants + index;
        /* A failed response does not prove that the participant did not begin. */
        participant->begun = 1;
        status =
            ud_connection_transaction_begin(participant->lease->connection, &participant->branch);
    }
    if (status) {
        int any_begun = 0;
        for (uint32_t index = 0; transaction->participants && index < count; index++)
            any_begun |= transaction->participants[index].begun;
        if (any_begun) {
            ud_status decision = transaction_record_decision(transaction, UD_TX_ROLLBACK);
            if (!decision)
                decision = transaction_resolve_all(transaction, UD_TX_ROLLBACK);
            if (decision)
                status = UD_COMMIT_UNKNOWN;
        }
        ud_status released = transaction_release(transaction);
        return status ? status : released;
    }
    activation->transaction = transaction;
    return UD_OK;
}
ud_status ud_transaction_leave(ud_activation *activation, ud_status outcome) {
    struct ud_transaction *transaction = activation->transaction;
    if (!transaction || transaction->owner != activation)
        return outcome;
    struct ud_connection_manager *manager = activation->execution->connections;
    ud_mutex_lock(&manager->pool->mutex);
    if (!outcome)
        outcome = transaction->sql_failure;
    ud_mutex_unlock(&manager->pool->mutex);
    ud_status status = UD_OK;
    if (!transaction->completed) {
        for (uint32_t index = 0; !outcome && index < transaction->count; index++) {
            transaction_participant *participant = transaction->participants + index;
            ud_status prepared = ud_connection_transaction_prepare(participant->lease->connection,
                                                                   &participant->branch);
            if (prepared)
                outcome = prepared;
        }
        if (!outcome)
            outcome = ud_activation_check(activation);
        int decision = outcome ? UD_TX_ROLLBACK : UD_TX_COMMIT;
        if (!outcome && transaction_is_local(transaction)) {
            /* SQLite has no durable prepared branch. Commit the single
             * provider transaction first; only a successful provider commit
             * may be followed by the Native journal decision record. */
            transaction_participant *participant = transaction->participants;
            participant->lease->cleanup = 1;
            status = ud_connection_transaction_resolve(participant->lease->connection,
                                                       &participant->branch, UD_TX_COMMIT);
            participant->lease->cleanup = 0;
        }
        if (!status)
            status = transaction_record_decision(transaction, decision);
        if (!status)
            status = transaction_resolve_all(transaction, decision);
        if (!status && !outcome) {
            ud_target_rows(activation, transaction->pending_rows, transaction->pending_errors, 0);
            status = ud_rule_delivered(activation, transaction->delivery, 0);
            if (!status && activation->execution->journal) {
                ud_target_rows(activation, transaction->pending_rows, transaction->pending_errors,
                               1);
                status = ud_rule_delivered(activation, transaction->delivery, 1);
            }
            if (!status)
                atomic_fetch_add(&activation->execution->durable_tasks, transaction->pending_tasks);
        } else if (status)
            ud_rule_delivered(activation, transaction->delivery, 2);
    }
    ud_mutex_lock(&manager->pool->mutex);
    if (status)
        manager->uncertain++;
    else if (outcome)
        manager->rollbacks++;
    else
        manager->commits++;
    ud_mutex_unlock(&manager->pool->mutex);
    activation->transaction = NULL;
    ud_status released = transaction_release(transaction);
    return status ? UD_COMMIT_UNKNOWN : released ? released : outcome;
}

ud_status ud_transaction_commit_epoch(ud_activation *activation) {
    if (!activation || !activation->transaction_epoch || !ud_transaction_active(activation))
        return UD_UNSUPPORTED;
    struct ud_transaction *transaction = activation->transaction;
    if (transaction_is_local(transaction))
        return UD_UNSUPPORTED;
    ud_status status = UD_OK;
    for (uint32_t index = 0; !status && index < transaction->count; index++) {
        transaction_participant *participant = transaction->participants + index;
        status =
            ud_connection_transaction_prepare(participant->lease->connection, &participant->branch);
    }
    int decision = status ? UD_TX_ROLLBACK : UD_TX_COMMIT;
    ud_status resolved = transaction_record_decision(transaction, decision);
    if (!resolved)
        resolved = transaction_resolve_all(transaction, decision);
    if (resolved)
        return UD_COMMIT_UNKNOWN;
    if (status)
        return status;
    ud_activation *owner = transaction->owner;
    ud_target_rows(owner, transaction->pending_rows, transaction->pending_errors, 0);
    UD_TRY(ud_rule_delivered(owner, transaction->delivery, 0));
    ud_target_rows(owner, transaction->pending_rows, transaction->pending_errors, 1);
    UD_TRY(ud_rule_delivered(owner, transaction->delivery, 1));
    atomic_fetch_add(&owner->execution->durable_tasks, transaction->pending_tasks);
    ud_rule_delivery_destroy(transaction->delivery);
    transaction->delivery = NULL;
    transaction->pending_rows = transaction->pending_errors = transaction->pending_tasks = 0;
    transaction->fact_order = 0;
    ud_debug_advance(owner);
    UD_TRY(ud_debug_boundary(owner, UD_DEBUG_EPOCH));
    if (transaction->attempt == UINT64_MAX)
        return UD_RESOURCE;
    transaction->attempt++;
    transaction->completed = 0;
    for (uint32_t index = 0; index < transaction->count; index++)
        transaction->participants[index].begun = 0;
    UD_TRY(transaction_identity(transaction));
    char *manifest = NULL;
    uint32_t length = 0;
    status = transaction_manifest(transaction, &manifest, &length);
    if (!status)
        status = ud_checkpoint_transaction_record(owner, UD_TX_JOURNAL_MANIFEST,
                                                  (ud_string){transaction->scope_identity, 64},
                                                  (ud_string){manifest, length});
    ud_free(manifest);
    for (uint32_t index = 0; !status && index < transaction->count; index++) {
        transaction_participant *participant = transaction->participants + index;
        status =
            ud_connection_transaction_begin(participant->lease->connection, &participant->branch);
        if (!status)
            participant->begun = 1;
    }
    return status;
}

int ud_transaction_rows(ud_activation *a, uint64_t rows, uint64_t errors) {
    if (!a->transaction)
        return 0;
    a->transaction->pending_rows += rows;
    a->transaction->pending_errors += errors;
    return 1;
}

ud_status ud_transaction_delivery(ud_activation *a, const ud_batch *input) {
    return a->transaction ? ud_rule_stage(a, input, &a->transaction->delivery) : UD_INVALID;
}

ud_status ud_connection_lease_resource(const ud_connection_lease *l, ud_string *out) {
    if (!l || !out)
        return UD_INVALID;
    *out = l->resource;
    return UD_OK;
}
ud_status ud_connection_lease_snapshot_grant(ud_connection_lease *source, ud_activation *owner,
                                             ud_connection_lease **out) {
    if (!source || !owner || !out || source->activation->execution != owner->execution)
        return UD_INVALID;
    *out = NULL;
    UD_TRY(ud_connection_lease_authorize(source, UD_ACCESS_READ));
    ud_connection_lease *grant = ud_alloc(owner->execution->ctx, sizeof(*grant));
    if (!grant)
        return UD_RESOURCE;
    grant->activation = owner;
    atomic_init(&grant->cancelled, 0);
    char *json = NULL;
    uint32_t length = 0;
    ud_status status = ud_json_serialize(source->definition, &json, &length);
    if (!status)
        status = ud_json_parse(owner->execution->ctx, json, length, &grant->definition, NULL);
    ud_free(json);
    if (!status)
        status = ud_json_string(ud_member(grant->definition, "id"), &grant->id);
    if (!status)
        status = ud_json_string(ud_member(grant->definition, "resource"), &grant->resource);
    if (!status)
        status = ud_json_string(ud_member(grant->definition, "driver"), &grant->driver);
    if (!status)
        status = ud_execution_resolve_driver(owner->execution, &grant->driver);
    track(grant);
    if (status) {
        ud_status closed = ud_connection_lease_release(grant);
        return closed ? closed : status;
    }
    *out = grant;
    return UD_OK;
}

ud_status ud_connection_lease_provision(ud_connection_lease *lease) {
    if (!lease || !lease->connection)
        return UD_UNSUPPORTED;
    UD_TRY(lease_operation_begin(lease, UD_ACCESS_WRITE));
    return lease_operation_end(lease, ud_connection_provision(lease->connection));
}

ud_status ud_connection_lease_cdc_open(ud_connection_lease *l, const ud_cdc_options *o) {
    if (!l || !l->connection)
        return UD_UNSUPPORTED;
    UD_TRY(lease_operation_begin(l, UD_ACCESS_READ));
    return lease_operation_end(l, ud_connection_cdc_open(l->connection, o));
}
ud_status ud_connection_lease_cdc_next(ud_connection_lease *l, ud_cdc_event *e) {
    if (!l || !l->connection)
        return UD_UNSUPPORTED;
    UD_TRY(lease_operation_begin(l, UD_ACCESS_READ));
    ud_status status = ud_connection_cdc_next(l->connection, e);
    if (!status)
        status = ud_batch_rehome(l->activation->execution->ctx, &e->after);
    if (!status)
        status = ud_batch_rehome(l->activation->execution->ctx, &e->before);
    if (status && status != UD_BUSY)
        ud_cdc_event_clear(e);
    return lease_operation_end(l, status);
}
ud_status ud_connection_lease_cdc_confirm(ud_connection_lease *l, ud_string p) {
    if (!l || !l->connection)
        return UD_UNSUPPORTED;
    UD_TRY(lease_operation_begin(l, UD_ACCESS_READ));
    return lease_operation_end(l, ud_connection_cdc_confirm(l->connection, p));
}
ud_status ud_connection_lease_cdc_schema(ud_connection_lease *lease, ud_batch **out) {
    if (!lease || !lease->connection)
        return UD_UNSUPPORTED;
    UD_TRY(lease_operation_begin(lease, UD_ACCESS_READ));
    ud_status status = ud_connection_cdc_schema(lease->connection, out);
    if (!status)
        status = ud_batch_rehome(lease->activation->execution->ctx, out);
    return lease_operation_end(lease, status);
}
ud_status ud_connection_lease_cdc_rebind(ud_connection_lease *lease, const ud_batch *schema,
                                         uint64_t version) {
    if (!lease || !lease->connection)
        return UD_UNSUPPORTED;
    UD_TRY(lease_operation_begin(lease, UD_ACCESS_READ));
    return lease_operation_end(lease, ud_connection_cdc_rebind(lease->connection, schema, version));
}
ud_status ud_connection_lease_cdc_close(ud_connection_lease *l) {
    if (!l || !l->connection)
        return UD_UNSUPPORTED;
    UD_TRY(ud_connection_operation_begin(l->connection, l, session_grant, NULL));
    return lease_operation_end(l, ud_connection_cdc_close(l->connection));
}

int32_t ud_connection_lease_dialect(const ud_connection_lease *l) {
    return l && l->connection ? ud_connection_dialect(l->connection) : UD_SQL_CANONICAL;
}
int32_t ud_connection_lease_parameter_style(const ud_connection_lease *l) {
    return l && l->connection ? ud_connection_parameter_style(l->connection) : 0;
}
