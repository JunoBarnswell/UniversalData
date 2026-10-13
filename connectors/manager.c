#include "ud/connection_manager.h"
#include "../components/contracts/connection_internal.h"
#include "../engines/package/internal.h"
#include "rest.h"
#include "ud/csv.h"

typedef struct session {
    ud_plugin *plugin;
    ud_connection *connection;
    ud_string driver, resource;
    uint8_t key[32];
    uint64_t idle_since, idle_timeout;
    uint64_t capabilities, plugin_capabilities;
    int active, unhealthy;
    struct session *next;
} session;
struct ud_connection_manager {
    ud_execution *execution;
    session *sessions;
    ud_connection_lease *leases;
    uint32_t capacity, count, active;
    uint64_t acquisitions, hits, failures, wait_ms, commits, rollbacks, uncertain;
    ud_mutex mutex;
    ud_condition changed;
};
typedef struct transaction_participant {
    ud_connection_lease *lease;
    ud_string connection;
    ud_transaction_branch branch;
    char branch_id[65];
    int begun;
} transaction_participant;
struct ud_transaction {
    ud_activation *owner;
    transaction_participant *participants;
    uint32_t count;
    char identity[65];
    uint64_t attempt, fact_order;
    int completed;
    uint64_t pending_rows, pending_errors;
    uint64_t pending_tasks;
    struct ud_rule_delivery *delivery;
};
struct ud_connection_lease {
    ud_activation *activation;
    ud_json *definition;
    ud_string id, resource, driver;
    session *session;
    ud_rest_target *rest;
    ud_connection *connection;
    uint64_t capabilities;
    uint32_t pins;
    int cancelled, shared, read_only, cleanup;
    struct ud_connection_lease *next;
};
static ud_status check_lease(void *user) {
    ud_connection_lease *l = user;
    return l && !l->cleanup ? ud_activation_check(l->activation) : UD_OK;
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
    if (!l || !ud_same(tenant, l->activation->execution->options.tenant) ||
        !ud_same(resource, l->resource))
        return UD_AUTH;
    return ud_connection_lease_authorize(l, action);
}
ud_status ud_connections_create(ud_execution *e) {
    if (!e)
        return UD_INVALID;
    struct ud_connection_manager *m = ud_alloc(e->ctx, sizeof(*m));
    if (!m)
        return UD_RESOURCE;
    m->execution = e;
    m->capacity = 64;
    const ud_json *policy = ud_member(ud_member(e->package->document, "policy"), "connections");
    if (policy) {
        static const char *keys[] = {"maxSessions"};
        ud_status s = ud_json_keys(policy, keys, 1);
        uint64_t capacity = 0;
        if (!s)
            s = ud_json_uint(ud_member(policy, "maxSessions"), &capacity);
        if (!s && (!capacity || capacity > 65536))
            s = UD_SCHEMA;
        if (s) {
            ud_free(m);
            return s;
        }
        m->capacity = (uint32_t)capacity;
    }
    ud_mutex_init(&m->mutex);
    ud_condition_init(&m->changed);
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
    ud_mutex_lock(&m->mutex);
    l->next = m->leases;
    m->leases = l;
    m->active++;
    m->acquisitions++;
    ud_mutex_unlock(&m->mutex);
}
static ud_status dispose_session(session *s) {
    if (s->connection) {
        UD_TRY(ud_connection_close(s->connection));
        s->connection = NULL;
    }
    if (s->plugin)
        UD_TRY(ud_plugin_unload(s->plugin));
    ud_free((void *)s->driver.data);
    ud_free((void *)s->resource.data);
    ud_free(s);
    return UD_OK;
}
static ud_status acquire_session(ud_connection_lease *l, uint64_t caps) {
    ud_execution *e = l->activation->execution;
    struct ud_connection_manager *m = e->connections;
    if (!e->options.resolve)
        return UD_AUTH;
    ud_string reference;
    UD_TRY(ud_json_string(ud_member(l->definition, "credentialReference"), &reference));
    /* Resolve on EVERY acquisition. Neither a ref nor a connection string alone
     * establishes current principal/credential identity. Secret bytes stay private. */
    ud_string configuration = {0};
    ud_status status = e->options.resolve(e->options.resolve_user, e->ctx, e->options.tenant,
                                          l->resource, reference, &configuration);
    if (status) {
        ud_free((void *)configuration.data);
        return status;
    }
    if (!configuration.data || !configuration.length ||
        memchr(configuration.data, 0, configuration.length)) {
        ud_free((void *)configuration.data);
        return UD_AUTH;
    }
    ud_bytes identity = {.ctx = e->ctx};
    ud_string parts[] = {e->options.tenant, l->driver, l->resource, reference, configuration};
    for (uint32_t i = 0; !status && i < 5; i++)
        status = ud_put_string(&identity, parts[i].data, parts[i].length);
    char *definition = NULL;
    uint32_t length = 0;
    if (!status)
        status = ud_json_serialize(l->definition, &definition, &length);
    if (!status)
        status = ud_put_string(&identity, definition, length);
    ud_free(definition);
    uint8_t key[32];
    if (!status)
        ud_hash(identity.data, identity.size, key);
    ud_bytes_clear(&identity);
    uint64_t limit = 0, timeout = 0, idle = 0;
    if (!status)
        status = definition_limit(l->definition, "maxLeases", 8, 65536, &limit);
    if (!status && !limit)
        status = UD_SCHEMA;
    if (!status)
        status = definition_limit(l->definition, "acquireTimeoutMs", 0, 3600000, &timeout);
    if (!status)
        status = definition_limit(l->definition, "idleTimeoutMs", 30000, 3600000, &idle);
    if (status) {
        ud_free((void *)configuration.data);
        return status;
    }
    uint64_t started = ud_now_ms();
    session *chosen = NULL;
    for (;;) {
        status = check_lease(l);
        if (status)
            break;
        ud_mutex_lock(&m->mutex);
        session **at = &m->sessions, *retired = NULL;
        uint32_t matching = 0;
        while (*at) {
            session *s = *at;
            if (!s->active && (s->unhealthy || ud_now_ms() - s->idle_since >= s->idle_timeout ||
                               (ud_same(s->driver, l->driver) &&
                                ud_same(s->resource, l->resource) && memcmp(s->key, key, 32)))) {
                *at = s->next;
                m->count--;
                s->next = retired;
                retired = s;
                continue;
            }
            if (!memcmp(s->key, key, 32)) {
                if (s->active)
                    matching++;
                else if (!chosen) {
                    chosen = s;
                    s->active = 1;
                    m->hits++;
                }
            }
            at = &s->next;
        }
        /* Capacity bounds physical sessions, including idle sessions for other
         * resources. Reclaim the oldest idle entry before rejecting a lease. */
        while (!chosen && matching < limit && m->count >= m->capacity) {
            session **oldest = NULL;
            for (session **entry = &m->sessions; *entry; entry = &(*entry)->next)
                if (!(*entry)->active && (!oldest || (*entry)->idle_since < (*oldest)->idle_since))
                    oldest = entry;
            if (!oldest)
                break; /* Active leases are never evicted. */
            session *s = *oldest;
            *oldest = s->next;
            m->count--;
            s->next = retired;
            retired = s;
        }
        if (!chosen && matching < limit && m->count < m->capacity) {
            chosen = ud_alloc(e->ctx, sizeof(*chosen));
            if (!chosen)
                status = UD_RESOURCE;
            else {
                memcpy(chosen->key, key, 32);
                chosen->active = 1;
                chosen->idle_timeout = idle;
                chosen->driver = (ud_string){ud_strdup_n(e->ctx, l->driver.data, l->driver.length),
                                             l->driver.length};
                chosen->resource = (ud_string){
                    ud_strdup_n(e->ctx, l->resource.data, l->resource.length), l->resource.length};
                if (!chosen->driver.data || !chosen->resource.data)
                    status = UD_RESOURCE;
                chosen->next = m->sessions;
                m->sessions = chosen;
                m->count++;
            }
        }
        if (chosen || status) {
            m->wait_ms += ud_now_ms() - started;
            ud_mutex_unlock(&m->mutex);
        } else {
            uint64_t elapsed = ud_now_ms() - started;
            if (!timeout || elapsed >= timeout)
                status = timeout ? UD_TIMEOUT : UD_BUSY;
            if (!status)
                ud_condition_wait_ms(&m->changed, &m->mutex,
                                     (uint32_t)(timeout - elapsed < 25 ? timeout - elapsed : 25));
            ud_mutex_unlock(&m->mutex);
        }
        while (retired) {
            session *next = retired->next;
            ud_status closed = dispose_session(retired);
            if (closed) {
                /* A failed close still owns the native handle. Keep it retired
                 * in the manager so final destruction can retry safely. */
                ud_mutex_lock(&m->mutex);
                retired->unhealthy = 1;
                retired->next = m->sessions;
                m->sessions = retired;
                m->count++;
                ud_mutex_unlock(&m->mutex);
                if (!status)
                    status = closed;
            }
            retired = next;
        }
        if (chosen || status)
            break;
    }
    l->session = chosen;
    if (!status && !chosen->plugin) {
        if (!e->options.authorize ||
            e->options.authorize(e->options.authorize_user, e->options.tenant, l->driver,
                                 UD_ACCESS_OPEN) != UD_OK)
            status = UD_AUTH;
        char *path = !status ? ud_strdup_n(e->ctx, l->driver.data, l->driver.length) : NULL;
        if (!status && !path)
            status = UD_RESOURCE;
        if (!status)
            status = ud_plugin_load(e->ctx, path, 0, &chosen->plugin);
        ud_free(path);
        ud_plugin_api info = {.struct_size = sizeof(info), .abi_version = UD_PLUGIN_ABI};
        if (!status)
            status = ud_plugin_info(chosen->plugin, &info);
        if (!status)
            chosen->plugin_capabilities = info.capabilities;
        if (!status && !info.connector)
            status = UD_UNSUPPORTED;
        if (!status && (info.connector->capabilities & caps) != caps)
            status = UD_UNSUPPORTED;
        if (!status && l->activation->parallel &&
            (!(info.capabilities & UD_CAP_THREAD_SAFE) ||
             (info.capabilities & UD_CAP_THREAD_AFFINE)))
            status = UD_UNSUPPORTED;
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
    ud_free((void *)configuration.data);
    if (!status && l->activation->parallel &&
        (!(chosen->plugin_capabilities & UD_CAP_THREAD_SAFE) ||
         (chosen->plugin_capabilities & UD_CAP_THREAD_AFFINE)))
        status = UD_UNSUPPORTED;
    if (!status) {
        l->connection = chosen->connection;
        l->capabilities = chosen->capabilities;
        status = ud_connection_bind_lease(l->connection, l, session_grant, check_lease);
    }
    if (!status && (l->capabilities & caps) != caps)
        status = UD_UNSUPPORTED;
    if (!status)
        status = ud_connection_test(l->connection);
    if (status && chosen)
        chosen->unhealthy = 1;
    return status;
}
static ud_status acquire(ud_activation *a, ud_string id, uint64_t caps, int kind,
                         ud_connection_lease **out) {
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
    if (!status)
        status = ud_json_string(ud_member(l->definition, "driver"), &l->driver);
    if (!status)
        status = ud_json_string(ud_member(l->definition, "resource"), &l->resource);
    if (!status)
        status = ud_connection_lease_authorize(l, UD_ACCESS_OPEN);
    if (!status && ((kind == 1 && !ud_named(l->driver, "file") && !ud_named(l->driver, "excel")) ||
                    (kind == 2 && !ud_named(l->driver, "process")) ||
                    (kind == 3 && !ud_named(l->driver, "http"))))
        status = UD_UNSUPPORTED;
    if (!status && kind == 4) {
        l->capabilities = 0;
    } else if (!status && a->transaction) {
        ud_connection_lease *owned = NULL;
        for (uint32_t index = 0; index < a->transaction->count; index++)
            if (ud_same(a->transaction->participants[index].connection, id))
                owned = a->transaction->participants[index].lease;
        if (caps & UD_CONNECT_NATIVE_CDC)
            owned = NULL; /* Replication cursors own a dedicated read session. */
        if (!owned && (caps & UD_CONNECT_READ) &&
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
            l->shared = 1;
            l->connection = owned->connection;
            l->capabilities = owned->capabilities;
            status = ud_connection_bind_lease(l->connection, l, session_grant, check_lease);
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
        l->capabilities = process ? UD_CONNECT_EXECUTE_PROCESS
                                  : UD_CONNECT_READ | UD_CONNECT_APPEND | UD_CONNECT_METADATA;
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
        ud_mutex_lock(&m->mutex);
        m->failures++;
        ud_mutex_unlock(&m->mutex);
        ud_status closed = ud_connection_lease_release(l);
        return closed ? closed : status;
    }
    *out = l;
    return UD_OK;
}
ud_status ud_activation_acquire_connection(ud_activation *a, ud_string id, uint64_t caps,
                                           ud_connection_lease **out) {
    return acquire(a, id, caps, 0, out);
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
    if (!ud_named(l->driver, "http"))
        return UD_UNSUPPORTED;
    UD_TRY(ud_connection_lease_authorize(l, UD_ACCESS_NETWORK));
    const ud_json *ref = ud_member(l->definition, "credentialReference");
    if (!ref)
        return UD_OK;
    ud_string reference;
    UD_TRY(ud_json_string(ref, &reference));
    ud_execution *e = l->activation->execution;
    if (!e->options.resolve)
        return UD_AUTH;
    ud_status status = e->options.resolve(e->options.resolve_user, e->ctx, e->options.tenant,
                                          l->resource, reference, out);
    if (status || !out->data || !out->length || out->length > 32768) {
        ud_free((void *)out->data);
        *out = (ud_string){0};
        return status ? status : UD_SCHEMA;
    }
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
ud_status ud_connection_lease_savepoint(ud_connection_lease *lease, ud_string name, int32_t phase) {
    if (!lease || !lease->connection)
        return UD_UNSUPPORTED;
    UD_TRY(ud_connection_lease_authorize(lease, UD_ACCESS_WRITE));
    return ud_connection_savepoint(lease->connection, name, phase);
}
ud_status ud_connection_lease_record_diagnostic(ud_connection_lease *lease, ud_string *out) {
    return lease && lease->connection ? ud_connection_record_diagnostic(lease->connection, out)
                                      : UD_UNSUPPORTED;
}
ud_status ud_connection_lease_read_open(ud_connection_lease *l, const ud_read_options *o,
                                        const ud_batch *b) {
    if (!l || !l->connection)
        return UD_UNSUPPORTED;
    UD_TRY(ud_connection_lease_authorize(l, UD_ACCESS_READ));
    return ud_connection_read_open(l->connection, o, b);
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
    return ud_connection_write(l->connection, o, b);
}
ud_status ud_connection_lease_receipt(ud_connection_lease *l, ud_string job, ud_string batch,
                                      ud_commit_receipt *out) {
    if (!l || (!l->connection && !l->rest))
        return UD_UNSUPPORTED;
    UD_TRY(ud_connection_lease_authorize(l, UD_ACCESS_RECEIPT));
    return l->rest ? ud_rest_target_receipt(l->rest, job, batch, out)
                   : ud_connection_receipt(l->connection, job, batch, out);
}
ud_status ud_connection_lease_execute_command(ud_connection_lease *l,
                                              const ud_read_options *options, uint64_t *processed) {
    if (!l || !l->connection)
        return UD_UNSUPPORTED;
    UD_TRY(ud_connection_lease_authorize(l, UD_ACCESS_WRITE));
    return ud_connection_execute_command(l->connection, options, processed);
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
    UD_TRY(ud_connection_lease_authorize(l, UD_ACCESS_DISCOVER));
    return ud_connection_metadata(l->connection, o, out);
}
ud_status ud_connection_lease_schema(ud_connection_lease *lease, ud_string table, ud_string scope,
                                     ud_batch **out) {
    if (!lease || !out)
        return UD_INVALID;
    UD_TRY(ud_connection_lease_authorize(lease, UD_ACCESS_DISCOVER));
    if (lease->connection)
        return ud_connection_schema(lease->connection, table, scope, out);
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
    UD_TRY(ud_connection_lease_authorize(l, UD_ACCESS_DISCOVER));
    return ud_connection_discover(l->connection, table, out);
}
ud_status ud_connection_lease_release(ud_connection_lease *l) {
    if (!l)
        return UD_OK;
    struct ud_connection_manager *m = l->activation->execution->connections;
    ud_mutex_lock(&m->mutex);
    while (l->pins)
        ud_condition_wait(&m->changed, &m->mutex);
    ud_mutex_unlock(&m->mutex);
    session *s = l->session;
    ud_status status = UD_OK;
    if (l->shared) {
        status = ud_connection_read_close(l->connection);
        if (!status && l->activation->transaction) {
            ud_connection_lease *owner = NULL;
            for (uint32_t index = 0; index < l->activation->transaction->count; index++)
                if (l->activation->transaction->participants[index].lease &&
                    l->activation->transaction->participants[index].lease->connection ==
                        l->connection)
                    owner = l->activation->transaction->participants[index].lease;
            status =
                owner ? ud_connection_bind_lease(l->connection, owner, session_grant, check_lease)
                      : UD_CORRUPT;
        }
    }
    if (s && s->connection) {
        status = ud_connection_read_close(s->connection);
        if (!status) {
            if (!ud_connection_reusable(s->connection))
                s->unhealthy = 1;
            else
                status = ud_connection_reset_session(s->connection);
            if (!status)
                status = ud_connection_bind_lease(s->connection, NULL, NULL, NULL);
        }
    }
    if (status == UD_BUSY)
        return status;
    ud_mutex_lock(&m->mutex);
    ud_connection_lease **at = &m->leases;
    while (*at && *at != l)
        at = &(*at)->next;
    if (*at) {
        *at = l->next;
        m->active--;
    }
    int retiring = 0;
    if (s) {
        s->idle_since = ud_now_ms();
        if (status || l->cancelled || check_lease(l))
            s->unhealthy = 1;
        retiring = s->unhealthy || (s->plugin_capabilities & UD_CAP_THREAD_AFFINE);
        if (retiring) {
            session **entry = &m->sessions;
            while (*entry && *entry != s)
                entry = &(*entry)->next;
            if (*entry) {
                *entry = s->next;
                m->count--;
            }
        }
        s->active = 0;
    }
    ud_condition_wake(&m->changed);
    ud_mutex_unlock(&m->mutex);
    if (retiring) {
        ud_status disposed = dispose_session(s);
        if (disposed) {
            ud_mutex_lock(&m->mutex);
            s->unhealthy = 1;
            s->next = m->sessions;
            m->sessions = s;
            m->count++;
            ud_mutex_unlock(&m->mutex);
            if (!status)
                status = disposed;
        }
    }
    ud_rest_target_close(l->rest);
    ud_json_destroy(l->definition);
    ud_free(l);
    return status;
}
void ud_connections_cancel(ud_execution *e) {
    if (!e || !e->connections)
        return;
    struct ud_connection_manager *m = e->connections;
    ud_mutex_lock(&m->mutex);
    for (;;) {
        ud_connection_lease *l = m->leases;
        while (l && (l->cancelled || !l->connection || l->shared))
            l = l->next;
        if (!l)
            break;
        l->cancelled = 1;
        l->pins++;
        ud_mutex_unlock(&m->mutex);
        ud_connection_cancel(l->connection);
        ud_mutex_lock(&m->mutex);
        l->pins--;
        ud_condition_wake(&m->changed);
    }
    ud_condition_wake(&m->changed);
    ud_mutex_unlock(&m->mutex);
}
ud_status ud_connections_release_activation(ud_activation *a) {
    struct ud_connection_manager *m = a->execution->connections;
    if (!m)
        return UD_OK;
    int leaked = 0;
    for (;;) {
        ud_mutex_lock(&m->mutex);
        ud_connection_lease *l = m->leases;
        while (l && l->activation != a)
            l = l->next;
        ud_mutex_unlock(&m->mutex);
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
    if (m->leases || m->active)
        return UD_BUSY;
    while (m->sessions) {
        session *s = m->sessions, *next = s->next;
        UD_TRY(dispose_session(s));
        m->sessions = next;
    }
    ud_condition_destroy(&m->changed);
    ud_mutex_destroy(&m->mutex);
    ud_free(m);
    e->connections = NULL;
    return UD_OK;
}
ud_status ud_connections_diagnostics(ud_execution *e, ud_bytes *b) {
    struct ud_connection_manager *m = e->connections;
    if (!m)
        return ud_bytes_put(b, "{}", 2);
    char text[512];
    ud_mutex_lock(&m->mutex);
    int n = snprintf(text, sizeof(text),
                     "{\"sessions\":%u,\"activeLeases\":%u,\"capacity\":%u,\"acquisitions\":%llu,"
                     "\"poolHits\":%llu,\"failures\":%llu,\"waitMs\":%llu,\"commits\":%llu,"
                     "\"rollbacks\":%llu,\"commitUnknown\":%llu}",
                     m->count, m->active, m->capacity, (unsigned long long)m->acquisitions,
                     (unsigned long long)m->hits, (unsigned long long)m->failures,
                     (unsigned long long)m->wait_ms, (unsigned long long)m->commits,
                     (unsigned long long)m->rollbacks, (unsigned long long)m->uncertain);
    ud_mutex_unlock(&m->mutex);
    return ud_bytes_put(b, text, (size_t)n);
}
int ud_transaction_active(const ud_activation *activation) {
    return activation && activation->transaction && !activation->transaction->completed;
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
    if (!ud_transaction_active(activation))
        return 0;
    activation->transaction->pending_tasks++;
    return 1;
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
        if (ud_named(driver, "file") || ud_named(driver, "excel") || ud_named(driver, "http") ||
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
    ud_status status = transaction->participants ? UD_OK : UD_RESOURCE;
    for (uint32_t index = 0; !status && index < count; index++) {
        transaction_participant *participant = transaction->participants + index;
        status = ud_json_string(ud_json_at(participants, index), &participant->connection);
        participant->branch = (ud_transaction_branch){.struct_size = sizeof(ud_transaction_branch),
                                                      .abi_version = UD_ABI_VERSION,
                                                      .isolation = isolation};
        if (!status)
            status = ud_activation_acquire_connection(
                activation, participant->connection,
                UD_CONNECT_TRANSACTION | UD_CONNECT_TRANSACTION_PARTICIPANT, &participant->lease);
    }
    /* Capability-check the complete set before provisioning or starting a branch. */
    for (uint32_t index = 0; !status && provision && index < count; index++)
        status = ud_connection_lease_provision(transaction->participants[index].lease);
    char *saved = NULL;
    uint32_t saved_length = 0;
    ud_status found =
        !status ? ud_checkpoint_transaction_read(activation, UD_TX_JOURNAL_MANIFEST,
                                                 activation->task_id, &saved, &saved_length)
                : UD_END;
    if (!status && found != UD_END && found)
        status = found;
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
        } else if (!status)
            status = transaction_record_decision(transaction, UD_TX_ROLLBACK);
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
        if (!status && decision == UD_TX_COMMIT && !activation->transaction_epoch) {
            activation->transaction = transaction;
            ud_free(saved);
            return UD_OK;
        }
        if (!status && transaction->attempt == UINT64_MAX)
            status = UD_RESOURCE;
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
    char *manifest = NULL;
    uint32_t length = 0;
    if (!status)
        status = transaction_manifest(transaction, &manifest, &length);
    if (!status)
        status = ud_checkpoint_transaction_record(
            activation, UD_TX_JOURNAL_MANIFEST, activation->task_id, (ud_string){manifest, length});
    ud_free(manifest);
    for (uint32_t index = 0; !status && index < count; index++) {
        transaction_participant *participant = transaction->participants + index;
        status =
            ud_connection_transaction_begin(participant->lease->connection, &participant->branch);
        if (!status)
            participant->begun = 1;
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
    ud_status status = UD_OK;
    if (!transaction->completed) {
        for (uint32_t index = 0; !outcome && index < transaction->count; index++) {
            transaction_participant *participant = transaction->participants + index;
            ud_status prepared = ud_connection_transaction_prepare(participant->lease->connection,
                                                                   &participant->branch);
            if (prepared)
                outcome = prepared;
        }
        int decision = outcome ? UD_TX_ROLLBACK : UD_TX_COMMIT;
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
    struct ud_connection_manager *manager = activation->execution->connections;
    ud_mutex_lock(&manager->mutex);
    if (status)
        manager->uncertain++;
    else if (outcome)
        manager->rollbacks++;
    else
        manager->commits++;
    ud_mutex_unlock(&manager->mutex);
    activation->transaction = NULL;
    ud_status released = transaction_release(transaction);
    return status ? UD_COMMIT_UNKNOWN : released ? released : outcome;
}

ud_status ud_transaction_commit_epoch(ud_activation *activation) {
    if (!activation || !activation->transaction_epoch || !ud_transaction_active(activation))
        return UD_UNSUPPORTED;
    struct ud_transaction *transaction = activation->transaction;
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
        status = ud_checkpoint_transaction_record(owner, UD_TX_JOURNAL_MANIFEST, owner->task_id,
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
    UD_TRY(ud_connection_lease_authorize(lease, UD_ACCESS_WRITE));
    return ud_connection_provision(lease->connection);
}

ud_status ud_connection_lease_cdc_open(ud_connection_lease *l, const ud_cdc_options *o) {
    if (!l || !l->connection)
        return UD_UNSUPPORTED;
    UD_TRY(ud_connection_lease_authorize(l, UD_ACCESS_READ));
    return ud_connection_cdc_open(l->connection, o);
}
ud_status ud_connection_lease_cdc_next(ud_connection_lease *l, ud_cdc_event *e) {
    if (!l || !l->connection)
        return UD_UNSUPPORTED;
    UD_TRY(ud_connection_lease_authorize(l, UD_ACCESS_READ));
    return ud_connection_cdc_next(l->connection, e);
}
ud_status ud_connection_lease_cdc_confirm(ud_connection_lease *l, ud_string p) {
    if (!l || !l->connection)
        return UD_UNSUPPORTED;
    UD_TRY(ud_connection_lease_authorize(l, UD_ACCESS_READ));
    return ud_connection_cdc_confirm(l->connection, p);
}
ud_status ud_connection_lease_cdc_schema(ud_connection_lease *lease, ud_batch **out) {
    return lease && lease->connection ? ud_connection_cdc_schema(lease->connection, out)
                                      : UD_UNSUPPORTED;
}
ud_status ud_connection_lease_cdc_rebind(ud_connection_lease *lease, const ud_batch *schema,
                                         uint64_t version) {
    if (!lease || !lease->connection)
        return UD_UNSUPPORTED;
    UD_TRY(ud_activation_check(lease->activation));
    return ud_connection_cdc_rebind(lease->connection, schema, version);
}
ud_status ud_connection_lease_cdc_close(ud_connection_lease *l) {
    return l && l->connection ? ud_connection_cdc_close(l->connection) : UD_UNSUPPORTED;
}

int32_t ud_connection_lease_dialect(const ud_connection_lease *l) {
    return l && l->connection ? ud_connection_dialect(l->connection) : UD_SQL_CANONICAL;
}
int32_t ud_connection_lease_parameter_style(const ud_connection_lease *l) {
    return l && l->connection ? ud_connection_parameter_style(l->connection) : 0;
}
