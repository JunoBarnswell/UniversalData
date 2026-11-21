#include "manager_pool.h"
#include "ud/plugin.h"

ud_connection_pool *ud_manager_pool_create(ud_context *ctx, uint32_t capacity, int owns_context) {
    ud_connection_pool *pool = ud_alloc(ctx, sizeof(*pool));
    if (!pool)
        return NULL;
    pool->ctx = ctx;
    pool->capacity = capacity;
    pool->owns_context = owns_context;
    ud_mutex_init(&pool->mutex);
    ud_condition_init(&pool->changed);
    return pool;
}

ud_status ud_connection_pool_create(uint32_t capacity, uint64_t memory_limit,
                                    ud_connection_pool **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!capacity || capacity > 65536 || !memory_limit)
        return UD_INVALID;
    ud_context_options options = {.struct_size = sizeof(options), .abi_version = UD_ABI_VERSION,
                                  .memory_limit = memory_limit};
    ud_context *ctx = NULL;
    UD_TRY(ud_context_create(&options, &ctx));
    ud_connection_pool *pool = ud_manager_pool_create(ctx, capacity, 1);
    if (!pool) {
        (void)ud_context_destroy(ctx);
        return UD_RESOURCE;
    }
    *out = pool;
    return UD_OK;
}

ud_status ud_connection_pool_set_node_limits(ud_connection_pool *pool, ud_string path,
                                             uint32_t max_sources) {
    if (!pool || !path.data || !path.length || path.length > 32768 ||
        memchr(path.data, 0, path.length) || !max_sources || max_sources > 64)
        return UD_INVALID;
    ud_mutex_lock(&pool->mutex);
    ud_status status = UD_BUSY;
    if (!pool->users && !pool->count && !pool->closing && !pool->quota_path) {
        pool->quota_path = ud_strdup_n(pool->ctx, path.data, path.length);
        pool->quota_slots = max_sources;
        status = pool->quota_path ? UD_OK : UD_RESOURCE;
    }
    ud_mutex_unlock(&pool->mutex);
    return status;
}

ud_status ud_manager_pool_reserve(ud_connection_pool *pool, ud_context *ctx, ud_string tenant,
                                  ud_string resource, uint32_t timeout,
                                  ud_status (*check)(void *), void *user, void **out) {
    if (!pool->quota_path)
        return UD_OK;
    ud_bytes identity = {.ctx = ctx};
    ud_status status = ud_put_string(&identity, tenant.data, tenant.length);
    if (!status)
        status = ud_put_string(&identity, resource.data, resource.length);
    uint8_t digest[32];
    if (!status)
        ud_hash(identity.data, identity.size, digest);
    ud_bytes_clear(&identity);
    UD_TRY(status);
    uint64_t offset = 0;
    for (uint32_t i = 0; i < 7; i++)
        offset = (offset << 8) | digest[i];
    offset <<= 6;
    uint64_t started = ud_now_ms();
    do {
        UD_TRY(check(user));
        status = ud_lock_slot(pool->quota_path, offset, pool->quota_slots, out);
        if (status != UD_BUSY || !timeout)
            return status;
        uint64_t waited = ud_now_ms() - started;
        if (waited >= timeout)
            break;
        ud_sleep_ms((uint32_t)(timeout - waited < 25 ? timeout - waited : 25));
    } while (1);
    return UD_TIMEOUT;
}

static ud_status dispose_session(session *s) {
    if (s->connection) {
        UD_TRY(ud_connection_close(s->connection));
        s->connection = NULL;
    }
    if (s->plugin) {
        UD_TRY(ud_plugin_unload(s->plugin));
        s->plugin = NULL;
    }
    ud_unlock_slot(s->node_slot);
    ud_free(s);
    return UD_OK;
}
/* Removing a session from the idle list does not free its capacity. Keep the
 * slot accounted until the physical connection has actually closed. */
ud_status ud_manager_pool_dispose(ud_connection_pool *pool, session *retired) {
    ud_status status = UD_OK;
    while (retired) {
        session *next = retired->next;
        ud_status closed = dispose_session(retired);
        ud_mutex_lock(&pool->mutex);
        if (!closed)
            pool->count--;
        else {
            retired->unhealthy = 1;
            retired->next = pool->sessions;
            pool->sessions = retired;
            if (!status)
                status = closed;
        }
        ud_condition_wake(&pool->changed);
        ud_mutex_unlock(&pool->mutex);
        retired = next;
    }
    return status;
}

ud_status ud_connection_pool_trim_idle(ud_connection_pool *pool) {
    if (!pool)
        return UD_INVALID;
    session *retired = NULL;
    ud_mutex_lock(&pool->mutex);
    uint64_t now = ud_now_ms();
    session **entry = &pool->sessions;
    while (*entry) {
        session *current = *entry;
        if (current->node_slot && !current->borrower && now - current->idle_since >= current->idle_timeout) {
            *entry = current->next;
            current->next = retired;
            retired = current;
        } else
            entry = &current->next;
    }
    ud_mutex_unlock(&pool->mutex);
    return ud_manager_pool_dispose(pool, retired);
}

ud_status ud_connection_pool_destroy(ud_connection_pool *pool) {
    if (!pool)
        return UD_OK;
    ud_mutex_lock(&pool->mutex);
    if (pool->users || pool->closing) {
        ud_mutex_unlock(&pool->mutex);
        return UD_BUSY;
    }
    pool->closing = 1;
    session *retired = pool->sessions;
    pool->sessions = NULL;
    ud_mutex_unlock(&pool->mutex);
    ud_status status = ud_manager_pool_dispose(pool, retired);
    ud_context *ctx = pool->ctx;
    int owned = pool->owns_context;
    if (!status && owned && ud_memory_used(ctx) != ud_allocation_bytes(pool) + ud_allocation_bytes(pool->quota_path))
        status = UD_BUSY;
    if (status) {
        ud_mutex_lock(&pool->mutex);
        pool->closing = 0;
        ud_mutex_unlock(&pool->mutex);
        return status;
    }
    ud_condition_destroy(&pool->changed);
    ud_mutex_destroy(&pool->mutex);
    ud_free(pool->quota_path);
    ud_free(pool);
    return owned ? ud_context_destroy(ctx) : UD_OK;
}
