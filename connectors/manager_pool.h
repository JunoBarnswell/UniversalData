#ifndef UD_MANAGER_POOL_H
#define UD_MANAGER_POOL_H

#include "../engines/package/internal.h"

typedef struct session {
    ud_plugin *plugin;
    ud_connection *connection;
    void *node_slot;
    uint8_t key[32], family[32];
    uint64_t idle_since, idle_timeout;
    uint64_t capabilities, plugin_capabilities;
    ud_connection_lease *borrower;
    int unhealthy;
    struct session *next;
} session;
struct ud_connection_pool {
    ud_context *ctx;
    session *sessions;
    uint32_t capacity, count, users;
    int closing, owns_context;
    char *quota_path;
    uint32_t quota_slots;
    ud_mutex mutex;
    ud_condition changed;
};
ud_connection_pool *ud_manager_pool_create(ud_context *, uint32_t capacity, int owns_context);
ud_status ud_manager_pool_dispose(ud_connection_pool *, session *retired);
ud_status ud_manager_pool_reserve(ud_connection_pool *, ud_context *, ud_string tenant, ud_string resource,
                                  uint32_t timeout, ud_status (*check)(void *), void *user, void **);

#endif
