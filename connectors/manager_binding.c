#include "manager_binding.h"
#include "../engines/package/internal.h"
#include <string.h>

struct ud_connector_binding {
    ud_string id, path;
    struct ud_connector_binding *next;
};

ud_status ud_execution_bind_connector(ud_execution *execution, ud_string id, ud_string path) {
    if (!execution || !id.data || id.length <= 3 || id.length > 48 ||
        memcmp(id.data, "ud:", 3) || !path.data || !path.length ||
        memchr(path.data, 0, path.length))
        return UD_INVALID;
    if (atomic_load(&execution->state) != UD_EXEC_READY)
        return UD_BUSY;
    for (struct ud_connector_binding *entry = execution->connector_bindings; entry; entry = entry->next)
        if (ud_same(entry->id, id))
            return ud_same(entry->path, path) ? UD_OK : UD_FENCED;
    struct ud_connector_binding *entry = ud_alloc(execution->ctx, sizeof(*entry));
    if (!entry)
        return UD_RESOURCE;
    char *name = ud_strdup_n(execution->ctx, id.data, id.length);
    char *filename = ud_strdup_n(execution->ctx, path.data, path.length);
    if (!name || !filename) {
        ud_free(name);
        ud_free(filename);
        ud_free(entry);
        return UD_RESOURCE;
    }
    *entry = (struct ud_connector_binding){
        {name, id.length}, {filename, path.length}, execution->connector_bindings};
    execution->connector_bindings = entry;
    return UD_OK;
}

void ud_execution_destroy_connectors(ud_execution *execution) {
    while (execution->connector_bindings) {
        struct ud_connector_binding *entry = execution->connector_bindings;
        execution->connector_bindings = entry->next;
        ud_free((void *)entry->id.data);
        ud_free((void *)entry->path.data);
        ud_free(entry);
    }
}

ud_status ud_execution_resolve_driver(ud_execution *execution, ud_string *driver) {
    if (driver->length <= 3 || memcmp(driver->data, "ud:", 3))
        return UD_OK;
    for (struct ud_connector_binding *entry = execution->connector_bindings; entry; entry = entry->next)
        if (ud_same(entry->id, *driver)) {
            *driver = entry->path;
            return UD_OK;
        }
    return UD_DRIVER_UNAVAILABLE;
}
