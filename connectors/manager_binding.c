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

static ud_status definition_limit(const ud_json *d, const char *name, uint64_t fallback,
                                  uint64_t maximum, uint64_t *out) {
    *out = fallback;
    if (ud_member(d, name))
        UD_TRY(ud_json_uint(ud_member(d, name), out));
    return *out <= maximum ? UD_OK : UD_SCHEMA;
}

ud_status ud_manager_bind_session(ud_execution *e, const ud_json *definition,
                                  ud_string driver, ud_string resource, ud_session_binding *binding,
                                  ud_string *configuration) {
    ud_string logical_driver = {0}, reference = {0};
    UD_TRY(ud_json_string(ud_member(definition, "driver"), &logical_driver));
    const ud_json *reference_value = ud_member(definition, "credentialReference");
    if (reference_value)
        UD_TRY(ud_json_string(reference_value, &reference));

    /* Local SQLite uses the authorized file path. Other drivers resolve a
     * credential on each borrow so rotation changes the physical pool key. */
    if (ud_named(logical_driver, "ud:sqlite") && !reference.length) {
        configuration->data = ud_strdup_n(e->ctx, resource.data, resource.length);
        if (!configuration->data)
            return UD_RESOURCE;
        configuration->length = resource.length;
    } else {
        if (!reference.length || !e->options.resolve)
            return UD_AUTH;
        UD_TRY(e->options.resolve(e->options.resolve_user, e->ctx, e->options.tenant,
                                  resource, reference, configuration));
    }
    if (!configuration->data || !configuration->length ||
        memchr(configuration->data, 0, configuration->length))
        return UD_AUTH;
    ud_status status = UD_OK;
    ud_bytes identity = {.ctx = e->ctx};
    uint8_t credential[32];
    /* A Package connection id is logical scope, not physical identity. Two
     * executions may borrow the same endpoint when tenant, driver, resource,
     * credential reference and resolved credential agree. */
    ud_string fields[] = {e->options.tenant, driver, resource};
    for (uint32_t index = 0; !status && index < sizeof(fields) / sizeof(fields[0]); index++)
        status = ud_put_string(&identity, fields[index].data, fields[index].length);
    if (!status) {
        ud_hash(identity.data, identity.size, binding->family);
        ud_hash(configuration->data, configuration->length, credential);
        status = ud_put_string(&identity, reference.data, reference.length);
    }
    if (!status)
        status = ud_bytes_put(&identity, credential, sizeof(credential));
    if (!status)
        ud_hash(identity.data, identity.size, binding->key);
    ud_bytes_clear(&identity);
    if (!status)
        status = definition_limit(definition, "maxLeases", 8, 65536, &binding->max_leases);
    const ud_json *job_execution = ud_member(ud_member(e->package->document, "policy"), "execution");
    const ud_json *per_source = ud_member(job_execution, "maxConnectionsPerSource");
    if (!status && per_source) {
        uint64_t maximum;
        status = ud_json_uint(per_source, &maximum);
        if (!status && (!ud_member(definition, "maxLeases") || binding->max_leases > maximum))
            binding->max_leases = maximum;
    }
    if (e->max_connections_per_source && binding->max_leases > e->max_connections_per_source)
        binding->max_leases = e->max_connections_per_source;
    if (!status && !binding->max_leases)
        status = UD_SCHEMA;
    if (!status)
        status = definition_limit(definition, "acquireTimeoutMs", e->connection_wait_ms, 3600000, &binding->acquire_timeout);
    if (!status)
        status = definition_limit(definition, "idleTimeoutMs", 30000, 3600000, &binding->idle_timeout);
    return status;
}

void ud_manager_clear_configuration(ud_string configuration) {
    volatile char *secret = (volatile char *)configuration.data;
    if (secret)
        for (uint32_t index = 0; index < configuration.length; index++)
            secret[index] = 0;
    ud_free((void *)configuration.data);
}
