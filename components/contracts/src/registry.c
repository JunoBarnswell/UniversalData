#include "../registry_internal.h"
#include "ud/json.h"
#include "../../foundation/internal.h"
#include "../../foundation/thread.h"
#include <stdatomic.h>

typedef struct registry_entry {
    ud_component_descriptor descriptor;
    void *owner;
    ud_component_owner_release release;
    uint64_t hash;
} registry_entry;
struct ud_registry {
    ud_context *ctx;
    registry_entry **entries, **index;
    uint32_t count, capacity;
    atomic_uint leases;
    ud_mutex mutex;
};
static uint64_t identifier_hash(ud_string id) {
    uint64_t hash = UINT64_C(14695981039346656037);
    for (uint32_t i = 0; i < id.length; i++)
        hash = (hash ^ (uint8_t)id.data[i]) * UINT64_C(1099511628211);
    return hash;
}
static int valid_string(ud_string text) {
    return text.data && text.length && text.length <= 65536 && !memchr(text.data, 0, text.length) &&
           ud_utf8_valid(text.data, text.length);
}
static registry_entry *lookup(const ud_registry *r, ud_string id, uint64_t hash) {
    if (!r->capacity)
        return NULL;
    uint32_t slot = (uint32_t)hash & (r->capacity - 1);
    for (uint32_t i = 0; i < r->capacity; i++) {
        registry_entry *entry = r->index[slot];
        if (!entry)
            return NULL;
        if (entry->hash == hash && entry->descriptor.id.length == id.length &&
            !memcmp(entry->descriptor.id.data, id.data, id.length))
            return entry;
        slot = (slot + 1) & (r->capacity - 1);
    }
    return NULL;
}
static void insert(registry_entry **index, uint32_t capacity, registry_entry *entry) {
    uint32_t slot = (uint32_t)entry->hash & (capacity - 1);
    while (index[slot])
        slot = (slot + 1) & (capacity - 1);
    index[slot] = entry;
}
static void destroy_entry(registry_entry *entry) {
    if (!entry)
        return;
    ud_free((void *)entry->descriptor.id.data);
    ud_free((void *)entry->descriptor.name.data);
    ud_free((void *)entry->descriptor.version.data);
    ud_free((void *)entry->descriptor.configuration_schema.data);
    if (entry->release)
        entry->release(entry->owner);
    ud_free(entry);
}
static ud_status descriptor_check(ud_registry *r, const ud_component_descriptor *d) {
    if (d->struct_size != sizeof(*d) || d->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!valid_string(d->id) || !valid_string(d->name) || !valid_string(d->version) ||
        !valid_string(d->configuration_schema) || d->inputs > 64 || d->outputs > 64 ||
        d->category < UD_COMPONENT_TASK || d->category > UD_COMPONENT_ENUMERATOR ||
        d->synchronous_input < -1 || d->error_output < -1 ||
        (d->synchronous_input >= 0 && (uint32_t)d->synchronous_input >= d->inputs) ||
        (d->error_output >= 0 && (uint32_t)d->error_output >= d->outputs))
        return UD_SCHEMA;
    if ((d->outputs < 64 && (d->optional_outputs >> d->outputs)) ||
        (d->inputs < 64 && (d->optional_inputs >> d->inputs)) ||
        ((d->capabilities & UD_COMPONENT_BLOCKING) &&
         (d->capabilities & UD_COMPONENT_PARTIALLY_BLOCKING)))
        return UD_SCHEMA;
    if((d->capabilities & UD_COMPONENT_WATERMARK_POLICY) && (d->category != UD_COMPONENT_TRANSFORM || d->inputs!=1 || !(d->capabilities&UD_COMPONENT_CONTROL_EVENTS)))return UD_SCHEMA;
    int function = d->category == UD_COMPONENT_FUNCTION;
    int task = d->category == UD_COMPONENT_TASK;
    int enumerator = d->category == UD_COMPONENT_ENUMERATOR;
    if ((function || task || enumerator) && (d->capabilities & UD_COMPONENT_CONTROL_EVENTS))
        return UD_SCHEMA;
    if ((d->capabilities & UD_COMPONENT_PREVIEW_SCHEMA) && d->category != UD_COMPONENT_SOURCE)
        return UD_SCHEMA;
    if ((function && d->cancel) || ((function || task || enumerator) && d->describe_plan))
        return UD_SCHEMA;
    if (!function &&
        (d->function_bind || d->function_evaluate || d->min_arguments || d->max_arguments ||
         (d->capabilities &
          (UD_FUNCTION_PURE | UD_FUNCTION_DETERMINISTIC | UD_FUNCTION_CONTEXTUAL))))
        return UD_SCHEMA;
    if (task && (!d->invoke || d->inputs || d->outputs || d->synchronous_input != -1 ||
                 d->error_output != -1 || d->step || (!!d->prepare != !!d->dispose) ||
                 d->enumerate_next || d->enumerate_position))
        return UD_SCHEMA;
    if (function &&
        (!(d->capabilities & (UD_FUNCTION_PURE | UD_FUNCTION_CONTEXTUAL)) || !d->function_bind ||
         !d->function_evaluate || d->min_arguments > d->max_arguments || d->max_arguments > 16 ||
         d->inputs || d->outputs || d->synchronous_input != -1 || d->error_output != -1 ||
         d->invoke || d->configure || d->prepare || d->step || d->finish || d->dispose))
        return UD_SCHEMA;
    if (enumerator &&
        (!d->configure || !d->prepare || !d->dispose || !d->enumerate_next || d->inputs ||
         d->outputs || d->synchronous_input != -1 || d->error_output != -1 || d->step ||
         d->invoke || d->function_bind || d->function_evaluate ||
         ((d->capabilities & UD_ENUMERATOR_RESUMABLE) &&
          (!d->enumerate_position || !(d->capabilities & UD_ENUMERATOR_DETERMINISTIC)))))
        return UD_SCHEMA;
    if (!enumerator && (d->enumerate_next || d->enumerate_position))
        return UD_SCHEMA;
    if (!function && !task && !enumerator &&
        (!d->configure || !d->prepare || !d->step || !d->finish || !d->dispose || d->invoke ||
         (d->category == UD_COMPONENT_SOURCE && (d->inputs || !d->outputs)) ||
         (d->category == UD_COMPONENT_DESTINATION && !d->inputs) ||
         (d->category == UD_COMPONENT_TRANSFORM && (!d->inputs || !d->outputs))))
        return UD_SCHEMA;
    ud_json *schema = NULL;
    ud_status status = ud_json_parse(r->ctx, d->configuration_schema.data,
                                     d->configuration_schema.length, &schema, NULL);
    if (!status && ud_json_kind(schema) != UD_JSON_OBJECT)
        status = UD_SCHEMA;
    ud_json_destroy(schema);
    return status;
}
static ud_status create_entry(ud_registry *r, const ud_component_descriptor *d,
                              registry_entry **out) {
    *out = NULL;
    UD_TRY(descriptor_check(r, d));
    registry_entry *entry = ud_alloc(r->ctx, sizeof(*entry));
    if (!entry)
        return UD_RESOURCE;
    entry->descriptor = *d;
    entry->hash = identifier_hash(d->id);
    ud_string *targets[] = {&entry->descriptor.id, &entry->descriptor.name,
                            &entry->descriptor.version, &entry->descriptor.configuration_schema};
    ud_string sources[] = {d->id, d->name, d->version, d->configuration_schema};
    for (uint32_t i = 0; i < 4; i++)
        targets[i]->data = NULL;
    for (uint32_t i = 0; i < 4; i++) {
        targets[i]->data = ud_strdup_n(r->ctx, sources[i].data, sources[i].length);
        if (!targets[i]->data) {
            destroy_entry(entry);
            return UD_RESOURCE;
        }
    }
    *out = entry;
    return UD_OK;
}
ud_status ud_registry_create(ud_context *ctx, ud_registry **out) {
    if (!ctx || !out)
        return UD_INVALID;
    *out = ud_alloc(ctx, sizeof(**out));
    if (!*out)
        return UD_RESOURCE;
    (*out)->ctx = ctx;
    atomic_init(&(*out)->leases, 0);
    ud_mutex_init(&(*out)->mutex);
    return UD_OK;
}
ud_status ud_registry_register_owned(ud_registry *r, const ud_component_descriptor *descriptors,
                                     uint32_t count, void *owner, ud_component_owner_retain retain,
                                     ud_component_owner_release release) {
    if (!r || !descriptors || !count || count > 4096 || !!retain != !!release || (retain && !owner))
        return UD_INVALID;
    ud_mutex_lock(&r->mutex);
    ud_status status = atomic_load(&r->leases) ? UD_BUSY : UD_OK;
    if (!status && count > UINT32_MAX - r->count)
        status = UD_RESOURCE;
    uint32_t total = !status ? r->count + count : 0, capacity = r->capacity ? r->capacity : 16;
    while (!status && total > capacity / 2) {
        if (capacity > UINT32_MAX / 2)
            status = UD_RESOURCE;
        else
            capacity *= 2;
    }
    registry_entry **entries = NULL, **index = NULL;
    if (!status) {
        entries = ud_alloc(r->ctx, (size_t)total * sizeof(*entries));
        index = ud_alloc(r->ctx, (size_t)capacity * sizeof(*index));
        if (!entries || !index)
            status = UD_RESOURCE;
    }
    if (!status)
        for (uint32_t i = 0; i < r->count; i++) {
            entries[i] = r->entries[i];
            insert(index, capacity, entries[i]);
        }
    uint32_t created = 0;
    for (uint32_t i = 0; !status && i < count; i++) {
        registry_entry *entry = NULL;
        status = create_entry(r, descriptors + i, &entry);
        if (!status) {
            ud_registry pending = {.index = index, .capacity = capacity};
            if (lookup(&pending, entry->descriptor.id, entry->hash))
                status = UD_SCHEMA;
        }
        if (!status && retain)
            status = retain(owner);
        if (status) {
            destroy_entry(entry);
            break;
        }
        entry->owner = owner;
        entry->release = release;
        entries[r->count + created++] = entry;
        insert(index, capacity, entry);
    }
    if (!status) {
        ud_free(r->entries);
        ud_free(r->index);
        r->entries = entries;
        r->index = index;
        r->count = total;
        r->capacity = capacity;
    } else {
        for (uint32_t i = 0; i < created; i++)
            destroy_entry(entries[r->count + i]);
        ud_free(entries);
        ud_free(index);
    }
    ud_mutex_unlock(&r->mutex);
    return status;
}
ud_status ud_registry_register(ud_registry *r, const ud_component_descriptor *descriptor) {
    return ud_registry_register_owned(r, descriptor, 1, NULL, NULL, NULL);
}
uint32_t ud_registry_count(const ud_registry *r) {
    if (!r)
        return 0;
    ud_mutex_lock((ud_mutex *)&r->mutex);
    uint32_t count = r->count;
    ud_mutex_unlock((ud_mutex *)&r->mutex);
    return count;
}
ud_status ud_registry_get(const ud_registry *r, uint32_t index, ud_component_descriptor *out) {
    if (!r || !out)
        return UD_INVALID;
    if (out->struct_size != sizeof(*out) || out->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    ud_mutex_lock((ud_mutex *)&r->mutex);
    ud_status status = index < r->count ? UD_OK : UD_INVALID;
    if (!status)
        *out = r->entries[index]->descriptor;
    ud_mutex_unlock((ud_mutex *)&r->mutex);
    return status;
}
ud_status ud_registry_find(const ud_registry *r, ud_string id, ud_component_descriptor *out) {
    if (!r || !out || !valid_string(id))
        return UD_INVALID;
    if (out->struct_size != sizeof(*out) || out->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    ud_mutex_lock((ud_mutex *)&r->mutex);
    registry_entry *entry = lookup(r, id, identifier_hash(id));
    if (entry)
        *out = entry->descriptor;
    ud_mutex_unlock((ud_mutex *)&r->mutex);
    return entry ? UD_OK : UD_UNSUPPORTED;
}
ud_status ud_registry_acquire(ud_registry *r) {
    if (!r)
        return UD_INVALID;
    ud_mutex_lock(&r->mutex);
    unsigned leases = atomic_load(&r->leases);
    ud_status status = leases == UINT32_MAX ? UD_RESOURCE : UD_OK;
    if (!status)
        atomic_fetch_add(&r->leases, 1);
    ud_mutex_unlock(&r->mutex);
    return status;
}
void ud_registry_release(ud_registry *r) {
    if (r)
        atomic_fetch_sub(&r->leases, 1);
}
ud_status ud_registry_destroy(ud_registry *r) {
    if (!r)
        return UD_OK;
    ud_mutex_lock(&r->mutex);
    if (atomic_load(&r->leases)) {
        ud_mutex_unlock(&r->mutex);
        return UD_BUSY;
    }
    for (uint32_t i = 0; i < r->count; i++)
        destroy_entry(r->entries[i]);
    ud_free(r->entries);
    ud_free(r->index);
    ud_mutex_unlock(&r->mutex);
    ud_mutex_destroy(&r->mutex);
    ud_free(r);
    return UD_OK;
}

ud_status ud_component_bind_configuration(ud_context *ctx, const ud_component_descriptor *d,
                                          const ud_json *config, ud_json **out) {
    if (!ctx || !d || !config || !out)
        return UD_INVALID;
    if (d->struct_size != sizeof(*d) || d->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    *out = NULL;
    ud_json *schema = NULL;
    ud_status s = ud_json_parse(ctx, d->configuration_schema.data, d->configuration_schema.length,
                                &schema, NULL);
    if (!s)
        s = ud_json_bind_schema(ctx, schema, config, out);
    ud_json_destroy(schema);
    return s;
}
static ud_status describe_entry(ud_bytes *bytes, const ud_component_descriptor *d) {
    UD_TRY(ud_bytes_put(bytes, "{\"id\":", 6));
    UD_TRY(ud_json_quote(bytes, d->id.data, d->id.length));
    UD_TRY(ud_bytes_put(bytes, ",\"name\":", 8));
    UD_TRY(ud_json_quote(bytes, d->name.data, d->name.length));
    UD_TRY(ud_bytes_put(bytes, ",\"version\":", 11));
    UD_TRY(ud_json_quote(bytes, d->version.data, d->version.length));
    UD_TRY(ud_bytes_put(bytes, ",\"configurationSchema\":", 23));
    UD_TRY(ud_bytes_put(bytes, d->configuration_schema.data, d->configuration_schema.length));
    char info[768];
    int n = snprintf(info, sizeof(info),
                     ",\"category\":%d,\"inputs\":%u,\"outputs\":%u,\"dynamicPorts\":%s,"
                     "\"errorOutput\":%d,\"synchronousInput\":%d,\"capabilities\":%llu,"
                     "\"optionalInputs\":%llu,\"optionalOutputs\":%llu,\"stateVersion\":%u,"
                     "\"effects\":%u,\"minimumArguments\":%u,\"maximumArguments\":%u,"
                     "\"checkpoint\":%s,\"restore\":%s,\"physicalPlan\":%s,\"previewSafe\":%s,"
                     "\"previewSchema\":%s,\"continuous\":%s,\"streamSafe\":%s,\"watermarkPolicy\":%s}",
                     d->category, d->inputs, d->outputs, d->bind_ports ? "true" : "false",
                     d->error_output, d->synchronous_input, (unsigned long long)d->capabilities,
                     (unsigned long long)d->optional_inputs,
                     (unsigned long long)d->optional_outputs, d->state_version, d->effects,
                     d->min_arguments, d->max_arguments, d->checkpoint ? "true" : "false",
                     d->restore ? "true" : "false", d->describe_plan ? "true" : "false",
                     d->capabilities & UD_COMPONENT_PREVIEW_SAFE ? "true" : "false",
                     d->capabilities & UD_COMPONENT_PREVIEW_SCHEMA ? "true" : "false",
                     d->capabilities & UD_COMPONENT_CONTINUOUS ? "true" : "false",
                     d->capabilities & UD_COMPONENT_STREAM_SAFE ? "true" : "false",
                     d->capabilities & UD_COMPONENT_WATERMARK_POLICY ? "true" : "false");
    return n < 0 || (size_t)n >= sizeof(info) ? UD_RESOURCE : ud_bytes_put(bytes, info, (size_t)n);
}
ud_status ud_registry_describe(const ud_registry *r, char **out, uint32_t *length) {
    if (!r || !out || !length)
        return UD_INVALID;
    *out = NULL;
    *length = 0;
    ud_bytes bytes = {.ctx = r->ctx};
    ud_mutex_lock((ud_mutex *)&r->mutex);
    ud_status s = ud_bytes_put(&bytes, "[", 1);
    for (uint32_t i = 0; !s && i < r->count; i++) {
        if (i)
            s = ud_bytes_put(&bytes, ",", 1);
        if (!s)
            s = describe_entry(&bytes, &r->entries[i]->descriptor);
    }
    ud_mutex_unlock((ud_mutex *)&r->mutex);
    if (!s)
        s = ud_bytes_put(&bytes, "]", 1);
    if (!s && bytes.size >= UINT32_MAX)
        s = UD_RESOURCE;
    if (!s) {
        *length = (uint32_t)bytes.size;
        s = ud_bytes_put(&bytes, "", 1);
    }
    if (!s) {
        *out = (char *)bytes.data;
        bytes.data = NULL;
    }
    ud_bytes_clear(&bytes);
    return s;
}

void ud_component_plan_clear(ud_component_plan *plan) {
    if (!plan)
        return;
    ud_free((void *)plan->physical_tree.data);
    plan->physical_tree = (ud_string){0};
}

ud_status ud_component_plan_copy(ud_context *ctx, const ud_component_plan *source,
                                 ud_component_plan *out) {
    if (!ctx || !source || !out)
        return UD_INVALID;
    *out = *source;
    out->physical_tree = (ud_string){0};
    if (source->physical_tree.length) {
        out->physical_tree =
            (ud_string){ud_strdup_n(ctx, source->physical_tree.data, source->physical_tree.length),
                        source->physical_tree.length};
        if (!out->physical_tree.data)
            return UD_RESOURCE;
    }
    return UD_OK;
}

uint64_t ud_registry_features(void) {
    return UD_COMPONENT_CONTROL_EVENTS | UD_REGISTRY_SCOPED_JOURNAL | UD_REGISTRY_READONLY_PREVIEW;
}
