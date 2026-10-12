#include "../registry_internal.h"
#include "../../foundation/internal.h"
typedef struct entry {
    ud_component_descriptor descriptor;
    struct entry *next;
} entry;
struct ud_registry {
    ud_context *ctx;
    entry *head, *tail;
    uint32_t count, leases;
};
static int same(ud_string a, ud_string b) {
    return a.length == b.length && !memcmp(a.data, b.data, a.length);
}
static int valid(ud_string s) {
    return s.data && s.length && s.length <= 65536 && !memchr(s.data, 0, s.length) &&
           ud_utf8_valid(s.data, s.length);
}
static void clear(entry *e) {
    ud_free((void *)e->descriptor.id.data);
    ud_free((void *)e->descriptor.name.data);
    ud_free((void *)e->descriptor.version.data);
    ud_free((void *)e->descriptor.configuration_schema.data);
    ud_free(e);
}
ud_status ud_registry_create(ud_context *c, ud_registry **out) {
    if (!c || !out)
        return UD_INVALID;
    *out = ud_alloc(c, sizeof(**out));
    if (!*out)
        return UD_RESOURCE;
    (*out)->ctx = c;
    return UD_OK;
}
ud_status ud_registry_register(ud_registry *r, const ud_component_descriptor *d) {
    if (!r || !d)
        return UD_INVALID;
    if (r->leases)
        return UD_BUSY;
    if (d->struct_size < sizeof(*d) || d->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!valid(d->id) || !valid(d->name) || !valid(d->version) || !valid(d->configuration_schema) ||
        d->inputs > 64 || d->outputs > 64 || d->category < UD_COMPONENT_TASK ||
        d->category > UD_COMPONENT_LOG_PROVIDER || d->synchronous_input < -1 ||
        d->error_output < -1 ||
        (d->synchronous_input >= 0 && (uint32_t)d->synchronous_input >= d->inputs) ||
        (d->error_output >= 0 && (uint32_t)d->error_output >= d->outputs))
        return UD_INVALID;
    if (d->category == UD_COMPONENT_TASK && !d->invoke)
        return UD_INVALID;
    if (d->category >= UD_COMPONENT_SOURCE && d->category <= UD_COMPONENT_DESTINATION &&
        (!d->configure || !d->prepare || !d->step || !d->finish || !d->dispose ||
         (d->category == UD_COMPONENT_SOURCE && (d->inputs || !d->outputs)) ||
         (d->category == UD_COMPONENT_DESTINATION && !d->inputs) ||
         (d->category == UD_COMPONENT_TRANSFORM && (!d->inputs || !d->outputs))))
        return UD_INVALID;
    for (entry *e = r->head; e; e = e->next)
        if (same(e->descriptor.id, d->id))
            return UD_SCHEMA;
    entry *e = ud_alloc(r->ctx, sizeof(*e));
    if (!e)
        return UD_RESOURCE;
    e->descriptor = *d;
    ud_string *dst[] = {&e->descriptor.id, &e->descriptor.name, &e->descriptor.version,
                        &e->descriptor.configuration_schema};
    ud_string src[] = {d->id, d->name, d->version, d->configuration_schema};
    for (int i = 0; i < 4; i++)
        dst[i]->data = NULL;
    for (int i = 0; i < 4; i++) {
        dst[i]->data = ud_strdup_n(r->ctx, src[i].data, src[i].length);
        if (!dst[i]->data) {
            clear(e);
            return UD_RESOURCE;
        }
    }
    if (r->tail)
        r->tail->next = e;
    else
        r->head = e;
    r->tail = e;
    r->count++;
    return UD_OK;
}
uint32_t ud_registry_count(const ud_registry *r) {
    return r ? r->count : 0;
}
ud_status ud_registry_get(const ud_registry *r, uint32_t index, ud_component_descriptor *out) {
    if (!r || !out || index >= r->count)
        return UD_INVALID;
    if (out->struct_size < sizeof(*out) || out->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    entry *e = r->head;
    while (index--)
        e = e->next;
    *out = e->descriptor;
    return UD_OK;
}
ud_status ud_registry_find(const ud_registry *r, ud_string id, ud_component_descriptor *out) {
    if (!r || !out || !valid(id))
        return UD_INVALID;
    if (out->struct_size < sizeof(*out) || out->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    for (entry *e = r->head; e; e = e->next)
        if (same(e->descriptor.id, id)) {
            *out = e->descriptor;
            return UD_OK;
        }
    return UD_UNSUPPORTED;
}
ud_status ud_registry_acquire(ud_registry *r) {
    if (!r)
        return UD_INVALID;
    if (r->leases == UINT32_MAX)
        return UD_RESOURCE;
    r->leases++;
    return UD_OK;
}
void ud_registry_release(ud_registry *r) {
    if (r && r->leases)
        r->leases--;
}
ud_status ud_registry_destroy(ud_registry *r) {
    if (!r)
        return UD_OK;
    if (r->leases)
        return UD_BUSY;
    entry *e = r->head;
    while (e) {
        entry *next = e->next;
        clear(e);
        e = next;
    }
    ud_free(r);
    return UD_OK;
}
