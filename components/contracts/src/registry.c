#include "../registry_internal.h"
#include "../../foundation/internal.h"
#include <stdatomic.h>
typedef struct entry {
    ud_component_descriptor descriptor;
    void *owner;
    ud_component_owner_release release;
    struct entry *next;
} entry;
struct ud_registry {
    ud_context *ctx;
    entry *head, *tail;
    uint32_t count;
    atomic_uint leases;
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
    if (e->release)
        e->release(e->owner);
    ud_free(e);
}
ud_status ud_registry_create(ud_context *c, ud_registry **out) {
    if (!c || !out)
        return UD_INVALID;
    *out = ud_alloc(c, sizeof(**out));
    if (!*out)
        return UD_RESOURCE;
    (*out)->ctx = c;
    atomic_init(&(*out)->leases, 0);
    return UD_OK;
}
static ud_status build_entry(ud_registry *r, const ud_component_descriptor *d, entry **out) {
    *out = NULL;
    if (!r || !d)
        return UD_INVALID;
    if (atomic_load(&r->leases))
        return UD_BUSY;
    if (d->struct_size != sizeof(*d) || d->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!valid(d->id) || !valid(d->name) || !valid(d->version) || !valid(d->configuration_schema) ||
        d->inputs > 64 || d->outputs > 64 || d->category < UD_COMPONENT_TASK ||
        d->category > UD_COMPONENT_FUNCTION ||
        (d->category > UD_COMPONENT_DESTINATION && d->category < UD_COMPONENT_FUNCTION) || d->synchronous_input < -1 ||
        d->error_output < -1 ||
        (d->synchronous_input >= 0 && (uint32_t)d->synchronous_input >= d->inputs) ||
        (d->error_output >= 0 && (uint32_t)d->error_output >= d->outputs))
        return UD_INVALID;
    if (d->category == UD_COMPONENT_TASK && !d->invoke)
        return UD_INVALID;
    if (d->category == UD_COMPONENT_FUNCTION &&
        (!(d->capabilities & UD_FUNCTION_PURE) || !d->function_bind || !d->function_evaluate ||
         d->min_arguments > d->max_arguments || d->max_arguments > 16 || d->inputs || d->outputs))
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
    *out = e;
    return UD_OK;
}
ud_status ud_registry_register_owned(ud_registry *r, const ud_component_descriptor *descriptors,
                                     uint32_t count, void *owner, ud_component_owner_retain retain,
                                     ud_component_owner_release release) {
    if (!r || !descriptors || !count || count > 4096 || !!retain != !!release || (retain && !owner))
        return UD_INVALID;
    if (atomic_load(&r->leases))
        return UD_BUSY;
    if (count > UINT32_MAX - r->count)
        return UD_RESOURCE;
    entry *head = NULL, *tail = NULL;
    ud_status s = UD_OK;
    for (uint32_t i = 0; !s && i < count; i++) {
        entry *e = NULL;
        s = build_entry(r, descriptors + i, &e);
        if (s)
            break;
        for (entry *earlier = head; earlier; earlier = earlier->next)
            if (same(earlier->descriptor.id, e->descriptor.id)) {
                s = UD_SCHEMA;
                break;
            }
        if (!s && retain)
            s = retain(owner);
        if (s) {
            clear(e);
            break;
        }
        e->owner = owner;
        e->release = release;
        if (tail)
            tail->next = e;
        else
            head = e;
        tail = e;
    }
    if (s) {
        while (head) {
            entry *next = head->next;
            clear(head);
            head = next;
        }
        return s;
    }
    if (r->tail)
        r->tail->next = head;
    else
        r->head = head;
    r->tail = tail;
    r->count += count;
    return UD_OK;
}
ud_status ud_registry_register(ud_registry *r, const ud_component_descriptor *descriptor) {
    return ud_registry_register_owned(r, descriptor, 1, NULL, NULL, NULL);
}
uint32_t ud_registry_count(const ud_registry *r) {
    return r ? r->count : 0;
}
ud_status ud_registry_get(const ud_registry *r, uint32_t index, ud_component_descriptor *out) {
    if (!r || !out || index >= r->count)
        return UD_INVALID;
    if (out->struct_size != sizeof(*out) || out->abi_version != UD_ABI_VERSION)
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
    if (out->struct_size != sizeof(*out) || out->abi_version != UD_ABI_VERSION)
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
    unsigned old = atomic_load(&r->leases);
    do {
        if (old == UINT32_MAX) return UD_RESOURCE;
    } while (!atomic_compare_exchange_weak(&r->leases, &old, old + 1));
    return UD_OK;
}
void ud_registry_release(ud_registry *r) {
    if (r) atomic_fetch_sub(&r->leases, 1);
}
ud_status ud_registry_destroy(ud_registry *r) {
    if (!r)
        return UD_OK;
    if (atomic_load(&r->leases))
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
