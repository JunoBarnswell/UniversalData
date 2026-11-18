#include "../state.h"
#include "../../data/internal.h"
#include "../../foundation/thread.h"
#include "../../foundation/domain.h"

typedef struct allocation_owner {
    const void *address;
    uint64_t bytes, working_charge;
    uint32_t references;
    int exchange;
    struct allocation_owner *next;
} allocation_owner;
typedef struct storage_part {
    const void *address;
    uint64_t bytes, working_charge;
    allocation_owner *owner;
    int created;
} storage_part;
typedef struct storage_list {
    ud_context *ctx;
    storage_part *parts;
    uint32_t count, capacity;
} storage_list;
struct ud_buffer_block {
    ud_buffer_manager *manager;
    ud_batch *batch;
    storage_part *parts;
    uint32_t part_count, readers, rows;
    int closing;
    char *path;
    uint64_t wire_bytes;
    uint8_t digest[32];
    struct ud_buffer_block *retired_next;
};
struct ud_buffer_view {
    ud_buffer_block *block;
    const ud_batch *batch;
    ud_batch *owned;
};
struct ud_buffer_manager {
    ud_context *ctx;
    ud_buffer_options options;
    ud_buffer_metrics metrics;
    ud_memory_domain *working;
    ud_mutex mutex;
    allocation_owner **index;
    uint32_t capacity, allocations;
    char *directory;
    void *writer;
    uint64_t sequence, exchange_bytes;
    ud_buffer_block *retired;
};
static ud_status check(const ud_buffer_manager *manager) {
    if (ud_is_cancelled(manager->ctx))
        return UD_CANCELLED;
    return manager->options.check ? manager->options.check(manager->options.user) : UD_OK;
}
static uint32_t bucket(const void *address, uint32_t capacity) {
    uint64_t value = (uint64_t)(uintptr_t)address;
    value ^= value >> 33;
    value *= UINT64_C(0xff51afd7ed558ccd);
    value ^= value >> 33;
    return (uint32_t)value & (capacity - 1);
}
static allocation_owner *find(const ud_buffer_manager *manager, const void *address) {
    if (!manager->capacity)
        return NULL;
    for (allocation_owner *owner = manager->index[bucket(address, manager->capacity)]; owner;
         owner = owner->next)
        if (owner->address == address)
            return owner;
    return NULL;
}
static ud_status reserve_index(ud_buffer_manager *manager, uint32_t required) {
    uint32_t capacity = manager->capacity ? manager->capacity : 64;
    while (required > capacity / 2) {
        if (capacity > UINT32_MAX / 2)
            return UD_RESOURCE;
        capacity *= 2;
    }
    if (capacity == manager->capacity)
        return UD_OK;
    allocation_owner **index = ud_alloc(manager->ctx, (size_t)capacity * sizeof(*index));
    if (!index)
        return UD_RESOURCE;
    for (uint32_t i = 0; i < manager->capacity; i++) {
        allocation_owner *owner = manager->index[i];
        while (owner) {
            allocation_owner *next = owner->next;
            uint32_t slot = bucket(owner->address, capacity);
            owner->next = index[slot];
            index[slot] = owner;
            owner = next;
        }
    }
    ud_free(manager->index);
    manager->index = index;
    manager->capacity = capacity;
    return UD_OK;
}
static ud_status collect(void *user, const void *address, uint64_t bytes) {
    storage_list *list = user;
    if (list->count == list->capacity) {
        uint32_t capacity = list->capacity ? list->capacity * 2 : 32;
        if (capacity <= list->capacity || (uint64_t)capacity > SIZE_MAX / sizeof(*list->parts))
            return UD_RESOURCE;
        storage_part *parts = ud_alloc(list->ctx, (size_t)capacity * sizeof(*parts));
        if (!parts)
            return UD_RESOURCE;
        if (list->count)
            memcpy(parts, list->parts, (size_t)list->count * sizeof(*parts));
        ud_free(list->parts);
        list->parts = parts;
        list->capacity = capacity;
    }
    list->parts[list->count++] = (storage_part){.address = address, .bytes = bytes};
    return UD_OK;
}
static int address_order(const void *left, const void *right) {
    uintptr_t a = (uintptr_t)((const storage_part *)left)->address;
    uintptr_t b = (uintptr_t)((const storage_part *)right)->address;
    return (a > b) - (a < b);
}
static ud_status ownership(const ud_batch *batch, storage_list *list) {
    UD_TRY(ud_batch_visit_storage(batch, collect, list));
    qsort(list->parts, list->count, sizeof(*list->parts), address_order);
    uint32_t count = 0;
    for (uint32_t i = 0; i < list->count; i++)
        if (!count || list->parts[i].address != list->parts[count - 1].address)
            list->parts[count++] = list->parts[i];
    list->count = count;
    return UD_OK;
}
static ud_status retain_storage(ud_buffer_manager *manager, ud_buffer_block *block,
                                const ud_batch *batch, storage_list *list, uint64_t additional,
                                int exchange, int in_working) {
    if (list->count > UINT32_MAX - manager->allocations)
        return UD_RESOURCE;
    UD_TRY(reserve_index(manager, manager->allocations + list->count));
    ud_status status = UD_OK;
    for (uint32_t i = 0; !status && i < list->count; i++) {
        storage_part *part = list->parts + i;
        if (!part->owner) {
            part->owner = ud_alloc(manager->ctx, sizeof(*part->owner));
            if (!part->owner) {
                status = UD_RESOURCE;
                break;
            }
            part->created = 1;
            part->owner->address = part->address;
            part->owner->bytes = part->bytes;
            part->owner->exchange = in_working;
            part->owner->working_charge = in_working ? part->working_charge : 0;
        } else if (part->owner->references == UINT32_MAX)
            status = UD_RESOURCE;
    }
    if (!status)
        status = ud_batch_retain((ud_batch *)batch);
    if (status) {
        for (uint32_t i = 0; i < list->count; i++)
            if (list->parts[i].created)
                ud_free(list->parts[i].owner);
        return status;
    }
    for (uint32_t i = 0; i < list->count; i++) {
        storage_part *part = list->parts + i;
        allocation_owner *owner = part->owner;
        if (part->created) {
            uint32_t slot = bucket(owner->address, manager->capacity);
            owner->next = manager->index[slot];
            manager->index[slot] = owner;
            manager->allocations++;
        }
        if (!exchange && owner->exchange) {
            manager->exchange_bytes -= owner->bytes;
            ud_memory_domain_credit(manager->working, owner->working_charge);
            owner->exchange = 0;
            owner->working_charge = 0;
        }
        owner->references++;
    }
    block->batch = (ud_batch *)batch;
    block->parts = list->parts;
    block->part_count = list->count;
    list->parts = NULL;
    if (in_working)
        manager->exchange_bytes += additional;
    else
        manager->metrics.resident_bytes += additional;
    if (manager->metrics.resident_bytes > manager->metrics.resident_high_water)
        manager->metrics.resident_high_water = manager->metrics.resident_bytes;
    return UD_OK;
}
static void release_storage(ud_buffer_manager *manager, const ud_buffer_block *block) {
    for (uint32_t i = 0; i < block->part_count; i++) {
        allocation_owner *owner = block->parts[i].owner;
        if (--owner->references)
            continue;
        uint32_t slot = bucket(owner->address, manager->capacity);
        allocation_owner **link = manager->index + slot;
        while (*link != owner)
            link = &(*link)->next;
        *link = owner->next;
        if (owner->exchange) {
            manager->exchange_bytes -= owner->bytes;
            ud_memory_domain_credit(manager->working, owner->working_charge);
        } else
            manager->metrics.resident_bytes -= owner->bytes;
        manager->allocations--;
        ud_free(owner);
    }
}
static ud_status reclaim_orphans(ud_buffer_manager *manager) {
    /* Writer lock owns the entire canonical temporary namespace. Durable data
     * is in Journal; these Buffer pages never survive their controller. */
    char **names = NULL;
    uint32_t count = 0;
    ud_status status = ud_list(manager->ctx, manager->directory, ".udw", &names, &count);
    for (uint32_t i = 0; !status && i < count; i++) {
        const char *name = names[i];
        if (strlen(name) != 34 || memcmp(name, "ud-buffer-", 10) || strcmp(name + 30, ".udw"))
            continue;
        int canonical = 1;
        for (uint32_t j = 10; j < 30; j++)
            canonical &= name[j] >= '0' && name[j] <= '9';
        if (!canonical)
            continue;
        char *path = NULL;
        status = check(manager);
        if (!status)
            status = ud_join_path(manager->ctx, manager->directory, name, &path);
        if (!status)
            status = ud_path_no_symlinks(manager->ctx, path);
        if (!status)
            status = ud_remove_file(manager->ctx, path);
        ud_free(path);
    }
    ud_list_free(names, count);
    return status;
}
ud_status ud_buffer_manager_create(ud_context *ctx, const ud_buffer_options *options,
                                   ud_buffer_manager **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!ctx || !options)
        return UD_INVALID;
    if (options->struct_size != sizeof(*options) || options->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!options->max_blocks || !options->resident_limit || !options->working_limit ||
        options->max_blocks > 1000000 ||
        (options->directory.length &&
         (!options->directory.data || !options->check || !options->spill_limit ||
          memchr(options->directory.data, 0, options->directory.length))))
        return UD_INVALID;
    ud_buffer_manager *manager = ud_alloc(ctx, sizeof(*manager));
    if (!manager)
        return UD_RESOURCE;
    manager->ctx = ctx;
    manager->options = *options;
    ud_mutex_init(&manager->mutex);
    ud_status status = ud_memory_domain_create(ctx, options->working_limit, &manager->working);
    if (!status)
        status = check(manager);
    if (!status && options->directory.length) {
        manager->directory = ud_strdup_n(ctx, options->directory.data, options->directory.length);
        if (!manager->directory)
            status = UD_RESOURCE;
        if (!status)
            status = ud_path_no_symlinks(ctx, manager->directory);
        int kind = 0;
        if (!status)
            status = ud_path_kind(ctx, manager->directory, &kind);
        if (!status && kind != 2)
            status = UD_INVALID;
        char *path = NULL;
        if (!status)
            status = ud_join_path(ctx, manager->directory, "ud-buffer.writer.lock", &path);
        if (!status)
            status = ud_lock(ctx, path, &manager->writer);
        ud_free(path);
        if (!status)
            status = reclaim_orphans(manager);
    }
    if (status) {
        ud_unlock(manager->writer);
        ud_free(manager->directory);
        ud_memory_domain_destroy(manager->working);
        ud_mutex_destroy(&manager->mutex);
        ud_free(manager);
        return status;
    }
    *out = manager;
    return UD_OK;
}
static ud_status store(ud_buffer_manager *manager, const ud_batch *batch, int exchange,
                       ud_buffer_block **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!manager || !batch || batch->ctx != manager->ctx || !batch->sealed)
        return UD_INVALID;
    UD_TRY(check(manager));
    storage_list list = {.ctx = manager->ctx};
    ud_status status = ownership(batch, &list);
    ud_buffer_block *block = NULL;
    if (!status) {
        block = ud_alloc(manager->ctx, sizeof(*block));
        if (!block)
            status = UD_RESOURCE;
        else
            block->manager = manager;
        if (block)
            block->rows = batch->rows;
    }
    if (status) {
        ud_free(list.parts);
        return status;
    }
    ud_mutex_lock(&manager->mutex);
    status = manager->metrics.blocks >= manager->options.max_blocks ? UD_RESOURCE : UD_OK;
    uint64_t additional = 0, charge = 0;
    for (uint32_t i = 0; !status && i < list.count; i++) {
        storage_part *part = list.parts + i;
        part->owner = find(manager, part->address);
        if (part->owner && (exchange || !part->owner->exchange))
            continue;
        if (part->bytes > UINT64_MAX - additional)
            status = UD_RESOURCE;
        else {
            additional += part->bytes;
            if (exchange && !ud_allocation_in_domain(part->address, manager->working)) {
                part->working_charge = part->bytes;
                charge += part->bytes;
            }
        }
    }
    /* Queued frames cycle through Working; long-lived pages may not use its
     * exchange allowance. Leave three quarters for indexes, views and codecs. */
    int in_working = !status && exchange && additional &&
                     additional <= manager->options.working_limit / 4 - manager->exchange_bytes &&
                     !ud_memory_domain_charge(manager->working, charge);
    if (!status && (in_working ||
                    additional <= manager->options.resident_limit - manager->metrics.resident_bytes)) {
        status = retain_storage(manager, block, batch, &list, additional, exchange, in_working);
        if (status && in_working)
            ud_memory_domain_credit(manager->working, charge);
    } else if (!status) {
        if (!manager->directory)
            status = UD_RESOURCE;
        uint8_t *wire = NULL;
        uint64_t length = 0;
        if (!status) {
            ud_memory_scope scope;
            ud_memory_domain_enter(manager->working, &scope);
            manager->metrics.wire_encodes++;
            uint64_t cpu = ud_thread_cpu_ns();
            status = ud_batch_serialize_spill(batch, &wire, &length);
            manager->metrics.encode_cpu_ns += ud_thread_cpu_ns() - cpu;
            if (!status)
                manager->metrics.encoded_bytes += length;
            ud_memory_domain_leave(&scope);
        }
        if (!status && length > manager->options.spill_limit - manager->metrics.spill_bytes)
            status = UD_RESOURCE;
        if (!status && manager->sequence == UINT64_MAX)
            status = UD_RESOURCE;
        if (!status) {
            char name[64];
            snprintf(name, sizeof(name), "ud-buffer-%020llu.udw",
                     (unsigned long long)++manager->sequence);
            status = ud_join_path(manager->ctx, manager->directory, name, &block->path);
        }
        if (!status)
            status = check(manager);
        if (!status)
            status = ud_reserve_file(manager->ctx, block->path);
        int owned = !status;
        if (!status) {
            uint64_t started = ud_now_ms();
            status = ud_write_file(manager->ctx, block->path, wire, (size_t)length);
            manager->metrics.write_ms += ud_now_ms() - started;
            if (!status)
                manager->metrics.write_bytes += length;
        }
        if (!status) {
            block->wire_bytes = length;
            ud_hash(wire, (size_t)length, block->digest);
            manager->metrics.spill_bytes += length;
            manager->metrics.total_spill_bytes += length;
            if (manager->metrics.spill_bytes > manager->metrics.spill_high_water)
                manager->metrics.spill_high_water = manager->metrics.spill_bytes;
        } else if (owned) {
            ud_status cleanup = ud_remove_file(manager->ctx, block->path);
            if (cleanup) {
                block->wire_bytes = length;
                manager->metrics.spill_bytes += length;
                block->retired_next = manager->retired;
                manager->retired = block;
                manager->metrics.cleanup_blocks++;
                block = NULL;
                status = cleanup;
            }
        }
        ud_free(wire);
    }
    if (!status)
        manager->metrics.blocks++;
    ud_mutex_unlock(&manager->mutex);
    ud_free(list.parts);
    if (status) {
        if (block) {
            ud_free(block->path);
            ud_free(block);
        }
        return status;
    }
    *out = block;
    return UD_OK;
}
ud_status ud_buffer_store(ud_buffer_manager *manager, const ud_batch *batch,
                           ud_buffer_block **out) {
    return store(manager, batch, 0, out);
}
ud_status ud_buffer_store_exchange(ud_buffer_manager *manager, const ud_batch *batch,
                                    ud_buffer_block **out) {
    return store(manager, batch, 1, out);
}
static ud_status read_spill(ud_buffer_block *block, ud_memory_domain *domain, ud_batch **out) {
    ud_buffer_manager *manager = block->manager;
    ud_memory_scope scope;
    ud_memory_domain_enter(manager->working, &scope);
    ud_status status = ud_path_no_symlinks(manager->ctx, block->path);
    ud_bytes wire = {.ctx = manager->ctx, .limit = (size_t)block->wire_bytes};
    if (!status) {
        uint64_t started = ud_now_ms();
        status = ud_read_file(manager->ctx, block->path, &wire);
        ud_mutex_lock(&manager->mutex);
        manager->metrics.read_ms += ud_now_ms() - started;
        if (!status)
            manager->metrics.read_bytes += wire.size;
        ud_mutex_unlock(&manager->mutex);
    }
    ud_memory_domain_leave(&scope);
    if (!status) {
        uint8_t digest[32];
        ud_hash(wire.data, wire.size, digest);
        if (wire.size != block->wire_bytes || memcmp(digest, block->digest, sizeof(digest)))
            status = UD_CORRUPT;
    }
    if (!status) {
        ud_memory_domain_enter(domain, &scope);
        uint64_t cpu = ud_thread_cpu_ns();
        status = ud_batch_deserialize_spill(manager->ctx, wire.data, wire.size, out);
        ud_memory_domain_leave(&scope);
        ud_mutex_lock(&manager->mutex);
        manager->metrics.wire_decodes++;
        manager->metrics.decode_cpu_ns += ud_thread_cpu_ns() - cpu;
        ud_mutex_unlock(&manager->mutex);
    }
    ud_bytes_clear(&wire);
    return status;
}
ud_status ud_buffer_read_in_domain(ud_buffer_block *block, ud_memory_domain *domain,
                                   ud_buffer_view **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!block)
        return UD_INVALID;
    ud_buffer_manager *manager = block->manager;
    if (!domain)
        domain = manager->working;
    ud_mutex_lock(&manager->mutex);
    if (block->closing || block->readers == UINT32_MAX || manager->metrics.views == UINT32_MAX) {
        ud_mutex_unlock(&manager->mutex);
        return UD_BUSY;
    }
    block->readers++;
    manager->metrics.views++;
    ud_mutex_unlock(&manager->mutex);
    ud_status status = check(manager);
    ud_buffer_view *view = NULL;
    if (!status) {
        ud_memory_scope scope;
        ud_memory_domain_enter(domain, &scope);
        view = ud_alloc(manager->ctx, sizeof(*view));
        ud_memory_domain_leave(&scope);
        if (!view)
            status = UD_RESOURCE;
    }
    if (!status) {
        view->block = block;
        if (!block->batch)
            status = read_spill(block, domain, &view->owned);
        view->batch = block->batch ? block->batch : view->owned;
    }
    if (status) {
        if (view) {
            ud_batch_destroy(view->owned);
            ud_free(view);
        }
        ud_mutex_lock(&manager->mutex);
        block->readers--;
        manager->metrics.views--;
        ud_mutex_unlock(&manager->mutex);
        return status;
    }
    *out = view;
    return UD_OK;
}
ud_status ud_buffer_read(ud_buffer_block *block, ud_buffer_view **out) {
    return ud_buffer_read_in_domain(block, NULL, out);
}
const ud_batch *ud_buffer_view_batch(const ud_buffer_view *view) {
    return view ? view->batch : NULL;
}
void ud_buffer_view_release(ud_buffer_view *view) {
    if (!view)
        return;
    ud_buffer_manager *manager = view->block->manager;
    ud_batch_destroy(view->owned);
    ud_mutex_lock(&manager->mutex);
    view->block->readers--;
    manager->metrics.views--;
    ud_mutex_unlock(&manager->mutex);
    ud_free(view);
}
ud_status ud_buffer_release(ud_buffer_block *block) {
    if (!block)
        return UD_OK;
    ud_buffer_manager *manager = block->manager;
    ud_mutex_lock(&manager->mutex);
    if (block->readers || block->closing) {
        ud_mutex_unlock(&manager->mutex);
        return UD_BUSY;
    }
    block->closing = 1;
    ud_mutex_unlock(&manager->mutex);
    ud_status status = block->path ? ud_remove_file(manager->ctx, block->path) : UD_OK;
    ud_mutex_lock(&manager->mutex);
    manager->metrics.blocks--;
    if (status) {
        block->retired_next = manager->retired;
        manager->retired = block;
        manager->metrics.cleanup_blocks++;
    } else {
        if (block->batch)
            release_storage(manager, block);
        else
            manager->metrics.spill_bytes -= block->wire_bytes;
    }
    ud_mutex_unlock(&manager->mutex);
    if (status)
        return status;
    ud_batch_destroy(block->batch);
    ud_free(block->parts);
    ud_free(block->path);
    ud_free(block);
    return UD_OK;
}
ud_status ud_buffer_manager_metrics(const ud_buffer_manager *manager, ud_buffer_metrics *out) {
    if (!manager || !out)
        return UD_INVALID;
    ud_mutex_lock((ud_mutex *)&manager->mutex);
    *out = manager->metrics;
    out->working_bytes = ud_memory_domain_used(manager->working);
    out->working_high_water = ud_memory_domain_peak(manager->working);
    ud_mutex_unlock((ud_mutex *)&manager->mutex);
    return UD_OK;
}
ud_status ud_buffer_manager_destroy(ud_buffer_manager *manager) {
    if (!manager)
        return UD_OK;
    ud_mutex_lock(&manager->mutex);
    if (manager->metrics.blocks || manager->metrics.views || manager->allocations) {
        ud_mutex_unlock(&manager->mutex);
        return UD_BUSY;
    }
    while (manager->retired) {
        ud_buffer_block *block = manager->retired;
        ud_status cleanup = ud_remove_file(manager->ctx, block->path);
        if (cleanup) {
            ud_mutex_unlock(&manager->mutex);
            return cleanup;
        }
        manager->retired = block->retired_next;
        manager->metrics.cleanup_blocks--;
        manager->metrics.spill_bytes -= block->wire_bytes;
        ud_free(block->path);
        ud_free(block);
    }
    ud_status status = ud_memory_domain_destroy(manager->working);
    ud_mutex_unlock(&manager->mutex);
    if (status)
        return status;
    ud_unlock(manager->writer);
    ud_free(manager->directory);
    ud_free(manager->index);
    ud_mutex_destroy(&manager->mutex);
    ud_free(manager);
    return UD_OK;
}

ud_status ud_buffer_reserve_external(ud_buffer_manager *m, uint64_t bytes) {
    if (!m)
        return UD_INVALID;
    ud_mutex_lock(&m->mutex);
    ud_status status =
        bytes > m->options.spill_limit - m->metrics.spill_bytes ? UD_RESOURCE : UD_OK;
    if (!status) {
        m->metrics.spill_bytes += bytes;
        m->metrics.total_spill_bytes += bytes;
        if (m->metrics.spill_bytes > m->metrics.spill_high_water)
            m->metrics.spill_high_water = m->metrics.spill_bytes;
    }
    ud_mutex_unlock(&m->mutex);
    return status;
}
void ud_buffer_release_external(ud_buffer_manager *m, uint64_t bytes) {
    if (!m)
        return;
    ud_mutex_lock(&m->mutex);
    if (bytes <= m->metrics.spill_bytes)
        m->metrics.spill_bytes -= bytes;
    ud_mutex_unlock(&m->mutex);
}

ud_status ud_buffer_working_allocate(ud_buffer_manager *manager, size_t bytes, void **out) {
    if (!manager || !out || !bytes)
        return UD_INVALID;
    *out = NULL;
    UD_TRY(check(manager));
    ud_memory_scope scope;
    ud_memory_domain_enter(manager->working, &scope);
    *out = ud_alloc(manager->ctx, bytes);
    ud_memory_domain_leave(&scope);
    return *out ? UD_OK : UD_RESOURCE;
}
ud_status ud_buffer_manager_limits(const ud_buffer_manager *manager, ud_buffer_options *out) {
    if (!manager || !out)
        return UD_INVALID;
    *out = manager->options;
    return UD_OK;
}

uint32_t ud_buffer_block_views(const ud_buffer_block *b) {
    if (!b)
        return 0;
    ud_buffer_manager *m = b->manager;
    ud_mutex_lock(&m->mutex);
    uint32_t count = b->readers;
    ud_mutex_unlock(&m->mutex);
    return count;
}

ud_status ud_buffer_working_copy_value(ud_buffer_manager *m, const ud_value *value, ud_value *out) {
    if (!m || !value || !out)
        return UD_INVALID;
    UD_TRY(check(m));
    ud_memory_scope scope;
    ud_memory_domain_enter(m->working, &scope);
    ud_status status = ud_copy_value(m->ctx, value, out, 0);
    ud_memory_domain_leave(&scope);
    return status;
}

uint64_t ud_buffer_block_bytes(const ud_buffer_block *block) {
    if (!block)
        return 0;
    ud_mutex_lock(&block->manager->mutex);
    uint64_t bytes = block->wire_bytes;
    if (block->batch) {
        bytes = 0;
        for (uint32_t i = 0; i < block->part_count; i++)
            bytes += block->parts[i].bytes;
    }
    ud_mutex_unlock(&block->manager->mutex);
    return bytes;
}
uint64_t ud_buffer_block_spill_bytes(const ud_buffer_block *block) {
    return block ? block->wire_bytes : 0;
}
uint32_t ud_buffer_block_rows(const ud_buffer_block *block) {
    return block ? block->rows : 0;
}

ud_status ud_buffer_state_domain_create(ud_buffer_manager *manager, uint64_t limit,
                                        ud_memory_domain **out) {
    if (!manager)
        return UD_INVALID;
    return ud_memory_domain_create_child(manager->ctx, limit, manager->working, out);
}
