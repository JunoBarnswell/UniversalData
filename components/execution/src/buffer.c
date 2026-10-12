#include "ud/buffer.h"
#include "../../data/internal.h"
#include "../../foundation/thread.h"

struct ud_buffer_block {
    ud_buffer_manager *manager;
    ud_batch *batch;
    char *path;
    uint64_t bytes;
    uint8_t digest[32];
};
struct ud_buffer_manager {
    ud_context *ctx;
    ud_buffer_options options;
    ud_buffer_metrics metrics;
    ud_mutex mutex;
    char *directory;
    void *writer;
    uint64_t sequence;
};
static ud_status check(const ud_buffer_manager *m) {
    if (ud_is_cancelled(m->ctx))
        return UD_CANCELLED;
    return m->options.check ? m->options.check(m->options.user) : UD_OK;
}
ud_status ud_buffer_manager_create(ud_context *ctx, const ud_buffer_options *o,
                                   ud_buffer_manager **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!ctx || !o)
        return UD_INVALID;
    if (o->struct_size != sizeof(*o) || o->abi_version != UD_ABI_VERSION)
        return UD_INCOMPATIBLE;
    if (!o->max_blocks || !o->resident_limit || o->max_blocks > 1000000 ||
        (o->directory.length && (!o->directory.data || !o->check || !o->spill_limit ||
                                 memchr(o->directory.data, 0, o->directory.length))))
        return UD_INVALID;
    ud_buffer_manager *m = ud_alloc(ctx, sizeof(*m));
    if (!m)
        return UD_RESOURCE;
    m->ctx = ctx;
    m->options = *o;
    ud_mutex_init(&m->mutex);
    ud_status s = check(m);
    if (!s && o->directory.length) {
        m->directory = ud_strdup_n(ctx, o->directory.data, o->directory.length);
        if (!m->directory)
            s = UD_RESOURCE;
        if (!s)
            s = ud_path_no_symlinks(ctx, m->directory);
        int kind = 0;
        if (!s)
            s = ud_path_kind(ctx, m->directory, &kind);
        if (!s && kind != 2)
            s = UD_INVALID;
        char *path = NULL;
        if (!s)
            s = ud_join_path(ctx, m->directory, "ud-buffer.writer.lock", &path);
        if (!s)
            s = ud_lock(ctx, path, &m->writer);
        ud_free(path);
    }
    if (s) {
        ud_unlock(m->writer);
        ud_free(m->directory);
        ud_mutex_destroy(&m->mutex);
        ud_free(m);
        return s;
    }
    *out = m;
    return UD_OK;
}
ud_status ud_buffer_store(ud_buffer_manager *m, const ud_batch *batch, ud_buffer_block **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!m || !batch || batch->ctx != m->ctx || !batch->sealed)
        return UD_INVALID;
    UD_TRY(check(m));
    uint8_t *wire = NULL;
    uint64_t length = 0;
    UD_TRY(ud_batch_serialize(batch, &wire, &length));
    ud_buffer_block *block = ud_alloc(m->ctx, sizeof(*block));
    if (!block) {
        ud_free(wire);
        return UD_RESOURCE;
    }
    block->manager = m;
    block->bytes = length;
    ud_hash(wire, (size_t)length, block->digest);
    ud_mutex_lock(&m->mutex);
    ud_status s = m->metrics.blocks >= m->options.max_blocks ? UD_RESOURCE : UD_OK;
    if (!s && length <= m->options.resident_limit - m->metrics.resident_bytes) {
        s = ud_batch_retain((ud_batch *)batch);
        if (!s) {
            block->batch = (ud_batch *)batch;
            m->metrics.resident_bytes += length;
        }
    } else if (!s) {
        if (!m->directory)
            s = UD_RESOURCE;
        if (!s && length > m->options.spill_limit - m->metrics.spill_bytes)
            s = UD_RESOURCE;
        char name[64];
        if (!s) {
            snprintf(name, sizeof(name), "ud-buffer-%020llu.udw",
                     (unsigned long long)++m->sequence);
            s = ud_join_path(m->ctx, m->directory, name, &block->path);
        }
        if (!s)
            s = check(m);
        if (!s)
            s = ud_reserve_file(m->ctx, block->path);
        int owned = !s;
        if (!s)
            s = ud_write_file(m->ctx, block->path, wire, (size_t)length);
        if (s && owned)
            (void)ud_remove_file(m->ctx, block->path);
        if (!s) {
            m->metrics.spill_bytes += length;
            m->metrics.total_spill_bytes += length;
        }
    }
    if (!s)
        m->metrics.blocks++;
    ud_mutex_unlock(&m->mutex);
    ud_free(wire);
    if (s) {
        ud_free(block->path);
        ud_free(block);
        return s;
    }
    *out = block;
    return UD_OK;
}
ud_status ud_buffer_read(const ud_buffer_block *block, ud_batch **out) {
    if (!out)
        return UD_INVALID;
    *out = NULL;
    if (!block)
        return UD_INVALID;
    ud_buffer_manager *m = block->manager;
    UD_TRY(check(m));
    if (block->batch) {
        UD_TRY(ud_batch_retain(block->batch));
        *out = block->batch;
        return UD_OK;
    }
    UD_TRY(ud_path_no_symlinks(m->ctx, block->path));
    ud_bytes wire = {.ctx = m->ctx, .limit = (size_t)block->bytes};
    ud_status s = ud_read_file(m->ctx, block->path, &wire);
    uint8_t digest[32];
    if (!s) {
        ud_hash(wire.data, wire.size, digest);
        if (wire.size != block->bytes || memcmp(digest, block->digest, 32))
            s = UD_CORRUPT;
    }
    if (!s)
        s = ud_batch_deserialize(m->ctx, wire.data, wire.size, out);
    if (!s)
        s = ud_batch_seal(*out);
    if (s) {
        ud_batch_destroy(*out);
        *out = NULL;
    }
    ud_bytes_clear(&wire);
    return s;
}
ud_status ud_buffer_release(ud_buffer_block *block) {
    if (!block)
        return UD_OK;
    ud_buffer_manager *m = block->manager;
    /* Cleanup is allowed after cancellation; never deletes somebody else's file. */
    ud_status s = block->path ? ud_remove_file(m->ctx, block->path) : UD_OK;
    if (s)
        return s;
    ud_mutex_lock(&m->mutex);
    if (block->batch)
        m->metrics.resident_bytes -= block->bytes;
    else
        m->metrics.spill_bytes -= block->bytes;
    m->metrics.blocks--;
    ud_mutex_unlock(&m->mutex);
    ud_batch_destroy(block->batch);
    ud_free(block->path);
    ud_free(block);
    return UD_OK;
}
ud_status ud_buffer_manager_metrics(const ud_buffer_manager *m, ud_buffer_metrics *out) {
    if (!m || !out)
        return UD_INVALID;
    ud_mutex_lock((ud_mutex *)&m->mutex);
    *out = m->metrics;
    ud_mutex_unlock((ud_mutex *)&m->mutex);
    return UD_OK;
}
ud_status ud_buffer_manager_destroy(ud_buffer_manager *m) {
    if (!m)
        return UD_OK;
    if (m->metrics.blocks)
        return UD_BUSY;
    ud_unlock(m->writer);
    ud_free(m->directory);
    ud_mutex_destroy(&m->mutex);
    ud_free(m);
    return UD_OK;
}