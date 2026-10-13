#ifndef UD_CACHE_H
#define UD_CACHE_H
#include "operator.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_cache ud_cache;
typedef struct ud_cache_reader ud_cache_reader;
enum { UD_CACHE_FULL = 0, UD_CACHE_PARTIAL = 1 };
enum { UD_CACHE_FIRST = 0, UD_CACHE_ERROR = 1, UD_CACHE_ALL = 2 };
typedef struct ud_cache_options {
    uint32_t struct_size, abi_version, max_batch_rows;
    const uint32_t *keys;
    uint32_t key_count;
    int32_t duplicates, error_policy;
    ud_buffer_manager *buffers;
    ud_executor *executor;
    ud_status (*check)(void *);
    void *user;
    int32_t record_state;
    uint32_t mode, max_entries;
    uint64_t ttl_ms;
} ud_cache_options;
typedef struct ud_cache_metrics {
    uint64_t loaded, hits, misses, duplicates, pages;
} ud_cache_metrics;
/* Builder has one controller. Seal creates an immutable shared Compute index
 * in native Buffer pages; no private storage format or private worker pool. */
UD_API ud_status ud_cache_create(ud_context *, const ud_batch *, const ud_cache_options *,
                                 ud_cache **);
UD_API ud_status ud_cache_append(ud_cache *, const ud_batch *);
UD_API ud_status ud_cache_seal(ud_cache *);
/* Error+Redirect builds a First index and explicitly quarantines subsequent
 * equal non-NULL keys; Error+Ignore records them and keeps First. */
UD_API ud_status ud_cache_duplicates(ud_cache *, uint64_t *position, ud_batch **);
UD_API ud_status ud_cache_retain(ud_cache *); /* Sealed only. */
UD_API ud_status ud_cache_release(ud_cache *);
UD_API uint32_t ud_cache_entry_limit(const ud_cache *);
UD_API int32_t ud_cache_is_sealed(const ud_cache *);
UD_API ud_status ud_cache_schema(const ud_cache *, ud_batch **);
UD_API ud_status ud_cache_fingerprint(const ud_cache *, uint8_t out[32]);
UD_API ud_status ud_cache_checkpoint(ud_cache *, ud_batch **);
/* Restore requires an idle controller with no active reader views. */
UD_API ud_status ud_cache_restore(ud_cache *, const ud_batch *);
UD_API void ud_cache_get_metrics(const ud_cache *, ud_cache_metrics *);
/* Each reader owns its view/controller. Probe matches composite canonical keys;
 * NULL is a miss. next borrows reference data until the next reader operation. */
UD_API ud_status ud_cache_reader_create(ud_cache *, const ud_batch *probe_schema,
                                        const uint32_t *keys, uint32_t key_count,
                                        ud_cache_reader **);
/* Partial CacheAsset put/probe uses the same Buffer-backed reader. NULL rows
 * explicitly cache a negative lookup. Entry count, bytes and TTL are bounded. */
UD_API ud_status ud_cache_partial_put(ud_cache *, const ud_batch *, uint32_t, const uint32_t *,
                                      const ud_batch *);
UD_API ud_status ud_cache_partial_probe(ud_cache_reader *, const ud_batch *, uint32_t,
                                        int32_t *cached);
/* begin fixes expiry time for this reader until its next input batch. */
UD_API void ud_cache_reader_begin(ud_cache_reader *, int32_t frozen_snapshot);
UD_API ud_status ud_cache_probe(ud_cache_reader *, const ud_batch *, uint32_t);
UD_API ud_status ud_cache_next(ud_cache_reader *, const ud_batch **, uint32_t *);
/* Scan and random row access borrow the same bounded Buffer reader view. */
UD_API ud_status ud_cache_scan_create(ud_cache *, ud_cache_reader **);
UD_API ud_status ud_cache_read_at(ud_cache_reader *, uint64_t, const ud_batch **, uint32_t *);
UD_API void ud_cache_reader_destroy(ud_cache_reader *);
#ifdef __cplusplus
}
#endif
#endif
