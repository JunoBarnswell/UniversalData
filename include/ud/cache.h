#ifndef UD_CACHE_H
#define UD_CACHE_H
#include "operator.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_cache ud_cache;
typedef struct ud_cache_reader ud_cache_reader;
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
UD_API ud_status ud_cache_schema(const ud_cache *, ud_batch **);
UD_API ud_status ud_cache_fingerprint(const ud_cache *, uint8_t out[32]);
UD_API void ud_cache_get_metrics(const ud_cache *, ud_cache_metrics *);
/* Each reader owns its view/controller. Probe matches composite canonical keys;
 * NULL is a miss. next borrows reference data until the next reader operation. */
UD_API ud_status ud_cache_reader_create(ud_cache *, const ud_batch *probe_schema,
                                        const uint32_t *keys, uint32_t key_count,
                                        ud_cache_reader **);
UD_API ud_status ud_cache_probe(ud_cache_reader *, const ud_batch *, uint32_t);
UD_API ud_status ud_cache_next(ud_cache_reader *, const ud_batch **, uint32_t *);
UD_API void ud_cache_reader_destroy(ud_cache_reader *);
#ifdef __cplusplus
}
#endif
#endif
