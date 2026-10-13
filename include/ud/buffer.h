#ifndef UD_BUFFER_H
#define UD_BUFFER_H
#include "data.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_buffer_manager ud_buffer_manager;
typedef struct ud_buffer_block ud_buffer_block;
typedef struct ud_buffer_view ud_buffer_view;
typedef struct ud_buffer_options {
    uint32_t struct_size, abi_version, max_blocks;
    uint64_t resident_limit, working_limit, spill_limit;
    ud_string directory;        /* Optional, already authorized by the owning resource lease. */
    ud_status (*check)(void *); /* Rechecks cancellation/deadline/resource grant on every I/O. */
    void *user;
} ud_buffer_options;
typedef struct ud_buffer_metrics {
    uint64_t resident_bytes, working_bytes, spill_bytes, total_spill_bytes;
    uint64_t resident_high_water, working_high_water, wire_encodes;
    uint64_t wire_decodes, encoded_bytes, read_bytes, write_bytes, spill_high_water;
    uint64_t encode_cpu_ns, decode_cpu_ns, read_ms, write_ms;
    uint32_t blocks, views, cleanup_blocks;
} ud_buffer_metrics;
UD_API ud_status ud_buffer_manager_create(ud_context *, const ud_buffer_options *,
                                          ud_buffer_manager **);
/* Store borrows a sealed batch and owns one retained reference or wire spill.
 * No implicit disk fallback without a supplied resource lease/directory. */
UD_API ud_status ud_buffer_store(ud_buffer_manager *, const ud_batch *, ud_buffer_block **);
/* View owns the pin/rehydration. Batch is borrowed sealed until view release.
 * Resident bytes count unique immutable allocations including capacity/headers;
 * working bytes count all codec/read allocations in the manager's domain. */
UD_API ud_status ud_buffer_read(ud_buffer_block *, ud_buffer_view **);
UD_API const ud_batch *ud_buffer_view_batch(const ud_buffer_view *);
UD_API void ud_buffer_view_release(ud_buffer_view *);
/* BUSY preserves the handle when views are alive. Other results consume it;
 * a failed spill deletion remains owned by the manager for destroy retry. */
UD_API ud_status ud_buffer_release(ud_buffer_block *);
UD_API uint32_t ud_buffer_block_views(const ud_buffer_block *);
UD_API ud_status ud_buffer_manager_metrics(const ud_buffer_manager *, ud_buffer_metrics *);
/* State indexes share the same bounded working domain as Wire/rehydration. */
UD_API ud_status ud_buffer_working_allocate(ud_buffer_manager *, size_t, void **);
UD_API ud_status ud_buffer_working_copy_value(ud_buffer_manager *, const ud_value *, ud_value *);
UD_API ud_status ud_buffer_manager_limits(const ud_buffer_manager *, ud_buffer_options *);
/* Native format staging shares the manager's disk budget and cleanup lifetime. */
UD_API ud_status ud_buffer_reserve_external(ud_buffer_manager *, uint64_t);
UD_API void ud_buffer_release_external(ud_buffer_manager *, uint64_t);
UD_API ud_status ud_buffer_manager_destroy(ud_buffer_manager *); /* BUSY with live blocks. */
#ifdef __cplusplus
}
#endif
#endif
