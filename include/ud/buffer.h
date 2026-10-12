#ifndef UD_BUFFER_H
#define UD_BUFFER_H
#include "data.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_buffer_manager ud_buffer_manager;
typedef struct ud_buffer_block ud_buffer_block;
typedef struct ud_buffer_options {
    uint32_t struct_size, abi_version, max_blocks;
    uint64_t resident_limit, spill_limit;
    ud_string directory;        /* Optional, already authorized by the owning resource lease. */
    ud_status (*check)(void *); /* Rechecks cancellation/deadline/resource grant on every I/O. */
    void *user;
} ud_buffer_options;
typedef struct ud_buffer_metrics {
    uint64_t resident_bytes, spill_bytes, total_spill_bytes;
    uint32_t blocks;
} ud_buffer_metrics;
UD_API ud_status ud_buffer_manager_create(ud_context *, const ud_buffer_options *,
                                          ud_buffer_manager **);
/* Store borrows a sealed batch and owns one retained reference or wire spill.
 * No implicit disk fallback without a supplied resource lease/directory. */
UD_API ud_status ud_buffer_store(ud_buffer_manager *, const ud_batch *, ud_buffer_block **);
UD_API ud_status ud_buffer_read(const ud_buffer_block *, ud_batch **);
UD_API ud_status ud_buffer_release(ud_buffer_block *);
UD_API ud_status ud_buffer_manager_metrics(const ud_buffer_manager *, ud_buffer_metrics *);
UD_API ud_status ud_buffer_manager_destroy(ud_buffer_manager *); /* BUSY with live blocks. */
#ifdef __cplusplus
}
#endif
#endif
