#ifndef UD_RUNTIME_STATE_MEMORY_H
#define UD_RUNTIME_STATE_MEMORY_H
#include "ud/buffer.h"
#include "../foundation/domain.h"
/* A subquota of the sole Buffer Manager working domain, never a second pool. */
ud_status ud_buffer_state_domain_create(ud_buffer_manager *, uint64_t, ud_memory_domain **);
/* Publish a short-lived frame against the manager's working budget. Retained
 * operator state still requires resident admission or uses the same Wire spill. */
ud_status ud_buffer_store_exchange(ud_buffer_manager *, const ud_batch *, ud_buffer_block **);
/* Wire bytes written by this block, zero for a memory-backed publication. */
uint64_t ud_buffer_block_spill_bytes(const ud_buffer_block *);
#endif
