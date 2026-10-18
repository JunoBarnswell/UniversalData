#ifndef UD_RUNTIME_STATE_MEMORY_H
#define UD_RUNTIME_STATE_MEMORY_H
#include "ud/buffer.h"
#include "../foundation/domain.h"
/* A subquota of the sole Buffer Manager working domain, never a second pool. */
ud_status ud_buffer_state_domain_create(ud_buffer_manager *, uint64_t, ud_memory_domain **);
#endif
