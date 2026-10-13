#ifndef UD_COMPUTE_REPLAY_H
#define UD_COMPUTE_REPLAY_H
#include "state.h"
#include "ud/operator.h"
typedef struct ud_replay ud_replay;
ud_status ud_replay_create(ud_context *, const ud_operator_options *, const ud_batch *const *,
                           ud_replay **);
ud_status ud_replay_record(ud_replay *, int32_t, uint32_t, const ud_batch *);
ud_status ud_replay_snapshot(ud_replay *, ud_batch **);
ud_status ud_replay_restore(ud_replay *, const ud_batch *, ud_operator **);
ud_status ud_replay_rebase(ud_replay *, const ud_batch *);
void ud_replay_destroy(ud_replay *);
#endif
