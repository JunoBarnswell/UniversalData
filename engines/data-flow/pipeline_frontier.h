#ifndef UD_PIPELINE_FRONTIER_H
#define UD_PIPELINE_FRONTIER_H

#include "../../components/foundation/internal.h"
#include "ud/component.h"

typedef struct ud_pipeline_frontier {
    uint64_t observed, idle;
    int64_t inputs[64], emitted;
} ud_pipeline_frontier;

ud_status ud_pipeline_frontier_observe(ud_pipeline_frontier *, uint32_t port, int32_t kind,
                                       int64_t watermark);
/* live includes connected, unfinished ports, including ports that are idle. */
int ud_pipeline_frontier_reduce(ud_pipeline_frontier *, uint64_t live, int64_t *watermark);
ud_status ud_pipeline_frontier_emit(ud_pipeline_frontier *, int64_t watermark);
ud_status ud_pipeline_frontier_write(ud_bytes *, const ud_pipeline_frontier *, uint32_t input_count);
ud_status ud_pipeline_frontier_read(ud_context *, const ud_json *, uint32_t input_count,
                                    uint64_t connected, ud_pipeline_frontier *);

#endif
