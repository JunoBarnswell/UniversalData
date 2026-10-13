#ifndef UD_STREAM_H
#define UD_STREAM_H
#include "buffer.h"
#ifdef __cplusplus
extern "C" {
#endif
enum {UD_STREAM_DATA=1,UD_STREAM_END,UD_STREAM_POLL,UD_STREAM_WATERMARK,UD_STREAM_BARRIER,UD_STREAM_SCHEMA_CHANGE};
/* Owned Queue event. DATA owns a sealed Buffer block; schema-change may own an
 * empty schema block. Source position is copied by push and transferred by pop.
 * END/WATERMARK/BARRIER use this same bounded queue and preserve FIFO order. */
typedef struct ud_stream_event {
    uint32_t struct_size,abi_version;int32_t kind;
    uint64_t epoch,schema_version;int64_t watermark;
    ud_string source_position;ud_buffer_block *block;
} ud_stream_event;
UD_API ud_status ud_stream_event_clear(ud_stream_event *);
#ifdef __cplusplus
}
#endif
#endif
