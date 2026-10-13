#ifndef UD_CDC_H
#define UD_CDC_H
#include "data.h"
#ifdef __cplusplus
extern "C" {
#endif
enum { UD_CDC_BEGIN = 1, UD_CDC_ROWS, UD_CDC_COMMIT, UD_CDC_SCHEMA_CHANGE, UD_CDC_HEARTBEAT };
enum { UD_CDC_STREAM = 1, UD_CDC_SNAPSHOT_STREAM };
typedef struct ud_cdc_options {
    uint32_t struct_size, abi_version, batch_rows, server_id;
    int32_t mode;
    ud_string table, position, slot, publication, source_owner;
    const ud_batch *schema;
} ud_cdc_options;
typedef struct ud_cdc_event {
    uint32_t struct_size, abi_version;
    int32_t kind;
    uint64_t schema_version;
    int64_t watermark;
    ud_string position, transaction_id;
    ud_batch *after, *before;
    int32_t *operations;
} ud_cdc_event;
UD_API void ud_cdc_event_clear(ud_cdc_event *);
#ifdef __cplusplus
}
#endif
#endif
