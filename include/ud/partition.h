#ifndef UD_PARTITION_H
#define UD_PARTITION_H
#include "data.h"
#ifdef __cplusplus
extern "C" {
#endif
enum { UD_PARTITION_HASH=1, UD_PARTITION_RANGE, UD_PARTITION_ROUND_ROBIN, UD_PARTITION_BROADCAST };
enum { UD_ORDER_UNORDERED=1, UD_ORDER_PARTITION, UD_ORDER_GLOBAL };
typedef struct ud_partition_options {
    uint32_t struct_size, abi_version;
    int32_t strategy;
    uint32_t count;
    const uint32_t *keys;
    uint32_t key_count;
    const ud_value *boundaries; /* RANGE: count-1 ascending canonical values, one key. */
    uint64_t row_offset; /* Stable source position for round robin. */
} ud_partition_options;
/* Returned array and every sealed output are owned. HASH uses the canonical
 * field/value/NULL/hash model. RANGE equality goes to the following partition. */
UD_API ud_status ud_batch_partition(ud_context *,const ud_batch *,const ud_partition_options *,ud_batch ***);
#ifdef __cplusplus
}
#endif
#endif
