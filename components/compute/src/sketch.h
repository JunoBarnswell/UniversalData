#ifndef UD_COMPUTE_SKETCH_H
#define UD_COMPUTE_SKETCH_H
#include "../internal.h"
#define UD_HLL_REGISTERS 1024
/* Canonical hashing precedes a shared deterministic avalanche. HLL state is
 * mergeable and bounded: 1024 registers, relative standard error 1.04/sqrt(1024). */
void ud_hll_add(uint8_t[UD_HLL_REGISTERS], const ud_value *);
void ud_hll_merge(uint8_t[UD_HLL_REGISTERS], const uint8_t[UD_HLL_REGISTERS]);
uint64_t ud_hll_count(const uint8_t[UD_HLL_REGISTERS]);
typedef struct ud_set_entry {
    uint64_t hash;
    ud_value value;
    uint32_t next;
} ud_set_entry;
typedef struct ud_value_set {
    ud_context *ctx;
    uint32_t count, capacity, maximum, bucket_count;
    uint64_t bytes;
    uint32_t *buckets;
    ud_set_entry *entries;
} ud_value_set;
ud_status ud_value_set_add(ud_value_set *, const ud_value *, int *added);
void ud_value_set_clear(ud_value_set *);
#endif
