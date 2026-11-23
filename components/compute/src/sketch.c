#include "sketch.h"
#include <math.h>
static uint64_t avalanche(uint64_t x) {
    x ^= x >> 30;
    x *= UINT64_C(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x *= UINT64_C(0x94d049bb133111eb);
    return x ^ (x >> 31);
}
static uint64_t value_bytes(const ud_value *value) {
    uint64_t bytes = sizeof(*value);
    if (value->is_absent || value->is_null)
        return bytes;
    if (value->type == UD_UTF8 || value->type == UD_BINARY)
        bytes += value->text_length;
    for (uint32_t i = 0; i < value->child_count; i++) {
        uint64_t child = value_bytes(value->children + i);
        if (child > UINT64_MAX - bytes)
            return UINT64_MAX;
        bytes += child;
    }
    return bytes;
}
void ud_hll_add(uint8_t registers[UD_HLL_REGISTERS], const ud_value *v) {
    if (v->is_null)
        return;
    uint64_t h = avalanche(ud_value_hash(v, UD_VALUE_HASH_SEED)), rest = h >> 10;
    uint32_t index = (uint32_t)(h & (UD_HLL_REGISTERS - 1));
    uint8_t rank = 1;
    while (rank < 55 && !(rest & 1)) {
        rank++;
        rest >>= 1;
    }
    if (rank > registers[index])
        registers[index] = rank;
}
void ud_hll_merge(uint8_t dst[UD_HLL_REGISTERS], const uint8_t src[UD_HLL_REGISTERS]) {
    for (uint32_t i = 0; i < UD_HLL_REGISTERS; i++)
        if (src[i] > dst[i])
            dst[i] = src[i];
}
uint64_t ud_hll_count(const uint8_t r[UD_HLL_REGISTERS]) {
    double sum = 0;
    uint32_t zeros = 0;
    for (uint32_t i = 0; i < UD_HLL_REGISTERS; i++) {
        sum += ldexp(1.0, -(int)r[i]);
        zeros += !r[i];
    }
    double n =
        (0.7213 / (1.0 + 1.079 / UD_HLL_REGISTERS)) * UD_HLL_REGISTERS * UD_HLL_REGISTERS / sum;
    if (n <= 2.5 * UD_HLL_REGISTERS && zeros)
        n = UD_HLL_REGISTERS * log((double)UD_HLL_REGISTERS / zeros);
    return n >= (double)INT64_MAX ? INT64_MAX : (uint64_t)llround(n);
}
ud_status ud_value_set_add(ud_value_set *s, const ud_value *value, int *added) {
    *added = 0;
    uint64_t bytes = value_bytes(value);
    if (bytes == UINT64_MAX || bytes > UINT64_MAX - s->bytes)
        return UD_OVERFLOW;
    uint64_t hash = ud_value_hash(value, UD_VALUE_HASH_SEED);
    if (!s->maximum)
        s->maximum = 65536;
    if (!s->bucket_count) {
        s->bucket_count = 1024;
        s->buckets = ud_alloc(s->ctx, s->bucket_count * sizeof(*s->buckets));
        if (!s->buckets)
            return UD_RESOURCE;
        memset(s->buckets, 255, s->bucket_count * sizeof(*s->buckets));
    }
    uint32_t slot = (uint32_t)(hash & (s->bucket_count - 1));
    for (uint32_t i = s->buckets[slot]; i != UINT32_MAX; i = s->entries[i].next)
        if (s->entries[i].hash == hash && !ud_value_compare(&s->entries[i].value, value))
            return UD_OK;
    if (s->count == s->maximum)
        return UD_RESOURCE;
    if (s->count == s->capacity) {
        uint32_t cap = s->capacity ? s->capacity * 2 : 64;
        if (cap > s->maximum)
            cap = s->maximum;
        ud_set_entry *entries = ud_alloc(s->ctx, (size_t)cap * sizeof(*entries));
        if (!entries)
            return UD_RESOURCE;
        if (s->count)
            memcpy(entries, s->entries, (size_t)s->count * sizeof(*entries));
        ud_free(s->entries);
        s->entries = entries;
        s->capacity = cap;
    }
    ud_set_entry *e = &s->entries[s->count];
    UD_TRY(ud_copy_value(s->ctx, value, &e->value, 0));
    e->hash = hash;
    e->next = s->buckets[slot];
    s->buckets[slot] = s->count++;
    s->bytes += bytes;
    *added = 1;
    return UD_OK;
}
void ud_value_set_clear(ud_value_set *s) {
    for (uint32_t i = 0; i < s->count; i++)
        ud_clear_value(&s->entries[i].value);
    ud_free(s->entries);
    ud_free(s->buckets);
    memset(s, 0, sizeof(*s));
}
