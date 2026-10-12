#include "ud/data.h"
#include <string.h>
#include <math.h>
int32_t ud_truth_and(int32_t a, int32_t b) {
    return !a || !b ? 0 : a < 0 || b < 0 ? -1 : 1;
}
int32_t ud_truth_or(int32_t a, int32_t b) {
    return a > 0 || b > 0 ? 1 : a < 0 || b < 0 ? -1 : 0;
}
int32_t ud_truth_not(int32_t a) {
    return a < 0 ? -1 : !a;
}
int32_t ud_value_compare(const ud_value *a, const ud_value *b) {
    if (a->is_null || b->is_null)
        return a->is_null && b->is_null ? 0 : a->is_null ? -1 : 1;
    if (a->type == UD_UTF8 || a->type == UD_BINARY) {
        uint32_t n = a->text_length < b->text_length ? a->text_length : b->text_length;
        int v = n ? memcmp(a->text, b->text, n) : 0;
        return v ? (v > 0 ? 1 : -1)
                 : (a->text_length > b->text_length) - (a->text_length < b->text_length);
    }
    if (a->type == UD_FLOAT64) {
        if (isnan(a->real) || isnan(b->real))
            return isnan(a->real) ? (isnan(b->real) ? 0 : 1) : -1;
        return (a->real > b->real) - (a->real < b->real);
    }
    if (a->type == UD_DECIMAL128)
        return ud_decimal_compare(a->decimal, b->decimal);
    if (a->type >= UD_STRUCT) {
        uint32_t count = a->child_count < b->child_count ? a->child_count : b->child_count;
        for (uint32_t j = 0; j < count; j++) {
            int cmp = ud_value_compare(a->children + j, b->children + j);
            if (cmp)
                return cmp;
        }
        return (a->child_count > b->child_count) - (a->child_count < b->child_count);
    }
    return (a->integer > b->integer) - (a->integer < b->integer);
}
static uint64_t hash_byte(uint64_t hash, uint8_t byte) {
    return (hash ^ byte) * UINT64_C(1099511628211);
}
static uint64_t hash_integer(uint64_t hash, uint64_t value) {
    for (uint32_t j = 0; j < 8; j++)
        hash = hash_byte(hash, (uint8_t)(value >> (8 * j)));
    return hash;
}
uint64_t ud_value_hash(const ud_value *v, uint64_t hash) {
    hash = hash_integer(hash, (uint64_t)v->type);
    hash = hash_byte(hash, (uint8_t)!!v->is_null);
    if (v->is_null)
        return hash;
    if (v->type == UD_UTF8 || v->type == UD_BINARY) {
        hash = hash_integer(hash, v->text_length);
        for (uint32_t j = 0; j < v->text_length; j++)
            hash = hash_byte(hash, (uint8_t)v->text[j]);
    } else if (v->type >= UD_STRUCT) {
        hash = hash_integer(hash, v->child_count);
        for (uint32_t j = 0; j < v->child_count; j++)
            hash = ud_value_hash(v->children + j, hash);
    } else if (v->type == UD_DECIMAL128) {
        hash = hash_integer(hash, v->decimal.low);
        hash = hash_integer(hash, (uint64_t)v->decimal.high);
    } else if (v->type == UD_FLOAT64) {
        uint64_t bits = 0;
        if (isnan(v->real))
            bits = UINT64_C(0x7ff8000000000000);
        else if (v->real != 0)
            memcpy(&bits, &v->real, sizeof(bits));
        hash = hash_integer(hash, bits);
    } else
        hash = hash_integer(hash, (uint64_t)v->integer);
    return hash;
}