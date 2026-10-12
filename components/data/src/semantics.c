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
        return a->is_null == b->is_null ? 0 : a->is_null ? -1 : 1;
    if (a->type == UD_UTF8 || a->type == UD_BINARY) {
        uint32_t n = a->text_length < b->text_length ? a->text_length : b->text_length;
        int v = n ? memcmp(a->text, b->text, n) : 0;
        return v ? (v > 0 ? 1 : -1)
                 : (a->text_length > b->text_length) - (a->text_length < b->text_length);
    }
    if (a->type == UD_FLOAT64) {
        if (isnan(a->real) || isnan(b->real)) return isnan(a->real) ? (isnan(b->real) ? 0 : 1) : -1;
        return (a->real > b->real) - (a->real < b->real);
    }
    if (a->type == UD_DECIMAL128)
        return ud_decimal_compare(a->decimal, b->decimal);
    if (a->type >= UD_STRUCT) {
        uint32_t count = a->child_count < b->child_count ? a->child_count : b->child_count;
        for (uint32_t j = 0; j < count; j++) {
            int cmp = ud_value_compare(a->children + j, b->children + j);
            if (cmp) return cmp;
        }
        return (a->child_count > b->child_count) - (a->child_count < b->child_count);
    }
    return (a->integer > b->integer) - (a->integer < b->integer);
}
