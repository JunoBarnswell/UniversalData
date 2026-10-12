#ifndef UD_COLUMNAR_INTERNAL_H
#define UD_COLUMNAR_INTERNAL_H
#include "ud/data.h"
#include "../foundation/internal.h"
#include <stdatomic.h>
typedef struct ud_column {
    ud_field field;
    uint8_t *validity;
    void *values;
    int32_t *offsets;
    char *strings;
    size_t string_bytes, string_capacity;
    ud_batch *nested;
    ud_value **views;
} ud_column;
struct ud_batch {
    atomic_uint references;
    int sealed;
    ud_context *ctx;
    uint32_t columns, rows, capacity;
    ud_column *column;
};
ud_status ud_batch_clone(const ud_batch *, ud_batch **);
ud_status ud_encode_batch(ud_bytes *, const ud_batch *);
ud_status ud_decode_batch(ud_bytes *, ud_batch **);
ud_status ud_copy_value(ud_context *, const ud_value *, ud_value *, uint32_t depth);
void ud_clear_value(ud_value *);
int ud_field_equal(const ud_field *, const ud_field *, int compare_name);
ud_status ud_cast_value(ud_context *, const ud_field *, const ud_value *, const ud_field *,
                         int32_t rounding, ud_value *, uint32_t depth);
#endif
