#ifndef UD_COLUMNAR_INTERNAL_H
#define UD_COLUMNAR_INTERNAL_H
#include "../foundation/internal.h"
typedef struct ud_column {
    ud_field field;
    uint8_t *validity;
    void *values;
    int32_t *offsets;
    char *strings;
    size_t string_bytes, string_capacity;
} ud_column;
struct ud_batch {
    ud_context *ctx;
    uint32_t columns, rows, capacity;
    ud_column *column;
};
ud_status ud_batch_clone(const ud_batch *, ud_batch **);
ud_status ud_encode_batch(ud_bytes *, const ud_batch *);
ud_status ud_decode_batch(ud_bytes *, ud_batch **);
#endif
