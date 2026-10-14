#ifndef UD_COMPUTE_STATE_H
#define UD_COMPUTE_STATE_H
#include "../internal.h"
#include "ud/buffer.h"
typedef struct ud_state_page {
    ud_buffer_block *block;
    uint64_t first;
    uint32_t rows;
} ud_state_page;
typedef struct ud_state {
    ud_context *ctx;
    ud_buffer_manager *buffers;
    ud_batch *schema, *chunk;
    ud_state_page *pages;
    uint32_t count, capacity, batch_size;
    uint64_t rows, page_limit, chunk_bytes;
    int sealed;
} ud_state;
typedef struct ud_state_reader {
    const ud_state *state;
    ud_buffer_view *view;
    uint32_t page;
} ud_state_reader;
ud_status ud_state_create(ud_context *, ud_buffer_manager *, const ud_batch *, uint32_t,
                          ud_state **);
ud_status ud_state_append(ud_state *, const ud_batch *, uint32_t);
ud_status ud_state_add_batch(ud_state *, const ud_batch *);
ud_status ud_state_seal(ud_state *);
ud_status ud_state_row(ud_state_reader *, const ud_state *, uint64_t, const ud_batch **,
                       uint32_t *);
void ud_state_reader_clear(ud_state_reader *);
void ud_state_destroy(ud_state *);
/* Canonical exact signed 256-bit SUM is shared by aggregate and window. */
typedef struct ud_sum_state {
    uint64_t words[4], calendar[2][4], count;
    double real;
    int32_t scale;
} ud_sum_state;
ud_status ud_sum_wide_add(uint64_t[4], const uint64_t[4]);
void ud_sum_wide_value(const ud_value *, uint64_t[4]);
ud_status ud_sum_wide_result(const ud_field *, const uint64_t[4], ud_value *);
ud_status ud_sum_update(ud_sum_state *, const ud_value *, int32_t direction);
ud_status ud_sum_result(const ud_sum_state *, const ud_field *, int average, int32_t rounding,
                        ud_value *);
#endif
