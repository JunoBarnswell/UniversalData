#ifndef UD_CURSOR_INTERNAL_H
#define UD_CURSOR_INTERNAL_H
#include "internal.h"
#include "ud/cursor.h"
struct ud_cursor {
    ud_context *ctx;
    ud_batch *schema, *after;
    uint32_t *keys, count;
    ud_value *last;
    uint64_t read, confirmed, overlap;
    int have_last, mode, commit_unknown;
    uint8_t fingerprint[32];
};
ud_status ud_cursor_validate(ud_cursor *, const ud_batch *);
void ud_cursor_reset(ud_cursor *);
#endif
