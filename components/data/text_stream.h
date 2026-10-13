#ifndef UD_TEXT_STREAM_INTERNAL_H
#define UD_TEXT_STREAM_INTERNAL_H
#include "internal.h"
enum { UD_TEXT_UTF8 = 1, UD_TEXT_UTF16LE, UD_TEXT_UTF16BE, UD_TEXT_LATIN1 };
typedef struct ud_text_input {
    ud_context *context;
    FILE *file;
    int encoding, pushed;
    uint32_t pushed_scalar;
    uint8_t raw[4], pushed_raw[4];
    uint32_t raw_size, pushed_size;
} ud_text_input;
ud_status ud_text_encoding(ud_string, int *);
ud_status ud_text_start(ud_text_input *, ud_context *, FILE *, int);
ud_status ud_text_next(ud_text_input *, uint32_t *);
void ud_text_unread(ud_text_input *, uint32_t);
uint64_t ud_text_offset(const ud_text_input *);
ud_status ud_text_utf8(ud_bytes *, uint32_t);
ud_status ud_text_write(FILE *, int, const char *, size_t);
ud_status ud_text_bom(FILE *, int);
#endif
