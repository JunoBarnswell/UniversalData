#ifndef UD_INTERNAL_H
#define UD_INTERNAL_H
#include "ud/memory.h"
#include <stdio.h>
#include <stdbool.h>
#include <stdarg.h>
#include <string.h>
#include <stdlib.h>
#include <limits.h>
#define UD_TRY(call)                                                                               \
    do {                                                                                           \
        ud_status ud_s_ = (call);                                                                  \
        if (ud_s_ != UD_OK)                                                                        \
            return ud_s_;                                                                          \
    } while (0)
ud_status ud_fail(ud_context *, ud_status, const char *format, ...);
char *ud_strdup_n(ud_context *, const char *, size_t);
int ud_utf8_valid(const char *, size_t);
FILE *ud_fopen(const char *, const char *);
ud_status ud_mkdir(ud_context *, const char *);
ud_status ud_join_path(ud_context *, const char *, const char *, char **);
ud_status ud_list(ud_context *, const char *, const char *, char ***, uint32_t *);
void ud_list_free(char **, uint32_t);
ud_status ud_file_identity(ud_context *, const char *, uint64_t *device, uint64_t *file);
ud_status ud_file_hash(ud_context *, const char *, uint8_t out[32]);
typedef struct ud_sha256 {
    uint32_t h[8];
    uint64_t bytes;
    uint8_t block[64];
    size_t used;
} ud_sha256;
void ud_sha256_init(ud_sha256 *);
void ud_sha256_update(ud_sha256 *, const void *, size_t);
void ud_sha256_finish(ud_sha256 *, uint8_t out[32]);
void ud_hash(const void *, size_t, uint8_t out[32]);
size_t ud_allocation_bytes(const void *);
typedef struct ud_memory_reservation ud_memory_reservation;
/* Charge bounded buffers owned by third-party codecs without allocating a
 * second copy. The reservation follows the current component memory domain. */
ud_status ud_memory_reserve(ud_context *, uint64_t, ud_memory_reservation **);
void ud_memory_release(ud_memory_reservation *);
ud_status ud_flush(ud_context *, FILE *);
ud_status ud_publish(ud_context *, const char *from, const char *to);
ud_status ud_replace_file(ud_context *, const char *from, const char *to);
ud_status ud_sync_parent(ud_context *, const char *path);
ud_status ud_lock(ud_context *, const char *, void **);
void ud_unlock(void *);
ud_status ud_remove_file(ud_context *, const char *);
ud_status ud_reserve_file(ud_context *, const char *);
ud_status ud_path_kind(ud_context *, const char *, int *); /* 0 absent, 1 file, 2 dir, 3 other */
ud_status ud_path_no_symlinks(ud_context *, const char *);
ud_status ud_open_regular(ud_context *, const char *, FILE **);
ud_status ud_move_noreplace(ud_context *, const char *, const char *);
ud_status ud_distinct_directories(ud_context *, const char *, const char *);
uint64_t ud_now_ms(void);
uint64_t ud_wall_ms(void); /* UTC Unix milliseconds for persisted event timestamps. */
uint64_t ud_monotonic_ns(void);
uint64_t ud_thread_cpu_ns(void);
/* OS entropy only. Never synthesize persistent identity from clocks/counters. */
ud_status ud_random_bytes(ud_context *, void *, uint32_t);
void ud_sleep_ms(uint32_t);
int ud_seek(FILE *, uint64_t);
int64_t ud_tell(FILE *);
/* Internal wire codec. Canonical little endian, explicit lengths, no pointers. */
typedef struct ud_bytes {
    ud_context *ctx;
    uint8_t *data;
    size_t size, capacity, pos;
    size_t limit; /* Zero = host allocation quota only. */
} ud_bytes;
ud_status ud_bytes_put(ud_bytes *, const void *, size_t);
ud_status ud_bytes_get(ud_bytes *, void *, size_t);
ud_status ud_put_u64(ud_bytes *, uint64_t);
ud_status ud_get_u64(ud_bytes *, uint64_t *);
ud_status ud_put_string(ud_bytes *, const char *, size_t);
ud_status ud_get_string(ud_bytes *, char **);
void ud_bytes_clear(ud_bytes *);
ud_status ud_json_quote(ud_bytes *, const char *, uint32_t);
ud_status ud_base64_encode(ud_bytes *, const uint8_t *, uint32_t, uint32_t line_width);
ud_status ud_read_file(ud_context *, const char *, ud_bytes *);
ud_status ud_write_file(ud_context *, const char *, const void *, size_t);
ud_status ud_append_file(ud_context *, const char *, const void *, size_t);
#endif
