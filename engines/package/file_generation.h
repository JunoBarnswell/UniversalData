#ifndef UD_FILE_GENERATION_INTERNAL_H
#define UD_FILE_GENERATION_INTERNAL_H
#include "internal.h"
#include "ud/connection_manager.h"
enum { UD_FILE_CREATE = 0, UD_FILE_OVERWRITE = 1, UD_FILE_APPEND = 2 };
typedef struct ud_file_generation {
    ud_activation *activation;
    ud_connection_lease *lease;
    char *target, *temporary, *original;
    char identity[65];
    void *lock;
    int mode, base_exists, prepared, published, owns_stage;
    uint8_t base_hash[32], output_hash[32];
} ud_file_generation;
ud_status ud_file_generation_open(ud_activation *, ud_connection_lease *, ud_string component,
                                  const char *target, int mode, ud_file_generation **);
ud_status ud_file_generation_check_original(ud_file_generation *);
ud_status ud_file_generation_publish(ud_file_generation *);
void ud_file_generation_destroy(ud_file_generation *);
#endif
