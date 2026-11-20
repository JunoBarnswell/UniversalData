#ifndef UD_MEMORY_H
#define UD_MEMORY_H
#include "status.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_string {
    const char *data;
    uint32_t length;
} ud_string;
typedef void *(*ud_allocate_fn)(void *user, size_t bytes);
typedef void (*ud_deallocate_fn)(void *user, void *ptr);
typedef struct ud_context_options {
    uint32_t struct_size, abi_version;
    uint64_t memory_limit;
    void *allocator_user;
    ud_allocate_fn allocate;
    ud_deallocate_fn deallocate;
} ud_context_options;
UD_API ud_status ud_context_create(const ud_context_options *options, ud_context **out);
/* Returns BUSY if any context-owned allocation is still alive. */
UD_API ud_status ud_context_destroy(ud_context *ctx);
UD_API uint64_t ud_memory_used(const ud_context *ctx);
/* Quota/counter cover ud_alloc blocks including their allocation headers. The
 * fixed context handle itself uses the host allocator but is outside the quota. */
UD_API void *ud_alloc(ud_context *ctx, size_t bytes);
UD_API void ud_free(void *ptr);
UD_API void ud_cancel(ud_context *ctx);
UD_API void ud_reset_cancel(ud_context *ctx);
UD_API int32_t ud_is_cancelled(const ud_context *ctx);
/* Language callbacks report into the same calling-thread native diagnostic.
 * The host controls message content; raw data/credentials are not included implicitly. */
UD_API ud_status ud_context_report(ud_context *, ud_status, ud_string message);
UD_API ud_status ud_context_report_error(ud_context *, ud_status, ud_string message,
                                        const ud_error_detail *);
UD_API void ud_context_clear_error(ud_context *);
#ifdef __cplusplus
}
#endif
#endif
