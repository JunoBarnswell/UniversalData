#ifndef UD_STATUS_H
#define UD_STATUS_H
#include <stddef.h>
#include <stdint.h>
#include "version.h"
#if defined(_WIN32) && !defined(UD_STATIC)
#ifdef UD_BUILD_SHARED
#define UD_API __declspec(dllexport)
#else
#define UD_API __declspec(dllimport)
#endif
#elif defined(__GNUC__) && !defined(UD_STATIC)
#define UD_API __attribute__((visibility("default")))
#else
#define UD_API
#endif
#ifdef __cplusplus
extern "C" {
#endif
typedef int32_t ud_status;
enum {
    UD_OK = 0,
    UD_END = 1,
    UD_INVALID = 2,
    UD_IO = 3,
    UD_RESOURCE = 4,
    UD_SCHEMA = 5,
    UD_PARSE = 6,
    UD_OVERFLOW = 7,
    UD_CANCELLED = 8,
    UD_CORRUPT = 9,
    UD_INCOMPATIBLE = 10,
    UD_BUSY = 11,
    UD_UNSUPPORTED = 12,
    UD_AUTH = 13,
    UD_QUALITY = 14,
    UD_TRANSIENT = 15,
    UD_COMMIT_UNKNOWN = 16,
    UD_FENCED = 17,
    UD_TIMEOUT = 18,
    UD_MORE = 19, /* Component has bounded pending output; scheduler polls it. */
    UD_TRUNCATED = 20,
    UD_DRIVER_UNAVAILABLE = 21,
    UD_WORKER_LOST = 22,
    UD_PROTOCOL = 23,
    UD_MANAGED_COMPONENT = 24,
    UD_HTTP_STATUS = 25
};
typedef struct ud_context ud_context;
UD_API const char *ud_version(void);
UD_API const char *ud_status_name(ud_status status);
/* Calling-thread diagnostic; borrowed until its next failing operation. */
UD_API const char *ud_error(const ud_context *ctx);
#define UD_STATUS_LAST UD_HTTP_STATUS
enum {
    UD_ERROR_NATIVE = 0,
    UD_ERROR_DATABASE = 1,
    UD_ERROR_HTTP = 2,
    UD_ERROR_TRANSPORT = 3,
    UD_ERROR_WORKER = 4,
    UD_ERROR_COMPONENT = 5
};
typedef struct ud_error_detail {
    uint32_t struct_size, abi_version;
    int32_t source;
    ud_status cause;
    int64_t native_code;
    char sqlstate[6];
} ud_error_detail;
/* Copies the calling thread's diagnostic details. No credentials or input values. */
UD_API ud_status ud_error_details(const ud_context *, ud_error_detail *);
#ifdef __cplusplus
}
#endif
#endif
