#ifndef UD_DATA_H
#define UD_DATA_H
#include "memory.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef int32_t ud_type;
enum { UD_BOOL = 1, UD_INT64 = 2, UD_FLOAT64 = 3, UD_UTF8 = 4 };
typedef struct ud_field {
    const char *name;
    uint32_t name_length;
    ud_type type;
    int32_t nullable;
} ud_field;
typedef struct ud_value {
    ud_type type;
    int32_t is_null;
    int64_t integer;
    double real;
    const char *text;
    uint32_t text_length;
} ud_value;
typedef struct ud_batch ud_batch;
UD_API ud_status ud_batch_create(ud_context *ctx, const ud_field *fields, uint32_t columns,
                                 uint32_t capacity, ud_batch **out);
/* A failed append does not add a partially initialized row. */
UD_API ud_status ud_batch_append(ud_batch *batch, const ud_value *values, uint32_t columns);
UD_API void ud_batch_destroy(ud_batch *batch);
UD_API uint32_t ud_batch_rows(const ud_batch *batch);
UD_API uint32_t ud_batch_columns(const ud_batch *batch);
UD_API ud_status ud_batch_field(const ud_batch *batch, uint32_t column, ud_field *out);
/* Returned strings are borrowed until next append/destruction, NOT NUL terminated. */
UD_API ud_status ud_batch_get(const ud_batch *batch, uint32_t row, uint32_t column, ud_value *out);
UD_API ud_status ud_value_parse(ud_context *ctx, ud_type type, const char *text, uint32_t length,
                                ud_value *out);
/* Arrow definitions retain the canonical guard and Apache-2.0 notice; see NOTICE. */
#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE
struct ArrowSchema {
    const char *format, *name, *metadata;
    int64_t flags, n_children;
    struct ArrowSchema **children, *dictionary;
    void (*release)(struct ArrowSchema *);
    void *private_data;
};
struct ArrowArray {
    int64_t length, null_count, offset, n_buffers, n_children;
    const void **buffers;
    struct ArrowArray **children, *dictionary;
    void (*release)(struct ArrowArray *);
    void *private_data;
};
#endif
#ifndef ARROW_C_STREAM_INTERFACE
#define ARROW_C_STREAM_INTERFACE
struct ArrowArrayStream {
    int (*get_schema)(struct ArrowArrayStream *, struct ArrowSchema *);
    int (*get_next)(struct ArrowArrayStream *, struct ArrowArray *);
    const char *(*get_last_error)(struct ArrowArrayStream *);
    void (*release)(struct ArrowArrayStream *);
    void *private_data;
};
#endif
/* Independent exports: consumer must release both; source may then be destroyed. */
UD_API ud_status ud_batch_export_arrow(const ud_batch *, struct ArrowArray *, struct ArrowSchema *);
UD_API ud_status ud_batch_export_stream(const ud_batch *, struct ArrowArrayStream *);
UD_API ud_status ud_batch_import_arrow(ud_context *, const struct ArrowArray *,
                                       const struct ArrowSchema *, ud_batch **);
#ifdef __cplusplus
}
#endif
#endif
