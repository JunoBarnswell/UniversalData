#ifndef UD_DATA_H
#define UD_DATA_H
#include "memory.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef int32_t ud_type;
enum {
    UD_BOOL = 1, UD_INT64 = 2, UD_FLOAT64 = 3, UD_UTF8 = 4,
    UD_DECIMAL128 = 5, UD_DATE32 = 6, UD_TIME64 = 7, UD_TIMESTAMP = 8, UD_BINARY = 9,
    UD_STRUCT = 10, UD_LIST = 11, UD_MAP = 12
};
/* Signed two's-complement coefficient, independent of compiler __int128 support. */
typedef struct ud_decimal128 { uint64_t low; int64_t high; } ud_decimal128;
enum {
    UD_ROUND_EXACT = 0, UD_ROUND_ZERO = 1, UD_ROUND_HALF_EVEN = 2,
    UD_ROUND_HALF_AWAY = 3, UD_ROUND_FLOOR = 4, UD_ROUND_CEILING = 5
};
typedef struct ud_field {
    const char *name;
    uint32_t name_length;
    ud_type type;
    int32_t nullable;
    int32_t precision, scale; /* Decimal: precision 1..38, scale -38..38. */
    const char *timezone; /* Timestamp only: required explicit UTC / IANA metadata. */
    uint32_t timezone_length;
    const struct ud_field *children;
    uint32_t child_count;
} ud_field;
typedef struct ud_value {
    ud_type type;
    int32_t is_null;
    int64_t integer;
    double real;
    const char *text;
    uint32_t text_length;
    ud_decimal128 decimal;
    int32_t precision, scale;
    const struct ud_value *children; /* Struct fields, or List/Map items. Borrowed. */
    uint32_t child_count;
} ud_value;
/* Date32 is days since 1970-01-01; Time64 is microseconds since midnight.
 * Timestamp is UTC microseconds since epoch. ISO input must include Z or an offset.
 * Returned UTF8/Binary data borrows input; other output is self-contained. */
UD_API ud_status ud_value_parse_field(ud_context *, const ud_field *, const char *, uint32_t,
                                      int32_t rounding, ud_value *);
/* Caller buffer receives a NUL terminator. required includes the terminator. */
UD_API ud_status ud_value_format(const ud_value *, char *, uint32_t capacity, uint32_t *required);
UD_API ud_status ud_decimal_parse(const char *, uint32_t, int32_t precision, int32_t scale,
                                  int32_t rounding, ud_decimal128 *);
UD_API ud_status ud_decimal_rescale(ud_decimal128, int32_t from_scale, int32_t precision,
                                    int32_t to_scale, int32_t rounding, ud_decimal128 *);
UD_API ud_status ud_decimal_add(ud_decimal128, ud_decimal128, int32_t precision, ud_decimal128 *);
UD_API int32_t ud_decimal_compare(ud_decimal128, ud_decimal128);
typedef struct ud_batch ud_batch;
UD_API ud_status ud_batch_create(ud_context *ctx, const ud_field *fields, uint32_t columns,
                                 uint32_t capacity, ud_batch **out);
/* A failed append does not add a partially initialized row. */
UD_API ud_status ud_batch_append(ud_batch *batch, const ud_value *values, uint32_t columns);
UD_API void ud_batch_destroy(ud_batch *batch);
UD_API uint32_t ud_batch_rows(const ud_batch *batch);
UD_API uint32_t ud_batch_columns(const ud_batch *batch);
/* Field metadata is borrowed until the next append / batch destruction. */
UD_API ud_status ud_batch_field(const ud_batch *batch, uint32_t column, ud_field *out);
/* Canonical versioned wire data. Caller releases serialized data with ud_free.
 * Deserialization borrows input only for the duration of the call and rejects trailing bytes. */
UD_API ud_status ud_batch_serialize(const ud_batch *, uint8_t **data, uint64_t *length);
UD_API ud_status ud_batch_deserialize(ud_context *, const uint8_t *, uint64_t, ud_batch **);
/* Position-based conversion / rename. Recursive types retain shape. NULL follows
 * target nullability. Timestamp text requires an offset; implicit temporal casts
 * and Decimal-to-float conversions are rejected. Integer/float conversion honors
 * rounding; EXACT rejects a nonrepresentable integer / fractional float.
 * Output owns all its data. */
UD_API ud_status ud_batch_cast(ud_context *, const ud_batch *, const ud_field *target,
                               uint32_t columns, int32_t rounding, ud_batch **);
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
