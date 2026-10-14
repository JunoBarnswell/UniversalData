#ifndef UD_DATA_H
#define UD_DATA_H
#include "memory.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef int32_t ud_type;
enum {
    UD_BOOL = 1,
    UD_INT64 = 2,
    UD_FLOAT64 = 3,
    UD_UTF8 = 4,
    UD_DECIMAL128 = 5,
    UD_DATE32 = 6,
    UD_TIME64 = 7,
    UD_TIMESTAMP = 8,
    UD_BINARY = 9,
    UD_UINT64 = 10,
    UD_DURATION = 11,
    UD_INTERVAL = 12,
    UD_STRUCT = 13,
    UD_LIST = 14,
    UD_MAP = 15
};
/* Signed two's-complement coefficient, independent of compiler __int128 support. */
typedef struct ud_decimal128 {
    uint64_t low;
    int64_t high;
} ud_decimal128;
/* Calendar components are independent; never normalize months into days or
 * days into microseconds. Tuple order is structural, not anchored chronology. */
typedef struct ud_interval {
    int32_t months, days;
    int64_t microseconds;
} ud_interval;
enum { UD_DICTIONARY_NONE = 0, UD_DICTIONARY_ENUM = 1, UD_DICTIONARY_SET = 2 };
UD_API const char *ud_type_name(ud_type);
UD_API ud_status ud_type_parse(ud_string, ud_type *);
/* Canonical 128-bit identity. Column IDs use logical schema namespaces;
 * record/rule IDs use their own explicit deterministic namespaces. */
typedef struct ud_id {
    uint64_t high, low;
} ud_id;
enum {
    UD_ROUND_EXACT = 0,
    UD_ROUND_ZERO = 1,
    UD_ROUND_HALF_EVEN = 2,
    UD_ROUND_HALF_AWAY = 3,
    UD_ROUND_FLOOR = 4,
    UD_ROUND_CEILING = 5
};
typedef struct ud_field {
    const char *name;
    uint32_t name_length;
    ud_type type;
    int32_t nullable;
    int32_t precision, scale; /* Decimal: precision 1..38, scale -38..38. */
    const char *timezone;     /* Timestamp only: required explicit UTC / IANA metadata. */
    uint32_t timezone_length;
    const struct ud_field *children;
    uint32_t child_count;
    ud_id identity;
    const ud_id *lineage; /* Canonical sorted distinct origin identities. */
    uint32_t lineage_count;
    uint32_t key_order; /* 0: not a declared primary key; positive canonical order. */
    /* Ordered provider vocabulary on UINT64. ENUM preserves ordinal 0 (the
     * provider's invalid-value sentinel) and 1..count; SET preserves every bit,
     * including empty/numeric labels. Strings never stand in for these values. */
    int32_t dictionary_kind;
    const ud_string *dictionary;
    uint32_t dictionary_count;
} ud_field;
enum { UD_FIELD_VALUE_TYPE = 0, UD_FIELD_PORT = 1, UD_FIELD_LAYOUT = 2 };
/* VALUE_TYPE ignores root display name, identity, lineage and nullability;
 * UTC Timestamp annotations do not change its value domain; recursive child
 * names remain part of a value shape. PORT includes all bound
 * metadata. LAYOUT includes names/nullability/types, ignoring identities. */
UD_API int32_t ud_field_matches(const ud_field *, const ud_field *, int32_t mode);
UD_API int32_t ud_id_compare(ud_id, ud_id);
/* Deterministic SHA-256-derived 128-bit identity in a logical namespace. Both
 * arguments are UTF8 with explicit lengths, nonempty, without embedded NUL. */
UD_API ud_status ud_id_make(ud_context *, ud_string name_space, ud_string symbol, ud_id *);
UD_API ud_status ud_id_parse(ud_string canonical_hex, ud_id *);
UD_API void ud_id_format(ud_id, char output[33]);
typedef struct ud_value {
    ud_type type;
    int32_t is_null;
    union {
        int64_t integer;
        uint64_t unsigned_integer;
    };
    double real;
    const char *text;
    uint32_t text_length;
    ud_decimal128 decimal;
    int32_t precision, scale;
    const struct ud_value *children; /* Struct fields, or List/Map items. Borrowed. */
    uint32_t child_count;
    ud_interval interval;
    /* CDC field omission is distinct from SQL NULL. An absent value carries
     * is_null=1, cannot participate in expressions, and is preserved by Wire. */
    int32_t is_absent;
} ud_value;
/* Date32 is days since 1970-01-01; Time64 is microseconds since midnight.
 * Duration is signed microseconds (including negative/long SQL TIME).
 * Interval preserves signed months, days and microseconds independently.
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
enum { UD_NUMERIC_ADD = 0, UD_NUMERIC_SUBTRACT = 1, UD_NUMERIC_MULTIPLY = 2 };
/* Exact native numeric kernels shared by expressions, functions and aggregation.
 * No cross-type coercion. Decimal multiplication infers summed scale (must fit
 * -38..38) and precision capped at 38. Overflow is diagnosed, never wrapped. */
UD_API ud_status ud_numeric_result_field(int32_t operation, const ud_field *, const ud_field *,
                                         ud_field *);
UD_API ud_status ud_value_numeric(int32_t operation, const ud_value *, const ud_value *,
                                  const ud_field *result, ud_value *);
/* Total order for values whose bound schemas match (including Decimal scale).
 * NULL first; NaN after all numeric values, all NaNs equal, +/-0 equal. */
UD_API int32_t ud_value_compare(const ud_value *, const ud_value *);
/* Seeded canonical FNV-1a key hash for valid values under matching schemas.
 * Equal values have equal hashes, including all NaNs, +/-0 and NULL payloads.
 * Hash collisions require Compare. Not a security digest. Start with this seed;
 * compose ordered keys by passing each result as the seed for the next value. */
#define UD_VALUE_HASH_SEED UINT64_C(14695981039346656037)
UD_API uint64_t ud_value_hash(const ud_value *, uint64_t seed);
UD_API int32_t ud_truth_and(int32_t, int32_t);
UD_API int32_t ud_truth_or(int32_t, int32_t);
UD_API int32_t ud_truth_not(int32_t);
typedef struct ud_batch ud_batch;
enum {
    UD_SQL_CANONICAL = 0,
    UD_SQL_SQLITE = 1,
    UD_SQL_POSTGRESQL = 2,
    UD_SQL_MYSQL,
    UD_SQL_MARIADB,
    UD_SQL_SQLSERVER,
    UD_SQL_ORACLE,
    UD_SQL_DB2,
    UD_SQL_DUCKDB
};
/* Pure provider-specific type proof. Zone-free datetime declarations require an
 * explicit provider UTC annotation, never the machine's local timezone. */
UD_API ud_status ud_field_from_sql(ud_string declaration, int32_t dialect, ud_field *);
/* Owned canonical schema/diff results. Native policy JSON accepts strict,
 * compatible, explicitMapping, or evolutionMapping with complete expected/actual
 * ID/name pairs. evolutionMapping also proves lossless values and unchanged keys. */
UD_API ud_status ud_schema_fingerprint(const ud_batch *, uint8_t out[32]);
UD_API ud_status ud_schema_from_metadata(ud_context *, const ud_batch *, ud_string, int32_t,
                                         ud_batch **);
/* Attach proven existing identities to discovered fields with the same name.
 * Rename/drop/key/type compatibility is still decided by schema_compare/map. */
UD_API ud_status ud_schema_bind_identity(ud_context *, const ud_batch *bound,
                                         const ud_batch *discovered, ud_batch **);
UD_API ud_status ud_schema_compare(ud_context *, const ud_batch *, const ud_batch *, ud_string,
                                   char **, uint32_t *);
UD_API ud_status ud_schema_compare_json(ud_context *, ud_string, ud_string, ud_string, char **,
                                        uint32_t *);
UD_API ud_status ud_schema_map(ud_context *, const ud_batch *actual, const ud_batch *expected,
                               ud_string, ud_batch **);

enum { UD_ROW_ERROR = 1, UD_ROW_JOINED = 2 };
enum { UD_ERROR_FAIL = 0, UD_ERROR_REDIRECT = 1, UD_ERROR_IGNORE = 2 };
typedef struct ud_row_info {
    ud_id id, rule, origins;   /* Associative source-identity accumulator for Join. */
    uint32_t flags, operation; /* 0 ordinary row; UD_CHANGE_* for a Changeset. */
    uint64_t source_position;
    uint32_t has_source_position;
} ud_row_info;
/* Metadata is native, owned with the Batch and preserved by Wire/Spill. Zero ID
 * means unspecified direct-API provenance. Mutation requires an owned mutable batch. */
/* Assign a frozen logical input role without changing source row IDs. */
UD_API ud_status ud_batch_join_origin(ud_context *, const ud_batch *, ud_id, ud_batch **);
UD_API ud_status ud_row_info_join(ud_context *, const ud_row_info *, const ud_row_info *,
                                  ud_row_info *);
UD_API ud_status ud_row_info_source(ud_id source_scope, uint64_t physical_record, ud_row_info *);
UD_API ud_status ud_batch_row_info(const ud_batch *, uint32_t, ud_row_info *);
UD_API ud_status ud_batch_set_row_info(ud_batch *, uint32_t, const ud_row_info *);
UD_API ud_status ud_batch_append_projected(ud_batch *, const ud_batch *, uint32_t row,
                                           const uint32_t *columns);
UD_API ud_status ud_batch_append_from(ud_batch *, const ud_batch *, uint32_t);
/* Returns a sealed column-sharing view with new deterministic source identities. */
UD_API ud_status ud_batch_identify(ud_context *, const ud_batch *, ud_id scope, uint64_t first,
                                   ud_batch **);
UD_API ud_status ud_batch_row_info_json(const ud_batch *, uint32_t, char **, uint32_t *);
UD_API ud_status ud_batch_identify_keys(ud_context *, const ud_batch *, ud_id, const uint32_t *,
                                        uint32_t, ud_batch **);
UD_API ud_status ud_batch_fingerprint(const ud_batch *, uint8_t out[32]);
UD_API uint64_t ud_batch_error_rows(const ud_batch *);

UD_API ud_status ud_batch_create(ud_context *ctx, const ud_field *fields, uint32_t columns,
                                 uint32_t capacity, ud_batch **out);
/* A failed append does not add a partially initialized row. */
UD_API ud_status ud_batch_append(ud_batch *batch, const ud_value *values, uint32_t columns);
UD_API void ud_batch_destroy(ud_batch *batch);
/* Sealed batches are immutable. retain requires sealing; destroy releases one
 * owned reference. Mutable batches have exactly one owner. Seal before sharing;
 * concurrent sealing/mutation is forbidden. */
UD_API ud_status ud_batch_seal(ud_batch *);
UD_API ud_status ud_batch_retain(ud_batch *);
/* Immutable zero-copy projection/relabel. Input must be sealed in this context;
 * each target field has the same value shape and permits the input nullability.
 * Output retains input buffers and owns canonical target metadata. Duplicate
 * source indices are allowed; output names/identities must be distinct. */
UD_API ud_status ud_batch_project(ud_context *, const ud_batch *, const uint32_t *source_columns,
                                  const ud_field *target_fields, uint32_t columns, ud_batch **);
/* Owned immutable row slice shares buffers and preserves row provenance. */
UD_API ud_status ud_batch_slice(ud_context *, const ud_batch *, uint32_t, uint32_t, ud_batch **);
UD_API uint32_t ud_batch_rows(const ud_batch *batch);
UD_API uint32_t ud_batch_columns(const ud_batch *batch);
/* Canonical immutable field metadata borrows the retained schema tree and stays
 * valid until batch destruction, including across mutable appends. */
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
/* Fast all-present path for ordinary batches. Recursive absence is considered.
 * JSON/Arrow projections reject partial images instead of silently making NULL. */
UD_API int32_t ud_batch_has_absent(const ud_batch *);
UD_API ud_status ud_batch_cell_present(const ud_batch *, uint32_t, uint32_t, int32_t *);
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
