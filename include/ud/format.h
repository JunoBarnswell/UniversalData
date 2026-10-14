#ifndef UD_FORMAT_H
#define UD_FORMAT_H
#include "error.h"
#ifdef __cplusplus
extern "C" {
#endif
/* Versioned schema JSON uses UD_CONFIG_VERSION. Each field has name,
 * type and nullable; Decimal also precision/scale, Timestamp timezone, and
 * nested types children. Optional namespace binds omitted column identities;
 * bound fields emit identity (32 lowercase hex) and canonical lineage array.
 * No unknown members or implicit type/temporal metadata defaults.
 * The returned empty batch owns all field metadata. */
UD_API ud_status ud_schema_from_json(ud_context *, const char *, uint32_t length,
                                     ud_string default_namespace, ud_batch **schema);
UD_API ud_status ud_schema_to_json(const ud_batch *, char **, uint32_t *length);
UD_API ud_status ud_batch_from_json_schema(ud_context *, const char *schema_json,
                                           uint32_t schema_length, const char *data_json,
                                           uint32_t data_length, int32_t rounding, ud_batch **);
UD_API ud_status ud_batch_cast_schema(ud_context *, const ud_batch *, const char *schema_json,
                                      uint32_t schema_length, int32_t rounding, ud_batch **);
/* Strict JSON array of named row objects. Unknown members are rejected; absent
 * nullable members become NULL. Nested Struct uses objects; List and Map use
 * arrays (Map entries are objects with the schema's key/value field names).
 * Decimal accepts exact JSON number or string and emits a decimal string.
 * Float64 nonfinite values use exactly "NaN", "Infinity", "-Infinity" strings;
 * finite floats use JSON numbers. Quoted finite floats are rejected.
 * Date/Time/Timestamp are explicit ISO strings; Binary is canonical base64.
 * No inferred schema, float Decimal conversion, or implicit local timezone. */
UD_API ud_status ud_batch_from_json(ud_context *, const ud_field *, uint32_t columns, const char *,
                                    uint32_t length, int32_t rounding, ud_batch **);
/* Owned compact JSON; length excludes final NUL. Caller uses ud_free. */
UD_API ud_status ud_batch_to_json(const ud_batch *, char **, uint32_t *length);
/* MySQL binary JSON -> lossless JSON text. Integer and opaque Decimal values
 * retain exact lexemes; calendar/time payloads retain their native meaning.
 * Other opaque values preserve type/bytes using MySQL base64 notation. Owned
 * output, native context budget/cancellation. Unknown JSON types are rejected. */
UD_API ud_status ud_mysql_json_decode(ud_context *, const uint8_t *, uint32_t, char **, uint32_t *);
/* NDJSON cursor with bounded rows / record bytes, immutable file offsets. */
typedef struct ud_json_reader ud_json_reader;
enum { UD_JSON_FRAMING_LINES = 0, UD_JSON_FRAMING_ARRAY = 1 };
typedef struct ud_json_options {
    uint32_t struct_size, version, batch_size, max_record_bytes;
    int32_t rounding;
    const ud_record_options *records;
    int32_t framing;
} ud_json_options;
UD_API ud_status ud_json_reader_open(ud_context *, const char *path, const ud_field *,
                                     uint32_t columns, const ud_json_options *, ud_json_reader **);
UD_API ud_status ud_json_reader_next(ud_json_reader *, ud_batch **accepted, ud_batch **errors);
UD_API ud_status ud_json_discover(ud_context *, const char *, ud_string name_space,
                                  const ud_json_options *, uint32_t sample_rows, ud_batch **schema);
UD_API uint64_t ud_json_reader_offset(const ud_json_reader *);
UD_API uint64_t ud_json_reader_record(const ud_json_reader *);
/* Requires offset/record from this unchanged input at a line boundary. */
UD_API ud_status ud_json_reader_resume(ud_json_reader *, uint64_t offset, uint64_t record);
UD_API void ud_json_reader_close(ud_json_reader *);
typedef struct ud_json_writer ud_json_writer;
/* Low-level NDJSON writer creates/truncates a host-selected file. Dataset atomic
 * overwrite publication is a separate Sink concern. Batches must retain schema.
 * Close flushes and syncs the file, and always releases the writer. */
UD_API ud_status ud_json_writer_open(ud_context *, const char *path, const ud_field *,
                                     uint32_t columns, uint32_t max_record_bytes,
                                     ud_json_writer **);
UD_API ud_status ud_json_writer_append(ud_json_writer *, const ud_batch *);
UD_API ud_status ud_json_writer_close(ud_json_writer *);
#ifdef __cplusplus
}
#endif
#endif
