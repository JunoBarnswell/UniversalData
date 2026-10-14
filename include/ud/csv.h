#ifndef UD_CSV_H
#define UD_CSV_H
#include "error.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_csv_reader ud_csv_reader;
typedef struct ud_csv_writer ud_csv_writer;
typedef struct ud_csv_options {
    uint32_t struct_size, abi_version, batch_size;
    uint32_t max_record_bytes;
    const ud_record_options *records;
    ud_string format; /* One native enterprise-file configuration for discovery/read/write. */
} ud_csv_options;
/* Widths count Unicode scalar values, not encoded bytes. positions maps each
 * physical field to its logical Schema index. Empty widths selects delimited.
 * UTF-16 encoding is explicit; BOM must agree. Raw errors retain original bytes. */
#define UD_CSV_FORMAT_SCHEMA                                                                       \
    "{\"type\":\"object\",\"properties\":{\"encoding\":{\"type\":\"string\",\"enum\":[\"UTF-8\","  \
    "\"UTF-16LE\",\"UTF-16BE\",\"ISO-8859-1\"],\"default\":\"UTF-8\"},\"delimiter\":{\"type\":"    \
    "\"string\",\"default\":\",\"},\"qualifier\":{\"type\":\"string\",\"default\":\"\\\"\"},"      \
    "\"header\":{\"type\":\"boolean\",\"default\":true},\"skipLines\":{\"type\":\"integer\","      \
    "\"minimum\":0,\"maximum\":1000000,\"default\":0},\"widths\":{\"type\":\"array\",\"items\":{"  \
    "\"type\":\"integer\",\"minimum\":1,\"maximum\":1048576}},\"positions\":{\"type\":\"array\","  \
    "\"items\":{\"type\":\"integer\",\"minimum\":0,\"maximum\":65535}},\"nullMarker\":{\"type\":"  \
    "\"string\",\"default\":\"\\\\N\"},\"trimFixedPadding\":{\"type\":\"boolean\",\"default\":"    \
    "true},\"rowDelimiter\":{\"type\":\"string\",\"enum\":[\"LF\",\"CRLF\"],\"default\":\"LF\"},"  \
    "\"writeBom\":{\"type\":\"boolean\"},\"preamble\":{\"type\":\"array\",\"items\":{\"type\":"    \
    "\"string\"}}},\"additionalProperties\":false}"
UD_API ud_status ud_csv_open(ud_context *, const char *path, const ud_field *, uint32_t columns,
                             const ud_csv_options *, ud_csv_reader **);
UD_API ud_status ud_csv_next(ud_csv_reader *, ud_batch **accepted, ud_batch **errors);
UD_API ud_status ud_csv_discover(ud_context *, const char *, ud_string name_space,
                                 const ud_csv_options *, uint32_t sample_rows, ud_batch **schema);
UD_API uint64_t ud_csv_offset(const ud_csv_reader *);
UD_API uint64_t ud_csv_record(const ud_csv_reader *);
/* Offset and record must originate from this immutable file at a record boundary. */
UD_API ud_status ud_csv_resume(ud_csv_reader *, uint64_t offset, uint64_t record);
UD_API void ud_csv_close(ud_csv_reader *);
UD_API ud_status ud_csv_write(ud_context *, const char *path, const ud_batch *,
                              const ud_csv_options *);
UD_API ud_status ud_csv_writer_open(ud_context *, const char *path, const ud_field *,
                                    uint32_t columns, const ud_csv_options *, ud_csv_writer **);
UD_API ud_status ud_csv_writer_append(ud_csv_writer *, const ud_batch *);
/* Close always destroys the writer, even if flushing fails. */
UD_API ud_status ud_csv_writer_close(ud_csv_writer *);
#ifdef __cplusplus
}
#endif
#endif
