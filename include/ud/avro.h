#ifndef UD_AVRO_H
#define UD_AVRO_H
#include "format.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_avro_reader ud_avro_reader;
typedef struct ud_avro_writer ud_avro_writer;
enum { UD_AVRO_NULL = 0, UD_AVRO_DEFLATE = 1 };
typedef struct ud_avro_options {
    uint32_t struct_size, abi_version, batch_rows, max_block_bytes;
    int32_t codec;
    const ud_record_options *records;
    ud_status (*check)(void *);
    void *user;
} ud_avro_options;
/* Actual Avro Object Container Files. The Native Data Schema is preserved in
 * the ud.schema header; signed calendar extensions never narrow to standard
 * unsigned millisecond duration. The file lease/publication belongs to Package. */
/* Explicit standard Reader/Writer schemas. Named references resolve once;
 * multi-member unions use a Native Struct {branch,variant0,...}, with exactly
 * one active branch. Recursive schemas without a finite Native shape reject. */
UD_API ud_status ud_avro_reader_open_schema(ud_context *, const char *, const ud_batch *, ud_string reader_schema,
                                            const ud_avro_options *, ud_avro_reader **);
UD_API ud_status ud_avro_writer_open_schema(ud_context *, const char *, const ud_batch *, ud_string writer_schema,
                                            const ud_avro_options *, ud_avro_writer **);
UD_API ud_status ud_avro_reader_open(ud_context *, const char *, const ud_batch *,
                                     const ud_avro_options *, ud_avro_reader **);
UD_API ud_status ud_avro_reader_next(ud_avro_reader *, ud_batch **, ud_batch **);
UD_API void ud_avro_reader_close(ud_avro_reader *);
UD_API ud_status ud_avro_discover(ud_context *, const char *, ud_string, ud_batch **);
UD_API ud_status ud_avro_writer_open(ud_context *, const char *, const ud_batch *,
                                     const ud_avro_options *, ud_avro_writer **);
UD_API ud_status ud_avro_writer_append(ud_avro_writer *, const ud_batch *);
UD_API ud_status ud_avro_writer_close(ud_avro_writer *);
#ifdef __cplusplus
}
#endif
#endif
