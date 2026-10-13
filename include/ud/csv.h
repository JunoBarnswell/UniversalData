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
    const char *null_marker;
    uint32_t null_marker_length;
    uint32_t max_record_bytes;
    const ud_record_options *records;
} ud_csv_options;
UD_API ud_status ud_csv_open(ud_context *, const char *path, const ud_field *, uint32_t columns,
                             const ud_csv_options *, ud_csv_reader **);
UD_API ud_status ud_csv_next(ud_csv_reader *, ud_batch **accepted, ud_batch **errors);
UD_API uint64_t ud_csv_offset(const ud_csv_reader *);
UD_API uint64_t ud_csv_record(const ud_csv_reader *);
/* Offset and record must originate from this immutable file at a record boundary. */
UD_API ud_status ud_csv_resume(ud_csv_reader *, uint64_t offset, uint64_t record);
UD_API void ud_csv_close(ud_csv_reader *);
UD_API ud_status ud_csv_write(ud_context *, const char *path, const ud_batch *,
                              const char *null_marker);
UD_API ud_status ud_csv_writer_open(ud_context *, const char *path, const ud_field *,
                                    uint32_t columns, const char *null_marker, ud_csv_writer **);
UD_API ud_status ud_csv_writer_append(ud_csv_writer *, const ud_batch *);
/* Close always destroys the writer, even if flushing fails. */
UD_API ud_status ud_csv_writer_close(ud_csv_writer *);
#ifdef __cplusplus
}
#endif
#endif
