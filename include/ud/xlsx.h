#ifndef UD_XLSX_H
#define UD_XLSX_H
#include "buffer.h"
#include "json.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_xlsx_reader ud_xlsx_reader;
typedef struct ud_xlsx_writer ud_xlsx_writer;
typedef struct ud_xlsx_options {
    uint32_t struct_size, abi_version, batch_size;
    ud_string configuration; /* sheet, range, header, formulaPolicy and bounded limits. */
    ud_buffer_manager *buffers;
    ud_status (*check)(void *);
    void *user;
    uint64_t *stage_bytes;     /* Writer reserves existing Buffer spill quota until publication. */
    const char *original_path; /* Writer-only frozen workbook generation for preserving append. */
} ud_xlsx_options;
UD_API ud_status ud_xlsx_reader_open(ud_context *, const char *, const ud_field *, uint32_t,
                                     const ud_xlsx_options *, ud_xlsx_reader **);
/* Rejected is an identified one-row original-schema value set with cell location
 * in reader diagnostics. Caller routes it through the regular error output. */
UD_API ud_status ud_xlsx_reader_next(ud_xlsx_reader *, ud_batch **, int32_t *rejected);
UD_API ud_status ud_xlsx_reader_metadata(const ud_xlsx_reader *, char **, uint32_t *);
UD_API void ud_xlsx_reader_close(ud_xlsx_reader *);
UD_API ud_status ud_xlsx_writer_open(ud_context *, const char *, const ud_field *, uint32_t,
                                     const ud_xlsx_options *, ud_xlsx_writer **);
UD_API ud_status ud_xlsx_writer_append(ud_xlsx_writer *, const ud_batch *);
UD_API ud_status ud_xlsx_writer_close(ud_xlsx_writer *);
#ifdef __cplusplus
}
#endif
#endif
