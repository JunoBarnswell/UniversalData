#ifndef UD_COLUMNAR_FILE_H
#define UD_COLUMNAR_FILE_H
#include "data.h"
#include "buffer.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_columnar_reader ud_columnar_reader;
typedef struct ud_columnar_writer ud_columnar_writer;
enum {UD_FILE_PARQUET=1,UD_FILE_ARROW_IPC,UD_FILE_ARROW_STREAM};
typedef struct ud_columnar_file_options {uint32_t struct_size,abi_version,batch_rows;int32_t format;ud_buffer_manager*buffers;ud_status(*check)(void*);void*user;uint64_t*stage_bytes;} ud_columnar_file_options;
/* Native format decoder/encoder. Resource authorization and atomic publication
 * remain with the Package file lease; typed values use Arrow C / Columnar. */
UD_API ud_status ud_columnar_reader_open(ud_context*,const char*,const ud_batch*,const ud_columnar_file_options*,ud_columnar_reader**);
UD_API ud_status ud_columnar_reader_next(ud_columnar_reader*,ud_batch**);
UD_API void ud_columnar_reader_close(ud_columnar_reader*);
UD_API ud_status ud_columnar_writer_open(ud_context*,const char*,const ud_batch*,const ud_columnar_file_options*,ud_columnar_writer**);
UD_API ud_status ud_columnar_writer_append(ud_columnar_writer*,const ud_batch*);
UD_API ud_status ud_columnar_writer_close(ud_columnar_writer*);
#ifdef __cplusplus
}
#endif
#endif
