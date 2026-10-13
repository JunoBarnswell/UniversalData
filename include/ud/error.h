#ifndef UD_ERROR_H
#define UD_ERROR_H
#include "data.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_error_context {
    uint32_t struct_size, abi_version;
    ud_context *memory;
    ud_string execution_id, package_id, task_id, pipeline_id, component_id;
    ud_id field_scope;
} ud_error_context;
enum {
    UD_RECORD_CONVERSION = 1,
    UD_RECORD_TRUNCATION = 2,
    UD_RECORD_CONSTRAINT = 3,
    UD_RECORD_EXECUTION = 4
};
typedef struct ud_record_error {
    uint32_t struct_size, abi_version;
    const ud_batch *input; /* Typed input, or NULL for a bounded raw source record. */
    uint32_t row;
    ud_row_info source; /* Required when input is NULL. */
    ud_string rule;
    uint32_t rule_version;
    ud_id column;
    ud_status code;
    int32_t kind, policy; /* kind=0 selects the native status classification. */
    ud_string provider_code, input_format;
    const uint8_t *raw;
    uint32_t raw_length;
} ud_record_error;
typedef struct ud_record_options {
    uint32_t struct_size, abi_version;
    ud_error_context errors; /* Strings borrow the host's execution lifetime. */
    ud_id source_scope;
    int32_t policy, include_input;
} ud_record_options;
/* Every error port has one schema. Optional input retains a canonical Wire
 * record or explicitly enabled raw bytes, identified by inputFormat. */
UD_API ud_status ud_error_schema(const ud_error_context *, const ud_batch *, int include_input,
                                 ud_batch **);
UD_API int32_t ud_record_error_kind(ud_status);
UD_API int32_t ud_record_error_recoverable(ud_status);
UD_API ud_status ud_error_append(const ud_error_context *, ud_batch *, const ud_record_error *);
UD_API ud_status ud_error_rule_token(const ud_error_context *, ud_string, uint32_t, ud_id *);
#ifdef __cplusplus
}
#endif
#endif
