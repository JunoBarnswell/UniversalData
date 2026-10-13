#ifndef UD_ERROR_H
#define UD_ERROR_H
#include "data.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ud_error_context {
    uint32_t struct_size,abi_version;
    ud_context *memory;
    ud_string execution_id,package_id,task_id,pipeline_id,component_id;
    ud_id field_scope;
} ud_error_context;
/* Optional input is one canonical Wire batch, retaining any input Schema,
 * exact values and row identity without changing the error port schema. */
UD_API ud_status ud_error_schema(const ud_error_context *,const ud_batch *,int include_input,ud_batch **);
UD_API ud_status ud_error_append(const ud_error_context *,ud_batch *,const ud_batch *,uint32_t,
                                  ud_string rule,uint32_t version,ud_id column,ud_status,int policy);
UD_API ud_status ud_error_rule_token(const ud_error_context *,ud_string,uint32_t,ud_id *);
#ifdef __cplusplus
}
#endif
#endif
