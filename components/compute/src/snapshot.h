#ifndef UD_COMPUTE_SNAPSHOT_H
#define UD_COMPUTE_SNAPSHOT_H
#include "../internal.h"
/* A state value is a canonical typed Batch. Buffer and Root Journal retain sole
 * ownership of paging, durability and hashes; this helper owns no storage. */
static inline ud_status ud_snapshot_batch(ud_context*ctx,const ud_bytes*bytes,ud_batch**out){
    if(bytes->size>UINT32_MAX)return UD_RESOURCE;
    ud_field field={.name="nativeState",.name_length=11,.type=UD_BINARY};
    UD_TRY(ud_id_make(ctx,(ud_string){"native.state",12},(ud_string){"version9",8},&field.identity));
    ud_value value={.type=UD_BINARY,.text=(const char*)bytes->data,.text_length=(uint32_t)bytes->size};
    ud_status status=ud_batch_create(ctx,&field,1,1,out);if(!status)status=ud_batch_append(*out,&value,1);if(!status)status=ud_batch_seal(*out);if(status){ud_batch_destroy(*out);*out=NULL;}return status;
}
static inline ud_status ud_snapshot_bytes(ud_context*ctx,const ud_batch*state,ud_bytes*out){
    if(!state||!state->sealed||state->columns!=1||state->rows!=1||state->column[0].field.type!=UD_BINARY)return UD_CORRUPT;
    ud_value value;UD_TRY(ud_batch_get(state,0,0,&value));if(value.is_null)return UD_CORRUPT;*out=(ud_bytes){.ctx=ctx,.data=(uint8_t*)value.text,.size=value.text_length};return UD_OK;
}
#endif
