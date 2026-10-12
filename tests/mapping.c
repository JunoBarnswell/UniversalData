#include "ud/compute.h"
#include "check.h"
#include <string.h>
int main(void) {
    ud_context *ctx=NULL;OK(ud_context_create(NULL,&ctx));
    ud_field input_fields[]={{.name="name",.name_length=4,.type=UD_UTF8,.nullable=1},
        {.name="value",.name_length=5,.type=UD_UTF8,.nullable=1}};
    ud_batch *input=NULL;OK(ud_batch_create(ctx,input_fields,2,2,&input));
    ud_value row[]={{.type=UD_UTF8,.text=" 中文 ",.text_length=8},{.type=UD_UTF8,.text=" 2.345 ",.text_length=7}};
    OK(ud_batch_append(input,row,2));row[0].is_null=row[1].is_null=1;OK(ud_batch_append(input,row,2));
    ud_mapping maps[]={
        {.struct_size=sizeof(ud_mapping),.version=UD_CONFIG_VERSION,
         .target={.name="full_name",.name_length=9,.type=UD_UTF8},.source_column=0,
         .flags=UD_MAP_TRIM_ASCII|UD_MAP_UPPER_ASCII|UD_MAP_DEFAULT,
         .default_value={.type=UD_UTF8,.text=" unknown ",.text_length=9}},
        {.struct_size=sizeof(ud_mapping),.version=UD_CONFIG_VERSION,
         .target={.name="id",.name_length=2,.type=UD_INT64},.source_column=-1,
         .constant={.type=UD_INT64,.integer=42}},
        {.struct_size=sizeof(ud_mapping),.version=UD_CONFIG_VERSION,
         .target={.name="amount",.name_length=6,.type=UD_DECIMAL128,.precision=5,.scale=2},.source_column=1,
         .flags=UD_MAP_TRIM_ASCII|UD_MAP_DEFAULT,
         .default_value={.type=UD_DECIMAL128,.precision=5,.scale=2}}};
    ud_batch *output=NULL;OK(ud_compute_map(ctx,input,maps,3,UD_ROUND_HALF_EVEN,&output));
    ud_value value;OK(ud_batch_get(output,0,0,&value));CHECK(value.text_length==6 && !memcmp(value.text,"中文",6));
    OK(ud_batch_get(output,1,0,&value));CHECK(value.text_length==7 && !memcmp(value.text,"UNKNOWN",7));
    OK(ud_batch_get(output,0,1,&value));CHECK(value.integer==42);
    OK(ud_batch_get(output,0,2,&value));CHECK(value.decimal.low==234 && value.decimal.high==0);
    OK(ud_batch_get(output,1,2,&value));CHECK(value.decimal.low==0 && value.precision==5 && value.scale==2);
    ud_batch_destroy(output);output=NULL;
    CHECK(ud_compute_map(ctx,input,maps,3,UD_ROUND_EXACT,&output)==UD_PARSE && !output);
    maps[0].flags|=UD_MAP_LOWER_ASCII;CHECK(ud_compute_map(ctx,input,maps,3,UD_ROUND_HALF_EVEN,&output)==UD_INVALID && !output);
    ud_batch_destroy(input);CHECK(ud_memory_used(ctx)==0);OK(ud_context_destroy(ctx));return 0;
}
