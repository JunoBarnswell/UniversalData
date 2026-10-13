#include "advanced_internal.h"
static ud_status number(const ud_json*j,uint64_t fallback,uint64_t*out){if(!j){*out=fallback;return UD_OK;}const char*p;uint32_t n;UD_TRY(ud_json_text(j,&p,&n));uint64_t value=0;for(uint32_t i=0;i<n;i++){if(p[i]<'0'||p[i]>'9'||value>(UINT64_MAX-(unsigned)(p[i]-'0'))/10)return UD_SCHEMA;value=value*10+(unsigned)(p[i]-'0');}*out=value;return UD_OK;}
static ud_status text(const ud_json*j,ud_string*out){return ud_json_kind(j)==UD_JSON_STRING?ud_json_text(j,&out->data,&out->length):UD_SCHEMA;}
ud_status ud_ordered_bind(ud_advanced*a){int k=a->options.kind;uint32_t*order=NULL,count=0;UD_TRY(ud_advanced_select(a,ud_advanced_member(a->config,"keys"),&order,&count));
    if(k==UD_OPERATOR_RANK){ud_operator_options o=a->options;o.kind=UD_OPERATOR_WINDOW;o.configuration=(ud_string){0};o.keys=order;o.key_count=count;uint64_t partition=0;ud_status s=number(ud_advanced_member(a->config,"partitionKeys"),0,&partition);if(!s&&partition>count)s=UD_SCHEMA;o.partition_key_count=(uint32_t)partition;o.window_function=ud_advanced_named(ud_advanced_member(a->config,"method"),"dense")?UD_WINDOW_DENSE_RANK:UD_WINDOW_RANK;if(!s)s=text(ud_advanced_member(a->config,"name"),&o.window_name);const ud_batch*in[]={a->inputs[0]};if(!s)s=ud_operator_create(a->ctx,&o,in,&a->window);if(!s)s=ud_operator_schema(a->window,0,&a->schema[0]);ud_free(order);return s;}
    a->outputs=k==UD_OPERATOR_DEDUPLICATE?3:2;a->error_port=a->outputs-1;
    if(k==UD_OPERATOR_TOP_N){uint64_t partition=0;UD_TRY(number(ud_advanced_member(a->config,"partitionKeys"),0,&partition));if(partition>count){ud_free(order);return UD_SCHEMA;}a->partition_keys=(uint32_t)partition;UD_TRY(number(ud_advanced_member(a->config,"count"),0,&a->limit));}
    else{a->partition_keys=count;a->mode=ud_advanced_named(ud_advanced_member(a->config,"keep"),"last");}
    a->key_count=a->partition_keys;a->keys=ud_alloc(a->ctx,(a->key_count+1)*sizeof(*a->keys));a->group_key=ud_alloc(a->ctx,(a->key_count+1)*sizeof(*a->group_key));if(!a->keys||!a->group_key){ud_free(order);return UD_RESOURCE;}if(a->key_count)memcpy(a->keys,order,a->key_count*sizeof(*order));
    if(count>15){ud_free(order);return UD_SCHEMA;}
    uint32_t columns=a->inputs[0]->columns;ud_field*f=ud_alloc(a->ctx,(columns+1)*sizeof(*f));uint32_t*keys=ud_alloc(a->ctx,(count+1)*sizeof(*keys));if(!f||!keys){ud_free(f);ud_free(keys);ud_free(order);return UD_RESOURCE;}
    for(uint32_t i=0;i<columns;i++)f[i]=a->inputs[0]->column[i].field;
    f[columns]=(ud_field){.name="__udStableRowId",.name_length=15,.type=UD_UTF8};ud_status s=ud_id_make(a->ctx,(ud_string){"native.ordered",14},(ud_string){"stableRowId",11},&f[columns].identity);
    if(!s)s=ud_batch_create(a->ctx,f,columns+1,0,&a->sorted_schema);if(!s){memcpy(keys,order,count*sizeof(*keys));keys[count]=columns;s=ud_advanced_sorted(a,keys,count+1);}ud_free(f);ud_free(keys);ud_free(order);
    if(!s)s=ud_advanced_clone(a->ctx,a->inputs[0],0,&a->schema[0]);return s;
}
ud_status ud_ordered_push(ud_advanced*a,const ud_batch*input){
    if(a->window)return ud_operator_push(a->window,0,input);
    ud_batch*batch=NULL;UD_TRY(ud_advanced_clone(a->ctx,a->sorted_schema,input->rows,&batch));ud_value*values=ud_alloc(a->ctx,batch->columns*sizeof(*values));ud_status s=values?UD_OK:UD_RESOURCE;
    for(uint32_t row=0;!s&&row<input->rows;row++){s=ud_advanced_check(a);for(uint32_t i=0;!s&&i<input->columns;i++)s=ud_batch_get(input,row,i,values+i);ud_row_info info;char id[33];if(!s)s=ud_batch_row_info(input,row,&info);if(!s&&!info.id.high&&!info.id.low)s=UD_SCHEMA;
      if(!s){ud_id_format(info.id,id);values[input->columns]=(ud_value){.type=UD_UTF8,.text=id,.text_length=32};s=ud_batch_append(batch,values,batch->columns);}if(!s)s=ud_batch_copy_row_info(batch,row,input,row);}
    if(!s)s=ud_batch_seal(batch);if(!s)s=ud_operator_push(a->sort,0,batch);ud_free(values);ud_batch_destroy(batch);return s;
}
static int same_group(ud_advanced*a){for(uint32_t k=0;k<a->key_count;k++){ud_value v;ud_batch_get(a->pending,a->row,a->keys[k],&v);if(ud_value_compare(&v,a->group_key+k))return 0;}return 1;}
static ud_status set_group(ud_advanced*a){for(uint32_t k=0;k<a->key_count;k++){ud_value v;UD_TRY(ud_batch_get(a->pending,a->row,a->keys[k],&v));ud_clear_value(a->group_key+k);UD_TRY(ud_copy_value(a->ctx,&v,a->group_key+k,0));}a->group_rows=0;return UD_OK;}
static ud_status row_copy(ud_advanced*a,ud_batch*out,const ud_batch*in,uint32_t row){(void)a;return ud_batch_append_projected(out,in,row,NULL);}
static ud_status hold(ud_advanced*a){ud_batch_destroy(a->held);a->held=NULL;UD_TRY(ud_advanced_clone(a->ctx,a->inputs[0],1,&a->held));return row_copy(a,a->held,a->pending,a->row);}
ud_status ud_ordered_pull(ud_advanced*a,ud_batch**out,uint32_t*output){*out=NULL;if(output)*output=0;if(a->window)return ud_operator_pull(a->window,out,output);if(!a->ended)return UD_BUSY;
    uint32_t port=0;ud_status s=UD_OK;
    while(!s){s=ud_advanced_check(a);if(s)break;ud_status next=ud_advanced_next(a);
      if(next!=UD_OK&&next!=UD_END){s=next;break;}
      int boundary=next==UD_END||!a->active||!same_group(a);
      if(a->options.kind==UD_OPERATOR_DEDUPLICATE&&a->mode&&a->held&&boundary){if(*out&&port!=0)break;if(!*out){port=0;s=ud_advanced_clone(a->ctx,a->schema[0],a->options.max_batch_rows,out);}if(!s)s=row_copy(a,*out,a->held,0);ud_batch_destroy(a->held);a->held=NULL;a->active=0;if(s||(*out)->rows==a->options.max_batch_rows)break;if(next==UD_END)break;continue;}
      if(next==UD_END)break;
      if(boundary){s=set_group(a);if(s)break;a->active=1;}
      uint32_t selected=a->options.kind==UD_OPERATOR_DEDUPLICATE?(a->group_rows?1:0):0;
      if(a->options.kind==UD_OPERATOR_TOP_N&&a->group_rows>=a->limit){a->row++;a->group_rows++;continue;}
      if(a->options.kind==UD_OPERATOR_DEDUPLICATE&&a->mode){if(!a->held){s=hold(a);if(!s){a->row++;a->group_rows++;}continue;}selected=1;}
      if(*out&&selected!=port)break;
      if(!*out){port=selected;s=ud_advanced_clone(a->ctx,a->schema[port],a->options.max_batch_rows,out);if(s)break;}
      s=row_copy(a,*out,a->mode&&a->options.kind==UD_OPERATOR_DEDUPLICATE?a->held:a->pending,a->mode&&a->options.kind==UD_OPERATOR_DEDUPLICATE?0:a->row);
      if(!s&&a->mode&&a->options.kind==UD_OPERATOR_DEDUPLICATE)s=hold(a);
      if(!s){a->row++;a->group_rows++;}if(s||(*out)->rows==a->options.max_batch_rows)break;
    }
    if(s||!*out||!(*out)->rows){ud_batch_destroy(*out);*out=NULL;return s?s:UD_END;}UD_TRY(ud_batch_seal(*out));a->emitted[port]+=(*out)->rows;if(output)*output=port;return UD_OK;
}
