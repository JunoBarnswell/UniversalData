#include "advanced_internal.h"
#include "ud/format.h"
const ud_json *ud_advanced_member(const ud_json*j,const char*key){return ud_json_find(j,key,(uint32_t)strlen(key));}
int ud_advanced_named(const ud_json*j,const char*name){const char*p;uint32_t n;return j&&!ud_json_text(j,&p,&n)&&n==strlen(name)&&!memcmp(p,name,n);}
ud_status ud_advanced_check(ud_advanced*a){if(ud_is_cancelled(a->ctx))return UD_CANCELLED;return a->options.check?a->options.check(a->options.user):UD_OK;}
ud_status ud_advanced_clone(ud_context*c,const ud_batch*b,uint32_t capacity,ud_batch**out){return ud_batch_create(c,b->schema->fields,b->columns,capacity,out);}
ud_status ud_advanced_derive(ud_advanced*a,const ud_field*field,ud_string name,ud_field*out){ud_field spec=*field;spec.key_order=0;char*json=NULL;uint32_t n;UD_TRY(ud_json_serialize(a->config,&json,&n));uint8_t hash[32];ud_hash(json,n,hash);ud_free(json);char scope[65];for(uint32_t i=0;i<32;i++)snprintf(scope+2*i,3,"%02x",hash[i]);return ud_field_derive(a->ctx,&spec,(ud_string){scope,64},name,out);}
ud_status ud_advanced_select(ud_advanced*a,const ud_json*j,uint32_t**out,uint32_t*count){*out=NULL;*count=ud_json_size(j);if(ud_json_kind(j)!=UD_JSON_ARRAY||!*count||*count>16)return UD_SCHEMA;*out=ud_alloc(a->ctx,*count*sizeof(**out));if(!*out)return UD_RESOURCE;for(uint32_t i=0;i<*count;i++){UD_TRY(ud_compute_field_index(a->inputs[0],ud_json_at(j,i),*out+i));for(uint32_t k=0;k<i;k++)if((*out)[i]==(*out)[k])return UD_SCHEMA;}return UD_OK;}
ud_status ud_advanced_sorted(ud_advanced*a,uint32_t*keys,uint32_t count){ud_operator_options o=a->options;o.kind=UD_OPERATOR_SORT;o.configuration=(ud_string){0};o.keys=keys;o.key_count=count;o.partition_count=1;o.aggregation=NULL;const ud_batch*input[]={a->sorted_schema?a->sorted_schema:a->inputs[0]};return ud_operator_create(a->ctx,&o,input,&a->sort);}
ud_status ud_advanced_next(ud_advanced*a){if(a->pending&&a->row<a->pending->rows)return UD_OK;ud_batch_destroy(a->pending);a->pending=NULL;a->row=a->element=0;if(a->sort)return ud_operator_pull(a->sort,&a->pending,NULL);return a->ended?UD_END:UD_BUSY;}
static ud_status kernel(ud_advanced*a,ud_value*v,uint32_t*port,int*advance){int k=a->options.kind;if(k==UD_OPERATOR_FUZZY_LOOKUP)return ud_matching_row(a,v,port,advance);return k>=UD_OPERATOR_STANDARDIZE&&k<=UD_OPERATOR_TERM_LOOKUP?ud_text_operator_row(a,v,port,advance):ud_nested_row(a,v,port,advance);}
ud_status ud_advanced_create(ud_context*ctx,const ud_operator_options*o,const ud_batch*const*inputs,ud_advanced**out){
    *out=NULL;if(o->input_count>64)return UD_SCHEMA;ud_advanced*a=ud_alloc(ctx,sizeof(*a));if(!a)return UD_RESOURCE;a->ctx=ctx;a->options=*o;a->ports=o->input_count?o->input_count:1;a->outputs=2;a->error_port=1;if(o->expression_bindings)a->expression_bindings=*o->expression_bindings;
    ud_json*parsed=NULL,*schema=NULL;ud_string declaration={0};ud_status s=ud_operator_configuration_schema(o->kind,&declaration);
    if(!s)s=ud_json_parse(ctx,o->configuration.data,o->configuration.length,&parsed,NULL);
    if(!s)s=ud_json_parse(ctx,declaration.data,declaration.length,&schema,NULL);
    if(!s)s=ud_json_bind_schema(ctx,schema,parsed,&a->config);ud_json_destroy(parsed);ud_json_destroy(schema);
    const ud_json*p=ud_advanced_member(a->config,"errorPolicy");a->policy=!p||ud_advanced_named(p,"fail")?UD_ERROR_FAIL:ud_advanced_named(p,"redirect")?UD_ERROR_REDIRECT:UD_ERROR_IGNORE;
    for(uint32_t i=0;!s&&i<a->ports;i++)s=inputs[i]?ud_advanced_clone(ctx,inputs[i],0,a->inputs+i):UD_SCHEMA;
    if(!s && o->reference_schema)s=ud_advanced_clone(ctx,o->reference_schema,0,&a->inputs[1]);
    if(!s){if(o->kind==UD_OPERATOR_FUZZY_LOOKUP||o->kind==UD_OPERATOR_FUZZY_GROUPING)s=ud_matching_bind(a);
      else if(o->kind==UD_OPERATOR_DATA_PROFILE)s=ud_profile_operator_bind(a);
      else if(o->kind==UD_OPERATOR_MULTIWAY_JOIN)s=ud_multiway_bind(a);
      else if(o->kind==UD_OPERATOR_DEDUPLICATE||o->kind==UD_OPERATOR_TOP_N||o->kind==UD_OPERATOR_RANK)s=ud_ordered_bind(a);
      else if(o->kind>=UD_OPERATOR_STANDARDIZE&&o->kind<=UD_OPERATOR_TERM_LOOKUP)s=ud_text_operator_bind(a);
      else s=ud_nested_bind(a);}
    if(!s){
        if(!a->options.errors.memory) {
            a->options.errors=(ud_error_context){.struct_size=sizeof(ud_error_context),.abi_version=UD_ABI_VERSION,.memory=ctx};
            s=ud_id_make(ctx,(ud_string){"native.compute",14},(ud_string){"errors",6},&a->options.errors.field_scope);
        }
        const ud_json *include=ud_advanced_member(a->config,"includeInput");
        if(!s)s=ud_error_schema(&a->options.errors,a->inputs[0],ud_advanced_named(include,"true"),&a->schema[a->error_port]);
    }
    for(uint32_t i=1;!s&&i<a->outputs;i++)if(!a->schema[i])s=ud_advanced_clone(ctx,a->inputs[0],0,a->schema+i);
    if(!s){a->owned_values=ud_alloc(ctx,a->schema[0]->columns*sizeof(*a->owned_values));if(!a->owned_values)s=UD_RESOURCE;}
    if(s){ud_advanced_destroy(a);return s;}*out=a;return UD_OK;
}
ud_status ud_advanced_schema(const ud_advanced*a,uint32_t port,ud_batch**out){return port<a->outputs?ud_advanced_clone(a->ctx,a->schema[port],0,out):UD_INVALID;}
ud_status ud_advanced_order(const ud_advanced*a,ud_sort_contract*out){memset(out,0,sizeof(*out));ud_status s=a->window?ud_operator_order(a->window,out):a->sort&&a->options.kind!=UD_OPERATOR_DEDUPLICATE?ud_operator_order(a->sort,out):UD_OK;
    if(!s&&a->options.kind==UD_OPERATOR_TOP_N&&out->count)out->count--;return s;}
uint64_t ud_advanced_demand(const ud_advanced*a){if(a->pending&&!a->sort&&!a->window)return 0;uint64_t mask=a->ports==64?UINT64_MAX:(UINT64_C(1)<<a->ports)-1;return mask&~a->ended;}
ud_status ud_advanced_push(ud_advanced*a,uint32_t port,const ud_batch*b){if(port>=a->ports||(a->ended&(UINT64_C(1)<<port))||!b||!b->sealed||b->rows>a->options.max_batch_rows)return UD_INVALID;UD_TRY(ud_advanced_check(a));
    if(b->ctx!=a->ctx||b->columns!=a->inputs[port]->columns)return UD_SCHEMA;
    for(uint32_t i=0;i<b->columns;i++)if(!ud_field_matches(&b->column[i].field,&a->inputs[port]->column[i].field,UD_FIELD_PORT))return UD_SCHEMA;
    if(a->options.kind==UD_OPERATOR_FUZZY_GROUPING)return ud_matching_push(a,b);
    if(a->options.kind==UD_OPERATOR_DATA_PROFILE)return ud_profile_operator_push(a,b);
    if(a->options.kind==UD_OPERATOR_MULTIWAY_JOIN)return ud_multiway_push(a,port,b);if(a->options.kind==UD_OPERATOR_DEDUPLICATE||a->options.kind==UD_OPERATOR_TOP_N||a->options.kind==UD_OPERATOR_RANK)return ud_ordered_push(a,b);if(a->sort)return ud_operator_push(a->sort,port,b);if(a->pending)return UD_BUSY;UD_TRY(ud_batch_retain((ud_batch*)b));a->pending=(ud_batch*)b;a->row=0;return UD_OK;}
ud_status ud_advanced_end(ud_advanced*a,uint32_t port){if(port>=a->ports||a->ended&(UINT64_C(1)<<port))return UD_INVALID;a->ended|=UINT64_C(1)<<port;if(a->options.kind==UD_OPERATOR_MULTIWAY_JOIN)return ud_multiway_end(a,port);if(a->window)return ud_operator_finish_input(a->window,port);if(a->sort)return ud_operator_finish_input(a->sort,port);return UD_OK;}
ud_status ud_advanced_pull(ud_advanced*a,ud_batch**out,uint32_t*output){
    *out=NULL;if(output)*output=0;UD_TRY(ud_advanced_check(a));
    if(a->options.kind==UD_OPERATOR_FUZZY_GROUPING)return ud_matching_pull(a,out,output);
    if(a->options.kind==UD_OPERATOR_DATA_PROFILE)return ud_profile_operator_pull(a,out,output);
    if(a->options.kind==UD_OPERATOR_MULTIWAY_JOIN)return ud_multiway_pull(a,out,output);
    if(a->options.kind==UD_OPERATOR_DEDUPLICATE||a->options.kind==UD_OPERATOR_TOP_N||a->options.kind==UD_OPERATOR_RANK)return ud_ordered_pull(a,out,output);
    if(!a->pending||a->row>=a->pending->rows){ud_batch_destroy(a->pending);a->pending=NULL;a->row=0;return a->ended?UD_END:UD_BUSY;}
    ud_value*v=ud_alloc(a->ctx,a->schema[0]->columns*sizeof(*v));ud_status s=v?UD_OK:UD_RESOURCE;uint32_t port=0;
    while(!s&&a->pending&&a->row<a->pending->rows){s=ud_advanced_check(a);if(s)break;
      for(uint32_t i=0;i<a->schema[0]->columns;i++)ud_clear_value(a->owned_values+i);
      a->override_info=0;
      uint32_t row=a->row,selected=0;int advance=1;ud_status cause=UD_OK;s=kernel(a,v,&selected,&advance);
      if(s){cause=s;
        if(s==UD_CANCELLED||s==UD_AUTH||s==UD_RESOURCE||(s==UD_TIMEOUT&&ud_advanced_check(a))||a->policy==UD_ERROR_FAIL)break;
        a->rejected++;if(a->policy==UD_ERROR_IGNORE){s=UD_OK;a->row++;continue;}s=UD_OK;selected=a->error_port;advance=1;}
      if(!s && advance==2){a->row++;a->processed++;a->element=0;ud_batch_destroy(a->tokens);a->tokens=NULL;a->token_row=0;a->active=0;continue;}
      if(*out&&selected!=port)break;
      if(!*out){port=selected;s=ud_advanced_clone(a->ctx,a->schema[port],a->options.max_batch_rows,out);if(s)break;}
      if(port==a->error_port)s=ud_error_append(&a->options.errors,*out,a->pending,row,(ud_string){"native.transform",16},1,(ud_id){0},cause,a->policy);
      else{s=ud_batch_append(*out,v,(*out)->columns);if(!s)s=a->override_info?ud_batch_set_row_info(*out,(*out)->rows-1,&a->next_info):ud_batch_copy_row_info(*out,(*out)->rows-1,a->pending,row);}
      if(!s && !advance)a->element++;
      if(!s&&advance){a->row++;a->element=0;ud_batch_destroy(a->tokens);a->tokens=NULL;a->token_row=0;}
      if(!s&&advance)a->processed++;if(s||(*out)->rows==a->options.max_batch_rows)break;
    }
    for(uint32_t i=0;i<a->schema[0]->columns;i++)ud_clear_value(a->owned_values+i);
    ud_free(v);if(s||!*out||!(*out)->rows){ud_batch_destroy(*out);*out=NULL;
      int end=a->ended&&a->pending&&a->row>=a->pending->rows;
      if(a->pending&&a->row>=a->pending->rows){ud_batch_destroy(a->pending);a->pending=NULL;a->row=0;}
      return s?s:end?UD_END:UD_BUSY;}
    UD_TRY(ud_batch_seal(*out));a->emitted[port]+=(*out)->rows;if(output)*output=port;return UD_OK;
}
ud_status ud_advanced_diagnostics(const ud_advanced*a,char**out,uint32_t*n){char text[384];int count=snprintf(text,sizeof(text),"{\"processed\":%llu,\"rejected\":%llu,\"outputs\":[%llu,%llu,%llu,%llu]}",(unsigned long long)a->processed,(unsigned long long)a->rejected,(unsigned long long)a->emitted[0],(unsigned long long)a->emitted[1],(unsigned long long)a->emitted[2],(unsigned long long)a->emitted[3]);if(count<0||(size_t)count>=sizeof(text))return UD_RESOURCE;*out=ud_strdup_n(a->ctx,text,(size_t)count);*n=(uint32_t)count;return *out?UD_OK:UD_RESOURCE;}
void ud_advanced_destroy(ud_advanced*a){if(!a)return;if(a->options.kind==UD_OPERATOR_DATA_PROFILE)ud_profile_operator_clear(a);if(a->options.kind==UD_OPERATOR_FUZZY_LOOKUP||a->options.kind==UD_OPERATOR_FUZZY_GROUPING)ud_matching_clear(a);if(a->owned_values&&a->schema[0])for(uint32_t i=0;i<a->schema[0]->columns;i++)ud_clear_value(a->owned_values+i);ud_free(a->owned_values);for(uint32_t i=0;i<64;i++)ud_batch_destroy(a->inputs[i]);for(uint32_t i=0;i<4;i++)ud_batch_destroy(a->schema[i]);ud_batch_destroy(a->pending);ud_batch_destroy(a->held);ud_batch_destroy(a->fingerprint_row);ud_batch_destroy(a->tokens);ud_batch_destroy(a->sorted_schema);ud_operator_destroy(a->sort);ud_operator_destroy(a->window);for(uint32_t i=0;i<63;i++)ud_operator_destroy(a->joins[i]);for(uint32_t i=0;i<a->field_count;i++){ud_field_clear(&a->fields[i].owned);for(uint32_t j=0;j<32;j++)ud_free((void*)a->fields[i].map_keys[j].data);}ud_free(a->fields);ud_free(a->keys);if(a->group_key){for(uint32_t i=0;i<a->key_count;i++)ud_clear_value(a->group_key+i);ud_free(a->group_key);}for(uint32_t i=0;i<a->condition_count;i++)ud_expression_destroy(a->conditions[i]);ud_free(a->conditions);ud_free(a->operations);ud_regex_destroy(a->regex);ud_cache_reader_destroy(a->cache_reader);ud_cache_release(a->reference);ud_json_destroy(a->config);ud_free(a);}

uint32_t ud_advanced_algorithm(const ud_advanced *a){return a->options.kind==UD_OPERATOR_FUZZY_LOOKUP||a->options.kind==UD_OPERATOR_FUZZY_GROUPING?UD_PHYSICAL_INDEX:a->options.kind==UD_OPERATOR_MULTIWAY_JOIN?UD_PHYSICAL_GRACE_HASH:a->sort||a->window?UD_PHYSICAL_SORT:UD_PHYSICAL_STREAM;}
uint32_t ud_advanced_output_count(const ud_advanced *a){return a->outputs;}
