#include "../package/internal.h"
typedef struct advanced_component {ud_component_context context;ud_operator *operator;} advanced_component;
static ud_status bind_operator(const ud_component_context*c,int kind,ud_string configuration,
                                const ud_batch *const *inputs,ud_operator **out){
    ud_operator_options options={.struct_size=sizeof(options),.abi_version=UD_ABI_VERSION,.kind=kind,
        .max_batch_rows=c->max_batch_rows,.buffers=c->execution->buffers,.check=(ud_status(*)(void*))ud_activation_check,
        .user=c->activation,.configuration=configuration,.executor=c->execution->executor,.partition_count=1};
    ud_expression_options expressions={.struct_size=sizeof(expressions),.abi_version=UD_ABI_VERSION,
        .schema=inputs[0],.resolve=c->resolve,.user=c->resolve_user,.resolve_field=c->resolve_field,
        .registry=c->execution->registry,.function_capabilities=c->activation->parallel?UD_COMPONENT_THREAD_SAFE:0};
    options.expression_bindings=&expressions;
    UD_TRY(ud_component_operator_options(c,&options));return ud_operator_create(c->memory,&options,inputs,out);
}
static uint32_t outputs(int kind){return kind==UD_OPERATOR_DEDUPLICATE?3:2;}
static ud_status configure(void*user,const ud_component_context*c,ud_string text,
                           const ud_batch*const*inputs,ud_batch**out){
    int kind=(int)(intptr_t)user;ud_operator*op=NULL;ud_status s=bind_operator(c,kind,text,inputs,&op);
    for(uint32_t i=0;!s&&i<outputs(kind);i++)s=ud_operator_schema(op,i,out+i);
    ud_operator_destroy(op);return s;
}
static ud_status prepare(void*user,const ud_component_context*c,ud_string text,
                         const ud_batch*const*inputs,const ud_batch*const*out,void**instance){
    (void)out;*instance=NULL;advanced_component*a=ud_alloc(c->memory,sizeof(*a));if(!a)return UD_RESOURCE;a->context=*c;
    ud_status s=bind_operator(c,(int)(intptr_t)user,text,inputs,&a->operator);if(s){ud_free(a);return s;}*instance=a;return UD_OK;
}
static ud_status step(void*instance,const ud_component_event*event){advanced_component*a=instance;
    UD_TRY(ud_activation_check(a->context.activation));
    if(event->kind==UD_COMPONENT_DATA)UD_TRY(ud_operator_push(a->operator,event->input,event->batch));
    else if(event->kind==UD_COMPONENT_INPUT_END)UD_TRY(ud_operator_finish_input(a->operator,event->input));
    else if(event->kind!=UD_COMPONENT_POLL)return UD_UNSUPPORTED;
    ud_batch*batch=NULL;uint32_t output=0;ud_status s=ud_operator_pull(a->operator,&batch,&output);
    if(s==UD_BUSY)return UD_OK;if(s)return s;
    if(a->context.connected_outputs&(UINT64_C(1)<<output))s=a->context.emit(a->context.emit_user,output,batch);
    ud_batch_destroy(batch);return s?s:UD_MORE;
}
static uint64_t demand(void*instance){return ud_operator_input_demand(((advanced_component*)instance)->operator);}
static ud_status finish(void*instance){(void)instance;return UD_OK;}
static void dispose(void*instance,ud_status status){(void)status;advanced_component*a=instance;if(!a)return;ud_operator_destroy(a->operator);ud_free(a);}
static ud_status diagnostics(void*instance,char**out,uint32_t*n){return ud_operator_diagnostics(((advanced_component*)instance)->operator,out,n);}
static ud_status describe(void*user,const ud_component_context*c,ud_string text,
                          const ud_batch*const*inputs,ud_component_plan*out){ud_operator*op=NULL;
    ud_status s=bind_operator(c,(int)(intptr_t)user,text,inputs,&op);if(!s)s=ud_component_operator_plan(op,out);ud_operator_destroy(op);return s;
}
static ud_status bind_ports(void *user,const ud_component_context *c,ud_string text,ud_component_ports *ports) {
    if((int)(intptr_t)user!=UD_OPERATOR_MULTIWAY_JOIN)return UD_INVALID;
    ud_json *config=NULL;UD_TRY(ud_json_parse(c->memory,text.data,text.length,&config,NULL));
    const ud_json *names=ud_member(config,"inputs");ports->input_count=ud_json_size(names);ports->output_count=2;
    ud_status status=ports->input_count<2||ports->input_count>64?UD_SCHEMA:UD_OK;
    if(!status){ports->inputs=ud_alloc(c->memory,ports->input_count*sizeof(*ports->inputs));ports->outputs=ud_alloc(c->memory,2*sizeof(*ports->outputs));if(!ports->inputs||!ports->outputs)status=UD_RESOURCE;}
    for(uint32_t i=0;!status&&i<ports->input_count;i++){ud_string name;status=ud_json_string(ud_json_at(names,i),&name);if(!status){ports->inputs[i].name=(ud_string){ud_strdup_n(c->memory,name.data,name.length),name.length};ports->inputs[i].flags=UD_PORT_REQUIRED;if(!ports->inputs[i].name.data)status=UD_RESOURCE;}}
    const char *outputs[]={"rows","errors"};for(uint32_t i=0;!status&&i<2;i++){ports->outputs[i].name=(ud_string){ud_strdup_n(c->memory,outputs[i],strlen(outputs[i])),(uint32_t)strlen(outputs[i])};ports->outputs[i].flags=i?UD_PORT_ERROR:UD_PORT_REQUIRED;if(!ports->outputs[i].name.data)status=UD_RESOURCE;}
    ud_json_destroy(config);return status;
}
ud_status ud_advanced_register(ud_registry*registry){
    static const struct{const char*id;int kind;}entries[]={
      {"multiwayJoin",UD_OPERATOR_MULTIWAY_JOIN},{"deduplicate",UD_OPERATOR_DEDUPLICATE},{"topN",UD_OPERATOR_TOP_N},{"rank",UD_OPERATOR_RANK},
      {"surrogateKey",UD_OPERATOR_SURROGATE_KEY},{"alterRow",UD_OPERATOR_ALTER_ROW},{"flatten",UD_OPERATOR_FLATTEN},
      {"explode",UD_OPERATOR_EXPLODE},{"parseJson",UD_OPERATOR_PARSE_JSON},{"jsonPath",UD_OPERATOR_JSON_PATH},
      {"standardize",UD_OPERATOR_STANDARDIZE},{"regexExtract",UD_OPERATOR_REGEX_EXTRACT},
      {"regexReplace",UD_OPERATOR_REGEX_REPLACE},{"termExtract",UD_OPERATOR_TERM_EXTRACT}};
    for(uint32_t i=0;i<sizeof(entries)/sizeof(*entries);i++){int k=entries[i].kind;ud_string schema;UD_TRY(ud_operator_configuration_schema(k,&schema));
      int blocking=k==UD_OPERATOR_MULTIWAY_JOIN||k==UD_OPERATOR_DEDUPLICATE||k==UD_OPERATOR_TOP_N||k==UD_OPERATOR_RANK;
      ud_component_descriptor d={.struct_size=sizeof(d),.abi_version=UD_ABI_VERSION,
        .id={entries[i].id,(uint32_t)strlen(entries[i].id)},.name={entries[i].id,(uint32_t)strlen(entries[i].id)},
        .version={UD_SDK_VERSION,sizeof(UD_SDK_VERSION)-1},.configuration_schema=schema,.category=UD_COMPONENT_TRANSFORM,
        .inputs=k==UD_OPERATOR_MULTIWAY_JOIN?2:1,.outputs=outputs(k),.synchronous_input=-1,.error_output=(int32_t)outputs(k)-1,
        .capabilities=UD_COMPONENT_THREAD_SAFE|UD_COMPONENT_REPLAY_SAFE|(blocking?UD_COMPONENT_STATEFUL|UD_COMPONENT_BLOCKING:0),
        .optional_outputs=UINT64_C(1)<<(outputs(k)-1),.user=(void*)(intptr_t)k,.configure=configure,.prepare=prepare,
        .bind_ports=k==UD_OPERATOR_MULTIWAY_JOIN?bind_ports:NULL,
        .step=step,.finish=finish,.dispose=dispose,.diagnostics=diagnostics,.describe_plan=describe,.input_demand=demand};
      if(k==UD_OPERATOR_DEDUPLICATE)d.optional_outputs|=2;
      UD_TRY(ud_registry_register(registry,&d));}
    return UD_OK;
}
