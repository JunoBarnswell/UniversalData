#include "../package/internal.h"
#include "ud/quality.h"
#include "ud/format.h"

enum { COPY = 1, CHARACTER, ROW_COUNT, AUDIT, QUALITY };
typedef struct transform {
    ud_component_context context;
    int kind;
    ud_json *configuration;
    ud_batch *schema, *errors;
    uint32_t *indices, count;
    ud_expression **expressions;
    ud_quality *quality;
    uint64_t rows;
    ud_string variable;
} transform;
static int field_index(const ud_batch *schema, ud_string name) {
    for (uint32_t i=0;i<schema->columns;i++)
        if (schema->column[i].field.name_length==name.length &&
            !memcmp(schema->column[i].field.name,name.data,name.length)) return (int)i;
    return -1;
}
static ud_status fields(ud_context *ctx,const ud_batch *schema,ud_field **out) {
    *out=ud_alloc(ctx,schema->columns*sizeof(**out));
    if(!*out)return UD_RESOURCE;
    for(uint32_t i=0;i<schema->columns;i++)(*out)[i]=schema->column[i].field;
    return UD_OK;
}
static ud_status character_expression(const ud_component_context *c,const ud_json *column,
                                       const ud_batch *schema,ud_expression **out) {
    ud_string name,function;
    UD_TRY(ud_json_string(ud_member(column,"column"),&name));
    UD_TRY(ud_json_string(ud_member(column,"function"),&function));
    if(!ud_named(function,"trim") && !ud_named(function,"upper_ascii") && !ud_named(function,"lower_ascii"))
        return UD_UNSUPPORTED;
    ud_bytes json={.ctx=c->memory};
    ud_status s=ud_bytes_put(&json,"{\"function\":",12);
    if(!s)s=ud_json_quote(&json,function.data,function.length);
    if(!s)s=ud_bytes_put(&json,",\"args\":[{\"column\":",19);
    if(!s)s=ud_json_quote(&json,name.data,name.length);
    if(!s)s=ud_bytes_put(&json,"}]}",3);
    ud_expression_options options={sizeof(options),UD_ABI_VERSION,schema,c->resolve,c->resolve_user,
                                    c->resolve_field,c->execution->registry,c->activation->parallel?UD_COMPONENT_THREAD_SAFE:0};
    if(!s)s=ud_expression_create(c->memory,(ud_string){(char*)json.data,(uint32_t)json.size},&options,out);
    ud_bytes_clear(&json);return s;
}
static ud_status quality_create(const ud_component_context *c,const ud_json *config,const ud_batch *schema,ud_quality **out) {
    const ud_json *rules=ud_member(config,"rules");
    uint32_t count=ud_json_size(rules);
    if(ud_json_kind(rules)!=UD_JSON_ARRAY || !count || count>65536)return UD_SCHEMA;
    ud_quality_rule *bound=ud_alloc(c->memory,count*sizeof(*bound));
    uint32_t **indices=ud_alloc(c->memory,count*sizeof(*indices));
    ud_field *input=NULL;
    ud_status s=(!bound || !indices)?UD_RESOURCE:fields(c->memory,schema,&input);
    for(uint32_t i=0;!s && i<count;i++) {
        const ud_json *rule=ud_json_at(rules,i);
        static const char *keys[]={"id","kind","columns","minimum","maximum","pattern","expectedRows"};
        s=ud_json_keys(rule,keys,7);
        ud_string id={0},kind={0};
        if(!s)s=ud_json_string(ud_member(rule,"id"),&id);
        if(!s)s=ud_json_string(ud_member(rule,"kind"),&kind);
        ud_quality_rule *r=bound+i;
        r->struct_size=sizeof(*r);r->version=UD_CONFIG_VERSION;r->id=id.data;r->id_length=id.length;
        r->kind=ud_named(kind,"notNull")?UD_RULE_NOT_NULL:ud_named(kind,"unique")?UD_RULE_UNIQUE:
            ud_named(kind,"primaryKey")?UD_RULE_PRIMARY_KEY:ud_named(kind,"range")?UD_RULE_RANGE:
            ud_named(kind,"regex")?UD_RULE_REGEX:ud_named(kind,"rowCount")?UD_RULE_ROW_COUNT:0;
        if(!r->kind)s=UD_UNSUPPORTED;
        const ud_json *columns=ud_member(rule,"columns");r->column_count=ud_json_size(columns);
        if(!s && r->kind!=UD_RULE_ROW_COUNT && (ud_json_kind(columns)!=UD_JSON_ARRAY || !r->column_count))s=UD_SCHEMA;
        indices[i]=!s?ud_alloc(c->memory,(r->column_count+1)*sizeof(**indices)):NULL;
        if(!s && !indices[i])s=UD_RESOURCE;
        r->columns=indices[i];
        for(uint32_t j=0;!s && j<r->column_count;j++) {
            ud_string name;s=ud_json_string(ud_json_at(columns,j),&name);
            int at=!s?field_index(schema,name):-1;
            if(at<0)s=UD_SCHEMA;else indices[i][j]=(uint32_t)at;
        }
        r->minimum.is_null=r->maximum.is_null=1;
        if(!s && r->kind==UD_RULE_RANGE) {
            if(r->column_count!=1)s=UD_SCHEMA;
            const ud_field *f=!s?input+indices[i][0]:NULL;
            if(!s && ud_member(rule,"minimum"))s=ud_json_value_decode(c->memory,f,ud_member(rule,"minimum"),UD_ROUND_EXACT,&r->minimum);
            if(!s && ud_member(rule,"maximum"))s=ud_json_value_decode(c->memory,f,ud_member(rule,"maximum"),UD_ROUND_EXACT,&r->maximum);
        }
        if(!s && r->kind==UD_RULE_REGEX) {
            ud_string pattern;s=ud_json_string(ud_member(rule,"pattern"),&pattern);
            r->pattern=pattern.data;r->pattern_length=pattern.length;
        }
        if(!s && r->kind==UD_RULE_ROW_COUNT)s=ud_json_uint(ud_member(rule,"expectedRows"),&r->expected_rows);
    }
    ud_string policy={"reject",6};
    if(!s && ud_member(config,"policy"))s=ud_json_string(ud_member(config,"policy"),&policy);
    if(!s && !ud_named(policy,"reject") && !ud_named(policy,"abort"))s=UD_SCHEMA;
    if(!s)s=ud_quality_create(c->memory,input,schema->columns,bound,count,
                              ud_named(policy,"abort")?UD_QUALITY_ABORT:UD_QUALITY_REJECT,out);
    for(uint32_t i=0;i<count;i++) {
        if(bound){ud_clear_value(&bound[i].minimum);ud_clear_value(&bound[i].maximum);}
        if(indices)ud_free(indices[i]);
    }
    ud_free(input);ud_free(indices);ud_free(bound);return s;
}
static ud_status configure(void *user,const ud_component_context *c,ud_string text,
                             const ud_batch *const *in,ud_batch **out) {
    int kind=(int)(intptr_t)user;ud_json *config=NULL;
    UD_TRY(ud_json_parse(c->memory,text.data,text.length,&config,NULL));
    static const char *map_keys[]={"columns"},*row_keys[]={"variable"},*quality_keys[]={"rules","policy"};
    ud_status s=kind==COPY || kind==CHARACTER?ud_json_keys(config,map_keys,1):kind==ROW_COUNT?
        ud_json_keys(config,row_keys,1):kind==QUALITY?ud_json_keys(config,quality_keys,2):
        ud_json_size(config)?UD_SCHEMA:UD_OK;
    const ud_json *columns=ud_member(config,"columns");
    uint32_t extra=kind==COPY?ud_json_size(columns):0;
    uint32_t count=in[0]->columns+extra;
    if(!s && (kind==COPY || kind==CHARACTER) &&
        (ud_json_kind(columns)!=UD_JSON_ARRAY || !ud_json_size(columns) || count>65536))s=UD_SCHEMA;
    if(!s && kind==CHARACTER && ud_json_size(columns)>in[0]->columns)s=UD_SCHEMA;
    ud_field *output=!s?ud_alloc(c->memory,count*sizeof(*output)):NULL;
    ud_field *derived=!s?ud_alloc(c->memory,(ud_json_size(columns)+1)*sizeof(*derived)):NULL;
    if(!s && (!output || !derived))s=UD_RESOURCE;
    for(uint32_t i=0;!s && i<in[0]->columns;i++)output[i]=in[0]->column[i].field;
    for(uint32_t i=0;!s && i<ud_json_size(columns);i++) {
        const ud_json *column=ud_json_at(columns,i);
        static const char *ck[]={"column","name"},*mk[]={"column","function"};
        s=ud_json_keys(column,kind==COPY?ck:mk,2);
        ud_string name={0},source={0};
        if(!s)s=ud_json_string(ud_member(column,"column"),&source);
        int at=!s?field_index(in[0],source):-1;
        if(!s && at<0)s=UD_SCHEMA;
        for(uint32_t j=0;!s && kind==CHARACTER && j<i;j++) {
            ud_string previous;s=ud_json_string(ud_member(ud_json_at(columns,j),"column"),&previous);
            if(!s && ud_same(previous,source))s=UD_SCHEMA;
        }
        if(!s && kind==COPY)s=ud_json_string(ud_member(column,"name"),&name);else name=source;
        ud_field field={0};ud_expression *expr=NULL;
        if(!s && kind==CHARACTER)s=character_expression(c,column,in[0],&expr);
        if(!s && kind==CHARACTER)s=ud_expression_field(expr,&field);
        if(!s && kind==COPY)field=in[0]->column[at].field;
        char identity[33];
        if(!s)s=ud_pipeline_field_namespace(c,identity);
        if(!s)s=ud_field_derive(c->memory,&field,(ud_string){identity,32},name,derived+i);
        if(!s)output[kind==COPY?in[0]->columns+i:(uint32_t)at]=derived[i];
        ud_expression_destroy(expr);
    }
    if(!s && kind==ROW_COUNT) {
        ud_string variable;s=ud_json_string(ud_member(config,"variable"),&variable);
        ud_variable *target=NULL;
        if(!s)s=ud_variable_writable(c->activation,variable,&target);
        if(!s && target->value.type!=UD_INT64)s=UD_SCHEMA;
    }
    ud_quality *quality=NULL;
    if(!s && kind==QUALITY)s=quality_create(c,config,in[0],&quality);
    ud_quality_destroy(quality);
    if(!s)s=ud_batch_create(c->memory,output,count,0,out);
    if(!s && kind==QUALITY) {
        ud_field *errors=ud_alloc(c->memory,(count+3)*sizeof(*errors));
        if(!errors)s=UD_RESOURCE;
        else {
            for(uint32_t i=0;i<count;i++)errors[i]=output[i];
            errors[count]=(ud_field){.name="__ud_rule_id",.name_length=12,.type=UD_UTF8};
            errors[count+1]=(ud_field){.name="__ud_source_row",.name_length=15,.type=UD_INT64};
            errors[count+2]=(ud_field){.name="__ud_error_code",.name_length=15,.type=UD_INT64};
            s=ud_batch_create(c->memory,errors,count+3,0,out+1);ud_free(errors);
        }
    }
    if(derived)for(uint32_t i=0;i<ud_json_size(columns);i++)ud_field_clear(derived+i);
    ud_free(derived);ud_free(output);ud_json_destroy(config);return s;
}
static void dispose(void *instance,ud_status result) {
    (void)result;transform *t=instance;if(!t)return;
    for(uint32_t i=0;t->expressions && i<t->count;i++)ud_expression_destroy(t->expressions[i]);
    ud_free(t->expressions);ud_free(t->indices);ud_quality_destroy(t->quality);
    ud_batch_destroy(t->schema);ud_batch_destroy(t->errors);ud_json_destroy(t->configuration);ud_free(t);
}
static ud_status prepare(void *user,const ud_component_context *c,ud_string text,
                           const ud_batch *const *in,const ud_batch *const *out,void **instance) {
    *instance=NULL;transform *t=ud_alloc(c->memory,sizeof(*t));if(!t)return UD_RESOURCE;
    t->context=*c;t->kind=(int)(intptr_t)user;
    ud_status s=ud_json_parse(c->memory,text.data,text.length,&t->configuration,NULL);
    ud_field *output=NULL;
    if(!s)s=fields(c->memory,out[0],&output);
    if(!s)s=ud_batch_create(c->memory,output,out[0]->columns,0,&t->schema);
    ud_free(output);output=NULL;
    if(!s && t->kind==QUALITY) {
        s=fields(c->memory,out[1],&output);
        if(!s)s=ud_batch_create(c->memory,output,out[1]->columns,0,&t->errors);
        ud_free(output);output=NULL;
        if(!s)s=quality_create(c,t->configuration,in[0],&t->quality);
    }
    const ud_json *columns=ud_member(t->configuration,"columns");t->count=ud_json_size(columns);
    if(!s && (t->kind==COPY || t->kind==CHARACTER)) {
        t->indices=ud_alloc(c->memory,t->schema->columns*sizeof(*t->indices));
        t->expressions=ud_alloc(c->memory,(t->count+1)*sizeof(*t->expressions));
        if(!t->indices || !t->expressions)s=UD_RESOURCE;
        for(uint32_t i=0;!s && i<in[0]->columns;i++)t->indices[i]=i;
        for(uint32_t i=0;!s && i<t->count;i++) {
            ud_string name;s=ud_json_string(ud_member(ud_json_at(columns,i),"column"),&name);
            int at=!s?field_index(in[0],name):-1;if(at<0)s=UD_SCHEMA;
            if(!s)t->indices[t->kind==COPY?in[0]->columns+i:i]=(uint32_t)at;
            if(!s && t->kind==CHARACTER)s=character_expression(c,ud_json_at(columns,i),in[0],t->expressions+i);
        }
    }
    if(!s && t->kind==ROW_COUNT)s=ud_json_string(ud_member(t->configuration,"variable"),&t->variable);
    if(s){dispose(t,s);return s;}*instance=t;return UD_OK;
}
static ud_status quality_step(transform *t,const ud_batch *input) {
    ud_batch *accepted=NULL,*rejected=NULL,*errors=NULL;
    ud_quality_violation *violations=NULL;uint32_t count=0;
    ud_status s=ud_quality_apply(t->quality,input,&accepted,&rejected,&violations,&count);
    ud_field *metadata=NULL;ud_value *values=NULL;
    if(!s && count)s=fields(t->context.memory,t->errors,&metadata);
    if(!s && count)s=ud_batch_create(t->context.memory,metadata,t->errors->columns,count,&errors);
    if(!s && count){values=ud_alloc(t->context.memory,t->errors->columns*sizeof(*values));if(!values)s=UD_RESOURCE;}
    for(uint32_t i=0;!s && i<count;i++) {
        for(uint32_t j=0;!s && j<input->columns;j++)s=ud_batch_get(rejected,i,j,values+j);
        ud_string id={0};
        const ud_json *rule=ud_json_at(ud_member(t->configuration,"rules"),violations[i].rule_index);
        if(!s)s=ud_json_string(ud_member(rule,"id"),&id);
        values[input->columns]=(ud_value){.type=UD_UTF8,.text=id.data,.text_length=id.length};
        values[input->columns+1]=(ud_value){.type=UD_INT64,.integer=(int64_t)(t->rows+violations[i].input_row)};
        values[input->columns+2]=(ud_value){.type=UD_INT64,.integer=UD_QUALITY};
        if(!s)s=ud_batch_append(errors,values,t->errors->columns);
    }
    if(!s && accepted->rows)s=t->context.emit(t->context.emit_user,0,accepted);
    if(!s && errors && errors->rows)s=t->context.emit(t->context.emit_user,1,errors);
    ud_free(metadata);ud_free(values);ud_free(violations);
    ud_batch_destroy(errors);ud_batch_destroy(rejected);ud_batch_destroy(accepted);return s;
}
static ud_status step(void *instance,const ud_component_event *event) {
    transform *t=instance;UD_TRY(ud_activation_check(t->context.activation));
    if(event->kind==UD_COMPONENT_POLL)return UD_END;
    if(event->kind==UD_COMPONENT_INPUT_END)return t->quality?ud_quality_finish(t->quality):UD_OK;
    const ud_batch *input=event->batch;
    if(input->rows>INT64_MAX-t->rows)return UD_OVERFLOW;
    ud_status s=UD_OK;
    if(t->kind==QUALITY)s=quality_step(t,input);
    else if(t->kind==COPY) {
        ud_field *output=NULL;ud_batch *projected=NULL;
        s=fields(t->context.memory,t->schema,&output);
        if(!s)s=ud_batch_project(t->context.memory,input,t->indices,output,t->schema->columns,&projected);
        if(!s)s=t->context.emit(t->context.emit_user,0,projected);
        ud_batch_destroy(projected);ud_free(output);
    } else if(t->kind==CHARACTER) {
        ud_field *output=NULL;ud_batch *batch=NULL;ud_value *values=NULL;
        s=fields(t->context.memory,t->schema,&output);
        if(!s)s=ud_batch_create(t->context.memory,output,t->schema->columns,input->rows,&batch);
        if(!s){values=ud_alloc(t->context.memory,t->schema->columns*sizeof(*values));if(!values)s=UD_RESOURCE;}
        for(uint32_t row=0;!s && row<input->rows;row++) {
            for(uint32_t j=0;!s && j<input->columns;j++)s=ud_batch_get(input,row,j,values+j);
            for(uint32_t j=0;!s && j<t->count;j++)s=ud_expression_evaluate(t->expressions[j],input,row,values+t->indices[j]);
            if(!s)s=ud_batch_append(batch,values,batch->columns);
        }
        if(!s)s=t->context.emit(t->context.emit_user,0,batch);
        ud_free(values);ud_free(output);ud_batch_destroy(batch);
    } else {
        if(t->kind==AUDIT)ud_activation_notify(t->context.activation,UD_EXEC_RUNNING,UD_OK);
        s=t->context.emit(t->context.emit_user,0,(ud_batch*)input);
    }
    if(!s)t->rows+=input->rows;return s;
}
static ud_status finish(void *instance) {
    transform *t=instance;
    if(t->kind==ROW_COUNT) {
        ud_value value={.type=UD_INT64,.integer=(int64_t)t->rows};
        return ud_activation_set_variable(t->context.activation,t->variable,&value,NULL);
    }
    return UD_OK;
}
ud_status ud_transforms_register(ud_registry *registry) {
    static const char *schemas[]={
        "{\"type\":\"object\",\"properties\":{\"columns\":{\"type\":\"array\"}},\"required\":[\"columns\"],\"additionalProperties\":false}",
        "{\"type\":\"object\",\"properties\":{\"columns\":{\"type\":\"array\"}},\"required\":[\"columns\"],\"additionalProperties\":false}",
        "{\"type\":\"object\",\"properties\":{\"variable\":{\"type\":\"string\"}},\"required\":[\"variable\"],\"additionalProperties\":false}",
        "{\"type\":\"object\",\"properties\":{},\"required\":[],\"additionalProperties\":false}",
        "{\"type\":\"object\",\"properties\":{\"rules\":{\"type\":\"array\"},\"policy\":{\"type\":\"string\"}},\"required\":[\"rules\"],\"additionalProperties\":false}"
    };
    static const char *names[]={"copyColumn","characterMap","rowCount","audit","quality"};
    for(uint32_t i=0;i<5;i++) {
        ud_component_descriptor d={.struct_size=sizeof(d),.abi_version=UD_ABI_VERSION,
            .id={names[i],(uint32_t)strlen(names[i])},.name={names[i],(uint32_t)strlen(names[i])},
            .version={"1.0.0",5},.configuration_schema={schemas[i],(uint32_t)strlen(schemas[i])},
            .category=UD_COMPONENT_TRANSFORM,.inputs=1,.outputs=i==4?2:1,
            .synchronous_input=i==2 || i==3?0:-1,.error_output=i==4?1:-1,
            .capabilities=UD_COMPONENT_REPLAY_SAFE|UD_COMPONENT_THREAD_SAFE,.user=(void*)(intptr_t)(i+1),
            .configure=configure,.prepare=prepare,.step=step,.finish=finish,.dispose=dispose};
        UD_TRY(ud_registry_register(registry,&d));
    }
    return UD_OK;
}
