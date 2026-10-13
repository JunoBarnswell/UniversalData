#include "advanced_internal.h"
#include "ud/format.h"
static ud_status text(const ud_json*j,ud_string*out){if(ud_json_kind(j)!=UD_JSON_STRING)return UD_SCHEMA;return ud_json_text(j,&out->data,&out->length);}
static ud_status append_field(ud_advanced*a,uint32_t root,const uint32_t*path,uint32_t depth,const ud_field*field,ud_string name,int nullable){
    uint32_t count=a->field_count+1;if(count>65536)return UD_RESOURCE;
    advanced_field*fields=ud_alloc(a->ctx,(size_t)count*sizeof(*fields));if(!fields)return UD_RESOURCE;
    if(a->field_count)memcpy(fields,a->fields,(size_t)a->field_count*sizeof(*fields));ud_free(a->fields);a->fields=fields;
    advanced_field*out=fields+a->field_count++;out->root=root;out->depth=depth;if(depth)memcpy(out->path,path,depth*sizeof(*path));
    ud_field spec=*field;spec.nullable|=nullable;return ud_advanced_derive(a,&spec,name,&out->owned);
}
static ud_status flatten(ud_advanced*a,uint32_t root,const ud_field*field,uint32_t*path,uint32_t depth,ud_string prefix,int nullable){
    if(depth>=32)return UD_RESOURCE;
    if(field->type!=UD_STRUCT)return append_field(a,root,path,depth,field,prefix,nullable);
    for(uint32_t i=0;i<field->child_count;i++){
      const ud_field*child=field->children+i;ud_bytes name={.ctx=a->ctx};ud_status s=ud_bytes_put(&name,prefix.data,prefix.length);
      if(!s&&prefix.length)s=ud_bytes_put(&name,".",1);if(!s)s=ud_bytes_put(&name,child->name,child->name_length);
      path[depth]=i;if(!s)s=flatten(a,root,child,path,depth+1,(ud_string){(char*)name.data,(uint32_t)name.size},nullable||field->nullable);ud_bytes_clear(&name);if(s)return s;
    }return UD_OK;
}
static ud_status output_schema(ud_advanced*a){ud_field*fields=ud_alloc(a->ctx,a->field_count*sizeof(*fields));if(!fields)return UD_RESOURCE;for(uint32_t i=0;i<a->field_count;i++)fields[i]=a->fields[i].owned;ud_status s=ud_batch_create(a->ctx,fields,a->field_count,0,&a->schema[0]);ud_free(fields);return s;}
static ud_status identity_schema(ud_advanced*a){return ud_advanced_clone(a->ctx,a->inputs[0],0,&a->schema[0]);}
static ud_status with_column(ud_advanced*a,const ud_field*value,ud_string name){ud_field*fields=ud_alloc(a->ctx,(a->inputs[0]->columns+1)*sizeof(*fields));if(!fields)return UD_RESOURCE;for(uint32_t i=0;i<a->inputs[0]->columns;i++)fields[i]=a->inputs[0]->column[i].field;ud_field derived={0};ud_status s=ud_advanced_derive(a,value,name,&derived);if(!s){fields[a->inputs[0]->columns]=derived;s=ud_batch_create(a->ctx,fields,a->inputs[0]->columns+1,0,&a->schema[0]);}ud_field_clear(&derived);ud_free(fields);return s;}
static ud_status path_bind(ud_advanced*a,const ud_field*field,ud_string path,ud_field*out){
    if(!path.length||path.data[0]!='$')return UD_SCHEMA;uint32_t pos=1,depth=0;advanced_field*step=ud_alloc(a->ctx,sizeof(*step));if(!step)return UD_RESOURCE;a->fields=step;a->field_count=1;
    int nullable=field->nullable;const ud_field*at=field;
    while(pos<path.length){if(depth==32)return UD_RESOURCE;ud_string member={0};int named=0;
        if(path.data[pos]=='.'){uint32_t begin=++pos;while(pos<path.length&&path.data[pos]!='.'&&path.data[pos]!='[')pos++;if(begin==pos)return UD_SCHEMA;member=(ud_string){path.data+begin,pos-begin};named=1;}
        else if(path.data[pos]=='['){pos++;
            if(pos<path.length&&path.data[pos]=='"'){uint32_t begin=pos++;int escaped=0;while(pos<path.length){char c=path.data[pos++];if(c=='"'&&!escaped)break;if(c=='\\'&&!escaped)escaped=1;else escaped=0;}if(pos==path.length||path.data[pos++]!=']')return UD_SCHEMA;ud_json*j=NULL;UD_TRY(ud_json_parse(a->ctx,path.data+begin,pos-begin-1,&j,NULL));const char*p;uint32_t n;ud_status s=ud_json_text(j,&p,&n);if(!s){step->map_keys[depth]=(ud_string){ud_strdup_n(a->ctx,p,n),n};if(!step->map_keys[depth].data)s=UD_RESOURCE;member=step->map_keys[depth];}ud_json_destroy(j);if(s)return s;named=1;}
            else{if(at->type!=UD_LIST||at->child_count!=1)return UD_SCHEMA;uint64_t index=0;uint32_t begin=pos;while(pos<path.length&&path.data[pos]>='0'&&path.data[pos]<='9'){if(index>UINT32_MAX/10)return UD_SCHEMA;index=index*10+(unsigned)(path.data[pos++]-'0');}if(pos==begin||pos==path.length||path.data[pos++]!=']'||index>=UINT32_C(0x40000000))return UD_SCHEMA;step->path[depth++]=UINT32_C(0x80000000)|(uint32_t)index;at=at->children;nullable=1;}
        }else return UD_SCHEMA;
        if(named){if(at->type==UD_MAP){if(at->child_count!=1||at->children[0].type!=UD_STRUCT||at->children[0].child_count!=2||at->children[0].children[0].type!=UD_UTF8)return UD_SCHEMA;if(!step->map_keys[depth].data){step->map_keys[depth]=(ud_string){ud_strdup_n(a->ctx,member.data,member.length),member.length};if(!step->map_keys[depth].data)return UD_RESOURCE;}step->path[depth++]=UINT32_C(0x40000000);at=at->children[0].children+1;nullable=1;}
            else{if(at->type!=UD_STRUCT)return UD_SCHEMA;uint32_t index=UINT32_MAX;for(uint32_t i=0;i<at->child_count;i++)if(at->children[i].name_length==member.length&&!memcmp(at->children[i].name,member.data,member.length))index=i;if(index==UINT32_MAX)return UD_SCHEMA;step->path[depth++]=index;at=at->children+index;}}
        nullable|=at->nullable;
    }
    step->depth=depth;*out=*at;out->nullable|=nullable;return UD_OK;
}
ud_status ud_nested_bind(ud_advanced*a){int kind=a->options.kind;ud_string name={0};const ud_json*config=a->config;
    if(kind==UD_OPERATOR_FLATTEN){uint32_t path[32];for(uint32_t i=0;i<a->inputs[0]->columns;i++){const ud_field*f=&a->inputs[0]->column[i].field;UD_TRY(flatten(a,i,f,path,0,(ud_string){f->name,f->name_length},0));}return output_schema(a);}
    if(kind==UD_OPERATOR_ALTER_ROW){const ud_json*rules=ud_advanced_member(config,"rules");a->condition_count=ud_json_size(rules);a->conditions=ud_alloc(a->ctx,a->condition_count*sizeof(*a->conditions));a->operations=ud_alloc(a->ctx,a->condition_count*sizeof(*a->operations));if(!a->conditions||!a->operations)return UD_RESOURCE;
      const char*names[]={"insert","update","delete","upsert"};for(uint32_t i=0;i<a->condition_count;i++){const ud_json*rule=ud_json_at(rules,i);char*json=NULL;uint32_t n;UD_TRY(ud_json_serialize(ud_advanced_member(rule,"condition"),&json,&n));ud_expression_options o=a->expression_bindings;o.struct_size=sizeof(o);o.abi_version=UD_ABI_VERSION;o.schema=a->inputs[0];ud_status s=ud_expression_create(a->ctx,(ud_string){json,n},&o,&a->conditions[i]);ud_free(json);if(s)return s;if(ud_expression_type(a->conditions[i])!=UD_BOOL)return UD_SCHEMA;for(uint32_t k=0;k<4;k++)if(ud_advanced_named(ud_advanced_member(rule,"operation"),names[k]))a->operations[i]=(int32_t)k+1;if(!a->operations[i])return UD_SCHEMA;}
      return identity_schema(a);
    }
    if(kind==UD_OPERATOR_SURROGATE_KEY){UD_TRY(ud_advanced_select(a,ud_advanced_member(config,"keys"),&a->keys,&a->key_count));UD_TRY(text(ud_advanced_member(config,"namespace"),&a->namespace_name));UD_TRY(text(ud_advanced_member(config,"name"),&name));ud_id*lineage=NULL;uint32_t n;ud_field*fields=ud_alloc(a->ctx,a->key_count*sizeof(*fields));if(!fields)return UD_RESOURCE;for(uint32_t i=0;i<a->key_count;i++)fields[i]=a->inputs[0]->column[a->keys[i]].field;ud_status s=ud_lineage_merge(a->ctx,fields,a->key_count,&lineage,&n);ud_free(fields);ud_field output={.type=UD_UTF8,.lineage=lineage,.lineage_count=n};if(!s)s=with_column(a,&output,name);ud_free(lineage);return s;}
    if(kind!=UD_OPERATOR_EXPLODE&&kind!=UD_OPERATOR_PARSE_JSON&&kind!=UD_OPERATOR_JSON_PATH)return UD_UNSUPPORTED;
    UD_TRY(ud_compute_field_index(a->inputs[0],ud_advanced_member(config,"column"),&a->column));UD_TRY(text(ud_advanced_member(config,"name"),&name));const ud_field*input=&a->inputs[0]->column[a->column].field;ud_field spec={0};ud_batch*schema=NULL;
    if(kind==UD_OPERATOR_EXPLODE){if((input->type!=UD_LIST&&input->type!=UD_MAP)||input->child_count!=1)return UD_SCHEMA;spec=input->children[0];a->mode=ud_advanced_named(ud_advanced_member(config,"outer"),"true");spec.nullable|=a->mode||input->nullable;}
    else if(kind==UD_OPERATOR_PARSE_JSON){if(input->type!=UD_UTF8)return UD_SCHEMA;char*json=NULL;uint32_t n;UD_TRY(ud_json_serialize(ud_advanced_member(config,"schema"),&json,&n));ud_status s=ud_schema_from_json(a->ctx,json,n,(ud_string){"native.parseJson",16},&schema);ud_free(json);if(s)return s;if(schema->columns!=1){ud_batch_destroy(schema);return UD_SCHEMA;}spec=schema->column[0].field;spec.nullable|=input->nullable;}
    else{UD_TRY(text(ud_advanced_member(config,"path"),&a->path));UD_TRY(path_bind(a,input,a->path,&spec));}
    ud_status s=with_column(a,&spec,name);ud_batch_destroy(schema);return s;
}
static ud_status digest_emit(void*user,const void*bytes,size_t n){ud_sha256_update(user,bytes,n);return UD_OK;}
static ud_status derive_info(ud_advanced*a,uint64_t element){ud_row_info info;UD_TRY(ud_batch_row_info(a->pending,a->row,&info));if(!info.id.high&&!info.id.low)return UD_SCHEMA;ud_bytes id={.ctx=a->ctx};UD_TRY(ud_put_u64(&id,info.id.high));UD_TRY(ud_put_u64(&id,info.id.low));UD_TRY(ud_put_u64(&id,a->inputs[0]->column[a->column].field.identity.high));UD_TRY(ud_put_u64(&id,a->inputs[0]->column[a->column].field.identity.low));UD_TRY(ud_put_u64(&id,element));uint8_t hash[32];ud_hash(id.data,id.size,hash);ud_bytes_clear(&id);info.id=(ud_id){0};for(uint32_t i=0;i<8;i++){info.id.high=(info.id.high<<8)|hash[i];info.id.low=(info.id.low<<8)|hash[i+8];}a->next_info=info;a->override_info=1;return UD_OK;}
ud_status ud_nested_row(ud_advanced*a,ud_value*v,uint32_t*port,int*advance){*port=0;*advance=1;int kind=a->options.kind;
    if(kind==UD_OPERATOR_FLATTEN){for(uint32_t i=0;i<a->field_count;i++){advanced_field*f=a->fields+i;ud_value value;UD_TRY(ud_batch_get(a->pending,a->row,f->root,&value));for(uint32_t k=0;k<f->depth&&!value.is_null;k++){if(f->path[k]>=value.child_count)return UD_SCHEMA;value=value.children[f->path[k]];}if(value.is_null)value=(ud_value){.type=f->owned.type,.is_null=1,.precision=f->owned.precision,.scale=f->owned.scale};v[i]=value;}return UD_OK;}
    for(uint32_t i=0;i<a->pending->columns;i++)UD_TRY(ud_batch_get(a->pending,a->row,i,v+i));
    if(kind==UD_OPERATOR_ALTER_ROW){uint32_t action=UD_CHANGE_UPSERT;for(uint32_t i=0;i<a->condition_count;i++){ud_value value;UD_TRY(ud_expression_evaluate(a->conditions[i],a->pending,a->row,&value));if(!value.is_null&&value.integer){action=(uint32_t)a->operations[i];break;}}UD_TRY(ud_batch_row_info(a->pending,a->row,&a->next_info));if(!a->next_info.id.high&&!a->next_info.id.low)return UD_SCHEMA;a->next_info.operation=action;a->override_info=1;return UD_OK;}
    ud_value*out=v+a->pending->columns;const ud_field*f=&a->schema[0]->column[a->pending->columns].field;*out=(ud_value){.type=f->type,.is_null=1,.precision=f->precision,.scale=f->scale};
    if(kind==UD_OPERATOR_SURROGATE_KEY){ud_sha256 hash;ud_sha256_init(&hash);ud_bytes domain={.ctx=a->ctx};UD_TRY(ud_put_u64(&domain,a->namespace_name.length));UD_TRY(ud_bytes_put(&domain,a->namespace_name.data,a->namespace_name.length));ud_sha256_update(&hash,domain.data,domain.size);ud_bytes_clear(&domain);for(uint32_t i=0;i<a->key_count;i++){const ud_value*key=v+a->keys[i];if(key->is_null)return UD_QUALITY;ud_bytes type={.ctx=a->ctx};UD_TRY(ud_put_u64(&type,(uint64_t)key->type));UD_TRY(ud_put_u64(&type,(uint64_t)key->scale));ud_sha256_update(&hash,type.data,type.size);ud_bytes_clear(&type);UD_TRY(ud_value_canonical(key,digest_emit,&hash,0));}uint8_t digest[32];ud_sha256_finish(&hash,digest);char*text=ud_alloc(a->ctx,65);if(!text)return UD_RESOURCE;for(uint32_t i=0;i<32;i++)snprintf(text+2*i,3,"%02x",digest[i]);*out=(ud_value){.type=UD_UTF8,.text=text,.text_length=64};a->owned_values[a->pending->columns]=*out;return UD_OK;}
    ud_value input=v[a->column];if(input.is_null){if(kind==UD_OPERATOR_EXPLODE){if(!a->mode)*advance=2;else UD_TRY(derive_info(a,UINT64_MAX));}return UD_OK;}
    if(kind==UD_OPERATOR_EXPLODE){if(!input.child_count){if(!a->mode)*advance=2;else UD_TRY(derive_info(a,UINT64_MAX));return UD_OK;}if(a->element>=input.child_count)return UD_CORRUPT;*out=input.children[a->element];*advance=a->element+1==input.child_count?1:0;return derive_info(a,a->element);}
    if(kind==UD_OPERATOR_PARSE_JSON){ud_json*json=NULL;ud_status s=ud_json_parse(a->ctx,input.text,input.text_length,&json,NULL);if(!s)s=ud_json_value_decode(a->ctx,f,json,UD_ROUND_EXACT,out);ud_json_destroy(json);if(!s)a->owned_values[a->pending->columns]=*out;return s;}
    if(kind==UD_OPERATOR_JSON_PATH){advanced_field*steps=a->fields;ud_value value=input;
        for(uint32_t i=0;i<steps->depth&&!value.is_null;i++){uint32_t index=steps->path[i]&UINT32_C(0x3fffffff);
            if(steps->path[i]&UINT32_C(0x40000000)){int found=0;for(uint32_t k=0;k<value.child_count;k++){const ud_value*entry=value.children+k;if(entry->type!=UD_STRUCT||entry->child_count!=2)return UD_SCHEMA;const ud_value*key=entry->children;if(!key->is_null&&key->type==UD_UTF8&&key->text_length==steps->map_keys[i].length&&!memcmp(key->text,steps->map_keys[i].data,key->text_length)){value=entry->children[1];found=1;break;}}if(!found)value.is_null=1;}
            else{if(index>=value.child_count){value.is_null=1;break;}value=value.children[index];}}
        if(value.is_null)value=*out;*out=value;return UD_OK;}
    return UD_UNSUPPORTED;
}
