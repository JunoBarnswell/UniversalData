#include "advanced_internal.h"
#include "ud/format.h"
static ud_status text(const ud_json*j,ud_string*out){if(ud_json_kind(j)!=UD_JSON_STRING)return UD_SCHEMA;return ud_json_text(j,&out->data,&out->length);}
static ud_status fields(ud_advanced*a,uint32_t extra){a->field_count=a->inputs[0]->columns+extra;a->fields=ud_alloc(a->ctx,a->field_count*sizeof(*a->fields));if(!a->fields)return UD_RESOURCE;for(uint32_t i=0;i<a->inputs[0]->columns;i++){a->fields[i].root=i;UD_TRY(ud_field_copy(a->ctx,&a->inputs[0]->column[i].field,(ud_id){0},&a->fields[i].owned));}return UD_OK;}
static ud_status schema(ud_advanced*a){ud_field*f=ud_alloc(a->ctx,a->field_count*sizeof(*f));if(!f)return UD_RESOURCE;for(uint32_t i=0;i<a->field_count;i++)f[i]=a->fields[i].owned;ud_status s=ud_batch_create(a->ctx,f,a->field_count,0,&a->schema[0]);ud_free(f);return s;}
static ud_status extra_field(ud_advanced*a,uint32_t at,ud_string name,ud_type type,int nullable){ud_field f=a->inputs[0]->column[a->column].field;f.type=type;f.nullable=nullable;f.children=NULL;f.child_count=0;f.precision=f.scale=0;f.timezone=NULL;f.timezone_length=0;return ud_advanced_derive(a,&f,name,&a->fields[at].owned);}
ud_status ud_text_operator_bind(ud_advanced*a){int kind=a->options.kind;const ud_json*c=a->config;
    if(kind==UD_OPERATOR_STANDARDIZE){
        UD_TRY(fields(a,0));a->condition_count=a->field_count;a->conditions=ud_alloc(a->ctx,a->condition_count*sizeof(*a->conditions));if(!a->conditions)return UD_RESOURCE;
        const ud_json*columns=ud_advanced_member(c,"columns");uint8_t*seen=ud_alloc(a->ctx,a->field_count);if(!seen)return UD_RESOURCE;ud_status status=UD_OK;
        for(uint32_t i=0;!status&&i<ud_json_size(columns);i++){const ud_json*entry=ud_json_at(columns,i);uint32_t column;status=ud_compute_field_index(a->inputs[0],ud_advanced_member(entry,"column"),&column);if(status)break;if(seen[column]){status=UD_SCHEMA;break;}seen[column]=1;advanced_field*f=a->fields+column;ud_field spec=a->inputs[0]->column[column].field;
            uint32_t flags=ud_advanced_named(ud_advanced_member(entry,"trim"),"true")?UD_TEXT_TRIM:0;flags|=ud_advanced_named(ud_advanced_member(entry,"normalization"),"NFC")?UD_TEXT_NFC:ud_advanced_named(ud_advanced_member(entry,"normalization"),"NFKC")?UD_TEXT_NFKC:0;flags|=ud_advanced_named(ud_advanced_member(entry,"case"),"upper")?UD_TEXT_UPPER:ud_advanced_named(ud_advanced_member(entry,"case"),"lower")?UD_TEXT_LOWER:ud_advanced_named(ud_advanced_member(entry,"case"),"fold")?UD_TEXT_FOLD:0;
            const ud_json*target=ud_advanced_member(entry,"schema"),*expression=ud_advanced_member(entry,"expression");ud_batch*schema=NULL;
            if(expression){char*json=NULL;uint32_t length;status=ud_json_serialize(expression,&json,&length);ud_expression_options options=a->expression_bindings;options.struct_size=sizeof(options);options.abi_version=UD_ABI_VERSION;options.schema=a->inputs[0];if(!status)status=ud_expression_create(a->ctx,(ud_string){json,length},&options,a->conditions+column);ud_free(json);if(!status)status=ud_expression_field(a->conditions[column],&spec);}
            if(!status&&target){char*json=NULL;uint32_t length;status=ud_json_serialize(target,&json,&length);if(!status)status=ud_schema_from_json(a->ctx,json,length,(ud_string){"native.standardize",18},&schema);ud_free(json);if(!status&&schema->columns!=1)status=UD_SCHEMA;if(!status){spec=schema->column[0].field;spec.identity=a->inputs[0]->column[column].field.identity;spec.lineage=a->inputs[0]->column[column].field.lineage;spec.lineage_count=a->inputs[0]->column[column].field.lineage_count;spec.nullable|=a->inputs[0]->column[column].field.nullable;flags|=UINT32_C(0x1000);}}
            if(!status&&!expression&&a->inputs[0]->column[column].field.type!=UD_UTF8)status=UD_SCHEMA;
            if(!status&&expression&&target){ud_field actual;status=ud_expression_field(a->conditions[column],&actual);if(!status&&!ud_field_matches(&spec,&actual,UD_FIELD_VALUE_TYPE))status=UD_SCHEMA;}
            ud_string name={a->inputs[0]->column[column].field.name,a->inputs[0]->column[column].field.name_length};if(!status&&ud_advanced_member(entry,"name"))status=text(ud_advanced_member(entry,"name"),&name);
            f->flags=flags;f->group=ud_advanced_named(ud_advanced_member(entry,"rounding"),"halfEven")?UD_ROUND_HALF_EVEN:UD_ROUND_EXACT;
            if(!status&&ud_advanced_member(entry,"dateFormat")){ud_string format;status=text(ud_advanced_member(entry,"dateFormat"),&format);if(!status){f->map_keys[0]=(ud_string){ud_strdup_n(a->ctx,format.data,format.length),format.length};if(!f->map_keys[0].data)status=UD_RESOURCE;}}
            f->root=ud_advanced_named(ud_advanced_member(entry,"decimalSeparator"),",")?1:0;
            if(!status){ud_field_clear(&f->owned);status=ud_advanced_derive(a,&spec,name,&f->owned);}ud_batch_destroy(schema);
        }
        ud_free(seen);return status?status:schema(a);
    }
    UD_TRY(ud_compute_field_index(a->inputs[0],ud_advanced_member(c,"column"),&a->column));if(a->inputs[0]->column[a->column].field.type!=UD_UTF8)return UD_SCHEMA;
    if(kind==UD_OPERATOR_REGEX_EXTRACT||kind==UD_OPERATOR_REGEX_REPLACE){ud_string pattern;UD_TRY(text(ud_advanced_member(c,"pattern"),&pattern));ud_regex_options o={.struct_size=sizeof(o),.abi_version=UD_ABI_VERSION,.max_steps=1000000,.max_depth=64,.heap_bytes=1048576,.check=a->options.check,.user=a->options.user};UD_TRY(ud_regex_compile(a->ctx,pattern.data,pattern.length,&o,&a->regex));
      if(kind==UD_OPERATOR_REGEX_EXTRACT){const ud_json*groups=ud_advanced_member(c,"groups");uint32_t n=ud_json_size(groups);UD_TRY(fields(a,n));for(uint32_t i=0;i<n;i++){const ud_json*g=ud_json_at(groups,i),*group=ud_advanced_member(g,"group");ud_string name;UD_TRY(text(ud_advanced_member(g,"name"),&name));uint32_t at=a->inputs[0]->columns+i;
        if(ud_json_kind(group)==UD_JSON_STRING){ud_string named;UD_TRY(text(group,&named));UD_TRY(ud_regex_group_index(a->regex,named,&a->fields[at].group));}
        else{const char*p;uint32_t length;UD_TRY(ud_json_text(group,&p,&length));uint32_t number=0;for(uint32_t j=0;j<length;j++){if(p[j]<'0'||p[j]>'9'||number>64)return UD_SCHEMA;number=number*10+(uint32_t)(p[j]-'0');}if(number>ud_regex_capture_count(a->regex))return UD_SCHEMA;a->fields[at].group=number;}
        UD_TRY(extra_field(a,at,name,UD_UTF8,1));}return schema(a);}
      UD_TRY(text(ud_advanced_member(c,"replacement"),&a->replacement));UD_TRY(fields(a,1));ud_string name;UD_TRY(text(ud_advanced_member(c,"name"),&name));UD_TRY(extra_field(a,a->inputs[0]->columns,name,UD_UTF8,1));return schema(a);
    }
    if(kind==UD_OPERATOR_TERM_LOOKUP){
        if(!a->inputs[1]||a->options.reference_key_count!=1||a->options.reference_keys[0]>=a->inputs[1]->columns||a->inputs[1]->column[a->options.reference_keys[0]].field.type!=UD_UTF8)return UD_SCHEMA;
        uint32_t n=a->inputs[0]->columns;UD_TRY(fields(a,a->inputs[1]->columns+2));
        for(uint32_t i=0;i<a->inputs[1]->columns;i++){const ud_field*f=&a->inputs[1]->column[i].field;ud_bytes name={.ctx=a->ctx};ud_status status=ud_bytes_put(&name,"dictionary.",11);if(!status)status=ud_bytes_put(&name,f->name,f->name_length);if(!status)status=ud_advanced_derive(a,f,(ud_string){(char*)name.data,(uint32_t)name.size},&a->fields[n+i].owned);ud_bytes_clear(&name);if(status)return status;}
        ud_string term,count;UD_TRY(text(ud_advanced_member(c,"name"),&term));UD_TRY(text(ud_advanced_member(c,"countName"),&count));UD_TRY(extra_field(a,n+a->inputs[1]->columns,term,UD_UTF8,0));UD_TRY(extra_field(a,n+a->inputs[1]->columns+1,count,UD_INT64,0));a->limit=1024;
        const ud_json*max=ud_advanced_member(c,"maxTokens");if(max){const char*p;uint32_t length;UD_TRY(ud_json_text(max,&p,&length));a->limit=0;for(uint32_t i=0;i<length;i++)a->limit=a->limit*10+(unsigned)(p[i]-'0');}
        if(a->options.reference){UD_TRY(ud_cache_retain(a->options.reference));a->reference=a->options.reference;ud_field f={.name="term",.name_length=4,.type=UD_UTF8};ud_batch*probe=NULL;UD_TRY(ud_batch_create(a->ctx,&f,1,0,&probe));uint32_t key=0;ud_status status=ud_cache_reader_create(a->reference,probe,&key,1,&a->cache_reader);ud_batch_destroy(probe);if(status)return status;}
        return schema(a);
    }
    if(kind==UD_OPERATOR_TERM_EXTRACT){UD_TRY(fields(a,2));ud_string name,count_name;UD_TRY(text(ud_advanced_member(c,"name"),&name));UD_TRY(text(ud_advanced_member(c,"countName"),&count_name));UD_TRY(extra_field(a,a->inputs[0]->columns,name,UD_UTF8,0));UD_TRY(extra_field(a,a->inputs[0]->columns+1,count_name,UD_INT64,0));a->limit=1024;const ud_json*max=ud_advanced_member(c,"maxTokens");if(max){const char*p;uint32_t n;UD_TRY(ud_json_text(max,&p,&n));a->limit=0;for(uint32_t i=0;i<n;i++)a->limit=a->limit*10+(unsigned)(p[i]-'0');}return schema(a);}
    return UD_UNSUPPORTED;
}
static ud_status term_counts(ud_advanced*a,const ud_value*input){ud_batch*tokens=NULL;UD_TRY(ud_text_terms(a->ctx,(ud_string){input->text,input->text_length},(uint32_t)a->limit,&tokens));
    const ud_field*f=tokens->schema->fields;ud_string definition={"{\"version\":" UD_PROTOCOL_VERSION_TEXT ",\"group\":[\"term\"],\"aggregates\":[{\"function\":\"count\",\"column\":\"*\",\"name\":\"frequency\"}]}",sizeof("{\"version\":" UD_PROTOCOL_VERSION_TEXT ",\"group\":[\"term\"],\"aggregates\":[{\"function\":\"count\",\"column\":\"*\",\"name\":\"frequency\"}]}")-1};
    ud_plan*plan=NULL;ud_accumulator*acc=NULL;ud_status s=ud_plan_create_fields(a->ctx,UD_PLAN_VERSION,f,tokens->columns,definition,NULL,&plan);if(!s)s=ud_accumulator_create(plan,&acc);if(!s)s=ud_accumulator_add(acc,tokens);if(!s)s=ud_accumulator_snapshot(acc,&a->tokens);if(!s)s=ud_batch_seal(a->tokens);ud_accumulator_destroy(acc);ud_plan_destroy(plan);ud_batch_destroy(tokens);return s;
}
ud_status ud_text_operator_row(ud_advanced*a,ud_value*v,uint32_t*port,int*advance){*port=0;*advance=1;int kind=a->options.kind;for(uint32_t i=0;i<a->inputs[0]->columns;i++)UD_TRY(ud_batch_get(a->pending,a->row,i,v+i));
    if(kind==UD_OPERATOR_STANDARDIZE){
        for(uint32_t i=0;i<a->field_count;i++){advanced_field*f=a->fields+i;
            if(a->conditions&&a->conditions[i]){UD_TRY(ud_expression_evaluate(a->conditions[i],a->pending,a->row,v+i));if(v[i].type!=UD_UTF8||!f->flags)continue;}
            if(v[i].is_null){v[i]=(ud_value){.type=f->owned.type,.is_null=1,.precision=f->owned.precision,.scale=f->owned.scale};continue;}
            if(!f->flags)continue;char*normalized=NULL;uint32_t length;UD_TRY(ud_text_transform(a->ctx,(ud_string){v[i].text,v[i].text_length},f->flags&63,&normalized,&length));
            if(f->flags&UINT32_C(0x1000)){
                if(f->root)for(uint32_t k=0;k<length;k++){if(normalized[k]=='.'){ud_free(normalized);return UD_PARSE;}if(normalized[k]==',')normalized[k]='.';}
                if(f->map_keys[0].length&&!(f->map_keys[0].length==3&&!memcmp(f->map_keys[0].data,"iso",3))){
                    if(length<10||normalized[2]!='/'||normalized[5]!='/'){if(!(f->map_keys[0].length==10&&!memcmp(f->map_keys[0].data,"yyyy/MM/dd",10)&&length>=10&&normalized[4]=='/'&&normalized[7]=='/')){ud_free(normalized);return UD_PARSE;}}
                    char prefix[10];memcpy(prefix,normalized,10);if(!memcmp(f->map_keys[0].data,"dd/MM/yyyy",10)){memcpy(normalized,prefix+6,4);normalized[4]='-';memcpy(normalized+5,prefix+3,2);normalized[7]='-';memcpy(normalized+8,prefix,2);}else{normalized[4]=normalized[7]='-';}
                }
                ud_status status=ud_value_parse_field(a->ctx,&f->owned,normalized,length,(int32_t)f->group,v+i);if(!status&&v[i].type==UD_UTF8){a->owned_values[i]=(ud_value){.type=UD_UTF8,.text=normalized,.text_length=length};normalized=NULL;}ud_free(normalized);if(status)return status;
            }else{v[i]=(ud_value){.type=UD_UTF8,.text=normalized,.text_length=length};a->owned_values[i]=v[i];}
        }
        return UD_OK;
    }
    const ud_value*input=v+a->column;uint32_t first=a->inputs[0]->columns;
    for(uint32_t i=first;i<a->field_count;i++)v[i]=(ud_value){.type=a->fields[i].owned.type,.is_null=1};
    if(input->is_null){if(kind==UD_OPERATOR_TERM_EXTRACT||kind==UD_OPERATOR_TERM_LOOKUP)*advance=2;return UD_OK;}
    if(kind==UD_OPERATOR_REGEX_EXTRACT){int32_t matched=0;UD_TRY(ud_regex_search(a->regex,input->text,input->text_length,0,a->spans,65,&matched));if(!matched)return UD_OK;for(uint32_t i=first;i<a->field_count;i++){ud_regex_span span=a->spans[a->fields[i].group];if(span.begin!=UINT32_MAX)v[i]=(ud_value){.type=UD_UTF8,.text=input->text+span.begin,.text_length=span.end-span.begin};}return UD_OK;}
    if(kind==UD_OPERATOR_REGEX_REPLACE){char*text=NULL;uint32_t n;UD_TRY(ud_regex_replace(a->regex,(ud_string){input->text,input->text_length},a->replacement,&text,&n));v[first]=(ud_value){.type=UD_UTF8,.text=text,.text_length=n};a->owned_values[first]=v[first];return UD_OK;}
    if(kind==UD_OPERATOR_TERM_LOOKUP){
        if(!a->reference||!a->cache_reader)return UD_INVALID;if(!a->tokens)UD_TRY(term_counts(a,input));
        for(;;){if(a->token_row>=a->tokens->rows){*advance=2;return UD_OK;}
            if(!a->active){UD_TRY(ud_cache_probe(a->cache_reader,a->tokens,a->token_row));a->active=1;}
            const ud_batch*match;uint32_t row;ud_status status=ud_cache_next(a->cache_reader,&match,&row);if(status==UD_END){a->active=0;a->token_row++;continue;}if(status)return status;
            for(uint32_t i=0;i<match->columns;i++)UD_TRY(ud_batch_get(match,row,i,v+first+i));uint32_t at=first+match->columns;UD_TRY(ud_batch_get(a->tokens,a->token_row,0,v+at));UD_TRY(ud_batch_get(a->tokens,a->token_row,1,v+at+1));
            ud_row_info source,reference;UD_TRY(ud_batch_row_info(a->pending,a->row,&source));UD_TRY(ud_batch_row_info(match,row,&reference));if(!source.id.high&&!source.id.low)return UD_SCHEMA;
            ud_sha256 hash;ud_sha256_init(&hash);ud_bytes domain={.ctx=a->ctx};UD_TRY(ud_put_u64(&domain,source.id.high));UD_TRY(ud_put_u64(&domain,source.id.low));ud_sha256_update(&hash,domain.data,domain.size);ud_bytes_clear(&domain);ud_sha256_update(&hash,v[at].text,v[at].text_length);uint8_t digest[32];ud_sha256_finish(&hash,digest);source.id=(ud_id){0};for(uint32_t i=0;i<8;i++){source.id.high=(source.id.high<<8)|digest[i];source.id.low=(source.id.low<<8)|digest[i+8];}
            UD_TRY(ud_row_info_join(a->ctx,&source,&reference,&a->next_info));a->override_info=1;*advance=0;return UD_OK;
        }
    }
    if(kind==UD_OPERATOR_TERM_EXTRACT){if(!a->tokens)UD_TRY(term_counts(a,input));if(!a->tokens->rows){*advance=2;return UD_OK;}if(a->element>=a->tokens->rows)return UD_CORRUPT;UD_TRY(ud_batch_get(a->tokens,a->element,0,v+first));UD_TRY(ud_batch_get(a->tokens,a->element,1,v+first+1));*advance=a->element+1==a->tokens->rows?1:0;
      ud_row_info info;UD_TRY(ud_batch_row_info(a->pending,a->row,&info));if(!info.id.high&&!info.id.low)return UD_SCHEMA;char id[33];ud_id_format(info.id,id);ud_bytes symbol={.ctx=a->ctx};UD_TRY(ud_bytes_put(&symbol,id,32));UD_TRY(ud_bytes_put(&symbol,v[first].text,v[first].text_length));uint8_t hash[32];ud_hash(symbol.data,symbol.size,hash);ud_bytes_clear(&symbol);char name[65];for(uint32_t i=0;i<32;i++)snprintf(name+2*i,3,"%02x",hash[i]);UD_TRY(ud_id_make(a->ctx,(ud_string){"native.term",11},(ud_string){name,64},&info.id));a->next_info=info;a->override_info=1;return UD_OK;}
    return UD_UNSUPPORTED;
}
