#include "ud/format.h"
#include "ud/json.h"
#include "../internal.h"
static const char *const type_names[]={"bool","int64","float64","utf8","decimal128","date32","time64","timestamp","binary","struct","list","map"};
static const ud_json *member(const ud_json *v,const char *name) { return ud_json_find(v,name,(uint32_t)strlen(name)); }
static int equals(const char *text,uint32_t length,const char *name) { return strlen(name)==length && !memcmp(text,name,length); }
static ud_status text_value(const ud_json *v,const char **text,uint32_t *length) {
    if(ud_json_kind(v)!=UD_JSON_STRING) return UD_SCHEMA;
    return ud_json_text(v,text,length);
}
static ud_status keys(const ud_json *v,const char *const *names,uint32_t count) {
    if(ud_json_kind(v)!=UD_JSON_OBJECT) return UD_SCHEMA;
    for(uint32_t i=0;i<ud_json_size(v);i++) {
        const char *key=NULL;uint32_t length=0;UD_TRY(ud_json_key(v,i,&key,&length));int found=0;
        for(uint32_t j=0;j<count;j++) if(equals(key,length,names[j])) { found=1;break; }
        if(!found) return UD_SCHEMA;
    }
    return UD_OK;
}
static ud_status integer(ud_context *ctx,const ud_json *json,int32_t *out) {
    if(ud_json_kind(json)!=UD_JSON_NUMBER) return UD_SCHEMA;
    const char *text=NULL;uint32_t length=0;UD_TRY(ud_json_text(json,&text,&length));ud_value value;
    UD_TRY(ud_value_parse(ctx,UD_INT64,text,length,&value));
    if(value.integer<INT32_MIN || value.integer>INT32_MAX) return UD_OVERFLOW;*out=(int32_t)value.integer;return UD_OK;
}
static void clear_fields(ud_field *fields,uint32_t count) {
    if(!fields) return;
    for(uint32_t j=0;j<count;j++) {
        clear_fields((ud_field *)fields[j].children,fields[j].child_count);
        ud_free((void *)fields[j].lineage);
    }
    ud_free(fields);
}
static ud_status parse_fields(ud_context *ctx,const ud_json *array,uint32_t depth,ud_field **out,uint32_t *count) {
    *out=NULL;*count=0;
    if(depth>32 || ud_json_kind(array)!=UD_JSON_ARRAY || !ud_json_size(array) || ud_json_size(array)>65536) return UD_SCHEMA;
    uint32_t n=ud_json_size(array);ud_field *fields=ud_alloc(ctx,n*sizeof(*fields));if(!fields) return UD_RESOURCE;
    *out=fields;*count=n;
    static const char *allowed[]={"name","type","nullable","precision","scale","timezone","children","identity","lineage"};
    for(uint32_t j=0;j<n;j++) {
        const ud_json *v=ud_json_at(array,j);ud_field *f=fields+j;UD_TRY(keys(v,allowed,9));
        UD_TRY(text_value(member(v,"name"),&f->name,&f->name_length));
        const ud_json *id=member(v,"identity"),*lineage=member(v,"lineage");
        if(id) {
            const char *hex=NULL;uint32_t bytes=0;UD_TRY(text_value(id,&hex,&bytes));
            UD_TRY(ud_column_id_parse((ud_string){hex,bytes},&f->identity));
        }
        if(lineage) {
            if(!id || ud_json_kind(lineage)!=UD_JSON_ARRAY || ud_json_size(lineage)>4096) return UD_SCHEMA;
            uint32_t n=ud_json_size(lineage);ud_column_id *ids=n?ud_alloc(ctx,n*sizeof(*ids)):NULL;
            if(n && !ids) return UD_RESOURCE;f->lineage=ids;f->lineage_count=n;
            for(uint32_t k=0;k<n;k++) {
                const char *hex=NULL;uint32_t bytes=0;UD_TRY(text_value(ud_json_at(lineage,k),&hex,&bytes));
                UD_TRY(ud_column_id_parse((ud_string){hex,bytes},ids+k));
            }
        } else if(id) {
            ud_column_id *origin=ud_alloc(ctx,sizeof(*origin));if(!origin)return UD_RESOURCE;
            *origin=f->identity;f->lineage=origin;f->lineage_count=1;
        }
        const char *type=NULL;uint32_t length=0;UD_TRY(text_value(member(v,"type"),&type,&length));
        for(uint32_t k=0;k<12;k++) if(equals(type,length,type_names[k])) f->type=(ud_type)k+1;
        if(!f->type) return UD_UNSUPPORTED;
        const ud_json *nullable=member(v,"nullable");
        if(ud_json_kind(nullable)!=UD_JSON_BOOL) return UD_SCHEMA;
        UD_TRY(ud_json_text(nullable,&type,&length));f->nullable=equals(type,length,"true");
        if(f->type==UD_DECIMAL128) { UD_TRY(integer(ctx,member(v,"precision"),&f->precision));UD_TRY(integer(ctx,member(v,"scale"),&f->scale)); }
        else if(member(v,"precision") || member(v,"scale")) return UD_SCHEMA;
        if(f->type==UD_TIMESTAMP) UD_TRY(text_value(member(v,"timezone"),&f->timezone,&f->timezone_length));
        else if(member(v,"timezone")) return UD_SCHEMA;
        if(f->type>=UD_STRUCT) {
            ud_field *children=NULL;uint32_t count_children=0;
            ud_status s=parse_fields(ctx,member(v,"children"),depth+1,&children,&count_children);
            f->children=children;f->child_count=count_children;if(s) return s;
        } else if(member(v,"children")) return UD_SCHEMA;
    }
    return UD_OK;
}
ud_status ud_schema_from_json(ud_context *ctx,const char *text,uint32_t length,ud_string scope,ud_batch **out) {
    if(!out) return UD_INVALID;
    *out=NULL;if(!ctx || (!text && length)) return UD_INVALID;
    ud_json *doc=NULL;UD_TRY(ud_json_parse(ctx,text,length,&doc,NULL));
    static const char *root_keys[]={"version","fields","namespace"};ud_status s=keys(doc,root_keys,3);int32_t version=0;
    if(!s) s=integer(ctx,member(doc,"version"),&version);if(!s && version!=UD_CONFIG_VERSION) s=UD_INCOMPATIBLE;
    ud_field *fields=NULL;uint32_t count=0;
    if(!s && member(doc,"namespace")) s=text_value(member(doc,"namespace"),&scope.data,&scope.length);
    if(!s) s=parse_fields(ctx,member(doc,"fields"),0,&fields,&count);
    if(!s) s=ud_batch_create_named(ctx,fields,count,0,scope,out);
    clear_fields(fields,count);ud_json_destroy(doc);return s;
}
static ud_status emit_fields(ud_bytes *bytes,const ud_field *fields,uint32_t count) {
    UD_TRY(ud_bytes_put(bytes,"[",1));
    for(uint32_t j=0;j<count;j++) {
        const ud_field *f=fields+j;if(j) UD_TRY(ud_bytes_put(bytes,",",1));
        UD_TRY(ud_bytes_put(bytes,"{\"name\":",8));UD_TRY(ud_json_quote(bytes,f->name,f->name_length));
        UD_TRY(ud_bytes_put(bytes,",\"type\":",8));UD_TRY(ud_json_quote(bytes,type_names[f->type-1],(uint32_t)strlen(type_names[f->type-1])));
        UD_TRY(ud_bytes_put(bytes,",\"nullable\":",12));UD_TRY(ud_bytes_put(bytes,f->nullable?"true":"false",f->nullable?4:5));
        char identity[33];ud_column_id_format(f->identity,identity);
        UD_TRY(ud_bytes_put(bytes,",\"identity\":",12));UD_TRY(ud_json_quote(bytes,identity,32));
        UD_TRY(ud_bytes_put(bytes,",\"lineage\":[",12));
        for(uint32_t k=0;k<f->lineage_count;k++) {
            if(k)UD_TRY(ud_bytes_put(bytes,",",1));ud_column_id_format(f->lineage[k],identity);
            UD_TRY(ud_json_quote(bytes,identity,32));
        }
        UD_TRY(ud_bytes_put(bytes,"]",1));
        if(f->type==UD_DECIMAL128) {
            char meta[100];int n=snprintf(meta,sizeof(meta),",\"precision\":%d,\"scale\":%d",f->precision,f->scale);UD_TRY(ud_bytes_put(bytes,meta,(size_t)n));
        }
        if(f->type==UD_TIMESTAMP) { UD_TRY(ud_bytes_put(bytes,",\"timezone\":",12));UD_TRY(ud_json_quote(bytes,f->timezone,f->timezone_length)); }
        if(f->type>=UD_STRUCT) { UD_TRY(ud_bytes_put(bytes,",\"children\":",12));UD_TRY(emit_fields(bytes,f->children,f->child_count)); }
        UD_TRY(ud_bytes_put(bytes,"}",1));
    }
    return ud_bytes_put(bytes,"]",1);
}
ud_status ud_schema_to_json(const ud_batch *schema,char **out,uint32_t *length) {
    if(!schema || !out || !length) return UD_INVALID;
    *out=NULL;*length=0;ud_field *fields=ud_alloc(schema->ctx,schema->columns*sizeof(*fields));if(!fields) return UD_RESOURCE;
    for(uint32_t j=0;j<schema->columns;j++) fields[j]=schema->column[j].field;
    ud_bytes bytes={0};bytes.ctx=schema->ctx;ud_status s=ud_bytes_put(&bytes,"{\"version\":4,\"fields\":",22);
    if(!s) s=emit_fields(&bytes,fields,schema->columns);if(!s) s=ud_bytes_put(&bytes,"}",1);
    if(!s && bytes.size>UINT32_MAX) s=UD_OVERFLOW;
    if(!s) { *length=(uint32_t)bytes.size;s=ud_bytes_put(&bytes,"\0",1); }
    if(!s) { *out=(char *)bytes.data;bytes.data=NULL; }else *length=0;
    ud_bytes_clear(&bytes);ud_free(fields);return s;
}
ud_status ud_batch_from_json_schema(ud_context *ctx,const char *schema_json,uint32_t schema_length,
                                    const char *data_json,uint32_t data_length,int32_t rounding,ud_batch **out) {
    if(!out) return UD_INVALID;
    *out=NULL;ud_batch *schema=NULL;UD_TRY(ud_schema_from_json(ctx,schema_json,schema_length,(ud_string){0},&schema));
    ud_field *fields=ud_alloc(ctx,schema->columns*sizeof(*fields));ud_status s=fields?UD_OK:UD_RESOURCE;
    for(uint32_t j=0;!s && j<schema->columns;j++) fields[j]=schema->column[j].field;
    if(!s) s=ud_batch_from_json(ctx,fields,schema->columns,data_json,data_length,rounding,out);
    ud_free(fields);ud_batch_destroy(schema);return s;
}
ud_status ud_batch_cast_schema(ud_context *ctx,const ud_batch *input,const char *schema_json,
                                uint32_t schema_length,int32_t rounding,ud_batch **out) {
    if(!out) return UD_INVALID;
    *out=NULL;if(!ctx || !input) return UD_INVALID;
    ud_json *doc=NULL;UD_TRY(ud_json_parse(ctx,schema_json,schema_length,&doc,NULL));
    static const char *allowed[]={"version","fields","namespace"};
    ud_status s=keys(doc,allowed,3);int32_t version=0;
    if(!s)s=integer(ctx,member(doc,"version"),&version);
    if(!s && version!=UD_CONFIG_VERSION)s=UD_INCOMPATIBLE;
    ud_field *fields=NULL;uint32_t count=0;
    if(!s)s=parse_fields(ctx,member(doc,"fields"),0,&fields,&count);
    if(!s)s=ud_batch_cast(ctx,input,fields,count,rounding,out);
    clear_fields(fields,count);ud_json_destroy(doc);return s;
}
