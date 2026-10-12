#include "../internal.h"
#include "ud/json.h"
#include "ud/format.h"
static const char alphabet[]="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
static int base64_digit(unsigned char c) {
    const char *p=c?strchr(alphabet,c):NULL;return p?(int)(p-alphabet):-1;
}
static ud_status unbase64(ud_context *ctx,const char *text,uint32_t length,ud_value *out) {
    if(length%4) return UD_PARSE;
    uint32_t bytes=length/4*3;
    if(length && text[length-1]=='=') bytes--;
    if(length>1 && text[length-2]=='=') bytes--;
    char *data=ud_alloc(ctx,bytes?bytes:1);if(!data) return UD_RESOURCE;
    out->text=data;out->text_length=bytes;uint32_t at=0;
    for(uint32_t i=0;i<length;i+=4) {
        int a=base64_digit((unsigned char)text[i]),b=base64_digit((unsigned char)text[i+1]);
        int c=base64_digit((unsigned char)text[i+2]),d=base64_digit((unsigned char)text[i+3]);
        int pad=text[i+2]=='='?2:text[i+3]=='='?1:0;
        if(a<0 || b<0 || (c<0 && pad!=2) || (d<0 && !pad) ||
           (pad && i+4!=length) || (pad==2 && text[i+3]!='=') ||
           (pad==2 && (b&15)) || (pad==1 && (c&3))) return UD_PARSE;
        data[at++]=(char)((a<<2)|(b>>4));
        if(pad<2) data[at++]=(char)((b<<4)|(c>>2));
        if(!pad) data[at++]=(char)((c<<6)|d);
    }
    return at==bytes?UD_OK:UD_PARSE;
}
static ud_status known_members(const ud_json *object,const ud_field *fields,uint32_t columns) {
    if(ud_json_kind(object)!=UD_JSON_OBJECT) return UD_SCHEMA;
    for(uint32_t j=0;j<ud_json_size(object);j++) {
        const char *name=NULL;uint32_t length=0;UD_TRY(ud_json_key(object,j,&name,&length));int found=0;
        for(uint32_t k=0;k<columns;k++)
            if(fields[k].name_length==length && !memcmp(fields[k].name,name,length)) { found=1;break; }
        if(!found) return UD_SCHEMA;
    }
    return UD_OK;
}
static ud_status parse_value(ud_context *ctx,const ud_field *field,const ud_json *json,
                              int32_t rounding,ud_value *out,uint32_t depth) {
    if(depth>32) return UD_RESOURCE;
    if(ud_is_cancelled(ctx)) return UD_CANCELLED;
    out->type=field->type;out->precision=field->precision;out->scale=field->scale;
    if(!json || ud_json_kind(json)==UD_JSON_NULL) { out->is_null=1;return field->nullable?UD_OK:UD_SCHEMA; }
    int32_t kind=ud_json_kind(json);
    if(field->type>=UD_STRUCT) {
        uint32_t count=field->type==UD_STRUCT?field->child_count:ud_json_size(json);
        if(field->type==UD_STRUCT) UD_TRY(known_members(json,field->children,field->child_count));
        else if(kind!=UD_JSON_ARRAY) return UD_SCHEMA;
        if(count && (size_t)count>SIZE_MAX/sizeof(ud_value)) return UD_OVERFLOW;
        ud_value *children=count?ud_alloc(ctx,(size_t)count*sizeof(*children)):NULL;
        if(count && !children) return UD_RESOURCE;
        out->children=children;out->child_count=count;
        for(uint32_t j=0;j<count;j++) {
            const ud_field *child=field->children+(field->type==UD_STRUCT?j:0);
            const ud_json *element=field->type==UD_STRUCT?ud_json_find(json,child->name,child->name_length):ud_json_at(json,j);
            UD_TRY(parse_value(ctx,child,element,rounding,children+j,depth+1));
        }
        return UD_OK;
    }
    if(field->type==UD_BOOL && kind!=UD_JSON_BOOL) return UD_SCHEMA;
    if((field->type==UD_INT64 || field->type==UD_DECIMAL128) && kind!=UD_JSON_NUMBER && kind!=UD_JSON_STRING) return UD_SCHEMA;
    if(field->type==UD_FLOAT64 && kind!=UD_JSON_NUMBER) return UD_SCHEMA;
    if((field->type==UD_UTF8 || field->type==UD_BINARY || field->type==UD_DATE32 ||
        field->type==UD_TIME64 || field->type==UD_TIMESTAMP) && kind!=UD_JSON_STRING) return UD_SCHEMA;
    const char *text=NULL;uint32_t length=0;UD_TRY(ud_json_text(json,&text,&length));
    if(field->type==UD_BINARY) return unbase64(ctx,text,length,out);
    ud_value parsed;UD_TRY(ud_value_parse_field(ctx,field,text,length,rounding,&parsed));
    return ud_copy_value(ctx,&parsed,out,0);
}
static ud_status append_object(ud_batch *batch,const ud_json *object,int32_t rounding) {
    ud_field *fields=ud_alloc(batch->ctx,batch->columns*sizeof(*fields));
    ud_value *values=ud_alloc(batch->ctx,batch->columns*sizeof(*values));
    if(!fields || !values) { ud_free(fields);ud_free(values);return UD_RESOURCE; }
    for(uint32_t j=0;j<batch->columns;j++) fields[j]=batch->column[j].field;
    ud_status s=known_members(object,fields,batch->columns);
    for(uint32_t j=0;!s && j<batch->columns;j++)
        s=parse_value(batch->ctx,fields+j,ud_json_find(object,fields[j].name,fields[j].name_length),rounding,values+j,0);
    if(!s) s=ud_batch_append(batch,values,batch->columns);
    for(uint32_t j=0;j<batch->columns;j++) ud_clear_value(values+j);
    ud_free(values);ud_free(fields);return s;
}
ud_status ud_batch_from_json(ud_context *ctx,const ud_field *fields,uint32_t columns,
                             const char *text,uint32_t length,int32_t rounding,ud_batch **out) {
    if(!out) return UD_INVALID;
    *out=NULL;
    if(!ctx || !fields || !columns || rounding<UD_ROUND_EXACT || rounding>UD_ROUND_CEILING) return UD_INVALID;
    ud_json *json=NULL;UD_TRY(ud_json_parse(ctx,text,length,&json,NULL));
    ud_status s=ud_json_kind(json)==UD_JSON_ARRAY?UD_OK:UD_SCHEMA;
    if(!s) s=ud_batch_create(ctx,fields,columns,ud_json_size(json),out);
    for(uint32_t j=0;!s && j<ud_json_size(json);j++) s=append_object(*out,ud_json_at(json,j),rounding);
    ud_json_destroy(json);
    if(s) { ud_batch_destroy(*out);*out=NULL; }
    return s;
}
static ud_status emit_value(ud_bytes *bytes,const ud_field *field,const ud_value *value,uint32_t depth) {
    if(depth>32) return UD_RESOURCE;
    if(ud_is_cancelled(bytes->ctx)) return UD_CANCELLED;
    if(value->is_null) return ud_bytes_put(bytes,"null",4);
    if(field->type>=UD_STRUCT) {
        int object=field->type==UD_STRUCT;UD_TRY(ud_bytes_put(bytes,object?"{":"[",1));
        for(uint32_t j=0;j<value->child_count;j++) {
            if(j) UD_TRY(ud_bytes_put(bytes,",",1));
            const ud_field *child=field->children+(object?j:0);
            if(object) { UD_TRY(ud_json_quote(bytes,child->name,child->name_length));UD_TRY(ud_bytes_put(bytes,":",1)); }
            UD_TRY(emit_value(bytes,child,value->children+j,depth+1));
        }
        return ud_bytes_put(bytes,object?"}":"]",1);
    }
    if(field->type==UD_UTF8) return ud_json_quote(bytes,value->text,value->text_length);
    if(field->type==UD_BINARY) {
        UD_TRY(ud_bytes_put(bytes,"\"",1));
        for(uint32_t i=0;i<value->text_length;) {
            uint32_t left=value->text_length-i;
            uint32_t x=(uint32_t)(unsigned char)value->text[i++]<<16;
            if(left>1) x|=(uint32_t)(unsigned char)value->text[i++]<<8;
            if(left>2) x|=(unsigned char)value->text[i++];
            char b[4]={alphabet[x>>18],alphabet[(x>>12)&63],left>1?alphabet[(x>>6)&63]:'=',left>2?alphabet[x&63]:'='};
            UD_TRY(ud_bytes_put(bytes,b,4));
        }
        return ud_bytes_put(bytes,"\"",1);
    }
    uint32_t required=0;ud_status s=ud_value_format(value,NULL,0,&required);
    if(s!=UD_RESOURCE) return s;
    char *text=ud_alloc(bytes->ctx,required);if(!text) return UD_RESOURCE;
    s=ud_value_format(value,text,required,&required);
    if(!s) s=field->type==UD_DECIMAL128 || field->type==UD_DATE32 || field->type==UD_TIME64 || field->type==UD_TIMESTAMP?
        ud_json_quote(bytes,text,required-1):ud_bytes_put(bytes,text,required-1);
    ud_free(text);return s;
}
ud_status ud_batch_to_json(const ud_batch *batch,char **out,uint32_t *length) {
    if(!batch || !out || !length) return UD_INVALID;
    *out=NULL;*length=0;ud_bytes bytes={0};bytes.ctx=batch->ctx;
    ud_status s=ud_bytes_put(&bytes,"[",1);
    for(uint32_t row=0;!s && row<batch->rows;row++) {
        if(row) s=ud_bytes_put(&bytes,",",1);
        if(!s) s=ud_bytes_put(&bytes,"{",1);
        for(uint32_t j=0;!s && j<batch->columns;j++) {
            if(j) s=ud_bytes_put(&bytes,",",1);
            ud_field *f=&batch->column[j].field;ud_value value;
            if(!s) s=ud_json_quote(&bytes,f->name,f->name_length);
            if(!s) s=ud_bytes_put(&bytes,":",1);
            if(!s) s=ud_batch_get(batch,row,j,&value);
            if(!s) s=emit_value(&bytes,f,&value,0);
        }
        if(!s) s=ud_bytes_put(&bytes,"}",1);
    }
    if(!s) s=ud_bytes_put(&bytes,"]",1);
    if(!s && bytes.size>UINT32_MAX) s=UD_OVERFLOW;
    if(!s) { *length=(uint32_t)bytes.size;s=ud_bytes_put(&bytes,"\0",1); }
    if(!s) { *out=(char *)bytes.data;bytes.data=NULL; } else *length=0;
    ud_bytes_clear(&bytes);return s;
}
struct ud_json_reader {
    ud_context *ctx;
    FILE *file;
    ud_batch *schema;
    ud_json_options options;
    uint64_t record;
    int poisoned, finished;
};
void ud_json_reader_close(ud_json_reader *reader) {
    if(!reader) return;
    if(reader->file) fclose(reader->file);
    ud_batch_destroy(reader->schema);ud_free(reader);
}
ud_status ud_json_reader_open(ud_context *ctx,const char *path,const ud_field *fields,
                              uint32_t columns,const ud_json_options *options,ud_json_reader **out) {
    if(!out) return UD_INVALID;
    *out=NULL;
    if(!ctx || !path || !options) return UD_INVALID;
    if(options->struct_size<sizeof(*options) || options->version!=UD_CONFIG_VERSION) return UD_INCOMPATIBLE;
    if(!options->batch_size || options->batch_size>1000000 || !options->max_record_bytes ||
       options->rounding<UD_ROUND_EXACT || options->rounding>UD_ROUND_CEILING) return UD_INVALID;
    ud_json_reader *r=ud_alloc(ctx,sizeof(*r));if(!r) return UD_RESOURCE;
    r->ctx=ctx;r->options=*options;ud_status s=ud_batch_create(ctx,fields,columns,0,&r->schema);
    if(!s) { r->file=ud_fopen(path,"rb");if(!r->file) s=UD_IO; }
    if(s) ud_json_reader_close(r);else *out=r;return s;
}
static ud_status read_record(ud_json_reader *reader,ud_json **out) {
    *out=NULL;if(reader->finished) return UD_END;
    ud_bytes line={0};line.ctx=reader->ctx;ud_status s=UD_OK;
    for(;;) {
        if(ud_is_cancelled(reader->ctx)) { s=UD_CANCELLED;break; }
        int ch=fgetc(reader->file);
        if(ch==EOF) { if(ferror(reader->file)) s=UD_IO;else { reader->finished=1;if(!line.size) s=UD_END; }break; }
        if(ch=='\n') break;
        if(line.size==reader->options.max_record_bytes) { s=UD_RESOURCE;break; }
        unsigned char c=(unsigned char)ch;s=ud_bytes_put(&line,&c,1);if(s) break;
    }
    if(!s) s=ud_json_parse(reader->ctx,(const char *)line.data,(uint32_t)line.size,out,NULL);
    ud_bytes_clear(&line);return s;
}
ud_status ud_json_reader_next(ud_json_reader *reader,ud_batch **out) {
    if(!reader || !out) return UD_INVALID;
    *out=NULL;if(reader->poisoned) return UD_CORRUPT;if(reader->finished) return UD_END;
    ud_field *fields=ud_alloc(reader->ctx,reader->schema->columns*sizeof(*fields));if(!fields) return UD_RESOURCE;
    for(uint32_t j=0;j<reader->schema->columns;j++) fields[j]=reader->schema->column[j].field;
    ud_status s=ud_batch_create(reader->ctx,fields,reader->schema->columns,reader->options.batch_size,out);ud_free(fields);
    while(!s && (*out)->rows<reader->options.batch_size) {
        ud_json *json=NULL;s=read_record(reader,&json);
        if(s==UD_END) { s=(*out)->rows?UD_OK:UD_END;break; }
        if(!s) s=append_object(*out,json,reader->options.rounding);
        ud_json_destroy(json);if(!s) reader->record++;
    }
    if(s) { if(s!=UD_END) reader->poisoned=1;ud_batch_destroy(*out);*out=NULL; }
    return s;
}
uint64_t ud_json_reader_offset(const ud_json_reader *reader) {
    int64_t n=reader?ud_tell(reader->file):-1;return n<0?UINT64_MAX:(uint64_t)n;
}
uint64_t ud_json_reader_record(const ud_json_reader *reader) { return reader?reader->record:0; }
ud_status ud_json_reader_resume(ud_json_reader *reader,uint64_t offset,uint64_t record) {
    if(!reader || offset>INT64_MAX) return UD_INVALID;
    int64_t old=ud_tell(reader->file);if(old<0 || fseek(reader->file,0,SEEK_END)) return UD_IO;
    int64_t end=ud_tell(reader->file);ud_status s=UD_OK;
    if(end<0) s=UD_IO;
    else if(offset>(uint64_t)end) s=UD_INVALID;
    else if(offset && offset!=(uint64_t)end && (ud_seek(reader->file,offset-1) || fgetc(reader->file)!='\n')) s=UD_INVALID;
    if(s) { if(ud_seek(reader->file,(uint64_t)old)) return UD_IO;return s; }
    if(ud_seek(reader->file,offset)) { ud_seek(reader->file,(uint64_t)old);return UD_IO; }
    clearerr(reader->file);reader->record=record;reader->poisoned=reader->finished=0;return UD_OK;
}
struct ud_json_writer {
    ud_context *ctx;
    FILE *file;
    ud_batch *schema;
    uint32_t max_record_bytes;
    int failed;
};
ud_status ud_json_writer_close(ud_json_writer *writer) {
    if(!writer) return UD_OK;
    ud_status s=writer->failed?writer->failed:UD_OK;
    if(writer->file) { ud_status flush=ud_flush(writer->ctx,writer->file);if(!s) s=flush;if(fclose(writer->file) && !s) s=UD_IO; }
    ud_batch_destroy(writer->schema);ud_free(writer);return s;
}
ud_status ud_json_writer_open(ud_context *ctx,const char *path,const ud_field *fields,
                              uint32_t columns,uint32_t max_record_bytes,ud_json_writer **out) {
    if(!out) return UD_INVALID;
    *out=NULL;if(!ctx || !path || !max_record_bytes) return UD_INVALID;
    if((uint64_t)max_record_bytes+1>SIZE_MAX) return UD_OVERFLOW;
    ud_json_writer *w=ud_alloc(ctx,sizeof(*w));if(!w) return UD_RESOURCE;w->ctx=ctx;w->max_record_bytes=max_record_bytes;
    ud_status s=ud_batch_create(ctx,fields,columns,0,&w->schema);
    if(!s) { w->file=ud_fopen(path,"wb");if(!w->file) s=UD_IO; }
    if(s) ud_json_writer_close(w);else *out=w;return s;
}
ud_status ud_json_writer_append(ud_json_writer *writer,const ud_batch *batch) {
    if(!writer || !batch || writer->failed || batch->columns!=writer->schema->columns) return UD_INVALID;
    for(uint32_t j=0;j<batch->columns;j++)
        if(!ud_field_equal(&batch->column[j].field,&writer->schema->column[j].field,1)) return UD_SCHEMA;
    ud_bytes line={0};line.ctx=writer->ctx;line.limit=(size_t)writer->max_record_bytes+1;ud_status s=UD_OK;
    for(uint32_t row=0;!s && row<batch->rows;row++) {
        if(ud_is_cancelled(writer->ctx)) { s=UD_CANCELLED;break; }
        line.size=0;s=ud_bytes_put(&line,"{",1);
        for(uint32_t j=0;!s && j<batch->columns;j++) {
            if(j) s=ud_bytes_put(&line,",",1);ud_field *f=&batch->column[j].field;ud_value value;
            if(!s) s=ud_json_quote(&line,f->name,f->name_length);if(!s) s=ud_bytes_put(&line,":",1);
            if(!s) s=ud_batch_get(batch,row,j,&value);if(!s) s=emit_value(&line,f,&value,0);
            if(!s && line.size>writer->max_record_bytes) s=UD_RESOURCE;
        }
        if(!s) s=ud_bytes_put(&line,"}",1);
        if(!s && line.size>writer->max_record_bytes) s=UD_RESOURCE;
        if(!s) s=ud_bytes_put(&line,"\n",1);
        if(!s && fwrite(line.data,1,line.size,writer->file)!=line.size) s=UD_IO;
    }
    if(s) writer->failed=s;ud_bytes_clear(&line);return s;
}
