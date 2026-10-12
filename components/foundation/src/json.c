#include "../internal.h"
#include "ud/json.h"
struct ud_json {
    ud_context *ctx;
    int32_t kind;
    char *text;
    uint32_t length, count, capacity;
    struct member { char *key; uint32_t length; ud_json *value; } *members;
};
typedef struct json_parser {
    ud_context *ctx;
    const char *text;
    uint32_t size, pos, nodes;
} json_parser;
static void whitespace(json_parser *p) {
    while(p->pos<p->size && (p->text[p->pos]==' ' || p->text[p->pos]=='\t' ||
          p->text[p->pos]=='\r' || p->text[p->pos]=='\n')) p->pos++;
}
void ud_json_destroy(ud_json *v) {
    if(!v) return;
    for(uint32_t i=0;i<v->count;i++) { ud_free(v->members[i].key);ud_json_destroy(v->members[i].value); }
    ud_free(v->members);ud_free(v->text);ud_free(v);
}
static int hex(unsigned char c) {
    if(c>='0' && c<='9') return c-'0';
    if(c>='a' && c<='f') return c-'a'+10;
    if(c>='A' && c<='F') return c-'A'+10;
    return -1;
}
static ud_status codepoint(json_parser *p,uint32_t *cp) {
    if(p->size-p->pos<4) return UD_PARSE;
    *cp=0;
    for(uint32_t i=0;i<4;i++) { int n=hex((unsigned char)p->text[p->pos++]);if(n<0) return UD_PARSE;*cp=(*cp<<4)|(uint32_t)n; }
    return UD_OK;
}
static ud_status string(json_parser *p,char **out,uint32_t *length) {
    *out=NULL;*length=0;
    if(p->pos>=p->size || p->text[p->pos++]!='"') return UD_PARSE;
    ud_bytes bytes={0};bytes.ctx=p->ctx;ud_status s=UD_OK;int complete=0;
    while(!s && p->pos<p->size) {
        unsigned char c=(unsigned char)p->text[p->pos++];
        if(c=='"') { complete=1;break; }
        if(c<0x20) { s=UD_PARSE;break; }
        if(c!='\\') { s=ud_bytes_put(&bytes,&c,1);continue; }
        if(p->pos==p->size) { s=UD_PARSE;break; }
        c=(unsigned char)p->text[p->pos++];
        if(c=='"' || c=='\\' || c=='/') s=ud_bytes_put(&bytes,&c,1);
        else if(c=='b' || c=='f' || c=='n' || c=='r' || c=='t') {
            c=c=='b'?8:c=='f'?12:c=='n'?10:c=='r'?13:9;s=ud_bytes_put(&bytes,&c,1);
        } else if(c=='u') {
            uint32_t cp=0;s=codepoint(p,&cp);
            if(!s && cp>=0xd800 && cp<=0xdbff) {
                if(p->size-p->pos<6 || p->text[p->pos]!='\\' || p->text[p->pos+1]!='u') s=UD_PARSE;
                else {
                    p->pos+=2;uint32_t low=0;s=codepoint(p,&low);
                    if(!s && (low<0xdc00 || low>0xdfff)) s=UD_PARSE;
                    if(!s) cp=0x10000+((cp-0xd800)<<10)+low-0xdc00;
                }
            } else if(!s && cp>=0xdc00 && cp<=0xdfff) s=UD_PARSE;
            if(!s) {
                uint8_t encoded[4];uint32_t n=0;
                if(cp<0x80) encoded[n++]=(uint8_t)cp;
                else if(cp<0x800) { encoded[n++]=(uint8_t)(0xc0|(cp>>6));encoded[n++]=(uint8_t)(0x80|(cp&63)); }
                else if(cp<0x10000) { encoded[n++]=(uint8_t)(0xe0|(cp>>12));encoded[n++]=(uint8_t)(0x80|((cp>>6)&63));encoded[n++]=(uint8_t)(0x80|(cp&63)); }
                else { encoded[n++]=(uint8_t)(0xf0|(cp>>18));encoded[n++]=(uint8_t)(0x80|((cp>>12)&63));encoded[n++]=(uint8_t)(0x80|((cp>>6)&63));encoded[n++]=(uint8_t)(0x80|(cp&63)); }
                s=ud_bytes_put(&bytes,encoded,n);
            }
        } else s=UD_PARSE;
    }
    if(!s && (!complete || !ud_utf8_valid((const char *)bytes.data,bytes.size))) s=UD_PARSE;
    if(!s && bytes.size>UINT32_MAX) s=UD_OVERFLOW;
    if(!s) { *length=(uint32_t)bytes.size;s=ud_bytes_put(&bytes,"\0",1); }
    if(!s) { *out=(char *)bytes.data;bytes.data=NULL; }
    ud_bytes_clear(&bytes);return s;
}
static ud_status add_member(ud_json *v,char *key,uint32_t length,ud_json *child) {
    if(v->kind==UD_JSON_OBJECT)
        for(uint32_t i=0;i<v->count;i++)
            if(length==v->members[i].length && !memcmp(key,v->members[i].key,length)) return UD_PARSE;
    if(v->count==v->capacity) {
        uint32_t capacity=v->capacity ? v->capacity*2 : 8;
        if(capacity<v->capacity || (size_t)capacity>SIZE_MAX/sizeof(*v->members)) return UD_OVERFLOW;
        struct member *members=ud_alloc(v->ctx,(size_t)capacity*sizeof(*members));if(!members) return UD_RESOURCE;
        if(v->count) memcpy(members,v->members,v->count*sizeof(*members));
        ud_free(v->members);v->members=members;v->capacity=capacity;
    }
    v->members[v->count].key=key;v->members[v->count].length=length;v->members[v->count++].value=child;return UD_OK;
}
static ud_status value(json_parser *p,uint32_t depth,ud_json **out) {
    *out=NULL;whitespace(p);
    if(ud_is_cancelled(p->ctx)) return UD_CANCELLED;
    if(depth>64 || p->nodes>=1000000) return UD_RESOURCE;
    if(p->pos==p->size) return UD_PARSE;
    p->nodes++;ud_json *v=ud_alloc(p->ctx,sizeof(*v));if(!v) return UD_RESOURCE;v->ctx=p->ctx;
    char c=p->text[p->pos];ud_status s=UD_OK;
    if(c=='"') { v->kind=UD_JSON_STRING;s=string(p,&v->text,&v->length); }
    else if(c=='[' || c=='{') {
        v->kind=c=='['?UD_JSON_ARRAY:UD_JSON_OBJECT;char end=c=='['?']':'}';p->pos++;whitespace(p);
        if(p->pos<p->size && p->text[p->pos]==end) p->pos++;
        else while(!s) {
            char *key=NULL;uint32_t length=0;ud_json *child=NULL;
            if(v->kind==UD_JSON_OBJECT) {
                s=string(p,&key,&length);whitespace(p);
                if(!s && (p->pos==p->size || p->text[p->pos++]!=':')) s=UD_PARSE;
            }
            if(!s) s=value(p,depth+1,&child);
            if(!s) s=add_member(v,key,length,child);
            if(s) { ud_free(key);ud_json_destroy(child);break; }
            whitespace(p);
            if(p->pos==p->size) { s=UD_PARSE;break; }
            c=p->text[p->pos++];if(c==end) break;
            if(c!=',') { s=UD_PARSE;break; }
            whitespace(p);
        }
    } else if(c=='t' || c=='f' || c=='n') {
        const char *literal=c=='t'?"true":c=='f'?"false":"null";uint32_t n=c=='t'?4:c=='f'?5:4;
        if(p->size-p->pos<n || memcmp(p->text+p->pos,literal,n)) s=UD_PARSE;
        else { v->kind=c=='n'?UD_JSON_NULL:UD_JSON_BOOL;v->text=ud_strdup_n(p->ctx,literal,n);v->length=n;p->pos+=n;if(!v->text) s=UD_RESOURCE; }
    } else {
        v->kind=UD_JSON_NUMBER;uint32_t start=p->pos;
        if(c=='-') p->pos++;
        if(p->pos==p->size) s=UD_PARSE;
        else if(p->text[p->pos]=='0') p->pos++;
        else if(p->text[p->pos]>='1' && p->text[p->pos]<='9')
            while(p->pos<p->size && p->text[p->pos]>='0' && p->text[p->pos]<='9') p->pos++;
        else s=UD_PARSE;
        if(!s && p->pos<p->size && p->text[p->pos]=='.') {
            p->pos++;uint32_t first=p->pos;
            while(p->pos<p->size && p->text[p->pos]>='0' && p->text[p->pos]<='9') p->pos++;
            if(p->pos==first) s=UD_PARSE;
        }
        if(!s && p->pos<p->size && (p->text[p->pos]=='e' || p->text[p->pos]=='E')) {
            p->pos++;if(p->pos<p->size && (p->text[p->pos]=='-' || p->text[p->pos]=='+')) p->pos++;
            uint32_t first=p->pos;
            while(p->pos<p->size && p->text[p->pos]>='0' && p->text[p->pos]<='9') p->pos++;
            if(p->pos==first) s=UD_PARSE;
        }
        if(!s) { v->length=p->pos-start;v->text=ud_strdup_n(p->ctx,p->text+start,v->length);if(!v->text) s=UD_RESOURCE; }
    }
    if(s) ud_json_destroy(v);else *out=v;return s;
}
ud_status ud_json_parse(ud_context *ctx,const char *text,uint32_t length,ud_json **out,uint32_t *error_offset) {
    if(!out) return UD_INVALID;
    *out=NULL;if(error_offset) *error_offset=0;
    if(!ctx || (!text && length)) return UD_INVALID;
    json_parser p={ctx,text,length,0,0};ud_status s=value(&p,0,out);whitespace(&p);
    if(!s && p.pos!=p.size) { ud_json_destroy(*out);*out=NULL;s=UD_PARSE; }
    if(error_offset) *error_offset=p.pos;
    return s?ud_fail(ctx,s,"JSON document rejected at byte %u",p.pos):UD_OK;
}
int32_t ud_json_kind(const ud_json *v) { return v?v->kind:-1; }
uint32_t ud_json_size(const ud_json *v) { return v?v->count:0; }
const ud_json *ud_json_at(const ud_json *v,uint32_t index) { return v && index<v->count?v->members[index].value:NULL; }
const ud_json *ud_json_find(const ud_json *v,const char *key,uint32_t length) {
    if(!v || v->kind!=UD_JSON_OBJECT || (!key && length)) return NULL;
    for(uint32_t i=0;i<v->count;i++)
        if(v->members[i].length==length && (!length || !memcmp(v->members[i].key,key,length))) return v->members[i].value;
    return NULL;
}
ud_status ud_json_key(const ud_json *v,uint32_t index,const char **key,uint32_t *length) {
    if(!v || !key || !length || v->kind!=UD_JSON_OBJECT || index>=v->count) return UD_INVALID;
    *key=v->members[index].key;*length=v->members[index].length;return UD_OK;
}
ud_status ud_json_text(const ud_json *v,const char **text,uint32_t *length) {
    if(!v || !text || !length) return UD_INVALID;
    if(v->kind!=UD_JSON_STRING && v->kind!=UD_JSON_NUMBER && v->kind!=UD_JSON_BOOL) return UD_SCHEMA;
    *text=v->text;*length=v->length;return UD_OK;
}
ud_status ud_json_quote(ud_bytes *b,const char *text,uint32_t length) {
    UD_TRY(ud_bytes_put(b,"\"",1));
    for(uint32_t i=0;i<length;i++) {
        unsigned char c=(unsigned char)text[i];
        if(c=='"' || c=='\\') { UD_TRY(ud_bytes_put(b,"\\",1));UD_TRY(ud_bytes_put(b,&c,1)); }
        else if(c<32) { char escape[7];snprintf(escape,sizeof(escape),"\\u%04x",c);UD_TRY(ud_bytes_put(b,escape,6)); }
        else UD_TRY(ud_bytes_put(b,&c,1));
    }
    return ud_bytes_put(b,"\"",1);
}
static ud_status emit(ud_bytes *b,const ud_json *v) {
    if(ud_is_cancelled(b->ctx)) return UD_CANCELLED;
    if(v->kind==UD_JSON_STRING) return ud_json_quote(b,v->text,v->length);
    if(v->kind<=UD_JSON_NUMBER) return ud_bytes_put(b,v->text,v->length);
    UD_TRY(ud_bytes_put(b,v->kind==UD_JSON_ARRAY?"[":"{",1));
    for(uint32_t i=0;i<v->count;i++) {
        if(i) UD_TRY(ud_bytes_put(b,",",1));
        if(v->kind==UD_JSON_OBJECT) { UD_TRY(ud_json_quote(b,v->members[i].key,v->members[i].length));UD_TRY(ud_bytes_put(b,":",1)); }
        UD_TRY(emit(b,v->members[i].value));
    }
    return ud_bytes_put(b,v->kind==UD_JSON_ARRAY?"]":"}",1);
}
ud_status ud_json_serialize(const ud_json *v,char **out,uint32_t *length) {
    if(!v || !out || !length) return UD_INVALID;
    *out=NULL;*length=0;ud_bytes b={0};b.ctx=v->ctx;ud_status s=emit(&b,v);
    if(!s && b.size>UINT32_MAX) s=UD_OVERFLOW;
    if(!s) { *length=(uint32_t)b.size;s=ud_bytes_put(&b,"\0",1); }
    if(!s) { *out=(char *)b.data;b.data=NULL; }
    else *length=0;
    ud_bytes_clear(&b);return s;
}
