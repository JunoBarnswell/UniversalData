#include "internal.h"
const ud_json *ud_member(const ud_json *v,const char *s) { return ud_json_find(v,s,(uint32_t)strlen(s)); }
int ud_named(ud_string s,const char *v) { return s.length==strlen(v) && !memcmp(s.data,v,s.length); }
int ud_same(ud_string a,ud_string b) { return a.length==b.length && (!a.length||!memcmp(a.data,b.data,a.length)); }
ud_status ud_json_string(const ud_json *v,ud_string *s) {
    if(ud_json_kind(v)!=UD_JSON_STRING) return UD_SCHEMA;
    UD_TRY(ud_json_text(v,&s->data,&s->length));
    if(!s->length || s->length>32767 || memchr(s->data,0,s->length)) return UD_SCHEMA;
    return UD_OK;
}
ud_status ud_json_uint(const ud_json *v,uint64_t *out) {
    if(ud_json_kind(v)!=UD_JSON_NUMBER) return UD_SCHEMA;
    const char *text;uint32_t n;UD_TRY(ud_json_text(v,&text,&n));uint64_t value=0;
    for(uint32_t i=0;i<n;i++) { unsigned d=(unsigned char)text[i]-'0';if(d>9 || value>(UINT64_MAX-d)/10) return UD_OVERFLOW;value=value*10+d; }
    *out=value;return UD_OK;
}
ud_status ud_json_keys(const ud_json *v,const char *const *keys,uint32_t n) {
    if(ud_json_kind(v)!=UD_JSON_OBJECT) return UD_SCHEMA;
    for(uint32_t i=0;i<ud_json_size(v);i++) {
        const char *key;uint32_t len;UD_TRY(ud_json_key(v,i,&key,&len));int found=0;
        for(uint32_t j=0;j<n;j++) if(len==strlen(keys[j])&&!memcmp(key,keys[j],len)) found=1;
        if(!found) return UD_SCHEMA;
    }
    return UD_OK;
}
ud_status ud_package_check(const ud_package *p) {
    static const char *keys[]={"version","id","parameters","variables","connections","controlFlow","dataFlows","policy","eventHandlers"};
    UD_TRY(ud_json_keys(p->document,keys,9));uint64_t version;UD_TRY(ud_json_uint(ud_member(p->document,"version"),&version));
    if(version!=UD_PACKAGE_VERSION) return UD_INCOMPATIBLE;
    if(ud_json_kind(ud_member(p->document,"controlFlow"))!=UD_JSON_OBJECT ||
       ud_json_kind(ud_member(p->document,"dataFlows"))!=UD_JSON_ARRAY) return UD_SCHEMA;
    const char *objects[]={"parameters","variables","policy","eventHandlers"};
    for(int i=0;i<4;i++) { const ud_json *v=ud_member(p->document,objects[i]);if(v&&ud_json_kind(v)!=UD_JSON_OBJECT) return UD_SCHEMA; }
    const ud_json *connections=ud_member(p->document,"connections");
    if(connections && ud_json_kind(connections)!=UD_JSON_ARRAY) return UD_SCHEMA;
    const ud_json *flows=ud_member(p->document,"dataFlows");
    for(uint32_t i=0;i<ud_json_size(flows);i++) {
        ud_string id;UD_TRY(ud_json_string(ud_member(ud_json_at(flows,i),"id"),&id));
        for(uint32_t j=0;j<i;j++) { ud_string other;UD_TRY(ud_json_string(ud_member(ud_json_at(flows,j),"id"),&other));if(ud_same(id,other)) return UD_SCHEMA; }
    }
    return UD_OK;
}
ud_status ud_package_create(ud_context *c,ud_string json,ud_package **out) {
    if(!out) return UD_INVALID;*out=NULL;if(!c||!json.data) return UD_INVALID;
    ud_package *p=ud_alloc(c,sizeof(*p));if(!p) return UD_RESOURCE;p->ctx=c;
    ud_status s=ud_json_parse(c,json.data,json.length,&p->document,NULL);
    if(!s) s=ud_package_check(p);
    if(!s) s=ud_json_string(ud_member(p->document,"id"),&p->id);
    if(!s) { char *text=NULL;uint32_t len;s=ud_json_serialize(p->document,&text,&len);if(!s) ud_hash(text,len,p->fingerprint);ud_free(text); }
    if(s) { ud_package_destroy(p);return ud_fail(c,s,"invalid Package definition"); }*out=p;return UD_OK;
}
ud_status ud_package_serialize(const ud_package *p,char **out,uint32_t *length) { return p?ud_json_serialize(p->document,out,length):UD_INVALID; }
void ud_package_destroy(ud_package *p) { if(p) { ud_json_destroy(p->document);ud_free(p); } }
ud_status ud_scalar_value(const ud_json *v,ud_type type,ud_value *out) {
    if(!v || type<UD_BOOL || type>UD_UTF8) return UD_SCHEMA;
    memset(out,0,sizeof(*out));out->type=type;
    int kind=ud_json_kind(v);if(kind==UD_JSON_NULL) { out->is_null=1;return UD_OK; }
    if((type==UD_BOOL&&kind!=UD_JSON_BOOL)||(type==UD_UTF8&&kind!=UD_JSON_STRING)||
       ((type==UD_INT64||type==UD_FLOAT64)&&kind!=UD_JSON_NUMBER)) return UD_SCHEMA;
    const char *text;uint32_t len;UD_TRY(ud_json_text(v,&text,&len));
    return ud_value_parse(NULL,type,text,len,out);
}
static ud_type type_name(ud_string s) { return ud_named(s,"bool")?UD_BOOL:ud_named(s,"int64")?UD_INT64:ud_named(s,"float64")?UD_FLOAT64:ud_named(s,"utf8")?UD_UTF8:0; }
ud_status ud_scope_enter(ud_execution *e,const ud_json *defs,ud_scope *scope) {
    scope->parent=e->scope;scope->values=NULL;e->scope=scope;
    if(defs && ud_json_kind(defs)!=UD_JSON_OBJECT) return UD_SCHEMA;
    for(uint32_t i=0;i<ud_json_size(defs);i++) {
        const char *key;uint32_t len;UD_TRY(ud_json_key(defs,i,&key,&len));
        const ud_json *def=ud_json_at(defs,i);ud_string t;UD_TRY(ud_json_string(ud_member(def,"type"),&t));
        ud_variable *v=ud_alloc(e->ctx,sizeof(*v));if(!v) return UD_RESOURCE;
        v->next=scope->values;scope->values=v;v->name=(ud_string){key,len};
        ud_value value;UD_TRY(ud_scalar_value(ud_member(def,"default"),type_name(t),&value));
        UD_TRY(ud_copy_value(e->ctx,&value,&v->value,0));
    }
    return UD_OK;
}
void ud_scope_leave(ud_execution *e,ud_scope *scope) {
    ud_variable *v=scope->values;while(v) { ud_variable *next=v->next;ud_clear_value(&v->value);ud_free(v);v=next; }
    e->scope=scope->parent;scope->values=NULL;
}
ud_status ud_execution_variable(const ud_execution *e,ud_string name,ud_value *out) {
    if(!e||!out) return UD_INVALID;
    for(ud_scope *s=e->scope;s;s=s->parent) for(ud_variable *v=s->values;v;v=v->next) if(ud_same(v->name,name)) { *out=v->value;return UD_OK; }
    const ud_json *def=ud_json_find(ud_member(e->package->document,"parameters"),name.data,name.length);
    if(!def) return UD_SCHEMA;
    const ud_json *provided=ud_json_find(e->parameters,name.data,name.length),*value=provided?provided:ud_member(def,"default");
    ud_string type;UD_TRY(ud_json_string(ud_member(def,"type"),&type));
    return ud_scalar_value(value,type_name(type),out);
}
ud_status ud_execution_set_variable(ud_execution *e,ud_string name,const ud_value *value) {
    if(!e||!value) return UD_INVALID;
    for(ud_scope *s=e->scope;s;s=s->parent) for(ud_variable *v=s->values;v;v=v->next) if(ud_same(v->name,name)) {
        if(v->readonly) return UD_AUTH;if(v->value.type!=value->type) return UD_SCHEMA;
        ud_value copy={0};UD_TRY(ud_copy_value(e->ctx,value,&copy,0));ud_clear_value(&v->value);v->value=copy;return UD_OK;
    }
    return UD_SCHEMA;
}
ud_status ud_resolve_value(void *user,ud_string name,ud_value *out) { return ud_execution_variable(user,name,out); }
ud_status ud_expression_json(ud_execution *e,const ud_json *value,const ud_batch *schema,ud_expression **out) {
    char *json=NULL;uint32_t length;UD_TRY(ud_json_serialize(value,&json,&length));
    ud_expression_options o={sizeof(o),UD_ABI_VERSION,schema,ud_resolve_value,e};
    ud_status s=ud_expression_create(e->ctx,(ud_string){json,length},&o,out);ud_free(json);return s;
}
static ud_status value_json(ud_bytes *b,const ud_value *v) {
    if(v->is_null) return ud_bytes_put(b,"null",4);
    if(v->type==UD_UTF8) return ud_json_quote(b,v->text,v->text_length);
    char buffer[128];uint32_t n;UD_TRY(ud_value_format(v,buffer,sizeof(buffer),&n));return ud_bytes_put(b,buffer,n-1);
}
static ud_status bound_put(ud_execution *e,ud_bytes *b,const ud_json *v) {
    const ud_json *p=ud_member(v,"$parameter"),*var=ud_member(v,"$variable");
    if(p||var) {
        if(ud_json_size(v)!=1 || (p&&var)) return UD_SCHEMA;
        ud_string name;UD_TRY(ud_json_string(p?p:var,&name));
        if(p) {
            const ud_json *def=ud_json_find(ud_member(e->package->document,"parameters"),name.data,name.length);
            if(!def) return UD_SCHEMA;const ud_json *provided=ud_json_find(e->parameters,name.data,name.length);
            const ud_json *value=provided?provided:ud_member(def,"default");if(!value) return UD_SCHEMA;
            return bound_put(e,b,value);
        }
        ud_value value;UD_TRY(ud_execution_variable(e,name,&value));return value_json(b,&value);
    }
    int kind=ud_json_kind(v);
    if(kind!=UD_JSON_OBJECT&&kind!=UD_JSON_ARRAY) {
        char *json;uint32_t length;UD_TRY(ud_json_serialize(v,&json,&length));ud_status s=ud_bytes_put(b,json,length);ud_free(json);return s;
    }
    UD_TRY(ud_bytes_put(b,kind==UD_JSON_OBJECT?"{":"[",1));
    for(uint32_t i=0;i<ud_json_size(v);i++) {
        if(i) UD_TRY(ud_bytes_put(b,",",1));
        if(kind==UD_JSON_OBJECT) { const char *key;uint32_t len;UD_TRY(ud_json_key(v,i,&key,&len));UD_TRY(ud_json_quote(b,key,len));UD_TRY(ud_bytes_put(b,":",1)); }
        UD_TRY(bound_put(e,b,ud_json_at(v,i)));
    }
    return ud_bytes_put(b,kind==UD_JSON_OBJECT?"}":"]",1);
}
ud_status ud_bound_config(ud_execution *e,const ud_json *v,ud_json **out) {
    ud_bytes b={.ctx=e->ctx};ud_status s=bound_put(e,&b,v);
    if(!s && b.size>UINT32_MAX) s=UD_OVERFLOW;
    if(!s) s=ud_json_parse(e->ctx,(const char *)b.data,(uint32_t)b.size,out,NULL);
    ud_bytes_clear(&b);return s;
}
