#include "internal.h"
/* Template instances expand once before ordinary package binding. Every node is
 * thereafter compiled by the existing Registry/Schema/Plan path. */
static ud_status serialize(ud_bytes*b,const ud_json*j){char*text=NULL;uint32_t n;UD_TRY(ud_json_serialize(j,&text,&n));ud_status s=ud_bytes_put(b,text,n);ud_free(text);return s;}
static ud_status instantiate(ud_bytes*b,const ud_json*j,const ud_json*parameters,uint32_t depth){if(depth>64)return UD_SCHEMA;int kind=ud_json_kind(j);const ud_json*slot=kind==UD_JSON_OBJECT?ud_member(j,"$template"):NULL;
    if(slot){if(ud_json_size(j)!=1)return UD_SCHEMA;ud_string name;UD_TRY(ud_json_string(slot,&name));const ud_json*value=ud_json_find(parameters,name.data,name.length);return value?serialize(b,value):UD_SCHEMA;}
    if(kind!=UD_JSON_OBJECT&&kind!=UD_JSON_ARRAY)return serialize(b,j);
    UD_TRY(ud_bytes_put(b,kind==UD_JSON_OBJECT?"{":"[",1));
    for(uint32_t i=0;i<ud_json_size(j);i++){if(i)UD_TRY(ud_bytes_put(b,",",1));if(kind==UD_JSON_OBJECT){const char*key;uint32_t n;UD_TRY(ud_json_key(j,i,&key,&n));UD_TRY(ud_json_quote(b,key,n));UD_TRY(ud_bytes_put(b,":",1));UD_TRY(instantiate(b,ud_json_find(j,key,n),parameters,depth+1));}else UD_TRY(instantiate(b,ud_json_at(j,i),parameters,depth+1));}
    return ud_bytes_put(b,kind==UD_JSON_OBJECT?"}":"]",1);
}
static ud_status instances(ud_context*c,const ud_json*root,const char*part,ud_bytes*b,uint32_t*written){const ud_json*definitions=ud_member(root,"templates"),*entries=ud_member(root,"instances");if(ud_json_kind(definitions)!=UD_JSON_ARRAY||!ud_json_size(definitions)||ud_json_kind(entries)!=UD_JSON_ARRAY||!ud_json_size(entries)||ud_json_size(entries)>1024)return UD_SCHEMA;
    for(uint32_t i=0;i<ud_json_size(entries);i++){const ud_json*entry=ud_json_at(entries,i);static const char*keys[]={"template","parameters"};UD_TRY(ud_json_keys(entry,keys,2));ud_string id;UD_TRY(ud_json_string(ud_member(entry,"template"),&id));const ud_json*definition=NULL;
        for(uint32_t k=0;k<ud_json_size(definitions);k++){const ud_json*d=ud_json_at(definitions,k);ud_string name;static const char*allowed[]={"id","parameters","dataFlow","task"};UD_TRY(ud_json_keys(d,allowed,4));UD_TRY(ud_json_string(ud_member(d,"id"),&name));if(ud_same(id,name)){if(definition)return UD_SCHEMA;definition=d;}}
        if(!definition||!ud_member(definition,part))return UD_SCHEMA;ud_json*parameters=NULL;UD_TRY(ud_json_bind_schema(c,ud_member(definition,"parameters"),ud_member(entry,"parameters"),&parameters));ud_status s=*written?ud_bytes_put(b,",",1):UD_OK;if(!s)s=instantiate(b,ud_member(definition,part),parameters,0);ud_json_destroy(parameters);if(s)return s;(*written)++;
    }
    return UD_OK;
}
ud_status ud_package_instantiate(ud_context*c,ud_string input,char**out,uint32_t*length){if(!c||!out||!length)return UD_INVALID;*out=NULL;*length=0;ud_json*root=NULL;UD_TRY(ud_json_parse(c,input.data,input.length,&root,NULL));ud_bytes b={.ctx=c,.limit=64u*1024u*1024u};ud_status s=ud_json_kind(root)==UD_JSON_OBJECT?UD_OK:UD_SCHEMA;uint32_t members=0;
    if(!s&&!ud_member(root,"templates")){s=ud_json_serialize(root,out,length);ud_json_destroy(root);return s;}
    if(!s)s=ud_bytes_put(&b,"{",1);
    for(uint32_t i=0;!s&&i<ud_json_size(root);i++){const char*key;uint32_t n;s=ud_json_key(root,i,&key,&n);if(s)break;if((n==9&&!memcmp(key,"templates",9))||(n==9&&!memcmp(key,"instances",9))||(n==9&&!memcmp(key,"dataFlows",9))||(n==11&&!memcmp(key,"controlFlow",11)))continue;if(members++)s=ud_bytes_put(&b,",",1);if(!s)s=ud_json_quote(&b,key,n);if(!s)s=ud_bytes_put(&b,":",1);if(!s)s=serialize(&b,ud_json_find(root,key,n));}
    if(!s&&members)s=ud_bytes_put(&b,",",1);if(!s)s=ud_bytes_put(&b,"\"dataFlows\":[",13);uint32_t written=0;const ud_json*flows=ud_member(root,"dataFlows");
    if(flows&&ud_json_kind(flows)!=UD_JSON_ARRAY)s=UD_SCHEMA;for(uint32_t i=0;!s&&i<ud_json_size(flows);i++){if(written++)s=ud_bytes_put(&b,",",1);if(!s)s=serialize(&b,ud_json_at(flows,i));}if(!s)s=instances(c,root,"dataFlow",&b,&written);
    if(!s)s=ud_bytes_put(&b,"],\"controlFlow\":{\"tasks\":[",26);written=0;const ud_json*control=ud_member(root,"controlFlow"),*tasks=ud_member(control,"tasks");if(tasks&&ud_json_kind(tasks)!=UD_JSON_ARRAY)s=UD_SCHEMA;
    for(uint32_t i=0;!s&&i<ud_json_size(tasks);i++){if(written++)s=ud_bytes_put(&b,",",1);if(!s)s=serialize(&b,ud_json_at(tasks,i));}if(!s)s=instances(c,root,"task",&b,&written);if(!s)s=ud_bytes_put(&b,"]",1);
    for(uint32_t i=0;!s&&i<ud_json_size(control);i++){const char*key;uint32_t n;s=ud_json_key(control,i,&key,&n);if(s)break;if(n==5&&!memcmp(key,"tasks",5))continue;if(!s)s=ud_bytes_put(&b,",",1);if(!s)s=ud_json_quote(&b,key,n);if(!s)s=ud_bytes_put(&b,":",1);if(!s)s=serialize(&b,ud_json_find(control,key,n));}
    if(!s)s=ud_bytes_put(&b,"}}",2);if(!s){*length=(uint32_t)b.size;s=ud_bytes_put(&b,"",1);}if(!s){*out=(char*)b.data;b.data=NULL;}ud_bytes_clear(&b);ud_json_destroy(root);return s;
}
