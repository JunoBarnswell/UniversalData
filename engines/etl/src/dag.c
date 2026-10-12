#include "ud/dag.h"
#include "ud/json.h"
#include "../../../components/data/internal.h"
typedef struct dag_node { ud_dag_node info;const ud_json *configuration;uint32_t incoming,outgoing; } dag_node;
typedef struct dag_edge { ud_dag_edge info;uint32_t from,to; } dag_edge;
struct ud_dag {
    ud_context *ctx;ud_json *document;
    dag_node *nodes;dag_edge *edges;
    uint32_t node_count,edge_count,*topology,*table,table_size;
    const ud_json *parameters;
    char *serialized;uint32_t length;uint8_t fingerprint[32];
};
static const ud_json *member(const ud_json *v,const char *name) { return ud_json_find(v,name,(uint32_t)strlen(name)); }
static int same(ud_string a,ud_string b) { return a.length==b.length && (!a.length || !memcmp(a.data,b.data,a.length)); }
static ud_status text(const ud_json *v,ud_string *s) {
    if(ud_json_kind(v)!=UD_JSON_STRING) return UD_SCHEMA;
    UD_TRY(ud_json_text(v,&s->data,&s->length));
    return s->length && s->length<=255 && !memchr(s->data,0,s->length)?UD_OK:UD_SCHEMA;
}
static uint32_t string_hash(ud_string key) {
    uint32_t hash=2166136261u;for(uint32_t j=0;j<key.length;j++) hash=(hash^(uint8_t)key.data[j])*16777619u;return hash;
}
static int lookup(const ud_dag *d,ud_string key) {
    uint32_t at=string_hash(key)&(d->table_size-1);
    while(d->table[at]) { uint32_t index=d->table[at]-1;if(same(d->nodes[index].info.id,key)) return (int)index;at=(at+1)&(d->table_size-1); }
    return -1;
}
static ud_status keys(const ud_json *v,const char *const *allowed,uint32_t count) {
    if(ud_json_kind(v)!=UD_JSON_OBJECT) return UD_SCHEMA;
    for(uint32_t j=0;j<ud_json_size(v);j++) {
        const char *name=NULL;uint32_t length=0;UD_TRY(ud_json_key(v,j,&name,&length));int found=0;
        for(uint32_t k=0;k<count;k++) if(strlen(allowed[k])==length && !memcmp(name,allowed[k],length)) { found=1;break; }
        if(!found) return UD_SCHEMA;
    }
    return UD_OK;
}
static ud_status unsigned_number(const ud_json *v,uint32_t *out) {
    const char *data=NULL;uint32_t length=0;
    if(ud_json_kind(v)!=UD_JSON_NUMBER) return UD_SCHEMA;
    UD_TRY(ud_json_text(v,&data,&length));uint32_t value=0;
    for(uint32_t i=0;i<length;i++) {
        uint32_t digit=(unsigned char)data[i]-'0';if(digit>9 || value>(UINT32_MAX-digit)/10) return UD_SCHEMA;value=value*10+digit;
    }
    *out=value;return UD_OK;
}
static int kind_name(const ud_json *json) {
    static const char *names[]={"null","boolean","number","string","array","object"};
    ud_string s={0};if(text(json,&s)) return -1;
    for(int j=0;j<6;j++) if(s.length==strlen(names[j]) && !memcmp(s.data,names[j],s.length)) return j;
    return -1;
}
static ud_status parameters(const ud_json *json) {
    if(ud_json_kind(json)!=UD_JSON_OBJECT || ud_json_size(json)>1024) return UD_SCHEMA;
    static const char *allowed[]={"kind","required","default"};
    for(uint32_t i=0;i<ud_json_size(json);i++) {
        const char *name=NULL;uint32_t length=0;UD_TRY(ud_json_key(json,i,&name,&length));
        if(!length || length>255 || memchr(name,0,length)) return UD_SCHEMA;
        const ud_json *def=ud_json_at(json,i);UD_TRY(keys(def,allowed,3));int kind=kind_name(member(def,"kind"));
        if(kind<0) return UD_SCHEMA;
        const ud_json *required=member(def,"required"),*value=member(def,"default");
        if(required && ud_json_kind(required)!=UD_JSON_BOOL) return UD_SCHEMA;
        if(value && ud_json_kind(value)!=kind) return UD_SCHEMA;
    }
    return UD_OK;
}
static ud_status references(const ud_json *configuration,const ud_json *params) {
    const ud_json *ref=member(configuration,"$parameter");
    if(ref) {
        ud_string name={0};UD_TRY(text(ref,&name));
        return ud_json_size(configuration)==1 && ud_json_find(params,name.data,name.length)?UD_OK:UD_SCHEMA;
    }
    for(uint32_t j=0;j<ud_json_size(configuration);j++) UD_TRY(references(ud_json_at(configuration,j),params));
    return UD_OK;
}
void ud_dag_destroy(ud_dag *d) {
    if(!d) return;
    for(uint32_t j=0;j<d->node_count;j++) ud_free((void *)d->nodes[j].info.configuration.data);
    ud_free(d->serialized);ud_free(d->nodes);ud_free(d->edges);ud_free(d->topology);ud_free(d->table);ud_json_destroy(d->document);ud_free(d);
}
static ud_status topology(ud_dag *d) {
    uint32_t n=d->node_count,m=d->edge_count;
    uint32_t *first=ud_alloc(d->ctx,n*sizeof(*first)),*next=ud_alloc(d->ctx,m*sizeof(*next));
    uint32_t *degree=ud_alloc(d->ctx,n*sizeof(*degree)),*ports=ud_alloc(d->ctx,n*sizeof(*ports));
    uint8_t *seen=ud_alloc(d->ctx,m?m:1);ud_status s=first && next && degree && ports && seen?UD_OK:UD_RESOURCE;
    if(!s) {
        uint32_t offset=0;
        for(uint32_t j=0;j<n;j++) {
            first[j]=UINT32_MAX;degree[j]=d->nodes[j].incoming;ports[j]=offset;offset+=degree[j];
            if((d->nodes[j].info.kind==UD_NODE_SOURCE && degree[j]) ||
               (d->nodes[j].info.kind!=UD_NODE_SOURCE && !degree[j]) ||
               (d->nodes[j].info.kind==UD_NODE_SINK && d->nodes[j].outgoing) ||
               (d->nodes[j].info.kind!=UD_NODE_SINK && !d->nodes[j].outgoing)) s=UD_SCHEMA;
        }
        for(uint32_t j=0;!s && j<m;j++) {
            dag_edge *e=d->edges+j;
            if(e->info.input_port>=degree[e->to] || seen[ports[e->to]+e->info.input_port]) { s=UD_SCHEMA;break; }
            seen[ports[e->to]+e->info.input_port]=1;next[j]=first[e->from];first[e->from]=j;
        }
        uint32_t head=0,tail=0;
        for(uint32_t j=0;!s && j<n;j++) if(!degree[j]) d->topology[tail++]=j;
        while(!s && head<tail) {
            uint32_t node=d->topology[head++];
            for(uint32_t edge=first[node];edge!=UINT32_MAX;edge=next[edge]) if(!--degree[d->edges[edge].to]) d->topology[tail++]=d->edges[edge].to;
        }
        if(!s && tail!=n) s=UD_SCHEMA;
    }
    ud_free(first);ud_free(next);ud_free(degree);ud_free(ports);ud_free(seen);return s;
}
ud_status ud_dag_create(ud_context *ctx,const char *json,uint32_t length,ud_dag **out) {
    if(!out) return UD_INVALID;
    *out=NULL;if(!ctx || (!json && length)) return UD_INVALID;
    ud_dag *d=ud_alloc(ctx,sizeof(*d));if(!d) return UD_RESOURCE;d->ctx=ctx;
    ud_status s=ud_json_parse(ctx,json,length,&d->document,NULL);
    static const char *root_keys[]={"version","nodes","edges","parameters"};
    if(!s) s=keys(d->document,root_keys,4);
    uint32_t version=0;if(!s) s=unsigned_number(member(d->document,"version"),&version);
    if(!s && version!=UD_PLAN_VERSION) s=UD_INCOMPATIBLE;
    const ud_json *nodes=member(d->document,"nodes"),*edges=member(d->document,"edges");d->parameters=member(d->document,"parameters");
    if(!s && (ud_json_kind(nodes)!=UD_JSON_ARRAY || ud_json_kind(edges)!=UD_JSON_ARRAY)) s=UD_SCHEMA;
    if(!s) s=parameters(d->parameters);
    uint32_t n=ud_json_size(nodes),m=ud_json_size(edges);
    if(!s && (!n || !m)) s=UD_SCHEMA;
    if(!s && (n>65536 || m>65536)) s=UD_RESOURCE;
    if(!s) {
        d->nodes=ud_alloc(ctx,n*sizeof(*d->nodes));d->edges=ud_alloc(ctx,m*sizeof(*d->edges));d->topology=ud_alloc(ctx,n*sizeof(*d->topology));
        d->table_size=1;while(d->table_size<n*2) d->table_size*=2;d->table=ud_alloc(ctx,d->table_size*sizeof(*d->table));
        if(!d->nodes || !d->edges || !d->topology || !d->table) s=UD_RESOURCE;
    }
    static const char *node_keys[]={"id","kind","operation","config"};
    for(uint32_t i=0;!s && i<n;i++) {
        const ud_json *node=ud_json_at(nodes,i);dag_node *v=d->nodes+i;d->node_count=i+1;
        s=keys(node,node_keys,4);v->info.struct_size=sizeof(v->info);v->info.version=UD_PLAN_VERSION;
        if(!s) s=text(member(node,"id"),&v->info.id);
        if(!s) s=text(member(node,"operation"),&v->info.operation);
        ud_string name={0};if(!s) s=text(member(node,"kind"),&name);
        const char *kinds[]={"source","transform","quality","sink"};
        for(uint32_t j=0;!s && j<4;j++) if(name.length==strlen(kinds[j]) && !memcmp(name.data,kinds[j],name.length)) v->info.kind=(int32_t)j+1;
        if(!s && !v->info.kind) s=UD_SCHEMA;
        v->configuration=member(node,"config");if(!s && ud_json_kind(v->configuration)!=UD_JSON_OBJECT) s=UD_SCHEMA;
        if(!s) s=references(v->configuration,d->parameters);
        char *config=NULL;uint32_t config_length=0;if(!s) s=ud_json_serialize(v->configuration,&config,&config_length);
        v->info.configuration=(ud_string){config,config_length};
        if(!s && lookup(d,v->info.id)>=0) s=UD_SCHEMA;
        if(!s) { uint32_t at=string_hash(v->info.id)&(d->table_size-1);while(d->table[at]) at=(at+1)&(d->table_size-1);d->table[at]=i+1; }
    }
    static const char *edge_keys[]={"from","to","port"};
    for(uint32_t i=0;!s && i<m;i++) {
        const ud_json *edge=ud_json_at(edges,i);dag_edge *v=d->edges+i;
        s=keys(edge,edge_keys,3);if(!s) s=text(member(edge,"from"),&v->info.from);if(!s) s=text(member(edge,"to"),&v->info.to);
        if(!s) s=unsigned_number(member(edge,"port"),&v->info.input_port);
        int a=-1,b=-1;if(!s) { a=lookup(d,v->info.from);b=lookup(d,v->info.to);if(a<0 || b<0) s=UD_SCHEMA; }
        if(!s) { v->from=(uint32_t)a;v->to=(uint32_t)b;d->nodes[a].outgoing++;d->nodes[b].incoming++;d->edge_count++; }
    }
    if(!s) s=topology(d);
    if(!s) s=ud_json_serialize(d->document,&d->serialized,&d->length);
    if(!s) ud_hash(d->serialized,d->length,d->fingerprint);
    if(s) { ud_dag_destroy(d);return ud_fail(ctx,s,"DAG contract validation failed"); }
    *out=d;return UD_OK;
}
uint32_t ud_dag_nodes(const ud_dag *d) { return d?d->node_count:0; }
uint32_t ud_dag_edges(const ud_dag *d) { return d?d->edge_count:0; }
ud_status ud_dag_node_get(const ud_dag *d,uint32_t index,ud_dag_node *out) { if(!d || !out || index>=d->node_count) return UD_INVALID;*out=d->nodes[index].info;return UD_OK; }
ud_status ud_dag_edge_get(const ud_dag *d,uint32_t index,ud_dag_edge *out) { if(!d || !out || index>=d->edge_count) return UD_INVALID;*out=d->edges[index].info;return UD_OK; }
ud_status ud_dag_topology(const ud_dag *d,const uint32_t **out) { if(!d || !out) return UD_INVALID;*out=d->topology;return UD_OK; }
void ud_dag_fingerprint(const ud_dag *d,uint8_t out[32]) { if(d && out) memcpy(out,d->fingerprint,32); }
ud_status ud_dag_serialize(const ud_dag *d,char **out,uint32_t *length) {
    if(!d || !out || !length) return UD_INVALID;
    *out=ud_strdup_n(d->ctx,d->serialized,d->length);*length=*out?d->length:0;return *out?UD_OK:UD_RESOURCE;
}
static ud_status json_put(ud_bytes *b,const ud_json *v) {
    char *text=NULL;uint32_t length=0;ud_status s=ud_json_serialize(v,&text,&length);
    if(!s) s=ud_bytes_put(b,text,length);ud_free(text);return s;
}
static ud_status config_put(ud_bytes *b,const ud_json *v,const ud_json *declarations,const ud_json *values) {
    const ud_json *ref=member(v,"$parameter");
    if(ref) {
        ud_string name={0};UD_TRY(text(ref,&name));const ud_json *bound=ud_json_find(values,name.data,name.length);
        if(!bound) bound=member(ud_json_find(declarations,name.data,name.length),"default");
        if(bound) return json_put(b,bound);
        return kind_name(member(ud_json_find(declarations,name.data,name.length),"kind"))==UD_JSON_NULL?
            ud_bytes_put(b,"null",4):UD_SCHEMA;
    }
    int kind=ud_json_kind(v);if(kind!=UD_JSON_OBJECT && kind!=UD_JSON_ARRAY) return json_put(b,v);
    UD_TRY(ud_bytes_put(b,kind==UD_JSON_OBJECT?"{":"[",1));
    for(uint32_t j=0;j<ud_json_size(v);j++) {
        if(j) UD_TRY(ud_bytes_put(b,",",1));
        if(kind==UD_JSON_OBJECT) { const char *name=NULL;uint32_t length=0;UD_TRY(ud_json_key(v,j,&name,&length));UD_TRY(ud_json_quote(b,name,length));UD_TRY(ud_bytes_put(b,":",1)); }
        UD_TRY(config_put(b,ud_json_at(v,j),declarations,values));
    }
    return ud_bytes_put(b,kind==UD_JSON_OBJECT?"}":"]",1);
}
static ud_status emit_node(ud_bytes *b,const ud_dag_node *v,const ud_json *configuration,const ud_json *params,const ud_json *values) {
    static const char *kinds[]={"source","transform","quality","sink"};
    UD_TRY(ud_bytes_put(b,"{\"id\":",6));UD_TRY(ud_json_quote(b,v->id.data,v->id.length));
    UD_TRY(ud_bytes_put(b,",\"kind\":",8));UD_TRY(ud_json_quote(b,kinds[v->kind-1],(uint32_t)strlen(kinds[v->kind-1])));
    UD_TRY(ud_bytes_put(b,",\"operation\":",13));UD_TRY(ud_json_quote(b,v->operation.data,v->operation.length));
    UD_TRY(ud_bytes_put(b,",\"config\":",10));
    if(values) UD_TRY(config_put(b,configuration,params,values));else UD_TRY(json_put(b,configuration));
    return ud_bytes_put(b,"}",1);
}
static ud_status emit_edge(ud_bytes *b,const ud_dag_edge *e) {
    UD_TRY(ud_bytes_put(b,"{\"from\":",8));UD_TRY(ud_json_quote(b,e->from.data,e->from.length));
    UD_TRY(ud_bytes_put(b,",\"to\":",6));UD_TRY(ud_json_quote(b,e->to.data,e->to.length));
    char port[40];int n=snprintf(port,sizeof(port),",\"port\":%u}",e->input_port);return ud_bytes_put(b,port,(size_t)n);
}
ud_status ud_dag_build(ud_context *ctx,uint32_t version,const ud_dag_node *nodes,uint32_t n,
                       const ud_dag_edge *edges,uint32_t m,ud_string params,ud_dag **out) {
    if(!out) return UD_INVALID;
    *out=NULL;if(!ctx || !nodes || !edges || !n || !m || n>65536 || m>65536 || (!params.data && params.length)) return UD_INVALID;
    if(version!=UD_PLAN_VERSION) return UD_INCOMPATIBLE;
    ud_json *parameters_json=NULL;ud_status s=ud_json_parse(ctx,params.data?params.data:"{}",params.data?params.length:2,&parameters_json,NULL);
    if(!s) s=parameters(parameters_json);ud_bytes b={0};b.ctx=ctx;
    if(!s) s=ud_bytes_put(&b,"{\"version\":2,\"nodes\":[",22);
    for(uint32_t j=0;!s && j<n;j++) {
        const ud_dag_node *v=nodes+j;
        if(v->struct_size<sizeof(*v) || v->version!=UD_PLAN_VERSION) { s=UD_INCOMPATIBLE;break; }
        if(!v->id.data || !v->id.length || v->id.length>255 || !ud_utf8_valid(v->id.data,v->id.length) ||
           !v->operation.data || !v->operation.length || v->operation.length>255 || !ud_utf8_valid(v->operation.data,v->operation.length) ||
           memchr(v->id.data,0,v->id.length) || memchr(v->operation.data,0,v->operation.length) || v->kind<1 || v->kind>4) { s=UD_INVALID;break; }
        ud_json *configuration=NULL;s=ud_json_parse(ctx,v->configuration.data,v->configuration.length,&configuration,NULL);
        if(!s && ud_json_kind(configuration)!=UD_JSON_OBJECT) s=UD_SCHEMA;
        if(!s && j) s=ud_bytes_put(&b,",",1);if(!s) s=emit_node(&b,v,configuration,parameters_json,NULL);ud_json_destroy(configuration);
    }
    if(!s) s=ud_bytes_put(&b,"],\"edges\":[",11);
    for(uint32_t j=0;!s && j<m;j++) {
        const ud_dag_edge *e=edges+j;
        if(!e->from.data || !e->from.length || e->from.length>255 || !e->to.data || !e->to.length || e->to.length>255 ||
           !ud_utf8_valid(e->from.data,e->from.length) || !ud_utf8_valid(e->to.data,e->to.length)) { s=UD_INVALID;break; }
        if(j) s=ud_bytes_put(&b,",",1);if(!s) s=emit_edge(&b,e);
    }
    if(!s) s=ud_bytes_put(&b,"],\"parameters\":",15);if(!s) s=json_put(&b,parameters_json);if(!s) s=ud_bytes_put(&b,"}",1);
    if(!s && b.size>UINT32_MAX) s=UD_OVERFLOW;if(!s) s=ud_dag_create(ctx,(const char *)b.data,(uint32_t)b.size,out);
    ud_json_destroy(parameters_json);ud_bytes_clear(&b);return s;
}
ud_status ud_dag_bind(const ud_dag *d,ud_string values,ud_dag **out) {
    if(!out) return UD_INVALID;
    *out=NULL;if(!d || (!values.data && values.length)) return UD_INVALID;
    ud_json *json=NULL;UD_TRY(ud_json_parse(d->ctx,values.data?values.data:"{}",values.data?values.length:2,&json,NULL));
    ud_status s=ud_json_kind(json)==UD_JSON_OBJECT?UD_OK:UD_SCHEMA;
    for(uint32_t j=0;!s && j<ud_json_size(json);j++) {
        const char *key=NULL;uint32_t length=0;s=ud_json_key(json,j,&key,&length);
        if(!s && !ud_json_find(d->parameters,key,length)) s=UD_SCHEMA;
    }
    for(uint32_t j=0;!s && j<ud_json_size(d->parameters);j++) {
        const char *key=NULL;uint32_t length=0;s=ud_json_key(d->parameters,j,&key,&length);
        const ud_json *def=ud_json_at(d->parameters,j),*provided=ud_json_find(json,key,length),*value=provided?provided:member(def,"default");
        if(value && ud_json_kind(value)!=kind_name(member(def,"kind"))) s=UD_SCHEMA;
        const ud_json *required=member(def,"required");const char *flag=NULL;uint32_t size=0;
        if(required && !value && !s) { s=ud_json_text(required,&flag,&size);if(!s && size==4 && !memcmp(flag,"true",4)) s=UD_SCHEMA; }
    }
    ud_bytes b={0};b.ctx=d->ctx;if(!s) s=ud_bytes_put(&b,"{\"version\":2,\"nodes\":[",22);
    for(uint32_t j=0;!s && j<d->node_count;j++) {
        if(j) s=ud_bytes_put(&b,",",1);if(!s) s=emit_node(&b,&d->nodes[j].info,d->nodes[j].configuration,d->parameters,json);
    }
    if(!s) s=ud_bytes_put(&b,"],\"edges\":[",11);
    for(uint32_t j=0;!s && j<d->edge_count;j++) { if(j) s=ud_bytes_put(&b,",",1);if(!s) s=emit_edge(&b,&d->edges[j].info); }
    if(!s) s=ud_bytes_put(&b,"],\"parameters\":{}}",18);
    if(!s && b.size>UINT32_MAX) s=UD_OVERFLOW;if(!s) s=ud_dag_create(d->ctx,(const char *)b.data,(uint32_t)b.size,out);
    ud_json_destroy(json);ud_bytes_clear(&b);return s;
}
