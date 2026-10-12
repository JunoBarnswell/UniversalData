#include "ud/dag.h"
#include "check.h"
#include <string.h>
static ud_string str(const char *s) { return (ud_string){s,(uint32_t)strlen(s)}; }
static ud_dag_node node(const char *id,int kind,const char *operation,const char *config) {
    return (ud_dag_node){sizeof(ud_dag_node),UD_PLAN_VERSION,str(id),kind,str(operation),str(config)};
}
int main(void) {
    ud_context *ctx=NULL;OK(ud_context_create(NULL,&ctx));
    ud_dag_node nodes[]={node("read",UD_NODE_SOURCE,"json.source","{\"path\":{\"$parameter\":\"path\"}}"),
        node("map",UD_NODE_TRANSFORM,"map","{}"),node("quality",UD_NODE_QUALITY,"quality","{}"),node("write",UD_NODE_SINK,"sqlite.sink","{}")};
    ud_dag_edge edges[]={{str("read"),str("map"),0},{str("read"),str("quality"),0},
        {str("map"),str("write"),0},{str("quality"),str("write"),1}};
    ud_string params=str("{\"path\":{\"kind\":\"string\",\"required\":true},\"batch\":{\"kind\":\"number\",\"default\":512}}");
    ud_dag *d=NULL;OK(ud_dag_build(ctx,UD_PLAN_VERSION,nodes,4,edges,4,params,&d));
    CHECK(ud_dag_nodes(d)==4 && ud_dag_edges(d)==4);const uint32_t *topology=NULL;OK(ud_dag_topology(d,&topology));
    CHECK(topology[0]==0 && topology[3]==3);
    char *json=NULL;uint32_t length=0;OK(ud_dag_serialize(d,&json,&length));ud_dag *copy=NULL;
    OK(ud_dag_create(ctx,json,length,&copy));uint8_t a[32],b[32];ud_dag_fingerprint(d,a);ud_dag_fingerprint(copy,b);CHECK(!memcmp(a,b,32));
    ud_free(json);ud_dag_destroy(copy);ud_dag *bound=NULL;
    OK(ud_dag_bind(d,str("{\"path\":\"中文\\\"/file.ndjson\"}"),&bound));
    ud_dag_node info;OK(ud_dag_node_get(bound,0,&info));CHECK(info.kind==UD_NODE_SOURCE);
    CHECK(info.configuration.length==31 && !memcmp(info.configuration.data,"{\"path\":\"中文\\\"/file.ndjson\"}",31));
    ud_dag_destroy(bound);
    CHECK(ud_dag_bind(d,str("{}"),&bound)==UD_SCHEMA && !bound);
    CHECK(ud_dag_bind(d,str("{\"path\":12}"),&bound)==UD_SCHEMA && !bound);
    CHECK(ud_dag_bind(d,str("{\"path\":\"x\",\"unknown\":1}"),&bound)==UD_SCHEMA && !bound);
    ud_dag_destroy(d);
    edges[3].input_port=0;CHECK(ud_dag_build(ctx,2,nodes,4,edges,4,params,&d)==UD_SCHEMA && !d);edges[3].input_port=1;
    edges[0].from=str("missing");CHECK(ud_dag_build(ctx,2,nodes,4,edges,4,params,&d)==UD_SCHEMA && !d);edges[0].from=str("read");
    nodes[1].id=str("read");CHECK(ud_dag_build(ctx,2,nodes,4,edges,4,params,&d)==UD_SCHEMA && !d);nodes[1].id=str("map");
    nodes[0].configuration=str("{\"$parameter\":\"unknown\"}");CHECK(ud_dag_build(ctx,2,nodes,4,edges,4,params,&d)==UD_SCHEMA && !d);
    nodes[0].configuration=str("{}");
    ud_dag_edge cyclic[]={{str("read"),str("map"),0},{str("map"),str("quality"),0},
        {str("quality"),str("map"),1},{str("quality"),str("write"),0}};
    CHECK(ud_dag_build(ctx,2,nodes,4,cyclic,4,(ud_string){0},&d)==UD_SCHEMA && !d);
    CHECK(ud_dag_build(ctx,1,nodes,4,edges,4,params,&d)==UD_INCOMPATIBLE && !d);
    const char *old="{\"version\":1,\"nodes\":[],\"edges\":[],\"parameters\":{}}";
    CHECK(ud_dag_create(ctx,old,(uint32_t)strlen(old),&d)==UD_INCOMPATIBLE && !d);
    CHECK(!ud_memory_used(ctx));OK(ud_context_destroy(ctx));puts("DAG JSON/C contract, topology, typed binding and rejection fixtures passed");return 0;
}
