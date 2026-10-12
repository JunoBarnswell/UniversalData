#include "ud/run.h"
#include "ud/engine.h"
#include "check.h"
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
typedef struct host { atomic_int live,closes;atomic_uint_fast64_t rows,sum;int oversized; } host;
typedef struct instance { ud_context *ctx;host *host;int kind;uint32_t next; } instance;
static void *allocate(void *user,size_t n) { void *p=malloc(n);if(p) atomic_fetch_add(&((host *)user)->live,1);return p; }
static void deallocate(void *user,void *p) { if(p) { atomic_fetch_sub(&((host *)user)->live,1);free(p); } }
static ud_status open_node(void *user,ud_context *ctx,const ud_dag_node *node,const ud_batch *const *inputs,uint32_t count,void **state,ud_batch **schema) {
    (void)inputs;CHECK(count==(node->kind==UD_NODE_SOURCE?0:1));*state=NULL;*schema=NULL;
    instance *i=ud_alloc(ctx,sizeof(*i));if(!i) return UD_RESOURCE;i->ctx=ctx;i->host=user;i->kind=node->kind;
    ud_field field={"id",2,UD_INT64,0,0,0,NULL,0,NULL,0};ud_status s=node->kind==UD_NODE_SINK?UD_OK:ud_batch_create(ctx,&field,1,0,schema);
    if(s) ud_free(i);else *state=i;return s;
}
static ud_status event_node(void *state,const ud_node_event *event,ud_batch **out) {
    instance *i=state;*out=NULL;ud_field field={"id",2,UD_INT64,0,0,0,NULL,0,NULL,0};
    if(i->kind==UD_NODE_SOURCE) {
        if(i->host->oversized==2) return UD_BUSY;
        if(i->next==20) return UD_END;ud_status s=ud_wait(i->ctx,1);if(s) return s;
        uint32_t rows=i->host->oversized?2:1;s=ud_batch_create(i->ctx,&field,1,rows,out);
        for(uint32_t j=0;!s && j<rows;j++) { ud_value v={0};v.type=UD_INT64;v.integer=i->next++;s=ud_batch_append(*out,&v,1); }
        if(s) { ud_batch_destroy(*out);*out=NULL; }return s;
    }
    if(event->kind==UD_EVENT_INPUT_END) return UD_OK;
    if(event->kind==UD_EVENT_POLL) return UD_END;
    ud_value v;ud_status s=ud_batch_get(event->batch,0,0,&v);if(s) return s;
    if(i->kind==UD_NODE_QUALITY) {
        if(v.integer%2) return UD_OK;s=ud_batch_create(i->ctx,&field,1,1,out);
        if(!s) s=ud_batch_append(*out,&v,1);if(s) { ud_batch_destroy(*out);*out=NULL; }return s;
    }
    atomic_fetch_add(&i->host->rows,1);atomic_fetch_add(&i->host->sum,(uint64_t)v.integer);return UD_OK;
}
static void close_node(void *state,ud_status outcome) { (void)outcome;instance *i=state;atomic_fetch_add(&i->host->closes,1);ud_free(i); }
static void init_host(host *h) { memset(h,0,sizeof(*h));atomic_init(&h->live,0);atomic_init(&h->closes,0);atomic_init(&h->rows,0);atomic_init(&h->sum,0); }
static ud_status create(host *h,ud_run **run) {
    const char *json="{\"version\":2,\"parameters\":{},\"nodes\":[{\"id\":\"a\",\"kind\":\"source\",\"operation\":\"test\",\"config\":{}},{\"id\":\"b\",\"kind\":\"quality\",\"operation\":\"test\",\"config\":{}},{\"id\":\"c\",\"kind\":\"sink\",\"operation\":\"test\",\"config\":{}}],\"edges\":[{\"from\":\"a\",\"to\":\"b\",\"port\":0},{\"from\":\"b\",\"to\":\"c\",\"port\":0}]}";
    ud_node_handler handlers[3];int kinds[]={UD_NODE_SOURCE,UD_NODE_QUALITY,UD_NODE_SINK};
    for(uint32_t j=0;j<3;j++) handlers[j]=(ud_node_handler){sizeof(ud_node_handler),UD_ABI_VERSION,kinds[j],{"test",4},h,open_node,event_node,close_node};
    ud_run_options options={0};options.struct_size=sizeof(options);options.abi_version=UD_ABI_VERSION;
    options.context=(ud_context_options){sizeof(ud_context_options),UD_ABI_VERSION,1048576,h,allocate,deallocate};
    options.edge_capacity=1;options.max_batch_rows=1;options.handlers=handlers;options.handler_count=3;
    return ud_run_create_json(json,(uint32_t)strlen(json),&options,run);
}
int main(void) {
    host a,b;init_host(&a);init_host(&b);ud_run *one=NULL,*two=NULL;OK(create(&a,&one));OK(create(&b,&two));
    OK(ud_run_start(one));OK(ud_run_start(two));ud_run_cancel(one);ud_status result;
    do { result=ud_run_wait(one,100); } while(result==UD_BUSY);CHECK(result==UD_CANCELLED);
    do { result=ud_run_wait(two,100); } while(result==UD_BUSY);CHECK(result==UD_OK);
    CHECK(atomic_load(&b.rows)==10 && atomic_load(&b.sum)==90);CHECK(atomic_load(&a.closes)==3 && atomic_load(&b.closes)==3);
    ud_run_metrics metrics;ud_run_get_metrics(two,&metrics);CHECK(metrics.queue_high_water<=2 && metrics.queued_batches==0);
    OK(ud_run_destroy(one));OK(ud_run_destroy(two));CHECK(!atomic_load(&a.live) && !atomic_load(&b.live));
    init_host(&a);a.oversized=1;OK(create(&a,&one));CHECK(ud_run_execute(one)==UD_SCHEMA);OK(ud_run_destroy(one));
    CHECK(!atomic_load(&a.live) && atomic_load(&a.closes)==3);
    init_host(&a);a.oversized=2;OK(create(&a,&one));CHECK(ud_run_execute(one)==UD_INVALID);
    CHECK(ud_run_wait(one,0)==UD_INVALID);OK(ud_run_destroy(one));CHECK(!atomic_load(&a.live) && atomic_load(&a.closes)==3);
    puts("custom DAG nodes, same-process context isolation, cancellation, host allocation and bounded batches fixtures passed");return 0;
}
