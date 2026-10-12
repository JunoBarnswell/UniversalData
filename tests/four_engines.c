#include "check.h"
#include "ud/package.h"
#include "ud/engine.h"
#include "ud/operator.h"
#include "ud/json.h"
#include "../components/execution/executor.h"
#include "../components/foundation/internal.h"
#include <stdatomic.h>
#include <string.h>
typedef struct fixture { atomic_int next, slow, prepared, disposed, cancelled; } fixture;
typedef struct instance { fixture *fixture; ud_context *ctx; atomic_int cancelled; } instance;
static ud_string text(const char *s) { return (ud_string){s,(uint32_t)strlen(s)}; }
static ud_status prepare(void *user,const ud_component_context *c,ud_string j,const ud_batch *const *in,const ud_batch *const *out,void **p) {
    (void)j;(void)in;(void)out;instance *i=ud_alloc(c->memory,sizeof(*i));if (!i) return UD_RESOURCE;
    i->fixture=user;i->ctx=c->memory;atomic_init(&i->cancelled,0);*p=i;atomic_fetch_add(&i->fixture->prepared,1);return UD_OK;
}
static ud_status invoke(void *user,const ud_component_context *c,ud_string j) {
    (void)j;instance *i=user;fixture *f=i->fixture;
    const char *leaf=strrchr(c->task_id.data,'/');leaf=leaf ? leaf+1 : c->task_id.data;
    if (strstr(leaf,"slow")) {
        atomic_store(&f->slow,1);
        while (!atomic_load(&f->next)) { UD_TRY(ud_activation_check(c->activation));UD_TRY(ud_wait(c->memory,1)); }
        atomic_store(&f->slow,0);
    } else if (strstr(leaf,"next")) {
        while (!atomic_load(&f->slow)) { UD_TRY(ud_activation_check(c->activation));UD_TRY(ud_wait(c->memory,1)); }
        atomic_store(&f->next,1);
    } else if (strstr(leaf,"fail")) return UD_INVALID;
    return UD_OK;
}
static void cancel(void *p) { instance *i=p;atomic_store(&i->cancelled,1);atomic_fetch_add(&i->fixture->cancelled,1); }
static void dispose(void *p,ud_status outcome) { (void)outcome;instance *i=p;atomic_fetch_add(&i->fixture->disposed,1);ud_free(i); }
static int control(void) {
    ud_context *ctx=NULL;OK(ud_context_create(NULL,&ctx));ud_registry *registry=NULL;OK(ud_registry_create(ctx,&registry));fixture f={0};
    ud_component_descriptor d={.struct_size=sizeof(d),.abi_version=UD_ABI_VERSION,.id={"native.task",11},.name={"Task",4},
        .version={"1",1},.configuration_schema={"{}",2},.category=UD_COMPONENT_TASK,.synchronous_input=-1,.error_output=-1,
        .capabilities=UD_COMPONENT_THREAD_SAFE|UD_COMPONENT_REPLAY_SAFE,.user=&f,.prepare=prepare,.invoke=invoke,.dispose=dispose,.cancel=cancel};
    OK(ud_registry_register(registry,&d));
    char definition[8192];int n=snprintf(definition,sizeof(definition),"{\"version\":%u,\"id\":\"ready\",\"controlFlow\":{\"tasks\":[{\"id\":\"slow\",\"kind\":\"native.task\",\"config\":{}},{\"id\":\"fast\",\"kind\":\"native.task\",\"config\":{}},{\"id\":\"next\",\"kind\":\"native.task\",\"config\":{}}",UD_PACKAGE_VERSION);
    for (int i=3;i<20;i++) n+=snprintf(definition+n,sizeof(definition)-(size_t)n,",{\"id\":\"task%d\",\"kind\":\"native.task\",\"config\":{}}",i);
    n+=snprintf(definition+n,sizeof(definition)-(size_t)n,"],\"constraints\":[{\"from\":\"fast\",\"to\":\"next\",\"on\":\"success\"}");
    for (int i=3;i<20;i++) n+=snprintf(definition+n,sizeof(definition)-(size_t)n,",{\"from\":\"next\",\"to\":\"task%d\",\"on\":\"completion\",\"evaluation\":\"or\",\"expression\":{\"literal\":false,\"type\":\"bool\"}}",i);
    n+=snprintf(definition+n,sizeof(definition)-(size_t)n,"]},\"dataFlows\":[]}");
    ud_execution_options o={.struct_size=sizeof(o),.abi_version=UD_ABI_VERSION,.context={.struct_size=sizeof(o.context),.abi_version=UD_ABI_VERSION},.registry=registry,.edge_capacity=1,.max_batch_rows=4096,.max_depth=32,.max_concurrency=2,.timeout_ms=5000};
    ud_execution *e=NULL;OK(ud_execution_create_json((ud_string){definition,(uint32_t)n},&o,&e));
    char *explain=NULL;uint32_t size=0;OK(ud_execution_explain(e,&explain,&size));
    ud_json *json=NULL;OK(ud_json_parse(ctx,explain,size,&json,NULL));ud_json_destroy(json);ud_free(explain);
    OK(ud_execution_execute(e));CHECK(atomic_load(&f.next) && atomic_load(&f.prepared)==20 && atomic_load(&f.disposed)==20);
    ud_execution_metrics m={.struct_size=sizeof(m),.abi_version=UD_ABI_VERSION};OK(ud_execution_get_metrics(e,&m));CHECK(m.executed_tasks==20 && m.worker_high_water==2);
    char *diagnostics=NULL;OK(ud_execution_diagnostics(e,&diagnostics,&size));OK(ud_json_parse(ctx,diagnostics,size,&json,NULL));ud_json_destroy(json);ud_free(diagnostics);OK(ud_execution_destroy(e));
    const char *conflict="{\"version\":" UD_PROTOCOL_VERSION_TEXT ",\"id\":\"scope\",\"variables\":{\"n\":{\"type\":\"int64\",\"default\":0}},\"controlFlow\":{\"tasks\":[{\"id\":\"a\",\"kind\":\"set\",\"config\":{\"name\":\"n\",\"value\":{\"literal\":1,\"type\":\"int64\"}}},{\"id\":\"b\",\"kind\":\"set\",\"config\":{\"name\":\"n\",\"value\":{\"literal\":2,\"type\":\"int64\"}}}],\"constraints\":[]},\"dataFlows\":[]}";
    CHECK(ud_execution_create_json(text(conflict),&o,&e)==UD_AUTH && !e);
    const char *limited="{\"version\":" UD_PROTOCOL_VERSION_TEXT ",\"id\":\"error-limit\",\"policy\":{\"maxErrorCount\":1,\"maxConcurrency\":1},\"eventHandlers\":{\"failure\":{\"tasks\":[{\"id\":\"rootHandler\",\"kind\":\"native.task\",\"config\":{}}],\"constraints\":[]}},\"controlFlow\":{\"tasks\":[{\"id\":\"fail\",\"kind\":\"native.task\",\"config\":{},\"eventHandlers\":{\"taskFailed\":{\"tasks\":[{\"id\":\"taskHandler\",\"kind\":\"native.task\",\"config\":{}}],\"constraints\":[]}}},{\"id\":\"after\",\"kind\":\"native.task\",\"config\":{}}],\"constraints\":[{\"from\":\"fail\",\"to\":\"after\",\"on\":\"completion\"}]},\"dataFlows\":[]}";
    int before=atomic_load(&f.prepared);
    OK(ud_execution_create_json(text(limited),&o,&e));
    CHECK(ud_execution_execute(e)==UD_INVALID);
    CHECK(atomic_load(&f.prepared)==before+3 && atomic_load(&f.prepared)==atomic_load(&f.disposed));
    OK(ud_execution_get_metrics(e,&m));CHECK(m.error_count==1);
    OK(ud_execution_destroy(e));
    OK(ud_registry_destroy(registry));CHECK(!ud_memory_used(ctx));OK(ud_context_destroy(ctx));return 0;
}
static int partitions(void) {
    ud_context *ctx=NULL;OK(ud_context_create(NULL,&ctx));ud_executor *pool=NULL;OK(ud_executor_create(ctx,4,&pool));OK(ud_executor_start(pool));
    ud_buffer_options bo={.struct_size=sizeof(bo),.abi_version=UD_ABI_VERSION,.max_blocks=10000,.resident_limit=16000000,.working_limit=16000000};
    ud_buffer_manager *buffers=NULL;OK(ud_buffer_manager_create(ctx,&bo,&buffers));
    ud_field fields[]={{.name="key",.name_length=3,.type=UD_INT64},{.name="v",.name_length=1,.type=UD_INT64}};
    ud_batch *schema=NULL;OK(ud_batch_create(ctx,fields,2,0,&schema));const ud_batch *inputs[]={schema,schema};uint32_t key=0;
    ud_operator_options o={.struct_size=sizeof(o),.abi_version=UD_ABI_VERSION,.kind=UD_OPERATOR_SORT,.max_batch_rows=128,.keys=&key,.key_count=1,.buffers=buffers,.partition_count=4,.executor=pool};
    ud_operator *sort=NULL;OK(ud_operator_create(ctx,&o,inputs,&sort));
    ud_batch *batch=NULL;OK(ud_batch_create(ctx,fields,2,128,&batch));
    for (int i=127;i>=0;i--) { ud_value values[]={{.type=UD_INT64,.integer=i%4},{.type=UD_INT64,.integer=i}};OK(ud_batch_append(batch,values,2)); }
    OK(ud_batch_seal(batch));OK(ud_operator_push(sort,0,batch));OK(ud_operator_finish_input(sort,0));
    uint32_t total=0;ud_batch *result=NULL;int previous=-1,last=128;ud_status status;
    while ((status=ud_operator_pull(sort,&result))==UD_OK) {
        for (uint32_t i=0;i<ud_batch_rows(result);i++) {
            ud_value k,v;OK(ud_batch_get(result,i,0,&k));OK(ud_batch_get(result,i,1,&v));
            CHECK(k.integer>=previous);if (k.integer==previous) CHECK(v.integer<last);previous=(int)k.integer;last=(int)v.integer;total++;
        }
        ud_batch_destroy(result);
    }
    CHECK(status==UD_END && total==128);OK(ud_operator_destroy(sort));
    const char *plan_text="{\"version\":" UD_PROTOCOL_VERSION_TEXT ",\"group\":[\"key\"],\"aggregates\":[{\"function\":\"count\",\"column\":\"*\",\"name\":\"n\"},{\"function\":\"sum\",\"column\":\"v\",\"name\":\"sum\"}]}";
    ud_plan *plan=NULL;OK(ud_plan_create_fields(ctx,UD_PLAN_VERSION,fields,2,text(plan_text),NULL,&plan));
    o.kind=UD_OPERATOR_AGGREGATE;o.aggregation=plan;ud_operator *agg=NULL;OK(ud_operator_create(ctx,&o,inputs,&agg));ud_plan_destroy(plan);
    OK(ud_operator_push(agg,0,batch));OK(ud_operator_finish_input(agg,0));OK(ud_operator_pull(agg,&result));CHECK(ud_batch_rows(result)==4);
    for (uint32_t i=0;i<4;i++) { ud_value k,n,sum;OK(ud_batch_get(result,i,0,&k));OK(ud_batch_get(result,i,1,&n));OK(ud_batch_get(result,i,2,&sum));CHECK(k.integer==i && n.integer==32 && sum.integer==32*i+1984); }
    ud_batch_destroy(result);CHECK(ud_operator_pull(agg,&result)==UD_END);OK(ud_operator_destroy(agg));
    const int64_t extreme[]={INT64_MAX,1,-1};
    ud_batch *large=NULL;OK(ud_batch_create(ctx,fields,2,3,&large));
    for (uint32_t i=0;i<3;i++) { ud_value values[]={{.type=UD_INT64,.integer=0},{.type=UD_INT64,.integer=extreme[i]}};OK(ud_batch_append(large,values,2)); }
    OK(ud_batch_seal(large));
    for (uint32_t width=1;width<=4;width+=3) {
        OK(ud_plan_create_fields(ctx,UD_PLAN_VERSION,fields,2,text(plan_text),NULL,&plan));
        o.kind=UD_OPERATOR_AGGREGATE;o.aggregation=plan;o.partition_count=width;
        OK(ud_operator_create(ctx,&o,inputs,&agg));ud_plan_destroy(plan);
        OK(ud_operator_push(agg,0,large));OK(ud_operator_finish_input(agg,0));OK(ud_operator_pull(agg,&result));
        ud_value sum;OK(ud_batch_get(result,0,2,&sum));CHECK(sum.integer==INT64_MAX);ud_batch_destroy(result);OK(ud_operator_destroy(agg));
    }
    ud_batch_destroy(large);o.partition_count=4;
    ud_field right_fields[]={{.name="right_key",.name_length=9,.type=UD_INT64},{.name="right_v",.name_length=7,.type=UD_INT64}};
    ud_batch *right=NULL;OK(ud_batch_create(ctx,right_fields,2,128,&right));
    for (uint32_t i=0;i<128;i++) {
        ud_value values[2];OK(ud_batch_get(batch,i,0,values));OK(ud_batch_get(batch,i,1,values+1));OK(ud_batch_append(right,values,2));
    }
    OK(ud_batch_seal(right));inputs[1]=right;
    o.kind=UD_OPERATOR_INNER_JOIN;o.aggregation=NULL;o.right_key=0;ud_operator *join=NULL;OK(ud_operator_create(ctx,&o,inputs,&join));
    OK(ud_operator_push(join,0,batch));OK(ud_operator_push(join,1,right));OK(ud_operator_finish_input(join,0));OK(ud_operator_finish_input(join,1));total=0;
    while ((status=ud_operator_pull(join,&result))==UD_OK) { total+=ud_batch_rows(result);ud_batch_destroy(result); }
    CHECK(status==UD_END && total==4096);OK(ud_operator_destroy(join));
    ud_partition_options po={.struct_size=sizeof(po),.abi_version=UD_ABI_VERSION,.strategy=UD_PARTITION_HASH,.count=4,.keys=&key,.key_count=1};ud_batch **parts=NULL;OK(ud_batch_partition(ctx,batch,&po,&parts));total=0;
    for (uint32_t i=0;i<4;i++) { total+=ud_batch_rows(parts[i]);ud_batch_destroy(parts[i]); }ud_free(parts);CHECK(total==128);
    ud_batch_destroy(right);ud_batch_destroy(batch);ud_batch_destroy(schema);OK(ud_buffer_manager_destroy(buffers));CHECK(ud_executor_high_water(pool)<=4);OK(ud_executor_destroy(pool));CHECK(!ud_memory_used(ctx));OK(ud_context_destroy(ctx));return 0;
}
int main(void) { CHECK(!control());CHECK(!partitions());puts("Twenty-task completion scheduling, pure scope rejection, stable parallel Sort, local/merge Aggregate, hash Join and shared pool passed");return 0; }
