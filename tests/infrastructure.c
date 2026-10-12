#include "check.h"
#include "ud/package.h"
#include "ud/engine.h"
#include "../components/execution/journal.h"
#include "../components/foundation/internal.h"
#include <stdatomic.h>
#include <string.h>

static ud_string text(const char *value) { return (ud_string){value,(uint32_t)strlen(value)}; }
typedef struct fault_allocator { unsigned calls, fail, live; } fault_allocator;
static void *allocate(void *user,size_t bytes) {
    fault_allocator *fault=user;
    if (++fault->calls==fault->fail)return NULL;
    void *p=malloc(bytes);if(p)fault->live++;return p;
}
static void deallocate(void *user,void *p) {
    fault_allocator *fault=user;if(p){fault->live--;free(p);}
}
static int ownership(void) {
    for(unsigned fail=1;fail<=90;fail++) {
        fault_allocator allocator={.fail=fail};
        ud_context_options options={sizeof(options),UD_ABI_VERSION,16777216,
                                     &allocator,allocate,deallocate};
        ud_context *ctx=NULL;ud_status s=ud_context_create(&options,&ctx);
        ud_batch *batch=NULL,*projection=NULL;ud_buffer_manager *manager=NULL;
        ud_buffer_block *first=NULL,*second=NULL,*third=NULL;
        ud_field item={.name="item",.name_length=4,.type=UD_UTF8};
        ud_field field={.name="list",.name_length=4,.type=UD_LIST,
                        .children=&item,.child_count=1};
        if(!s)s=ud_batch_create(ctx,&field,1,64,&batch);
        ud_value children[]={{.type=UD_UTF8,.text="中文",.text_length=6},
                             {.type=UD_UTF8,.text="data",.text_length=4}};
        ud_value value={.type=UD_LIST,.children=children,.child_count=2};
        if(!s)s=ud_batch_append(batch,&value,1);
        if(!s)s=ud_batch_seal(batch);
        ud_buffer_options buffers={.struct_size=sizeof(buffers),.abi_version=UD_ABI_VERSION,
            .max_blocks=8,.resident_limit=1048576,.working_limit=1048576};
        if(!s)s=ud_buffer_manager_create(ctx,&buffers,&manager);
        if(!s)s=ud_buffer_store(manager,batch,&first);
        ud_buffer_metrics before={0},after={0};
        if(!s)s=ud_buffer_manager_metrics(manager,&before);
        if(!s)s=ud_buffer_store(manager,batch,&second);
        if(!s)s=ud_buffer_manager_metrics(manager,&after);
        if(!s)CHECK(before.resident_bytes==after.resident_bytes && !after.wire_encodes &&
                    !after.spill_bytes && after.blocks==2 && before.resident_bytes>1024);
        uint32_t column=0;
        if(!s)s=ud_batch_project(batch,&column,1,&projection);
        if(!s)s=ud_buffer_store(manager,projection,&third);
        if(!s)s=ud_buffer_manager_metrics(manager,&after);
        if(!s)CHECK(after.resident_bytes>before.resident_bytes &&
                    after.resident_bytes-before.resident_bytes<2048 && !after.wire_encodes);
        ud_buffer_view *view=NULL;
        if(!s)s=ud_buffer_read(first,&view);
        if(!s) {
            CHECK(ud_buffer_view_batch(view)==batch && ud_buffer_release(first)==UD_BUSY);
            CHECK(ud_buffer_manager_destroy(manager)==UD_BUSY);
        }
        ud_buffer_view_release(view);
        OK(ud_buffer_release(third));OK(ud_buffer_release(second));OK(ud_buffer_release(first));
        if(manager) {
            OK(ud_buffer_manager_metrics(manager,&after));
            CHECK(!after.resident_bytes && !after.blocks && !after.views && !after.working_bytes);
        }
        OK(ud_buffer_manager_destroy(manager));
        ud_batch_destroy(projection);ud_batch_destroy(batch);
        if(ctx){CHECK(!ud_memory_used(ctx));OK(ud_context_destroy(ctx));}
        CHECK(!allocator.live && (s==UD_OK || s==UD_RESOURCE));
    }
    puts("Block Store: nested/projection ownership, no resident codec, leases and 90 injected allocation positions");
    return 0;
}

typedef struct tree_fixture {
    atomic_int arrivals, total_rows, total_sum;
    int overlap, cancel;
    const ud_activation *frames[2];
} tree_fixture;
typedef struct component {
    tree_fixture *fixture;
    ud_component_context context;
    ud_field field;
    uint32_t row;
    int started, ended;
} component;
static ud_status configure_source(void *u,const ud_component_context *c,ud_string j,
                                  const ud_batch *const *in,ud_batch **out) {
    (void)u;(void)j;(void)in;
    ud_field field={.name="x",.name_length=1,.type=UD_INT64};
    return ud_batch_create(c->memory,&field,1,0,out);
}
static ud_status prepare_component(void *u,const ud_component_context *c,ud_string j,
                                   const ud_batch *const *in,const ud_batch *const *out,void **p) {
    (void)j;
    component *instance=ud_alloc(c->memory,sizeof(*instance));
    if(!instance)return UD_RESOURCE;
    instance->fixture=u;instance->context=*c;
    ud_status s=ud_batch_field(out ? out[0] : in[0],0,&instance->field);
    if(s){ud_free(instance);return s;}
    if(c->component_id.data[0]=='s') {
        unsigned index=c->component_id.data[1]=='1'?0:1;
        instance->fixture->frames[index]=c->activation;
    }
    *p=instance;return UD_OK;
}
static ud_status prepare_sink(void *u,const ud_component_context *c,ud_string j,
                              const ud_batch *const *in,const ud_batch *const *out,void **p) {
    (void)out;return prepare_component(u,c,j,in,NULL,p);
}
static ud_status configure_sink(void *u,const ud_component_context *c,ud_string j,
                                const ud_batch *const *in,ud_batch **out) {
    (void)u;(void)c;(void)j;(void)in;(void)out;return UD_OK;
}
static ud_status source_step(void *p,const ud_component_event *event) {
    component *instance=p;tree_fixture *f=instance->fixture;
    if(event->kind!=UD_COMPONENT_POLL)return UD_INVALID;
    if(!instance->started) {
        instance->started=1;
        int arrived=atomic_fetch_add(&f->arrivals,1)+1;
        if(f->cancel && arrived==2)ud_execution_cancel(instance->context.execution);
        if(f->overlap) {
            uint64_t deadline=ud_now_ms()+5000;
            while(atomic_load(&f->arrivals)<2 && ud_now_ms()<deadline) {
                UD_TRY(ud_activation_check(instance->context.activation));ud_sleep_ms(1);
            }
            if(atomic_load(&f->arrivals)<2)return UD_TIMEOUT;
        }
        ud_value v={.type=UD_INT64,.integer=99};
        if(ud_activation_set_variable(instance->context.activation,text("n"),&v,NULL)!=UD_AUTH)
            return UD_CORRUPT;
    }
    UD_TRY(ud_activation_check(instance->context.activation));
    if(instance->row==16)return UD_END;
    ud_batch *batch=NULL;
    ud_status s=ud_batch_create(instance->context.memory,&instance->field,1,1,&batch);
    ud_value value={.type=UD_INT64,.integer=instance->row++};
    if(!s)s=ud_batch_append(batch,&value,1);
    if(!s)s=instance->context.emit(instance->context.emit_user,0,batch);
    ud_batch_destroy(batch);return s;
}
static ud_status sink_step(void *p,const ud_component_event *event) {
    component *instance=p;
    if(event->kind==UD_COMPONENT_INPUT_END){instance->ended=1;return UD_OK;}
    if(event->kind==UD_COMPONENT_POLL)return instance->ended?UD_END:UD_INVALID;
    for(uint32_t i=0;i<ud_batch_rows(event->batch);i++) {
        ud_value value;UD_TRY(ud_batch_get(event->batch,i,0,&value));
        atomic_fetch_add(&instance->fixture->total_rows,1);
        atomic_fetch_add(&instance->fixture->total_sum,(int)value.integer);
    }
    return UD_OK;
}
static ud_status finish(void *p){(void)p;return UD_OK;}
static void dispose(void *p,ud_status s){(void)s;ud_free(p);}
static const char *tree_document=
    "{\"version\":" UD_PROTOCOL_VERSION_TEXT ",\"id\":\"trees\","
    "\"variables\":{\"n\":{\"type\":\"int64\",\"default\":0}},"
    "\"controlFlow\":{\"tasks\":[{\"id\":\"flow\",\"kind\":\"dataFlow\","
    "\"config\":{\"pipeline\":\"p\"}}],\"constraints\":[]},"
    "\"dataFlows\":[{\"id\":\"p\",\"components\":["
    "{\"id\":\"s1\",\"component\":\"native.source\",\"config\":{}},"
    "{\"id\":\"d1\",\"component\":\"native.sink\",\"config\":{}},"
    "{\"id\":\"s2\",\"component\":\"native.source\",\"config\":{}},"
    "{\"id\":\"d2\",\"component\":\"native.sink\",\"config\":{}}],"
    "\"paths\":[{\"from\":\"s1\",\"output\":0,\"to\":\"d1\",\"input\":0},"
    "{\"from\":\"s2\",\"output\":0,\"to\":\"d2\",\"input\":0}]}]}";
static int trees(void) {
    for(unsigned mode=0;mode<4;mode++) {
        ud_context *ctx=NULL;OK(ud_context_create(NULL,&ctx));
        ud_registry *registry=NULL;OK(ud_registry_create(ctx,&registry));
        tree_fixture fixture={.overlap=mode==1 || mode==3,.cancel=mode==3};
        ud_component_descriptor source={.struct_size=sizeof(source),.abi_version=UD_ABI_VERSION,
            .id={"native.source",13},.name={"Source",6},.version={"1",1},
            .configuration_schema={"{}",2},.category=UD_COMPONENT_SOURCE,.outputs=1,
            .synchronous_input=-1,.error_output=-1,.capabilities=UD_COMPONENT_THREAD_SAFE,
            .user=&fixture,.configure=configure_source,.prepare=prepare_component,
            .step=source_step,.finish=finish,.dispose=dispose};
        if(mode==2)source.capabilities=0; /* Same execution model, serialized contract. */
        ud_component_descriptor sink=source;
        sink.id=text("native.sink");sink.name=text("Sink");sink.category=UD_COMPONENT_DESTINATION;
        sink.inputs=1;sink.outputs=0;sink.synchronous_input=0;
        sink.configure=configure_sink;sink.prepare=prepare_sink;sink.step=sink_step;
        OK(ud_registry_register(registry,&source));OK(ud_registry_register(registry,&sink));
        ud_execution_options o={.struct_size=sizeof(o),.abi_version=UD_ABI_VERSION,
            .context={.struct_size=sizeof(o.context),.abi_version=UD_ABI_VERSION,.memory_limit=8388608},
            .registry=registry,.max_concurrency=mode?2:1,.edge_capacity=1};
        ud_execution *execution=NULL;OK(ud_execution_create_json(text(tree_document),&o,&execution));
        CHECK(ud_registry_register(registry,&source)==UD_BUSY);
        ud_status s=ud_execution_execute(execution);
        CHECK(s==(mode==3?UD_CANCELLED:UD_OK));
        ud_execution_metrics m={.struct_size=sizeof(m),.abi_version=UD_ABI_VERSION};
        OK(ud_execution_get_metrics(execution,&m));
        CHECK(!m.queued_batches && m.pipeline_trees==2 && m.worker_high_water<=(mode?2:1));
        CHECK(fixture.frames[0] && fixture.frames[1] && fixture.frames[0]!=fixture.frames[1]);
        if(mode!=3)CHECK(m.input_rows==32 && m.output_rows==32 && m.executed_tasks==1 &&
            !m.durable_tasks && !m.recovery_tasks && atomic_load(&fixture.total_rows)==32 &&
            atomic_load(&fixture.total_sum)==240 && m.tree_high_water==(mode==1?2:1));
        if(mode==3)CHECK(m.state==UD_EXEC_CANCELLED && !m.executed_tasks && m.tree_high_water==2);
        OK(ud_execution_destroy(execution));OK(ud_registry_destroy(registry));
        CHECK(!ud_memory_used(ctx));OK(ud_context_destroy(ctx));
    }
    puts("Execution Trees: real barrier overlap, one shared worker limit, serialized non-thread-safe contract, read-only Step and cancellation");
    return 0;
}
static int journal(const char *directory,int mode) {
    ud_context *ctx=NULL;OK(ud_context_create(NULL,&ctx));
    ud_journal *store=NULL;ud_status s=ud_journal_open(ctx,text(directory),mode!=0,&store);
    if(mode==2) {
        CHECK(s==UD_CORRUPT || s==UD_INCOMPATIBLE);
        CHECK(!store && !ud_memory_used(ctx));OK(ud_context_destroy(ctx));return 0;
    }
    OK(s);
    if(mode==0) {
        ud_journal *other=NULL;CHECK(ud_journal_open(ctx,text(directory),1,&other)==UD_BUSY && !other);
        for(uint32_t group=0;group<512;group++) {
            ud_journal_record records[32];char keys[32][48];
            for(uint32_t i=0;i<32;i++) {
                snprintf(keys[i],sizeof(keys[i]),"key-%08u",group*32+i);
                records[i]=(ud_journal_record){7,text(keys[i]),text("active-task"),
                    text("{\"hash\":\"0123456789012345678901234567890123456789012345678901234567890123\"}")};
            }
            OK(ud_journal_append(store,records,32));
        }
    }
    ud_journal_metrics before,after;ud_journal_get_metrics(store,&before);
    for(uint32_t i=0;i<16384;i++) {
        char key[48];snprintf(key,sizeof(key),"key-%08u",i);
        CHECK(ud_journal_find(store,7,text(key)));
    }
    ud_journal_get_metrics(store,&after);
    CHECK(after.entries==(mode==0?16384:16385) && after.probes-before.probes<16384*8);
    if(mode==0) {
        CHECK(after.snapshots>0 && after.segment_files<=1);
        ud_journal_record last={3,text("done"),{0},text("{\"complete\":true}")};
        OK(ud_journal_append(store,&last,1));
    } else {
        ud_journal_record complete[]={{3,text("done"),{0},text("{\"complete\":true}")},
                                      {0,{0},text("active-task"),{0}}};
        OK(ud_journal_append(store,complete,2));
        ud_journal_get_metrics(store,&after);CHECK(after.entries==1);
        CHECK(!ud_journal_find(store,7,text("key-00000000")));
        ud_journal_close(store);store=NULL;
        OK(ud_journal_open(ctx,text(directory),1,&store));
        ud_journal_get_metrics(store,&after);CHECK(after.entries==1);
        CHECK(ud_journal_find(store,3,text("done")) && !ud_journal_find(store,7,text("key-00000000")));
    }
    ud_journal_close(store);CHECK(!ud_memory_used(ctx));OK(ud_context_destroy(ctx));
    printf("Journal: 16384 indexed keys, probes=%llu, snapshots=%llu, segments=%u\n",
        (unsigned long long)(after.probes-before.probes),(unsigned long long)after.snapshots,
        after.segment_files);
    return 0;
}
typedef struct fact_fixture { int writes, reject;ud_execution *execution; } fact_fixture;
static ud_status authorize(void *u,ud_string t,ud_string r,int32_t action) {
    fact_fixture *fixture=u;(void)t;(void)r;
    return fixture->reject && action==UD_ACCESS_WRITE?UD_AUTH:UD_OK;
}
static ud_status fact_task(void *u,const ud_component_context *c,ud_string config) {
    fact_fixture *fixture=u;(void)config;
    ud_value value={.type=UD_INT64,.integer=++fixture->writes};
    return ud_activation_set_variable(c->activation,text("n"),&value,NULL);
}
static void observe(void *u,const ud_execution_event *event) {
    fact_fixture *fixture=u;
    if(event->kind==UD_EXEC_EVENT_BEFORE_CHECKPOINT && fixture->execution) {
        ud_execution_metrics m={.struct_size=sizeof(m),.abi_version=UD_ABI_VERSION};
        if(!ud_execution_get_metrics(fixture->execution,&m) && m.executed_tasks==1)
            fixture->reject=1;
    }
}
static int facts(const char *directory) {
    ud_context *ctx=NULL;OK(ud_context_create(NULL,&ctx));
    ud_registry *registry=NULL;OK(ud_registry_create(ctx,&registry));
    fact_fixture fixture={0};
    ud_component_descriptor d={.struct_size=sizeof(d),.abi_version=UD_ABI_VERSION,
        .id={"effect",6},.name={"Effect",6},.version={"1",1},.configuration_schema={"{}",2},
        .category=UD_COMPONENT_TASK,.synchronous_input=-1,.error_output=-1,
        .capabilities=UD_COMPONENT_REPLAY_SAFE,.user=&fixture,.invoke=fact_task};
    OK(ud_registry_register(registry,&d));
    const char *document="{\"version\":" UD_PROTOCOL_VERSION_TEXT ",\"id\":\"facts\","
        "\"variables\":{\"n\":{\"type\":\"int64\",\"default\":0}},"
        "\"controlFlow\":{\"tasks\":[{\"id\":\"one\",\"kind\":\"effect\",\"config\":{}}],"
        "\"constraints\":[]},\"dataFlows\":[]}";
    for(unsigned run=0;run<3;run++) {
        fixture.reject=0;
        ud_execution_options o={.struct_size=sizeof(o),.abi_version=UD_ABI_VERSION,
            .context={.struct_size=sizeof(o.context),.abi_version=UD_ABI_VERSION,.memory_limit=4194304},
            .registry=registry,.tenant={"test",4},.execution_id={"facts",5},
            .authorize=authorize,.authorize_user=&fixture,.state_directory=text(directory),
            .resume=run!=0,.observe=run==0?observe:NULL,.observe_user=&fixture};
        ud_execution *e=NULL;OK(ud_execution_create_json(text(document),&o,&e));
        fixture.execution=e;
        CHECK(ud_execution_execute(e)==(run==0?UD_COMMIT_UNKNOWN:UD_OK));
        ud_execution_metrics m={.struct_size=sizeof(m),.abi_version=UD_ABI_VERSION};
        OK(ud_execution_get_metrics(e,&m));
        CHECK(m.executed_tasks==1 && m.durable_tasks==(run?1:0) &&
              m.recovery_tasks==(run?0:1) && m.resumed_tasks==(run==2?1:0));
        CHECK(m.state==(run==0?UD_EXEC_RECOVERY_REQUIRED:UD_EXEC_SUCCEEDED));
        fixture.execution=NULL;OK(ud_execution_destroy(e));
    }
    CHECK(fixture.writes==2);OK(ud_registry_destroy(registry));
    CHECK(!ud_memory_used(ctx));OK(ud_context_destroy(ctx));
    puts("Task facts: executed=1 / durable=0 on denied acknowledgement; recovery clears only after durable replay; resume skips invoke");
    return 0;
}
int main(int argc,char **argv) {
    CHECK(argc>=2);
    if(!strcmp(argv[1],"ownership"))return ownership();
    if(!strcmp(argv[1],"trees"))return trees();
    CHECK(argc==3);
    if(!strcmp(argv[1],"facts"))return facts(argv[2]);
    if(!strcmp(argv[1],"journal"))return journal(argv[2],0);
    if(!strcmp(argv[1],"journal-read"))return journal(argv[2],1);
    if(!strcmp(argv[1],"journal-corrupt"))return journal(argv[2],2);
    return 2;
}
