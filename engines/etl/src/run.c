#include "ud/run.h"
#include "ud/engine.h"
#include "ud/format.h"
#include "ud/csv.h"
#include "ud/json.h"
#include "../../../components/data/internal.h"
#include <stdatomic.h>
#include <errno.h>
#include <sys/stat.h>
#ifdef _WIN32
#include <windows.h>
#include <process.h>
#else
#include <pthread.h>
#endif
typedef struct run_edge { uint32_t from,to,port;ud_queue *queue;uint32_t count;int ended; } run_edge;
typedef struct run_node {
    ud_dag_node info;ud_node_handler handler;void *instance;ud_batch *schema;
    uint32_t input_count,output_count,*inputs,*outputs,ended,next_port;
    atomic_int state;int opened;
} run_node;
struct ud_run {
    ud_context *ctx;ud_dag *dag;run_node *nodes;run_edge *edges;
    uint32_t node_count,edge_count,capacity,max_rows;const uint32_t *topology;
    ud_string tenant;ud_authorize_fn authorize;void *authorize_user;
    atomic_int state,result;
    atomic_uint_fast64_t input_rows,output_rows,batches,queued,high_water;
    int threaded;
#ifdef _WIN32
    HANDLE thread;
#else
    pthread_t thread;
#endif
};
typedef struct file_node {
    ud_run *run;ud_context *ctx;int32_t kind;ud_string operation;ud_batch *schema;
    ud_field *fields;uint32_t columns;char *path,*temporary;void *lock;
    ud_csv_reader *csv;ud_json_reader *reader;ud_json_writer *writer;
    ud_json *configuration;int32_t rounding;int published;
} file_node;
static int equal(ud_string a,ud_string b) { return a.length==b.length && (!a.length || !memcmp(a.data,b.data,a.length)); }
static int named(ud_string a,const char *name) { return a.length==strlen(name) && !memcmp(a.data,name,a.length); }
static const ud_json *member(const ud_json *v,const char *name) { return ud_json_find(v,name,(uint32_t)strlen(name)); }
static ud_status config_keys(const ud_json *v,const char *const *allowed,uint32_t n) {
    for(uint32_t j=0;j<ud_json_size(v);j++) {
        const char *key=NULL;uint32_t length=0;UD_TRY(ud_json_key(v,j,&key,&length));int found=0;
        for(uint32_t i=0;i<n;i++) if(length==strlen(allowed[i]) && !memcmp(key,allowed[i],length)) found=1;
        if(!found) return UD_SCHEMA;
    }
    return UD_OK;
}
static ud_status config_uint(const ud_json *v,const char *name,uint32_t fallback,uint32_t *out) {
    const ud_json *value=member(v,name);*out=fallback;if(!value) return UD_OK;
    const char *text=NULL;uint32_t length=0;if(ud_json_kind(value)!=UD_JSON_NUMBER) return UD_SCHEMA;
    UD_TRY(ud_json_text(value,&text,&length));uint32_t number=0;
    for(uint32_t i=0;i<length;i++) { unsigned digit=(unsigned char)text[i]-'0';if(digit>9 || number>(UINT32_MAX-digit)/10) return UD_SCHEMA;number=number*10+digit; }
    *out=number;return UD_OK;
}
static ud_status config_schema(file_node *f,const ud_json *configuration) {
    const ud_json *schema=member(configuration,"schema");if(ud_json_kind(schema)!=UD_JSON_OBJECT) return UD_SCHEMA;
    char *json=NULL;uint32_t length=0;ud_status s=ud_json_serialize(schema,&json,&length);
    if(!s) s=ud_schema_from_json(f->ctx,json,length,&f->schema);ud_free(json);return s;
}
static ud_status schema_fields(file_node *f) {
    f->columns=ud_batch_columns(f->schema);f->fields=ud_alloc(f->ctx,f->columns*sizeof(*f->fields));
    if(!f->fields) return UD_RESOURCE;
    for(uint32_t j=0;j<f->columns;j++) UD_TRY(ud_batch_field(f->schema,j,f->fields+j));
    return UD_OK;
}
static ud_status clone_schema(ud_context *ctx,const ud_batch *schema,ud_batch **out) {
    uint32_t n=ud_batch_columns(schema);ud_field *fields=ud_alloc(ctx,n*sizeof(*fields));if(!fields) return UD_RESOURCE;
    ud_status s=UD_OK;for(uint32_t j=0;!s && j<n;j++) s=ud_batch_field(schema,j,fields+j);
    if(!s) s=ud_batch_create(ctx,fields,n,0,out);ud_free(fields);return s;
}
static ud_status authorize(file_node *f,int action) {
    if(!f->run->authorize || !f->run->tenant.length) return UD_AUTH;
    ud_string resource={f->path,(uint32_t)strlen(f->path)};
    return f->run->authorize(f->run->authorize_user,f->run->tenant,resource,action)==UD_OK?UD_OK:UD_AUTH;
}
static void file_close(void *instance,ud_status outcome) {
    (void)outcome;file_node *f=instance;if(!f) return;
    ud_csv_close(f->csv);ud_json_reader_close(f->reader);
    if(f->writer) (void)ud_json_writer_close(f->writer);
    if(f->temporary && !f->published) (void)ud_remove_file(f->ctx,f->temporary);
    ud_unlock(f->lock);ud_batch_destroy(f->schema);ud_free(f->fields);
    ud_json_destroy(f->configuration);ud_free(f->path);ud_free(f->temporary);ud_free(f);
}
static ud_status file_open(void *user,ud_context *ctx,const ud_dag_node *node,
                            const ud_batch *const *inputs,uint32_t count,void **instance,ud_batch **schema) {
    *instance=NULL;*schema=NULL;ud_run *run=user;
    file_node *f=ud_alloc(ctx,sizeof(*f));if(!f) return UD_RESOURCE;
    f->ctx=ctx;f->run=run;f->kind=node->kind;f->operation=node->operation;
    ud_status s=ud_json_parse(ctx,node->configuration.data,node->configuration.length,&f->configuration,NULL);
    uint32_t batch=run->max_rows,record=1048576,rounding=UD_ROUND_EXACT;
    if(!s) s=config_uint(f->configuration,"rounding",UD_ROUND_EXACT,&rounding);
    if(!s && rounding>UD_ROUND_CEILING) s=UD_SCHEMA;f->rounding=(int32_t)rounding;
    if(!s && node->kind==UD_NODE_TRANSFORM) {
        static const char *pass_keys[]={"rounding"},*cast_keys[]={"schema","rounding"};
        s=config_keys(f->configuration,named(node->operation,"pass")?pass_keys:cast_keys,named(node->operation,"pass")?1:2);
        if(!s && count!=1) s=UD_SCHEMA;
        if(!s) s=named(node->operation,"pass")?clone_schema(ctx,inputs[0],&f->schema):config_schema(f,f->configuration);
        if(!s && ud_batch_columns(f->schema)!=ud_batch_columns(inputs[0])) s=UD_SCHEMA;
    } else if(!s) {
        static const char *source_keys[]={"schema","path","batch_size","max_record_bytes","rounding","null_marker"};
        static const char *sink_keys[]={"path","max_record_bytes"};
        s=config_keys(f->configuration,node->kind==UD_NODE_SOURCE?source_keys:sink_keys,node->kind==UD_NODE_SOURCE?6:2);
        const ud_json *path=member(f->configuration,"path");const char *data=NULL;uint32_t length=0;
        if(!s && ud_json_kind(path)!=UD_JSON_STRING) s=UD_SCHEMA;
        if(!s) s=ud_json_text(path,&data,&length);
        if(!s && (!length || length>32767 || memchr(data,0,length))) s=UD_SCHEMA;
#ifdef _WIN32
        if(!s && !((length>=3 && ((data[0]>='A' && data[0]<='Z') || (data[0]>='a' && data[0]<='z')) && data[1]==':' && (data[2]=='/' || data[2]=='\\')) ||
                   (length>=2 && data[0]=='\\' && data[1]=='\\'))) s=UD_INVALID;
#else
        if(!s && data[0]!='/') s=UD_INVALID;
#endif
        if(!s) { f->path=ud_strdup_n(ctx,data,length);if(!f->path) s=UD_RESOURCE; }
        if(!s) s=authorize(f,node->kind==UD_NODE_SOURCE?UD_RUN_ACCESS_READ:UD_RUN_ACCESS_WRITE);
        if(!s && node->kind==UD_NODE_SOURCE) {
            struct stat source;
            if(stat(f->path,&source)) s=UD_IO;
            else {
#ifdef _WIN32
                int regular=(source.st_mode&_S_IFREG)!=0;
#else
                int regular=S_ISREG(source.st_mode);
#endif
                if(!regular) s=UD_UNSUPPORTED;
            }
        }
        if(!s) s=config_uint(f->configuration,"max_record_bytes",1048576,&record);
        if(!s && (!record || record>UINT32_C(67108864))) s=UD_SCHEMA;
        if(!s && node->kind==UD_NODE_SOURCE) {
            if(count) s=UD_SCHEMA;
            if(!s) s=config_schema(f,f->configuration);
            if(!s) s=config_uint(f->configuration,"batch_size",run->max_rows,&batch);
            if(!s && (!batch || batch>run->max_rows)) s=UD_SCHEMA;
            if(!s && named(node->operation,"ndjson") && member(f->configuration,"null_marker")) s=UD_SCHEMA;
            if(!s && named(node->operation,"csv") && (rounding!=UD_ROUND_EXACT || member(f->configuration,"max_record_bytes"))) s=UD_UNSUPPORTED;
        } else if(!s) {
            if(count!=1) s=UD_SCHEMA;else s=clone_schema(ctx,inputs[0],&f->schema);
        }
    }
    if(!s) s=schema_fields(f);
    if(!s && node->kind==UD_NODE_SOURCE && named(node->operation,"ndjson")) {
        ud_json_options options={sizeof(options),UD_CONFIG_VERSION,batch,record,(int32_t)rounding};
        s=ud_json_reader_open(ctx,f->path,f->fields,f->columns,&options,&f->reader);
    }
    if(!s && node->kind==UD_NODE_SOURCE && named(node->operation,"csv")) {
        const char *null_marker="\\N";uint32_t null_length=2;const ud_json *null_value=member(f->configuration,"null_marker");
        if(null_value) { if(ud_json_kind(null_value)!=UD_JSON_STRING) s=UD_SCHEMA;else s=ud_json_text(null_value,&null_marker,&null_length); }
        ud_csv_options options={sizeof(options),UD_ABI_VERSION,batch,null_marker,null_length};
        if(!s) s=ud_csv_open(ctx,f->path,f->fields,f->columns,&options,&f->csv);
    }
    if(!s && node->kind==UD_NODE_SINK) {
        size_t length=strlen(f->path);char *lock=ud_alloc(ctx,length+16);if(!lock) s=UD_RESOURCE;
        else { memcpy(lock,f->path,length);memcpy(lock+length,".ud-run.lock",13);s=ud_lock(ctx,lock,&f->lock);ud_free(lock); }
        struct stat info;
        if(!s && !stat(f->path,&info)) s=UD_BUSY;
        else if(!s && errno!=ENOENT) s=UD_IO;
        if(!s) {
            f->temporary=ud_alloc(ctx,length+16);if(!f->temporary) s=UD_RESOURCE;
            else { memcpy(f->temporary,f->path,length);memcpy(f->temporary+length,".ud-run.part",13);s=ud_reserve_file(ctx,f->temporary);if(s) { ud_free(f->temporary);f->temporary=NULL; } }
        }
        if(!s) s=ud_json_writer_open(ctx,f->temporary,f->fields,f->columns,record,&f->writer);
    }
    if(!s && node->kind!=UD_NODE_SINK) s=clone_schema(ctx,f->schema,schema);
    if(s) { file_close(f,s);return s; }
    *instance=f;return UD_OK;
}
static ud_status file_event(void *instance,const ud_node_event *event,ud_batch **out) {
    file_node *f=instance;*out=NULL;
    if(f->kind==UD_NODE_SOURCE) {
        UD_TRY(authorize(f,UD_RUN_ACCESS_READ));
        return f->csv?ud_csv_next(f->csv,out):ud_json_reader_next(f->reader,out);
    }
    if(f->kind==UD_NODE_TRANSFORM) {
        if(event->kind==UD_EVENT_INPUT_END) return UD_OK;
        if(event->kind==UD_EVENT_POLL) return UD_END;
        return ud_batch_cast(f->ctx,event->batch,f->fields,f->columns,f->rounding,out);
    }
    UD_TRY(authorize(f,UD_RUN_ACCESS_WRITE));
    if(event->kind==UD_EVENT_INPUT_END) return UD_OK;
    if(event->kind==UD_EVENT_DATA) return ud_json_writer_append(f->writer,event->batch);
    ud_status s=ud_json_writer_close(f->writer);f->writer=NULL;
    if(!s && ud_is_cancelled(f->ctx)) s=UD_CANCELLED;
    if(!s) s=authorize(f,UD_RUN_ACCESS_WRITE);
    if(!s) s=ud_publish(f->ctx,f->temporary,f->path);
    if(!s) f->published=1;
    return s?s:UD_END;
}
static int builtin(const ud_dag_node *node) {
    return (node->kind==UD_NODE_SOURCE && (named(node->operation,"csv") || named(node->operation,"ndjson"))) ||
           (node->kind==UD_NODE_TRANSFORM && (named(node->operation,"pass") || named(node->operation,"cast"))) ||
           (node->kind==UD_NODE_SINK && named(node->operation,"ndjson"));
}
static void clear_nodes(ud_run *run,ud_status result) {
    if(run->edges) for(uint32_t i=0;i<run->edge_count;i++) { ud_queue_destroy(run->edges[i].queue);run->edges[i].queue=NULL; }
    if(run->nodes) for(uint32_t i=run->node_count;i;i--) {
        uint32_t index=run->topology?run->topology[i-1]:i-1;run_node *n=run->nodes+index;
        if(n->opened) { n->handler.close(n->instance,result);n->opened=0;n->instance=NULL; }
        ud_batch_destroy(n->schema);n->schema=NULL;ud_free(n->inputs);n->inputs=NULL;ud_free(n->outputs);n->outputs=NULL;
    }
}
static uint32_t id_hash(ud_string s) { uint32_t v=2166136261u;for(uint32_t i=0;i<s.length;i++) v=(v^(uint8_t)s.data[i])*16777619u;return v; }
static uint32_t node_lookup(const ud_run *r,const uint32_t *table,uint32_t size,ud_string s) {
    uint32_t at=id_hash(s)&(size-1);while(table[at]) { uint32_t i=table[at]-1;if(equal(r->nodes[i].info.id,s)) return i;at=(at+1)&(size-1); }return UINT32_MAX;
}
ud_status ud_run_create(const ud_dag *dag,const ud_run_options *options,ud_run **out) {
    if(!out) return UD_INVALID;*out=NULL;
    if(!dag || !options) return UD_INVALID;
    if(options->struct_size<sizeof(*options) || options->abi_version!=UD_ABI_VERSION) return UD_INCOMPATIBLE;
    if(!options->edge_capacity || options->edge_capacity>65536 || !options->max_batch_rows || options->max_batch_rows>65536 ||
       options->handler_count>65536 || (!options->handlers && options->handler_count) ||
       (!options->tenant.data && options->tenant.length) || options->tenant.length>255 ||
       (options->tenant.length && (memchr(options->tenant.data,0,options->tenant.length) || !ud_utf8_valid(options->tenant.data,options->tenant.length)))) return UD_INVALID;
    ud_context *ctx=NULL;ud_status s=ud_context_create(&options->context,&ctx);if(s) return s;
    ud_run *r=ud_alloc(ctx,sizeof(*r));if(!r) { ud_context_destroy(ctx);return UD_RESOURCE; }
    r->ctx=ctx;r->capacity=options->edge_capacity;r->max_rows=options->max_batch_rows;
    r->authorize=options->authorize;r->authorize_user=options->authorize_user;
    atomic_init(&r->state,UD_RUN_CREATED);atomic_init(&r->result,UD_BUSY);
    atomic_init(&r->input_rows,0);atomic_init(&r->output_rows,0);atomic_init(&r->batches,0);atomic_init(&r->queued,0);atomic_init(&r->high_water,0);
    char *serialized=NULL;uint32_t length=0;s=ud_dag_serialize(dag,&serialized,&length);
    if(!s) s=ud_dag_create(ctx,serialized,length,&r->dag);ud_free(serialized);
    if(!s) { ud_dag *bound=NULL;s=ud_dag_bind(r->dag,options->parameter_values,&bound);if(!s) { ud_dag_destroy(r->dag);r->dag=bound; } }
    if(!s) { r->tenant.data=ud_strdup_n(ctx,options->tenant.data?options->tenant.data:"",options->tenant.length);r->tenant.length=options->tenant.length;if(!r->tenant.data) s=UD_RESOURCE; }
    for(uint32_t j=0;!s && j<options->handler_count;j++) {
        const ud_node_handler *h=options->handlers+j;
        if(h->struct_size<sizeof(*h) || h->abi_version!=UD_ABI_VERSION) s=UD_INCOMPATIBLE;
        else if(h->kind<UD_NODE_SOURCE || h->kind>UD_NODE_SINK || !h->operation.data || !h->operation.length ||
                h->operation.length>255 || memchr(h->operation.data,0,h->operation.length) || !ud_utf8_valid(h->operation.data,h->operation.length) ||
                !h->open || !h->event || !h->close) s=UD_INVALID;
        for(uint32_t k=0;!s && k<j;k++) if(h->kind==options->handlers[k].kind && equal(h->operation,options->handlers[k].operation)) s=UD_INVALID;
    }
    if(!s) {
        r->node_count=ud_dag_nodes(r->dag);r->edge_count=ud_dag_edges(r->dag);s=ud_dag_topology(r->dag,&r->topology);
        r->nodes=ud_alloc(ctx,r->node_count*sizeof(*r->nodes));r->edges=ud_alloc(ctx,r->edge_count*sizeof(*r->edges));if(!r->nodes || !r->edges) s=UD_RESOURCE;
    }
    uint32_t table_size=1;while(table_size<r->node_count*2) table_size*=2;
    uint32_t *table=NULL;if(!s) { table=ud_alloc(ctx,table_size*sizeof(*table));if(!table) s=UD_RESOURCE; }
    for(uint32_t i=0;!s && i<r->node_count;i++) {
        run_node *n=r->nodes+i;atomic_init(&n->state,UD_NODE_READY);s=ud_dag_node_get(r->dag,i,&n->info);
        uint32_t at=id_hash(n->info.id)&(table_size-1);while(table[at]) at=(at+1)&(table_size-1);table[at]=i+1;
        if(!s && builtin(&n->info)) n->handler=(ud_node_handler){sizeof(ud_node_handler),UD_ABI_VERSION,n->info.kind,n->info.operation,r,file_open,file_event,file_close};
        for(uint32_t j=0;!s && j<options->handler_count;j++) {
            const ud_node_handler *h=options->handlers+j;
            if(h->kind==n->info.kind && equal(h->operation,n->info.operation)) {
                if(builtin(&n->info)) { s=UD_INVALID;break; }n->handler=*h;n->handler.operation=n->info.operation;
            }
        }
        if(!s && !n->handler.event) s=UD_UNSUPPORTED;
    }
    for(uint32_t j=0;!s && j<r->edge_count;j++) {
        ud_dag_edge info;s=ud_dag_edge_get(r->dag,j,&info);run_edge *e=r->edges+j;
        if(!s) { e->from=node_lookup(r,table,table_size,info.from);e->to=node_lookup(r,table,table_size,info.to);e->port=info.input_port;
            if(e->from==UINT32_MAX || e->to==UINT32_MAX) s=UD_CORRUPT; }
        if(!s) { r->nodes[e->from].output_count++;r->nodes[e->to].input_count++;s=ud_queue_create(ctx,r->capacity,&e->queue); }
    }
    ud_free(table);
    for(uint32_t i=0;!s && i<r->node_count;i++) {
        run_node *n=r->nodes+i;n->inputs=ud_alloc(ctx,(n->input_count?n->input_count:1)*sizeof(*n->inputs));
        n->outputs=ud_alloc(ctx,(n->output_count?n->output_count:1)*sizeof(*n->outputs));if(!n->inputs || !n->outputs) s=UD_RESOURCE;
        n->output_count=0;
    }
    for(uint32_t j=0;!s && j<r->edge_count;j++) { run_edge *e=r->edges+j;r->nodes[e->to].inputs[e->port]=j;run_node *n=r->nodes+e->from;n->outputs[n->output_count++]=j; }
    for(uint32_t i=0;!s && i<r->node_count;i++) {
        run_node *n=r->nodes+r->topology[i];const ud_batch **inputs=ud_alloc(ctx,(n->input_count?n->input_count:1)*sizeof(*inputs));
        if(!inputs) { s=UD_RESOURCE;break; }
        for(uint32_t j=0;j<n->input_count;j++) inputs[j]=r->nodes[r->edges[n->inputs[j]].from].schema;
        s=n->handler.open(n->handler.user,ctx,&n->info,inputs,n->input_count,&n->instance,&n->schema);ud_free(inputs);
        if(!s) { n->opened=1;if(n->info.kind==UD_NODE_SINK ? n->schema!=NULL : (!n->schema || n->schema->ctx!=ctx || ud_batch_rows(n->schema))) s=UD_SCHEMA; }
    }
    if(s) { clear_nodes(r,s);ud_free(r->nodes);ud_free(r->edges);ud_free((void *)r->tenant.data);ud_dag_destroy(r->dag);ud_free(r);ud_status closed=ud_context_destroy(ctx);return closed==UD_OK?s:closed; }
    *out=r;return UD_OK;
}
ud_status ud_run_create_json(const char *json,uint32_t length,const ud_run_options *options,ud_run **out) {
    if(!out) return UD_INVALID;*out=NULL;if(!options) return UD_INVALID;
    if(options->struct_size<sizeof(*options) || options->abi_version!=UD_ABI_VERSION) return UD_INCOMPATIBLE;
    ud_context *ctx=NULL;UD_TRY(ud_context_create(&options->context,&ctx));ud_dag *dag=NULL;
    ud_status s=ud_dag_create(ctx,json,length,&dag);if(!s) s=ud_run_create(dag,options,out);
    ud_dag_destroy(dag);ud_status closed=ud_context_destroy(ctx);return s?s:closed;
}
static ud_status emit(ud_run *r,run_node *n,ud_batch *batch) {
    if(!batch) return UD_OK;
    if(n->info.kind==UD_NODE_SINK || batch->ctx!=r->ctx || !n->schema || batch->columns!=n->schema->columns || batch->rows>r->max_rows) return UD_SCHEMA;
    for(uint32_t j=0;j<batch->columns;j++) if(!ud_field_equal(&batch->column[j].field,&n->schema->column[j].field,1)) return UD_SCHEMA;
    ud_batch **copies=ud_alloc(r->ctx,n->output_count*sizeof(*copies));if(!copies) return UD_RESOURCE;
    ud_status s=UD_OK;for(uint32_t j=0;!s && j<n->output_count;j++) s=ud_batch_clone(batch,copies+j);
    if(!s) for(uint32_t j=0;j<n->output_count;j++) {
        run_edge *e=r->edges+n->outputs[j];s=ud_queue_push(e->queue,copies[j]);
        if(s) break;copies[j]=NULL;e->count++;uint64_t queued=atomic_fetch_add(&r->queued,1)+1;
        if(queued>atomic_load(&r->high_water)) atomic_store(&r->high_water,queued);
    }
    for(uint32_t j=0;j<n->output_count;j++) ud_batch_destroy(copies[j]);ud_free(copies);
    if(!s) { atomic_fetch_add(&r->batches,1);if(n->info.kind==UD_NODE_SOURCE) atomic_fetch_add(&r->input_rows,batch->rows); }
    return s;
}
static ud_status execute_node(ud_run *r,run_node *n,int *progress) {
    if(atomic_load(&n->state)==UD_NODE_COMPLETE) return UD_OK;
    for(uint32_t j=0;j<n->output_count;j++) if(r->edges[n->outputs[j]].count==r->capacity) return UD_OK;
    ud_node_event event={UD_EVENT_POLL,0,NULL};ud_batch *input=NULL;int selected=n->info.kind==UD_NODE_SOURCE || n->ended==n->input_count;
    if(!selected) for(uint32_t j=0;j<n->input_count;j++) {
        uint32_t port=(n->next_port+j)%n->input_count;run_edge *e=r->edges+n->inputs[port];
        if(e->count) { UD_TRY(ud_queue_pop(e->queue,&input));e->count--;atomic_fetch_sub(&r->queued,1);event=(ud_node_event){UD_EVENT_DATA,port,input};selected=1;n->next_port=(port+1)%n->input_count;break; }
        if(!e->ended && atomic_load(&r->nodes[e->from].state)==UD_NODE_COMPLETE) { e->ended=1;n->ended++;event=(ud_node_event){UD_EVENT_INPUT_END,port,NULL};selected=1;n->next_port=(port+1)%n->input_count;break; }
    }
    if(!selected) return UD_OK;
    atomic_store(&n->state,n->ended==n->input_count && n->info.kind!=UD_NODE_SOURCE?UD_NODE_DRAINING:UD_NODE_ACTIVE);
    ud_batch *output=NULL;ud_status s=n->handler.event(n->instance,&event,&output);
    /* BUSY is reserved by wait for a nonterminal timeout. An event is a
     * blocking consumption boundary; it cannot hand input back for retry. */
    if(s==UD_BUSY || s<UD_OK || s>UD_FENCED) s=UD_INVALID;
    if(output && output==input) { output=NULL;s=UD_INVALID; }
    if(!s && event.kind==UD_EVENT_POLL && !output) s=UD_INVALID;
    if(s==UD_END) {
        if(output || (n->info.kind!=UD_NODE_SOURCE && n->ended!=n->input_count)) s=UD_INVALID;
        else { atomic_store(&n->state,UD_NODE_COMPLETE);s=UD_OK; }
    } else if(!s) s=emit(r,n,output);
    if(!s && event.kind==UD_EVENT_DATA && n->info.kind==UD_NODE_SINK) atomic_fetch_add(&r->output_rows,ud_batch_rows(input));
    ud_batch_destroy(output);ud_batch_destroy(input);
    if(s) atomic_store(&n->state,UD_NODE_FAILED);else *progress=1;
    return s;
}
static ud_status execute(ud_run *r) {
    ud_status s=UD_OK;
    while(!s) {
        if(ud_is_cancelled(r->ctx)) { s=UD_CANCELLED;break; }
        int progress=0;uint32_t complete=0;
        for(uint32_t j=r->node_count;!s && j;j--) {
            run_node *n=r->nodes+r->topology[j-1];s=execute_node(r,n,&progress);
            if(atomic_load(&n->state)==UD_NODE_COMPLETE) complete++;
            if(!s && ud_is_cancelled(r->ctx)) s=UD_CANCELLED;
        }
        if(s || complete==r->node_count) break;
        if(!progress) s=UD_CORRUPT;
    }
    /* Instance destruction is kept on the execution thread. Metrics remain
     * available; queues/schemas and handles are released by the controller. */
    for(uint32_t j=r->node_count;j;j--) { run_node *n=r->nodes+r->topology[j-1];if(n->opened) { n->handler.close(n->instance,s);n->opened=0;n->instance=NULL; } }
    atomic_store(&r->result,s);atomic_store(&r->state,s?UD_RUN_FAILED:UD_RUN_COMPLETE);return s;
}
ud_status ud_run_execute(ud_run *r) {
    if(!r) return UD_INVALID;int expected=UD_RUN_CREATED;
    if(!atomic_compare_exchange_strong(&r->state,&expected,UD_RUN_RUNNING)) return UD_BUSY;
    return execute(r);
}
#ifdef _WIN32
static unsigned __stdcall run_thread(void *p) { (void)execute(p);return 0; }
#else
static void *run_thread(void *p) { (void)execute(p);return NULL; }
#endif
ud_status ud_run_start(ud_run *r) {
    if(!r) return UD_INVALID;int expected=UD_RUN_CREATED;
    if(!atomic_compare_exchange_strong(&r->state,&expected,UD_RUN_RUNNING)) return UD_BUSY;
#ifdef _WIN32
    r->thread=(HANDLE)_beginthreadex(NULL,0,run_thread,r,0,NULL);int failed=!r->thread;
#else
    int failed=pthread_create(&r->thread,NULL,run_thread,r)!=0;
#endif
    if(failed) { atomic_store(&r->result,UD_RESOURCE);atomic_store(&r->state,UD_RUN_FAILED);return UD_RESOURCE; }
    r->threaded=1;return UD_OK;
}
ud_status ud_run_wait(ud_run *r,uint32_t timeout) {
    if(!r) return UD_INVALID;uint64_t start=ud_now_ms();
    while(atomic_load(&r->state)==UD_RUN_RUNNING) {
        uint64_t elapsed=ud_now_ms()-start;if(elapsed>=timeout) return UD_BUSY;
        uint64_t left=timeout-elapsed;ud_sleep_ms(left<20?(uint32_t)left:20);
    }
    return atomic_load(&r->state)==UD_RUN_CREATED?UD_BUSY:atomic_load(&r->result);
}
void ud_run_cancel(ud_run *r) { if(r) ud_cancel(r->ctx); }
void ud_run_get_metrics(const ud_run *r,ud_run_metrics *m) {
    if(!m) return;memset(m,0,sizeof(*m));if(!r) { m->result=UD_INVALID;return; }
    m->state=atomic_load(&r->state);m->result=atomic_load(&r->result);m->input_rows=atomic_load(&r->input_rows);
    m->output_rows=atomic_load(&r->output_rows);m->batches=atomic_load(&r->batches);m->queued_batches=atomic_load(&r->queued);
    m->queue_high_water=atomic_load(&r->high_water);m->memory_used=ud_memory_used(r->ctx);
}
ud_status ud_run_node_state(const ud_run *r,uint32_t node,int32_t *state) {
    if(!r || !state || node>=r->node_count) return UD_INVALID;*state=atomic_load(&r->nodes[node].state);return UD_OK;
}
ud_status ud_run_destroy(ud_run *r) {
    if(!r) return UD_OK;if(atomic_load(&r->state)==UD_RUN_RUNNING) return UD_BUSY;
    if(r->threaded) {
#ifdef _WIN32
        if(WaitForSingleObject(r->thread,INFINITE)!=WAIT_OBJECT_0) return UD_IO;CloseHandle(r->thread);
#else
        if(pthread_join(r->thread,NULL)) return UD_IO;
#endif
    }
    ud_context *ctx=r->ctx;clear_nodes(r,atomic_load(&r->state)==UD_RUN_CREATED?UD_CANCELLED:atomic_load(&r->result));ud_free(r->nodes);ud_free(r->edges);
    ud_free((void *)r->tenant.data);ud_dag_destroy(r->dag);ud_free(r);return ud_context_destroy(ctx);
}
