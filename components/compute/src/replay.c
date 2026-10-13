#include "replay.h"
#include "ud/cache.h"
#include "ud/format.h"
/* Finite, budgeted event-sourced state. Recovery calls the same Native kernels
 * and verifies emitted Batch hashes. No effects, alternate operator or scheduler
 * are introduced. The log is enabled only for journal-backed root operators. */
struct ud_replay {
    ud_context *ctx;ud_operator_options options;ud_expression_options expressions;
    ud_batch *inputs[64],*reference_schema,*schema;ud_state *events;
    ud_sort_contract *order;uint32_t *keys,*right_keys,*reference_keys,count;
    uint8_t fingerprint[32];int active;
};
static ud_status digest_schema(ud_sha256*h,const ud_batch*b){uint8_t digest[32];UD_TRY(ud_schema_fingerprint(b,digest));ud_sha256_update(h,digest,32);return UD_OK;}
static ud_status copy_indices(ud_context*c,const uint32_t*input,uint32_t n,uint32_t**out){if(!input)return UD_OK;*out=ud_alloc(c,(size_t)n*sizeof(**out));if(!*out)return UD_RESOURCE;memcpy(*out,input,(size_t)n*sizeof(**out));return UD_OK;}
ud_status ud_replay_create(ud_context*ctx,const ud_operator_options*o,const ud_batch*const*inputs,ud_replay**out){
    *out=NULL;if(!o->record_state)return UD_OK;ud_replay*r=ud_alloc(ctx,sizeof(*r));if(!r)return UD_RESOURCE;r->ctx=ctx;r->options=*o;r->options.aggregation=NULL;r->options.reference=NULL;r->count=o->input_count?o->input_count:o->kind==UD_OPERATOR_JOIN||o->kind==UD_OPERATOR_MERGE||o->kind==UD_OPERATOR_MERGE_JOIN||o->kind==UD_OPERATOR_ASOF_JOIN||o->kind==UD_OPERATOR_TEMPORAL_JOIN||o->kind==UD_OPERATOR_DIFF_COMPARE?2:1;if(o->kind==UD_OPERATOR_SCD)r->count=2;
    ud_status s=r->count>64?UD_SCHEMA:UD_OK;ud_sha256 h;ud_sha256_init(&h);
    for(uint32_t i=0;!s&&i<r->count;i++){if(!inputs[i]){s=UD_SCHEMA;break;}s=ud_batch_create(ctx,inputs[i]->schema->fields,inputs[i]->columns,0,r->inputs+i);if(!s)s=digest_schema(&h,inputs[i]);}
    if(!s)s=copy_indices(ctx,o->keys,o->key_count,&r->keys);if(!s)s=copy_indices(ctx,o->right_keys,o->key_count,&r->right_keys);if(!s)s=copy_indices(ctx,o->reference_keys,o->reference_key_count,&r->reference_keys);
    r->options.keys=r->keys;r->options.right_keys=r->right_keys;r->options.reference_keys=r->reference_keys;
    if(!s&&o->input_order){r->order=ud_alloc(ctx,r->count*sizeof(*r->order));if(!r->order)s=UD_RESOURCE;else memcpy(r->order,o->input_order,r->count*sizeof(*r->order));}r->options.input_order=r->order;
    r->options.configuration=(ud_string){NULL,o->configuration.length};if(!s&&o->configuration.length){r->options.configuration.data=ud_strdup_n(ctx,o->configuration.data,o->configuration.length);if(!r->options.configuration.data)s=UD_RESOURCE;}
    r->options.window_name=(ud_string){NULL,o->window_name.length};if(!s&&o->window_name.length){r->options.window_name.data=ud_strdup_n(ctx,o->window_name.data,o->window_name.length);if(!r->options.window_name.data)s=UD_RESOURCE;}
    if(!s&&o->reference_schema){s=ud_batch_create(ctx,o->reference_schema->schema->fields,o->reference_schema->columns,0,&r->reference_schema);r->options.reference_schema=r->reference_schema;}
    if(!s&&o->expression_bindings){r->expressions=*o->expression_bindings;r->expressions.schema=r->inputs[0];r->options.expression_bindings=&r->expressions;}
    if(!s&&o->aggregation){s=ud_plan_retain(o->aggregation);if(!s)r->options.aggregation=o->aggregation;}
    if(!s&&o->reference){s=ud_cache_retain(o->reference);if(!s){r->options.reference=o->reference;uint8_t cache[32];s=ud_cache_fingerprint(o->reference,cache);if(!s)ud_sha256_update(&h,cache,32);}}
    ud_bytes metadata={.ctx=ctx};uint64_t values[]={UD_ABI_VERSION,o->kind,o->max_batch_rows,o->partition_count,o->key_count,o->descending,o->partition_key_count,o->window_column,o->window_function,o->window_offset,o->frame_kind,o->frame_preceding,o->frame_following,o->average_rounding};
    for(uint32_t i=0;!s&&i<sizeof(values)/sizeof(*values);i++)s=ud_put_u64(&metadata,values[i]);for(uint32_t i=0;!s&&i<o->key_count&&o->keys;i++)s=ud_put_u64(&metadata,o->keys[i]);for(uint32_t i=0;!s&&i<o->key_count&&o->right_keys;i++)s=ud_put_u64(&metadata,o->right_keys[i]);
    if(!s){ud_sha256_update(&h,metadata.data,metadata.size);ud_sha256_update(&h,o->configuration.data,o->configuration.length);ud_sha256_update(&h,o->window_name.data,o->window_name.length);if(o->aggregation){uint8_t plan[32];ud_plan_fingerprint(o->aggregation,plan);ud_sha256_update(&h,plan,32);}ud_sha256_finish(&h,r->fingerprint);}ud_bytes_clear(&metadata);
    ud_field f[]={{.name="kind",.name_length=4,.type=UD_INT64},{.name="port",.name_length=4,.type=UD_INT64},{.name="payload",.name_length=7,.type=UD_BINARY,.nullable=1}};
    if(!s)s=ud_batch_create(ctx,f,3,0,&r->schema);if(s){ud_replay_destroy(r);return s;}*out=r;return UD_OK;
}
static ud_status append(ud_replay*r,int kind,uint32_t port,const void*data,uint32_t length){ud_batch*b=NULL;UD_TRY(ud_batch_create(r->ctx,r->schema->schema->fields,3,1,&b));ud_value v[]={{.type=UD_INT64,.integer=kind},{.type=UD_INT64,.integer=port},{.type=UD_BINARY,.is_null=!data,.text=data,.text_length=length}};ud_status s=ud_batch_append(b,v,3);if(!s)s=ud_batch_seal(b);if(!s)s=ud_state_append(r->events,b,0);ud_batch_destroy(b);return s;}
ud_status ud_replay_record(ud_replay*r,int32_t kind,uint32_t port,const ud_batch*b){if(!r||r->active)return UD_OK;
    if(!r->events){UD_TRY(ud_state_create(r->ctx,r->options.buffers,r->schema,r->options.max_batch_rows,&r->events));UD_TRY(append(r,0,0,r->fingerprint,32));}
    if(kind==2)return append(r,kind,port,NULL,0);
    if(kind==3){uint8_t digest[32];UD_TRY(ud_batch_fingerprint(b,digest));return append(r,kind,port,digest,32);}
    size_t size;UD_TRY(ud_wire_size(b,&size));if(size>UINT32_MAX)return UD_RESOURCE;void*bytes=ud_alloc(r->ctx,size);if(!bytes)return UD_RESOURCE;size_t written;ud_status s=ud_wire_write(b,bytes,size,&written);if(!s)s=append(r,kind,port,bytes,(uint32_t)size);ud_free(bytes);return s;
}
ud_status ud_replay_snapshot(ud_replay*r,ud_batch**out){if(!r||!out)return UD_UNSUPPORTED;*out=NULL;if(!r->events){UD_TRY(ud_state_create(r->ctx,r->options.buffers,r->schema,r->options.max_batch_rows,&r->events));UD_TRY(append(r,0,0,r->fingerprint,32));}
    if(r->events->rows>UINT32_MAX)return UD_RESOURCE;UD_TRY(ud_batch_create(r->ctx,r->schema->schema->fields,3,(uint32_t)r->events->rows,out));ud_state_reader reader={0};ud_status s=UD_OK;
    for(uint64_t i=0;!s&&i<r->events->rows;i++){const ud_batch*b;uint32_t row;s=ud_state_row(&reader,r->events,i,&b,&row);if(!s)s=ud_batch_append_from(*out,b,row);}ud_state_reader_clear(&reader);if(!s)s=ud_batch_seal(*out);if(s){ud_batch_destroy(*out);*out=NULL;}return s;
}
ud_status ud_replay_restore(ud_replay*r,const ud_batch*snapshot,ud_operator**out){if(!r||!snapshot||!out||!snapshot->sealed||snapshot->columns!=3||!snapshot->rows)return UD_INVALID;*out=NULL;
    for(uint32_t i=0;i<3;i++)if(!ud_field_matches(&r->schema->column[i].field,&snapshot->column[i].field,UD_FIELD_PORT))return UD_CORRUPT;
    ud_value kind,port,payload;UD_TRY(ud_batch_get(snapshot,0,0,&kind));UD_TRY(ud_batch_get(snapshot,0,1,&port));UD_TRY(ud_batch_get(snapshot,0,2,&payload));if(kind.integer||port.integer||payload.is_null||payload.text_length!=32||memcmp(payload.text,r->fingerprint,32))return UD_FENCED;
    ud_operator_options options=r->options;options.record_state=0;ud_status s=ud_operator_create(r->ctx,&options,(const ud_batch*const*)r->inputs,out);
    for(uint32_t i=1;!s&&i<snapshot->rows;i++){s=ud_batch_get(snapshot,i,0,&kind);if(!s)s=ud_batch_get(snapshot,i,1,&port);if(!s)s=ud_batch_get(snapshot,i,2,&payload);if(!s&&(kind.is_null||port.is_null||port.integer<0||port.integer>63))s=UD_CORRUPT;
        ud_batch*b=NULL;uint32_t output=0;
        if(!s&&kind.integer==1){if(payload.is_null)s=UD_CORRUPT;else s=ud_wire_read(r->ctx,payload.text,payload.text_length,&b);if(!s)s=ud_operator_push(*out,(uint32_t)port.integer,b);}
        else if(!s&&kind.integer==2){if(!payload.is_null)s=UD_CORRUPT;else s=ud_operator_finish_input(*out,(uint32_t)port.integer);}
        else if(!s&&kind.integer==3){s=ud_operator_pull(*out,&b,&output);if(!s){uint8_t digest[32];s=ud_batch_fingerprint(b,digest);if(!s&&(output!=(uint32_t)port.integer||payload.is_null||payload.text_length!=32||memcmp(payload.text,digest,32)))s=UD_CORRUPT;}}
        else if(!s)s=UD_CORRUPT;ud_batch_destroy(b);
    }
    if(s){ud_operator_destroy(*out);*out=NULL;}return s;
}
void ud_replay_destroy(ud_replay*r){if(!r)return;for(uint32_t i=0;i<64;i++)ud_batch_destroy(r->inputs[i]);ud_batch_destroy(r->reference_schema);ud_batch_destroy(r->schema);ud_state_destroy(r->events);ud_free(r->keys);ud_free(r->right_keys);ud_free(r->reference_keys);ud_free(r->order);ud_free((void*)r->options.configuration.data);ud_free((void*)r->options.window_name.data);ud_plan_destroy(r->options.aggregation);ud_cache_release(r->options.reference);ud_free(r);}

ud_status ud_replay_rebase(ud_replay*r,const ud_batch*b){ud_state*state=NULL;UD_TRY(ud_state_create(r->ctx,r->options.buffers,r->schema,r->options.max_batch_rows,&state));ud_status s=ud_state_add_batch(state,b);if(s){ud_state_destroy(state);return s;}ud_state_destroy(r->events);r->events=state;return UD_OK;}
