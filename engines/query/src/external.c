#include "ud/query.h"
#include "ud/format.h"
#include "../../../components/compute/internal.h"
typedef struct owned_file { char *path;struct owned_file *next; } owned_file;
typedef struct external_state {
    ud_context *ctx;ud_external_options options;char *directory;
    uint64_t next, written;void *lock;owned_file *files;
} external_state;
typedef struct bucket { char *path;ud_json_writer *writer; } bucket;
static ud_status path_copy(ud_context *ctx,ud_string s,char **out) {
    *out=NULL;
    if(!s.data || !s.length || s.length>1048576 || memchr(s.data,0,s.length)) return UD_INVALID;
    if(!ud_utf8_valid(s.data,s.length)) return UD_PARSE;
    *out=ud_strdup_n(ctx,s.data,s.length);return *out?UD_OK:UD_RESOURCE;
}
static ud_status reserve(external_state *state,char **out) {
    *out=NULL;owned_file *owned=ud_alloc(state->ctx,sizeof(*owned));if(!owned) return UD_RESOURCE;
    char name[80];snprintf(name,sizeof(name),"ud-external-%020llu.ndjson",(unsigned long long)state->next++);
    ud_status s=ud_join_path(state->ctx,state->directory,name,&owned->path);
    if(!s) s=ud_reserve_file(state->ctx,owned->path);
    if(s) { ud_free(owned->path);ud_free(owned);return s; }
    owned->next=state->files;state->files=owned;*out=owned->path;return UD_OK;
}
static ud_status initialize(ud_context *ctx,const ud_external_options *options,external_state *state) {
    memset(state,0,sizeof(*state));state->ctx=ctx;
    if(!ctx || !options) return UD_INVALID;
    if(options->struct_size<sizeof(*options) || options->version!=UD_CONFIG_VERSION) return UD_INCOMPATIBLE;
    if(!options->block_rows || options->block_rows>65536 || !options->max_record_bytes || !options->spill_limit) return UD_INVALID;
    state->options=*options;state->next=1;
    ud_status s=path_copy(ctx,options->temporary_directory,&state->directory);if(s) return s;
    char *lock_path=NULL;s=ud_join_path(ctx,state->directory,"ud-external.lock",&lock_path);
    if(!s) s=ud_lock(ctx,lock_path,&state->lock);ud_free(lock_path);return s;
}
static ud_status cleanup(external_state *state,ud_status status) {
    owned_file *file=state->files;
    while(file) { owned_file *next=file->next;ud_status s=ud_remove_file(state->ctx,file->path);if(!status && s) status=s;ud_free(file->path);ud_free(file);file=next; }
    ud_unlock(state->lock);ud_free(state->directory);return status;
}
static ud_status fields_of(ud_context *ctx,const ud_batch *schema,ud_field **out) {
    *out=ud_alloc(ctx,schema->columns*sizeof(**out));if(!*out) return UD_RESOURCE;
    for(uint32_t j=0;j<schema->columns;j++) (*out)[j]=schema->column[j].field;return UD_OK;
}
static ud_status open_reader(external_state *state,const char *path,const ud_field *fields,uint32_t n,ud_json_reader **out) {
    ud_json_options opts={sizeof(opts),UD_CONFIG_VERSION,state->options.block_rows,state->options.max_record_bytes,UD_ROUND_EXACT};
    return ud_json_reader_open(state->ctx,path,fields,n,&opts,out);
}
static ud_status append(external_state *state,ud_json_writer *writer,const ud_batch *batch) {
    /* Exact logical NDJSON size: an array of N objects replaces N-1 commas with
     * newlines, removes two brackets and adds the final newline => length-1. */
    if(!ud_batch_rows(batch)) return UD_OK;
    char *json=NULL;uint32_t length=0;UD_TRY(ud_batch_to_json(batch,&json,&length));ud_free(json);
    uint64_t bytes=(uint64_t)length-1;
    if(state->written>state->options.spill_limit || bytes>state->options.spill_limit-state->written) return UD_RESOURCE;
    UD_TRY(ud_json_writer_append(writer,batch));state->written+=bytes;return UD_OK;
}
static uint64_t hash_byte(uint64_t hash,uint8_t byte) { return (hash^byte)*UINT64_C(1099511628211); }
static uint64_t hash_integer(uint64_t hash,uint64_t value) {
    for(uint32_t j=0;j<8;j++) hash=hash_byte(hash,(uint8_t)(value>>(8*j)));return hash;
}
static uint64_t hash_value(uint64_t hash,const ud_value *v) {
    hash=hash_integer(hash,(uint64_t)v->type);hash=hash_byte(hash,(uint8_t)v->is_null);if(v->is_null) return hash;
    if(v->type==UD_UTF8 || v->type==UD_BINARY) {
        hash=hash_integer(hash,v->text_length);for(uint32_t j=0;j<v->text_length;j++) hash=hash_byte(hash,(uint8_t)v->text[j]);
    } else if(v->type>=UD_STRUCT) {
        hash=hash_integer(hash,v->child_count);for(uint32_t j=0;j<v->child_count;j++) hash=hash_value(hash,v->children+j);
    } else if(v->type==UD_DECIMAL128) { hash=hash_integer(hash,v->decimal.low);hash=hash_integer(hash,(uint64_t)v->decimal.high); }
    else if(v->type==UD_FLOAT64) { uint64_t bits=0;if(v->real!=0) memcpy(&bits,&v->real,8);hash=hash_integer(hash,bits); }
    else hash=hash_integer(hash,(uint64_t)v->integer);return hash;
}
static ud_status buckets_close(bucket buckets[16],ud_status status) {
    for(uint32_t j=0;j<16;j++) { ud_status s=ud_json_writer_close(buckets[j].writer);buckets[j].writer=NULL;if(!status && s) status=s; }return status;
}
static ud_status partition(external_state *state,const char *path,const ud_field *fields,uint32_t n,
                            const uint32_t *keys,uint32_t key_count,const ud_plan *filter,uint32_t depth,int skip_null,bucket buckets[16]) {
    ud_json_reader *reader=NULL;ud_status s=open_reader(state,path,fields,n,&reader);
    while(!s) {
        ud_batch *batch=NULL;s=ud_json_reader_next(reader,&batch);
        if(s==UD_END) { s=UD_OK;break; }if(s) break;
        for(uint32_t row=0;!s && row<batch->rows;row++) {
            if(ud_is_cancelled(state->ctx)) { s=UD_CANCELLED;break; }
            if(filter) { int accepted=0;s=ud_plan_accept(filter,batch,row,&accepted);if(s || !accepted) continue; }
            /* Murmur finalization of a depth-salted FNV key gives independent
             * buckets on repartition, including common prefixes / integer keys. */
            uint64_t hash=UINT64_C(14695981039346656037)^((uint64_t)depth*UINT64_C(0x9e3779b97f4a7c15));int null=0;
            for(uint32_t j=0;!s && j<key_count;j++) { ud_value v;s=ud_batch_get(batch,row,keys[j],&v);if(!s) { null|=v.is_null;hash=hash_value(hash,&v); } }
            if(s || (skip_null && null)) continue;
            hash^=hash>>33;hash*=UINT64_C(0xff51afd7ed558ccd);hash^=hash>>33;hash*=UINT64_C(0xc4ceb9fe1a85ec53);hash^=hash>>33;
            bucket *b=buckets+(hash&15);
            if(!b->writer) { s=reserve(state,&b->path);if(!s) s=ud_json_writer_open(state->ctx,b->path,fields,n,state->options.max_record_bytes,&b->writer); }
            ud_batch *one=NULL;if(!s) s=ud_query_slice(state->ctx,batch,row,1,&one);if(!s) s=append(state,b->writer,one);ud_batch_destroy(one);
        }
        ud_batch_destroy(batch);
    }
    ud_json_reader_close(reader);return buckets_close(buckets,s);
}
static ud_status aggregate_file(external_state *state,const char *path,const ud_plan *plan,
                                 uint32_t depth,ud_json_writer *writer) {
    ud_json_reader *reader=NULL;ud_accumulator *acc=NULL;ud_status s=ud_accumulator_create(plan,&acc);
    if(!s && path) s=open_reader(state,path,plan->fields,plan->n,&reader);
    while(!s && reader) {
        ud_batch *batch=NULL;s=ud_json_reader_next(reader,&batch);if(s==UD_END) { s=UD_OK;break; }
        if(!s) s=ud_accumulator_add(acc,batch);ud_batch_destroy(batch);
    }
    ud_json_reader_close(reader);ud_batch *result=NULL;
    if(!s) s=ud_accumulator_snapshot(acc,&result);ud_accumulator_destroy(acc);
    if(s==UD_RESOURCE && path && plan->key_n && depth<8) {
        ud_batch_destroy(result);result=NULL;bucket buckets[16]={{0}};
        s=partition(state,path,plan->fields,plan->n,plan->keys,plan->key_n,NULL,depth+1,0,buckets);
        for(uint32_t j=0;!s && j<16;j++) if(buckets[j].path) s=aggregate_file(state,buckets[j].path,plan,depth+1,writer);
        return s;
    }
    /* Do not retry quota failures after any result is emitted. */
    if(!s) s=append(state,writer,result);ud_batch_destroy(result);return s;
}
static ud_status destination(external_state *state,ud_string output,char **out,char **staged) {
    *staged=NULL;UD_TRY(path_copy(state->ctx,output,out));
    uint64_t device=0,file=0;
    if(!ud_file_identity(state->ctx,*out,&device,&file)) return UD_BUSY;
    /* Reserve an exclusive staging path; final ud_publish never replaces a name. */
    return reserve(state,staged);
}
ud_status ud_query_aggregate_ndjson(ud_context *ctx,ud_string input,ud_string output,
                                    const ud_plan *plan,const ud_external_options *options) {
    if(!ctx || !plan || plan->ctx!=ctx || !plan->agg_n) return UD_INVALID;
    external_state state;ud_status s=initialize(ctx,options,&state);
    if(s) return cleanup(&state,s);
    char *source=NULL,*target=NULL,*staged=NULL;s=path_copy(ctx,input,&source);
    if(!s) s=destination(&state,output,&target,&staged);
    ud_accumulator *empty=NULL;ud_batch *schema=NULL;ud_field *fields=NULL;ud_json_writer *writer=NULL;
    if(!s) s=ud_accumulator_create(plan,&empty);if(!s) s=ud_accumulator_snapshot(empty,&schema);ud_accumulator_destroy(empty);
    if(!s) s=fields_of(ctx,schema,&fields);
    if(!s) s=ud_json_writer_open(ctx,staged,fields,schema->columns,state.options.max_record_bytes,&writer);
    ud_free(fields);ud_batch_destroy(schema);
    if(!s && !plan->key_n) s=aggregate_file(&state,source,plan,0,writer);
    else if(!s) {
        bucket buckets[16]={{0}};s=partition(&state,source,plan->fields,plan->n,plan->keys,plan->key_n,plan,0,0,buckets);
        for(uint32_t j=0;!s && j<16;j++) if(buckets[j].path) s=aggregate_file(&state,buckets[j].path,plan,0,writer);
    }
    ud_status close=ud_json_writer_close(writer);if(!s) s=close;
    if(!s && ud_is_cancelled(ctx)) s=UD_CANCELLED;if(!s) s=ud_publish(ctx,staged,target);
    ud_free(source);ud_free(target);return cleanup(&state,s);
}
static ud_status join_bucket(external_state *state,const char *left,const ud_field *lf,uint32_t ln,uint32_t lk,
                              const char *right,const ud_field *rf,uint32_t rn,uint32_t rk,ud_json_writer *writer) {
    ud_json_reader *left_reader=NULL;ud_status s=open_reader(state,left,lf,ln,&left_reader);ud_field *fields=ud_alloc(state->ctx,(ln+rn)*sizeof(*fields));
    ud_value *values=ud_alloc(state->ctx,(ln+rn)*sizeof(*values));if(!fields || !values) s=UD_RESOURCE;
    if(!s) { memcpy(fields,lf,ln*sizeof(*fields));memcpy(fields+ln,rf,rn*sizeof(*fields)); }
    while(!s) {
        ud_batch *l=NULL;s=ud_json_reader_next(left_reader,&l);if(s==UD_END) { s=UD_OK;break; }if(s) break;
        ud_json_reader *right_reader=NULL;s=open_reader(state,right,rf,rn,&right_reader);ud_batch *output=NULL;
        if(!s) s=ud_batch_create(state->ctx,fields,ln+rn,state->options.block_rows,&output);
        while(!s) {
            ud_batch *r=NULL;s=ud_json_reader_next(right_reader,&r);if(s==UD_END) { s=UD_OK;break; }if(s) break;
            for(uint32_t a=0;!s && a<l->rows;a++) {
                ud_value key_l;ud_batch_get(l,a,lk,&key_l);
                for(uint32_t b=0;!s && b<r->rows;b++) {
                    if(ud_is_cancelled(state->ctx)) { s=UD_CANCELLED;break; }
                    ud_value key_r;ud_batch_get(r,b,rk,&key_r);if(key_l.is_null || key_r.is_null || ud_value_compare(&key_l,&key_r)) continue;
                    for(uint32_t j=0;!s && j<ln;j++) s=ud_batch_get(l,a,j,values+j);
                    for(uint32_t j=0;!s && j<rn;j++) s=ud_batch_get(r,b,j,values+ln+j);
                    if(!s) s=ud_batch_append(output,values,ln+rn);
                    if(!s && output->rows==state->options.block_rows) {
                        s=append(state,writer,output);ud_batch_destroy(output);output=NULL;
                        if(!s) s=ud_batch_create(state->ctx,fields,ln+rn,state->options.block_rows,&output);
                    }
                }
            }
            ud_batch_destroy(r);
        }
        if(!s && output && output->rows) s=append(state,writer,output);
        ud_batch_destroy(output);ud_json_reader_close(right_reader);ud_batch_destroy(l);
    }
    ud_json_reader_close(left_reader);ud_free(fields);ud_free(values);return s;
}
ud_status ud_query_join_ndjson(ud_context *ctx,ud_string left,const ud_batch *left_schema,uint32_t lk,
                               ud_string right,const ud_batch *right_schema,uint32_t rk,ud_string output,const ud_external_options *options) {
    if(!ctx || !left_schema || !right_schema || lk>=left_schema->columns || rk>=right_schema->columns || left_schema->columns>65536-right_schema->columns) return UD_INVALID;
    ud_field a=left_schema->column[lk].field,b=right_schema->column[rk].field;b.nullable=a.nullable;
    if(!ud_field_equal(&a,&b,0)) return UD_SCHEMA;
    external_state state;ud_status s=initialize(ctx,options,&state);if(s) return cleanup(&state,s);
    char *lp=NULL,*rp=NULL,*target=NULL,*staged=NULL;
    s=path_copy(ctx,left,&lp);if(!s) s=path_copy(ctx,right,&rp);ud_field *lf=NULL,*rf=NULL,*joined=NULL;ud_batch *schema=NULL;ud_json_writer *writer=NULL;
    if(!s) s=fields_of(ctx,left_schema,&lf);if(!s) s=fields_of(ctx,right_schema,&rf);
    uint32_t ln=left_schema->columns,rn=right_schema->columns;
    if(!s) { joined=ud_alloc(ctx,(ln+rn)*sizeof(*joined));if(!joined) s=UD_RESOURCE;else { memcpy(joined,lf,ln*sizeof(*joined));memcpy(joined+ln,rf,rn*sizeof(*joined)); } }
    if(!s) s=ud_batch_create(ctx,joined,ln+rn,0,&schema); /* Reject ambiguous field names before reading. */
    if(!s) s=destination(&state,output,&target,&staged);
    if(!s) s=ud_json_writer_open(ctx,staged,joined,ln+rn,state.options.max_record_bytes,&writer);
    bucket l[16]={{0}},r[16]={{0}};
    if(!s) s=partition(&state,lp,lf,ln,&lk,1,NULL,0,1,l);
    if(!s) s=partition(&state,rp,rf,rn,&rk,1,NULL,0,1,r);
    for(uint32_t j=0;!s && j<16;j++) if(l[j].path && r[j].path) s=join_bucket(&state,l[j].path,lf,ln,lk,r[j].path,rf,rn,rk,writer);
    ud_status close=ud_json_writer_close(writer);if(!s) s=close;
    if(!s && ud_is_cancelled(ctx)) s=UD_CANCELLED;if(!s) s=ud_publish(ctx,staged,target);
    ud_batch_destroy(schema);ud_free(joined);ud_free(lf);ud_free(rf);ud_free(lp);ud_free(rp);ud_free(target);return cleanup(&state,s);
}
