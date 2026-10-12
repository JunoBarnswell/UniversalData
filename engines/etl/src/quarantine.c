#include "ud/quarantine.h"
#include "../../../components/data/internal.h"
#include <errno.h>
struct ud_quarantine_store {
    ud_context *ctx;
    char *directory, *tenant, *job, *batch_id, *position;
    uint32_t tenant_length, job_length, batch_length, position_length;
    uint8_t plan[32];
    void *authorize_user;
    ud_authorize_fn authorize;
};
static int valid_string(ud_string s,int allow_empty) {
    return (s.length || allow_empty) && (s.data || !s.length) && s.length<=1048576 &&
        (!s.length || (!memchr(s.data,0,s.length) && ud_utf8_valid(s.data,s.length)));
}
static ud_status grant(ud_quarantine_store *s,int action) {
    ud_string tenant={s->tenant,s->tenant_length},job={s->job,s->job_length};
    return s->authorize && s->authorize(s->authorize_user,tenant,job,action)==UD_OK?UD_OK:UD_AUTH;
}
static void hex_hash(const uint8_t digest[32],char out[65]) {
    static const char h[]="0123456789abcdef";
    for(uint32_t i=0;i<32;i++) { out[2*i]=h[digest[i]>>4];out[2*i+1]=h[digest[i]&15]; }out[64]=0;
}
static ud_status batch_path(ud_quarantine_store *s,ud_string id,const char *suffix,char **out) {
    if(!valid_string(id,0)) return UD_INVALID;
    uint8_t digest[32];char name[80];ud_hash(id.data,id.length,digest);hex_hash(digest,name);
    size_t n=strlen(suffix);if(n>15) return UD_INVALID;memcpy(name+64,suffix,n+1);
    return ud_join_path(s->ctx,s->directory,name,out);
}
void ud_quarantine_close(ud_quarantine_store *s) {
    if(!s) return;
    ud_free(s->directory);ud_free(s->tenant);ud_free(s->job);ud_free(s->batch_id);ud_free(s->position);ud_free(s);
}
ud_status ud_quarantine_open(ud_context *ctx,const ud_quarantine_options *opts,ud_quarantine_store **out) {
    if(!out) return UD_INVALID;
    *out=NULL;if(!ctx || !opts) return UD_INVALID;
    if(opts->struct_size<sizeof(*opts) || opts->version!=UD_CONFIG_VERSION) return UD_INCOMPATIBLE;
    if(!valid_string(opts->directory,0) || !valid_string(opts->tenant,0) || !valid_string(opts->job,0)) return UD_INVALID;
    if(!opts->authorize || opts->authorize(opts->authorize_user,opts->tenant,opts->job,UD_ACCESS_QUARANTINE_WRITE)!=UD_OK) return UD_AUTH;
    ud_quarantine_store *s=ud_alloc(ctx,sizeof(*s));if(!s) return UD_RESOURCE;
    s->ctx=ctx;s->authorize=opts->authorize;s->authorize_user=opts->authorize_user;
    s->tenant_length=opts->tenant.length;s->job_length=opts->job.length;memcpy(s->plan,opts->plan_fingerprint,32);
    s->tenant=ud_strdup_n(ctx,opts->tenant.data,opts->tenant.length);s->job=ud_strdup_n(ctx,opts->job.data,opts->job.length);
    char *root=ud_strdup_n(ctx,opts->directory.data,opts->directory.length),*tenant_dir=NULL;
    ud_status status=s->tenant && s->job && root?UD_OK:UD_RESOURCE;
    uint8_t digest[32];char name[65];
    if(!status) status=ud_mkdir(ctx,root);
    if(!status) { ud_hash(s->tenant,s->tenant_length,digest);hex_hash(digest,name);status=ud_join_path(ctx,root,name,&tenant_dir); }
    if(!status) status=ud_mkdir(ctx,tenant_dir);
    if(!status) { ud_hash(s->job,s->job_length,digest);hex_hash(digest,name);status=ud_join_path(ctx,tenant_dir,name,&s->directory); }
    if(!status) status=ud_mkdir(ctx,s->directory);
    if(!status) status=ud_sync_parent(ctx,s->directory);
    if(!status) status=ud_sync_parent(ctx,tenant_dir);
    if(!status) status=ud_sync_parent(ctx,root);
    ud_free(root);ud_free(tenant_dir);
    if(status) ud_quarantine_close(s);else *out=s;return status;
}
ud_status ud_quarantine_set_batch(ud_quarantine_store *s,ud_string id,ud_string position) {
    if(!s || !valid_string(id,0) || !valid_string(position,1)) return UD_INVALID;
    UD_TRY(grant(s,UD_ACCESS_QUARANTINE_WRITE));
    char *a=ud_strdup_n(s->ctx,id.data,id.length),*b=ud_strdup_n(s->ctx,position.data?position.data:"",position.length);
    if(!a || !b) { ud_free(a);ud_free(b);return UD_RESOURCE; }
    ud_free(s->batch_id);ud_free(s->position);s->batch_id=a;s->position=b;s->batch_length=id.length;s->position_length=position.length;return UD_OK;
}
static ud_status envelope(ud_quarantine_store *s,const ud_batch *batch,const ud_quality_violation *violations,uint32_t count,ud_bytes *bytes) {
    bytes->ctx=s->ctx;UD_TRY(ud_bytes_put(bytes,"UDQ2",4));UD_TRY(ud_put_u64(bytes,UD_CHECKPOINT_VERSION));
    UD_TRY(ud_put_string(bytes,s->tenant,s->tenant_length));UD_TRY(ud_put_string(bytes,s->job,s->job_length));
    UD_TRY(ud_put_string(bytes,s->batch_id,s->batch_length));UD_TRY(ud_put_string(bytes,s->position,s->position_length));
    UD_TRY(ud_bytes_put(bytes,s->plan,32));
    uint8_t *wire=NULL;uint64_t length=0;ud_status status=ud_batch_serialize(batch,&wire,&length);
    if(!status) status=ud_put_u64(bytes,length);
    if(!status) status=ud_bytes_put(bytes,wire,(size_t)length);ud_free(wire);
    if(status) return status;
    UD_TRY(ud_put_u64(bytes,count));
    for(uint32_t i=0;i<count;i++) {
        if(violations[i].rejected_row>=ud_batch_rows(batch)) return UD_INVALID;
        UD_TRY(ud_put_u64(bytes,violations[i].input_row));UD_TRY(ud_put_u64(bytes,violations[i].rejected_row));UD_TRY(ud_put_u64(bytes,violations[i].rule_index));
    }
    uint8_t digest[32];ud_hash(bytes->data,bytes->size,digest);return ud_bytes_put(bytes,digest,32);
}
static ud_status load(ud_quarantine_store *s,const char *path,ud_bytes *bytes) {
    /* fopen distinguishes missing state from permission / read failure. */
    FILE *f=ud_fopen(path,"rb");if(!f) return errno==ENOENT?UD_END:UD_IO;
    if(fclose(f)) return UD_IO;return ud_read_file(s->ctx,path,bytes);
}
ud_status ud_quarantine_write(void *ptr,const ud_batch *batch,const ud_quality_violation *violations,uint32_t count) {
    ud_quarantine_store *s=ptr;
    if(!s || !s->batch_id || !batch || !ud_batch_rows(batch) || !violations || !count) return UD_INVALID;
    UD_TRY(grant(s,UD_ACCESS_QUARANTINE_WRITE));if(ud_is_cancelled(s->ctx)) return UD_CANCELLED;
    uint32_t rows=ud_batch_rows(batch);uint8_t *covered=ud_alloc(s->ctx,rows);if(!covered) return UD_RESOURCE;
    ud_status coverage=UD_OK;
    for(uint32_t i=0;i<count;i++) {
        if(violations[i].rejected_row>=rows) { coverage=UD_INVALID;break; }
        covered[violations[i].rejected_row]=1;
    }
    for(uint32_t i=0;!coverage && i<rows;i++) if(!covered[i]) coverage=UD_INVALID;
    ud_free(covered);if(coverage) return coverage;
    ud_string id={s->batch_id,s->batch_length};char *path=NULL,*tmp=NULL,*lock_path=NULL;void *lock=NULL;ud_bytes bytes={0},old={0};
    ud_status status=envelope(s,batch,violations,count,&bytes);
    if(!status) status=batch_path(s,id,".udq",&path);
    if(!status) status=batch_path(s,id,".tmp",&tmp);
    if(!status) status=batch_path(s,id,".lock",&lock_path);
    if(!status) status=ud_lock(s->ctx,lock_path,&lock);
    if(!status) {
        ud_status found=load(s,path,&old);
        if(!found) status=old.size==bytes.size && !memcmp(old.data,bytes.data,bytes.size)?UD_OK:UD_CORRUPT;
        else if(found!=UD_END) status=found;
        else {
            status=ud_write_file(s->ctx,tmp,bytes.data,bytes.size);
            if(!status && ud_is_cancelled(s->ctx)) status=UD_CANCELLED;
            if(!status) status=ud_publish(s->ctx,tmp,path);
        }
    }
    if(!status) status=ud_sync_parent(s->ctx,path); /* Revalidate durability after an uncertain publication. */
    ud_unlock(lock);ud_free(path);ud_free(tmp);ud_free(lock_path);ud_bytes_clear(&old);ud_bytes_clear(&bytes);return status;
}
ud_status ud_quarantine_read(ud_quarantine_store *s,ud_string id,ud_batch **batch,
                              ud_quality_violation **violations,uint32_t *count,ud_string *position) {
    if(!s || !batch || !violations || !count || !position || !valid_string(id,0)) return UD_INVALID;
    *batch=NULL;*violations=NULL;*count=0;*position=(ud_string){0};UD_TRY(grant(s,UD_ACCESS_QUARANTINE_READ));
    char *path=NULL;ud_bytes bytes={0};ud_status status=batch_path(s,id,".udq",&path);
    if(!status) status=load(s,path,&bytes);ud_free(path);
    if(status) { ud_bytes_clear(&bytes);return status; }
    uint8_t digest[32];
    if(bytes.size<36) { ud_bytes_clear(&bytes);return UD_CORRUPT; }
    ud_hash(bytes.data,bytes.size-32,digest);
    if(memcmp(digest,bytes.data+bytes.size-32,32)) { ud_bytes_clear(&bytes);return UD_CORRUPT; }
    bytes.size-=32;char magic[4];uint64_t version=0,n=0;
    char *tenant=NULL,*job=NULL,*batch_id=NULL,*source=NULL;
    status=ud_bytes_get(&bytes,magic,4);if(!status && memcmp(magic,"UDQ2",4)) status=UD_INCOMPATIBLE;
    if(!status) status=ud_get_u64(&bytes,&version);if(!status && version!=UD_CHECKPOINT_VERSION) status=UD_INCOMPATIBLE;
    if(!status) status=ud_get_string(&bytes,&tenant);if(!status) status=ud_get_string(&bytes,&job);
    if(!status) status=ud_get_string(&bytes,&batch_id);if(!status) status=ud_get_string(&bytes,&source);
    if(!status && strlen(source)>1048576) status=UD_CORRUPT;
    if(!status && (strcmp(tenant,s->tenant) || strcmp(job,s->job) || strlen(batch_id)!=id.length || memcmp(batch_id,id.data,id.length))) status=UD_CORRUPT;
    if(!status) status=ud_bytes_get(&bytes,digest,32);if(!status && memcmp(digest,s->plan,32)) status=UD_INCOMPATIBLE;
    if(!status) status=ud_get_u64(&bytes,&n);
    if(!status && (n>SIZE_MAX || n>bytes.size-bytes.pos)) status=UD_CORRUPT;
    if(!status) { status=ud_batch_deserialize(s->ctx,bytes.data+bytes.pos,n,batch);bytes.pos+=(size_t)n; }
    if(!status) status=ud_get_u64(&bytes,&n);
    if(!status && (!n || n>UINT32_MAX || n>SIZE_MAX/sizeof(**violations) || n>(bytes.size-bytes.pos)/24)) status=UD_CORRUPT;
    if(!status) { *count=(uint32_t)n;*violations=ud_alloc(s->ctx,(size_t)n*sizeof(**violations));if(!*violations) status=UD_RESOURCE; }
    for(uint32_t i=0;!status && i<*count;i++) {
        uint64_t a=0,b=0,c=0;status=ud_get_u64(&bytes,&a);if(!status) status=ud_get_u64(&bytes,&b);if(!status) status=ud_get_u64(&bytes,&c);
        if(!status && (a>UINT32_MAX || b>=ud_batch_rows(*batch) || c>UINT32_MAX)) status=UD_CORRUPT;
        if(!status) (*violations)[i]=(ud_quality_violation){(uint32_t)a,(uint32_t)b,(uint32_t)c};
    }
    if(!status && bytes.pos!=bytes.size) status=UD_CORRUPT;
    if(!status) { position->length=(uint32_t)strlen(source);position->data=source;source=NULL; }
    else { ud_batch_destroy(*batch);*batch=NULL;ud_free(*violations);*violations=NULL;*count=0; }
    ud_free(source);ud_free(tenant);ud_free(job);ud_free(batch_id);ud_bytes_clear(&bytes);return status;
}
