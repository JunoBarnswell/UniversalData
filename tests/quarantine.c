#include "ud/quarantine.h"
#include "../components/foundation/internal.h"
#include "check.h"
static ud_status authorize(void *ptr,ud_string tenant,ud_string resource,int32_t action) {
    (void)action;return *(int *)ptr && tenant.length==8 && !memcmp(tenant.data,"tenant-a",8) &&
        resource.length==5 && !memcmp(resource.data,"job-a",5)?UD_OK:UD_AUTH;
}
static ud_string str(const char *s) { return (ud_string){s,(uint32_t)strlen(s)}; }
static void digest_hex(const void *p,size_t n,char out[65]) {
    uint8_t hash[32];ud_hash(p,n,hash);static const char h[]="0123456789abcdef";
    for(uint32_t i=0;i<32;i++) { out[2*i]=h[hash[i]>>4];out[2*i+1]=h[hash[i]&15]; }out[64]=0;
}
int main(int argc,char **argv) {
    CHECK(argc==2);ud_context *ctx=NULL;OK(ud_context_create(NULL,&ctx));int allowed=1;
    ud_quarantine_options opts={0};opts.struct_size=sizeof(opts);opts.version=UD_CONFIG_VERSION;
    opts.directory=str(argv[1]);opts.tenant=str("tenant-a");opts.job=str("job-a");opts.plan_fingerprint[0]=42;
    opts.authorize=authorize;opts.authorize_user=&allowed;ud_quarantine_store *store=NULL;
    OK(ud_quarantine_open(ctx,&opts,&store));
    OK(ud_quarantine_set_batch(store,str("batch-1"),str("cursor/1")));
    ud_field field={.name="id",.name_length=2,.type=UD_INT64,.nullable=0};ud_batch *batch=NULL;
    OK(ud_batch_create(ctx,&field,1,2,&batch));ud_value row={.type=UD_INT64,.integer=-1};OK(ud_batch_append(batch,&row,1));
    ud_quality_violation violation={0,0,0};OK(ud_quarantine_write(store,batch,&violation,1));
    OK(ud_quarantine_write(store,batch,&violation,1)); /* Idempotent complete receipt. */
    violation.rule_index=1;CHECK(ud_quarantine_write(store,batch,&violation,1)==UD_CORRUPT);violation.rule_index=0;
    ud_quarantine_close(store);OK(ud_quarantine_open(ctx,&opts,&store));
    ud_batch *bad=NULL;ud_quality_violation *found=NULL;uint32_t count=0;ud_string position={0};
    OK(ud_quarantine_read(store,str("batch-1"),&bad,&found,&count,&position));
    CHECK(count==1 && found[0].rule_index==0 && position.length==8 && !memcmp(position.data,"cursor/1",8));
    ud_value value;OK(ud_batch_get(bad,0,0,&value));CHECK(value.integer==-1);
    ud_batch_destroy(bad);ud_free(found);ud_free((void *)position.data);
    CHECK(ud_quarantine_read(store,str("missing"),&bad,&found,&count,&position)==UD_END);
    allowed=0;CHECK(ud_quarantine_read(store,str("batch-1"),&bad,&found,&count,&position)==UD_AUTH);
    allowed=1;
    ud_quality_rule rule={0};rule.struct_size=sizeof(rule);rule.version=UD_CONFIG_VERSION;rule.id="positive";rule.id_length=8;
    uint32_t column=0;rule.kind=UD_RULE_RANGE;rule.columns=&column;rule.column_count=1;
    rule.minimum=(ud_value){.type=UD_INT64,.integer=0};rule.maximum=(ud_value){.type=UD_INT64,.is_null=1};
    ud_quality *quality=NULL;OK(ud_quality_create(ctx,&field,1,&rule,1,UD_QUALITY_QUARANTINE,ud_quarantine_write,store,&quality));
    OK(ud_quarantine_set_batch(store,str("batch-2"),str("cursor/2")));
    OK(ud_quality_apply(quality,batch,&bad));CHECK(!ud_batch_rows(bad));ud_batch_destroy(bad);
    ud_quality_metrics metrics;ud_quality_get_metrics(quality,&metrics);CHECK(metrics.processed==1 && metrics.quarantined==1);
    OK(ud_quarantine_read(store,str("batch-2"),&bad,&found,&count,&position));CHECK(count==1 && ud_batch_rows(bad)==1);
    ud_batch_destroy(bad);ud_free(found);ud_free((void *)position.data);ud_quality_destroy(quality);
    OK(ud_quality_create(ctx,&field,1,&rule,1,UD_QUALITY_QUARANTINE,ud_quarantine_write,store,&quality));
    allowed=0;CHECK(ud_quality_apply(quality,batch,&bad)==UD_AUTH);
    ud_quality_get_metrics(quality,&metrics);CHECK(!metrics.processed);ud_quality_destroy(quality);allowed=1;
    ud_quarantine_close(store);opts.plan_fingerprint[0]=41;OK(ud_quarantine_open(ctx,&opts,&store));
    CHECK(ud_quarantine_read(store,str("batch-1"),&bad,&found,&count,&position)==UD_INCOMPATIBLE);ud_quarantine_close(store);
    opts.plan_fingerprint[0]=42;OK(ud_quarantine_open(ctx,&opts,&store));
    char tenant[65],job[65],id[65],path[4096];digest_hex("tenant-a",8,tenant);digest_hex("job-a",5,job);digest_hex("batch-1",7,id);
    CHECK(snprintf(path,sizeof(path),"%s/%s/%s/%s.udq",argv[1],tenant,job,id)>0);
    FILE *file=ud_fopen(path,"r+b");CHECK(file);CHECK(fputc('X',file)!=EOF);CHECK(!fclose(file));
    CHECK(ud_quarantine_read(store,str("batch-1"),&bad,&found,&count,&position)==UD_CORRUPT);
    ud_quarantine_close(store);ud_batch_destroy(batch);CHECK(!ud_memory_used(ctx));OK(ud_context_destroy(ctx));
    puts("durable quarantine: reopen, replay, mismatch, auth, corruption and quality ack fixtures passed");return 0;
}
